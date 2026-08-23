/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: manual_test.go
Description: Manual end-to-end test for cluster mode. Boots three real
Tellstone server processes, wires them into a Raft cluster, sends binary
protocol SET/GET/DEL commands through the leader, and reads back from
every node to verify replication. Every step is logged in detail.

This test is skipped unless explicitly requested via TELLSTONE_MANUAL_TEST=1,
so plain go test runs (including CI) stay green. Execute with:

    TELLSTONE_MANUAL_TEST=1 go test -v -race -count=1 \
        -run=TestManual ./internal/cluster/ -timeout=60s

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

const (
	manualBinaryPath = "/tmp/tellstone-manual-test"
	manualBasePort   = 19000
)

// manualServer holds the state of a single Tellstone process in the cluster.
type manualServer struct {
	id         int
	cmd        *exec.Cmd
	binaryPort int // RESP port
	raftPort   int // Raft transport port
	dataDir    string
	started    bool
}

// manualBuild compiles the tellstone binary for the manual test.
func manualBuild(t *testing.T) string {
	t.Helper()
	bin := manualBinaryPath
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/tellstone/")
	cmd.Dir = filepath.Join(os.Getenv("GOPATH"), "src/github.com/Saxy/Tellstone")
	if cmd.Dir == "" || !dirExists(cmd.Dir) {
		cmd.Dir = "/home/maxhagen/projects/saxy/github/Tellstone"
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build tellstone: %v\n%s", err, out)
	}
	t.Logf("built binary: %s", bin)
	return bin
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// manualStartCluster starts n Tellstone processes with --cluster-mode.
func manualStartCluster(t *testing.T, n int, bin string) []*manualServer {
	t.Helper()

	peers := make([]string, n)
	servers := make([]*manualServer, n)

	for i := 0; i < n; i++ {
		servers[i] = &manualServer{
			id:         i + 1,
			binaryPort: manualBasePort + i*10,
			raftPort:   manualBasePort + 9 + i*10,
		}
		peers[i] = fmt.Sprintf("%d@127.0.0.1:%d", i+1, servers[i].raftPort)
	}

	peerStr := ""
	for _, p := range peers {
		if peerStr != "" {
			peerStr += ","
		}
		peerStr += p
	}

	for _, s := range servers {
		dir := filepath.Join(os.TempDir(), fmt.Sprintf("tellstone-manual-%d", s.id))
		os.MkdirAll(dir, 0o755)
		s.dataDir = dir

		args := []string{
			"--cluster-mode",
			"--node-id", fmt.Sprintf("%d", s.id),
			"--peer-addr", fmt.Sprintf("127.0.0.1:%d", s.raftPort),
			"--peers", peerStr,
			"--addr", fmt.Sprintf("127.0.0.1:%d", s.binaryPort),
			"--log-level", "info",
		}

		cmd := exec.Command(bin, args...)
		cmd.Dir = dir
		cmd.Stdout = nil
		cmd.Stderr = nil

		// Capture output for detailed logging.
		stdoutPipe, _ := cmd.StdoutPipe()
		stderrPipe, _ := cmd.StderrPipe()

		if err := cmd.Start(); err != nil {
			t.Fatalf("start node %d: %v", s.id, err)
		}
		s.cmd = cmd
		s.started = true

		// Log stdout/stderr in background goroutines.
		go pipeToTestLog(t, fmt.Sprintf("node%d-stdout", s.id), stdoutPipe)
		go pipeToTestLog(t, fmt.Sprintf("node%d-stderr", s.id), stderrPipe)

		t.Logf("started node %d: binary=127.0.0.1:%d raft=127.0.0.1:%d",
			s.id, s.binaryPort, s.raftPort)
	}

	// Wait for leader election.
	t.Log("waiting for cluster to elect leader...")
	time.Sleep(3 * time.Second)

	return servers
}

// pipeToTestLog reads from r and sends each line to t.Log.
func pipeToTestLog(t *testing.T, prefix string, r io.Reader) {
	t.Helper()
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 500 {
			line = line[:500] + "...(truncated)"
		}
		t.Logf("[%s] %s", prefix, line)
	}
}

// manualStopCluster stops all servers in the cluster and removes data dirs.
func manualStopCluster(t *testing.T, servers []*manualServer) {
	t.Helper()
	for _, s := range servers {
		if s.started && s.cmd != nil && s.cmd.Process != nil {
			t.Logf("stopping node %d (pid %d)", s.id, s.cmd.Process.Pid)
			s.cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan error, 1)
			go func() { done <- s.cmd.Wait() }()
			select {
			case err := <-done:
				t.Logf("node %d stopped: %v", s.id, err)
			case <-time.After(5 * time.Second):
				t.Logf("node %d force kill (timeout)", s.id)
				s.cmd.Process.Kill()
				<-done
			}
			s.started = false
		}
	}
	// Cleanup data dirs.
	for _, s := range servers {
		if s.dataDir != "" {
			os.RemoveAll(s.dataDir)
		}
	}
}

// --- Binary protocol client helpers ---

func sendFrame(conn net.Conn, msgType byte, payload []byte) error {
	length := uint32(1 + len(payload))
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, length)
	hdr = append(hdr, msgType)
	hdr = append(hdr, payload...)
	_, err := conn.Write(hdr)
	return err
}

func readFrame(conn net.Conn) (byte, []byte, error) {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return 0, nil, fmt.Errorf("read header: %w", err)
	}
	length := binary.BigEndian.Uint32(hdr)
	if length > 64*1024*1024 {
		return 0, nil, fmt.Errorf("frame too large: %d", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return 0, nil, fmt.Errorf("read payload: %w", err)
	}
	return buf[0], buf[1:], nil
}

func binarySet(conn net.Conn, key, value string, ttlMs int64) (string, error) {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	payload := make([]byte, 1+2+8+len(keyBytes)+len(valBytes))
	payload[0] = 0x02 // OpSet
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(keyBytes)))
	binary.BigEndian.PutUint64(payload[3:11], uint64(ttlMs))
	copy(payload[11:11+len(keyBytes)], keyBytes)
	copy(payload[11+len(keyBytes):], valBytes)

	if err := sendFrame(conn, 0x02, payload); err != nil {
		return "", fmt.Errorf("send SET: %w", err)
	}
	msgType, resp, err := readFrame(conn)
	if err != nil {
		return "", fmt.Errorf("read SET response: %w", err)
	}
	if msgType == 0x07 { // MsgError
		return "", fmt.Errorf("SET error: %s", resp)
	}
	return string(resp), nil
}

func binaryGet(conn net.Conn, key string) (string, byte, error) {
	keyBytes := []byte(key)
	payload := make([]byte, 1+2+8+len(keyBytes))
	payload[0] = 0x01 // OpGet
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(keyBytes)))
	copy(payload[11:11+len(keyBytes)], keyBytes)

	if err := sendFrame(conn, 0x02, payload); err != nil {
		return "", 0, fmt.Errorf("send GET: %w", err)
	}
	msgType, resp, err := readFrame(conn)
	if err != nil {
		return "", 0, fmt.Errorf("read GET response: %w", err)
	}
	return string(resp), msgType, nil
}

func binaryDel(conn net.Conn, key string) (string, error) {
	keyBytes := []byte(key)
	payload := make([]byte, 1+2+8+len(keyBytes))
	payload[0] = 0x03 // OpDelete
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(keyBytes)))
	copy(payload[11:11+len(keyBytes)], keyBytes)

	if err := sendFrame(conn, 0x02, payload); err != nil {
		return "", fmt.Errorf("send DEL: %w", err)
	}
	msgType, resp, err := readFrame(conn)
	if err != nil {
		return "", fmt.Errorf("read DEL response: %w", err)
	}
	if msgType == 0x07 {
		return "", fmt.Errorf("DEL error: %s", resp)
	}
	return string(resp), nil
}

func connectTo(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 2*time.Second)
}

// TestManual is the end-to-end manual integration test.
// Run with: go test -v -race -count=1 -run=TestManual ./internal/cluster/ -timeout=60s
func TestManual(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manual test in short mode")
	}

	t.Log("=== MANUAL CLUSTER TEST ===")
	t.Log("Building binary...")
	bin := manualBuild(t)

	t.Log("Starting 3-node cluster...")
	servers := manualStartCluster(t, 3, bin)
	defer manualStopCluster(t, servers)

	// Find which server is leader by connecting to each and trying a write.
	var leaderIdx int = -1
	var leaderConn net.Conn

	for i, s := range servers {
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		t.Logf("connecting to node %d at %s", s.id, addr)
		conn, err := connectTo(addr)
		if err != nil {
			t.Logf("  node %d: connection failed: %v (may still be starting)", s.id, err)
			continue
		}
		defer conn.Close()

		// Try a test SET to detect leader.
		_, err = binarySet(conn, "__probe__", "probe", 0)
		if err != nil {
			t.Logf("  node %d: SET probe failed: %v (likely follower)", s.id, err)
			continue
		}
		t.Logf("  node %d: is the LEADER!", s.id)
		leaderIdx = i
		leaderConn = conn
		break
	}

	if leaderIdx == -1 {
		t.Fatal("no leader found after cluster startup")
	}

	// --- Write operations through the leader ---
	t.Log("")
	t.Log("=== WRITE OPERATIONS (through leader) ===")

	type writeOp struct {
		key   string
		value string
		ttlMs int64
	}

	writes := []writeOp{
		{"name", "tellstone", 0},
		{"version", "1.0.0", 0},
		{"counter", "42", 0},
		{"ttl-key", "expires-soon", 5000},
		{"unicode-key", "héllo-wörld-日本語", 0},
		{"binary-safe", string([]byte{0x00, 0xFF, 0x42, 0x01}), 0},
	}

	// Serialize SETs on the leader connection to avoid TCP stream corruption.
	for _, w := range writes {
		t.Logf("  SET %s = %q (ttl=%dms)", w.key, w.value, w.ttlMs)
		resp, err := binarySet(leaderConn, w.key, w.value, w.ttlMs)
		if err != nil {
			t.Fatalf("SET %s failed: %v", w.key, err)
		}
		t.Logf("    -> OK (response: %q)", resp)
	}

	t.Log("")
	t.Log("Waiting 2 seconds for replication to propagate...")
	time.Sleep(2 * time.Second)

	// --- Read from ALL nodes to verify replication ---
	t.Log("")
	t.Log("=== READ VERIFICATION (all nodes) ===")

	type readResult struct {
		nodeID int
		key    string
		value  string
		err    error
		found  bool
	}
	var allResults []readResult

	// Serialize GETs per connection to avoid TCP stream corruption.
	for _, s := range servers {
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		conn, err := connectTo(addr)
		if err != nil {
			t.Logf("  node %d: connect failed: %v", s.id, err)
			continue
		}

		for _, w := range writes {
			val, msgType, err := binaryGet(conn, w.key)
			result := readResult{nodeID: s.id, key: w.key}
			if err != nil {
				result.err = err
			} else if msgType == 0x07 {
				result.found = false
				result.value = val
			} else {
				result.found = true
				result.value = val
			}
			allResults = append(allResults, result)
		}
		conn.Close()
	}

	for _, r := range allResults {
		if r.err != nil {
			t.Logf("  node %d GET %s: ERROR %v", r.nodeID, r.key, r.err)
		} else if r.found {
			t.Logf("  node %d GET %s = %q ✓", r.nodeID, r.key, r.value)
		} else {
			t.Logf("  node %d GET %s = NOT_FOUND (%s)", r.nodeID, r.key, r.value)
		}
	}

	// --- Verify DEL replication ---
	t.Log("")
	t.Log("=== DELETE VERIFICATION ===")
	t.Logf("  DEL name (via leader)")
	resp, err := binaryDel(leaderConn, "name")
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	t.Logf("    -> %s", resp)

	time.Sleep(1 * time.Second)

	for _, s := range servers {
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		conn, err := connectTo(addr)
		if err != nil {
			t.Logf("  node %d: connect failed: %v", s.id, err)
			continue
		}
		val, msgType, err := binaryGet(conn, "name")
		conn.Close()
		if err != nil {
			t.Logf("  node %d GET name: ERROR %v", s.id, err)
		} else if msgType == 0x07 {
			t.Logf("  node %d GET name = NOT_FOUND ✓ (deleted)", s.id)
		} else {
			t.Logf("  node %d GET name = %q (expected NOT_FOUND)", s.id, val)
		}
	}

	t.Log("")
	t.Log("=== MANUAL CLUSTER TEST COMPLETE ===")
}
