/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: manual_test.go
Description: Manual end-to-end tests for cluster mode. TestManual boots
three real Tellstone server processes, wires them into a Raft cluster, and
sends binary protocol SET/GET/DEL through the leader to verify
replication. TestManualPDTSO boots the same 3-node --cluster-mode cluster
and proves the phase 2 Placement Driver + Timestamp Oracle is live by
dialing each node's embedded etcd and asserting globally disjoint
timestamp grants. Every step is logged in detail.

These tests are skipped unless explicitly requested via
TELLSTONE_MANUAL_TEST=1, so plain go test runs (including CI) stay green.
Execute with:

	TELLSTONE_MANUAL_TEST=1 go test -v -count=1 \
	    -run='TestManual|TestManualPDTSO' ./internal/cluster/ -timeout=120s

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
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

// manualRepoRoot returns the repository root, derived from this file's own
// location so the build works from any checkout on any machine (including CI).
func manualRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine repo root: runtime.Caller failed")
	}
	// This file lives in <repo>/internal/cluster/ — two levels up is the root.
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// manualBuild compiles the tellstone binary for the manual test.
func manualBuild(t *testing.T) string {
	t.Helper()
	bin := manualBinaryPath
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/tellstone/")
	cmd.Dir = manualRepoRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build tellstone: %v\n%s", err, out)
	}
	t.Logf("built binary: %s", bin)
	return bin
}

// manualStartCluster starts n Tellstone processes with --cluster-mode.
func manualStartCluster(t *testing.T, n int, bin string) []*manualServer {
	t.Helper()

	peers := make([]string, n)
	pdMembers := make([]string, n)
	servers := make([]*manualServer, n)

	for i := 0; i < n; i++ {
		servers[i] = &manualServer{
			id:         i + 1,
			binaryPort: manualBasePort + i*10,
			raftPort:   manualBasePort + 9 + i*10,
		}
		peers[i] = fmt.Sprintf("%d@127.0.0.1:%d", i+1, servers[i].raftPort)
		// PD membership is keyed on the *data* address; the embedded etcd
		// client/peer ports are derived from it by a fixed offset.
		pdMembers[i] = fmt.Sprintf("%d@127.0.0.1:%d", i+1, servers[i].binaryPort)
	}

	peerStr := ""
	for _, p := range peers {
		if peerStr != "" {
			peerStr += ","
		}
		peerStr += p
	}
	pdStr := ""
	for _, p := range pdMembers {
		if pdStr != "" {
			pdStr += ","
		}
		pdStr += p
	}

	for _, s := range servers {
		dir := filepath.Join(os.TempDir(), fmt.Sprintf("tellstone-manual-%d", s.id))
		os.MkdirAll(dir, 0o755)
		s.dataDir = dir

		args := []string{
			"--cluster-mode",
			"--node-role", "hybrid",
			"--node-id", fmt.Sprintf("%d", s.id),
			"--peer-addr", fmt.Sprintf("127.0.0.1:%d", s.raftPort),
			"--peers", peerStr,
			"--pd-members", pdStr,
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
//
//	Run with: TELLSTONE_MANUAL_TEST=1 go test -v -race -count=1 \
//	    -run=TestManual ./internal/cluster/ -timeout=60s
func TestManual(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manual test in short mode")
	}
	if os.Getenv("TELLSTONE_MANUAL_TEST") == "" {
		t.Skip("manual cluster test only runs with TELLSTONE_MANUAL_TEST=1")
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
			t.Errorf("node %d: connect failed during read verification: %v", s.id, err)
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

	// Assert the read results instead of only logging them: every written
	// key must be present with its exact value on every node.
	want := make(map[string]string, len(writes))
	for _, w := range writes {
		want[w.key] = w.value
	}
	for _, r := range allResults {
		if r.err != nil {
			t.Errorf("node %d GET %s: error %v", r.nodeID, r.key, r.err)
			t.Logf("  node %d GET %s: ERROR %v", r.nodeID, r.key, r.err)
			continue
		}
		if !r.found {
			t.Errorf("node %d GET %s: NOT_FOUND, want %q", r.nodeID, r.key, want[r.key])
			t.Logf("  node %d GET %s = NOT_FOUND (%s)", r.nodeID, r.key, r.value)
			continue
		}
		if r.value != want[r.key] {
			t.Errorf("node %d GET %s = %q, want %q", r.nodeID, r.key, r.value, want[r.key])
		}
		t.Logf("  node %d GET %s = %q ✓", r.nodeID, r.key, r.value)
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
			t.Errorf("node %d: connect failed during delete verification: %v", s.id, err)
			t.Logf("  node %d: connect failed: %v", s.id, err)
			continue
		}
		val, msgType, err := binaryGet(conn, "name")
		conn.Close()
		if err != nil {
			t.Errorf("node %d GET name: error %v", s.id, err)
			t.Logf("  node %d GET name: ERROR %v", s.id, err)
			continue
		}
		if msgType != 0x07 {
			t.Errorf("node %d GET name = %q, want NOT_FOUND (msgType 0x07) after delete", s.id, val)
			t.Logf("  node %d GET name = %q (expected NOT_FOUND)", s.id, val)
			continue
		}
		t.Logf("  node %d GET name = NOT_FOUND ✓ (deleted)", s.id)
	}

	t.Log("")
	t.Log("=== MANUAL CLUSTER TEST COMPLETE ===")
}

// TestManualPDTSO is the manual end-to-end proof for the phase 2
// Placement Driver + Timestamp Oracle. It boots a real 3-node
// --cluster-mode Tellstone (which now brings up the embedded PD/TSO stack
// on every hybrid node), then dials each node's embedded etcd client port
// and exercises the watermark-CAS grant path directly. Two invariants are
// asserted: every node grants successfully, and the ranges handed out
// across the whole cluster are globally disjoint (cluster-wide monotonic).
//
// Run with:
//
//	TELLSTONE_MANUAL_TEST=1 go test -v -count=1 \
//	    -run=TestManualPDTSO ./internal/cluster/ -timeout=120s
func TestManualPDTSO(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manual test in short mode")
	}
	if os.Getenv("TELLSTONE_MANUAL_TEST") == "" {
		t.Skip("manual PD/TSO test only runs with TELLSTONE_MANUAL_TEST=1")
	}

	t.Log("=== MANUAL PD/TSO TEST ===")
	bin := manualBuild(t)
	servers := manualStartCluster(t, 3, bin)
	defer manualStopCluster(t, servers)

	// Give the embedded etcd members a moment to form a quorum and for the
	// per-node pools to prime before we poke the grant path.
	time.Sleep(3 * time.Second)

	type grant struct {
		node uint64
		lo   uint64
		hi   uint64
	}
	var grants []grant
	ctx := context.Background()

	for _, s := range servers {
		// The PD client endpoint is the data address + 10000 (ADR-010 §6).
		pdAddr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort+10000)
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{pdAddr},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("node %d: dial PD at %s: %v", s.id, pdAddr, err)
		}
		g := NewEtcdGranter(cli)
		for k := 0; k < 5; k++ {
			lo, hi, err := g.GrantRange(ctx, 100)
			if err != nil {
				cli.Close()
				t.Fatalf("node %d: grant %d: %v", s.id, k, err)
			}
			if lo == 0 || hi < lo {
				cli.Close()
				t.Fatalf("node %d: grant %d returned bad range [%d,%d]", s.id, k, lo, hi)
			}
			t.Logf("  node %d granted [%d,%d] (%d timestamps)", s.id, lo, hi, hi-lo+1)
			grants = append(grants, grant{node: uint64(s.id), lo: lo, hi: hi})
		}
		cli.Close()
	}

	// Global monotonicity: sort by lower bound and assert no overlaps. The
	// decentralized CAS guarantees this; the test proves it holds across
	// the live cluster rather than only in-process unit tests.
	sort.Slice(grants, func(i, j int) bool { return grants[i].lo < grants[j].lo })
	for i := 1; i < len(grants); i++ {
		if grants[i].lo <= grants[i-1].hi {
			t.Fatalf("PD grants overlap: node %d [%d,%d] vs node %d [%d,%d]",
				grants[i-1].node, grants[i-1].lo, grants[i-1].hi,
				grants[i].node, grants[i].lo, grants[i].hi)
		}
	}
	t.Logf("OK: %d PD grants across 3 nodes are globally disjoint", len(grants))
	t.Log("=== MANUAL PD/TSO TEST COMPLETE ===")
}
