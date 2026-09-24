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
timestamp grants. TestManualPipeline is the phase 5 proof: it drives
concurrent forwarded writes against every node (each non-leader write is
serialized as a pipeline request/response multiplexed over the shared
per-peer TCP connection to the region leader), verifies read-anywhere
replication, and reports end-to-end throughput. Every step is logged in
detail.

These tests are skipped unless explicitly requested via
TELLSTONE_MANUAL_TEST=1, so plain go test runs (including CI) stay green.
Execute with:

	TELLSTONE_MANUAL_TEST=1 go test -v -count=1 \
	    -run='TestManual|TestManualPDTSO|TestManualPipeline' ./internal/cluster/ -timeout=180s

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
	"sync"
	"sync/atomic"
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
	id          int
	cmd         *exec.Cmd
	args        []string
	binaryPort  int // RESP port
	raftPort    int // Raft transport port
	gatewayPort int // Phase 7 cross-cluster gateway port (0 = none)
	dataDir     string
	zone        string // geo availability zone ("" = unset, Phase 6)
	started     bool
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
		if s.zone != "" {
			args = append(args, "--zone", s.zone)
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
	return readFrameDeadline(conn, 5*time.Second)
}

func readFrameDeadline(conn net.Conn, timeout time.Duration) (byte, []byte, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
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
	return binarySetDeadline(conn, key, value, ttlMs, 5*time.Second)
}

// binarySetDeadline is binarySet with a caller-chosen client read timeout.
// Used by the federation test where a dead gateway can take the server's
// cross-cluster Call budget (~8s) to answer with CLUSTERDOWN, which a 5s
// default deadline would miss and leave a stale frame on the connection.
func binarySetDeadline(conn net.Conn, key, value string, ttlMs int64, timeout time.Duration) (string, error) {
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
	msgType, resp, err := readFrameDeadline(conn, timeout)
	if err != nil {
		return "", fmt.Errorf("read SET response: %w", err)
	}
	if msgType == 0x07 { // MsgError
		return "", fmt.Errorf("SET error: %s", resp)
	}
	return string(resp), nil
}

func binaryGet(conn net.Conn, key string) (string, byte, error) {
	return binaryGetDeadline(conn, key, 5*time.Second)
}

func binaryGetDeadline(conn net.Conn, key string, timeout time.Duration) (string, byte, error) {
	keyBytes := []byte(key)
	payload := make([]byte, 1+2+8+len(keyBytes))
	payload[0] = 0x01 // OpGet
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(keyBytes)))
	copy(payload[11:11+len(keyBytes)], keyBytes)

	if err := sendFrame(conn, 0x02, payload); err != nil {
		return "", 0, fmt.Errorf("send GET: %w", err)
	}
	msgType, resp, err := readFrameDeadline(conn, timeout)
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
			if hi-lo+1 != 100 {
				cli.Close()
				t.Fatalf("node %d: grant %d returned bad size %d (want 100)", s.id, k, hi-lo+1)
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

// TestManualRouting is the manual end-to-end proof for the phase 3 region
// routing layer. It boots a real 3-node --cluster-mode Tellstone cluster and
// asserts two invariants:
//
//   - Write-forwarding (D2/D3): a SET issued against EVERY node — including
//     followers — succeeds. Followers resolve the region leader from the
//     routing table and forward the op over the cluster transport.
//   - Read-anywhere (D1): once a write has replicated, a GET against every
//     node returns the value, served from the local replica after a
//     linearizable ReadIndex round-trip.
//
// Run with:
//
//	TELLSTONE_MANUAL_TEST=1 go test -v -count=1 \
//	    -run=TestManualRouting ./internal/cluster/ -timeout=120s
func TestManualRouting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manual test in short mode")
	}
	if os.Getenv("TELLSTONE_MANUAL_TEST") == "" {
		t.Skip("manual routing test only runs with TELLSTONE_MANUAL_TEST=1")
	}

	t.Log("=== MANUAL ROUTING TEST ===")
	bin := manualBuild(t)
	servers := manualStartCluster(t, 3, bin)
	defer manualStopCluster(t, servers)

	// Give the cluster time to elect a Raft leader and for the RegionManager to
	// claim the default region (routing table convergence, D4).
	time.Sleep(3 * time.Second)

	// D2/D3: issue a SET against each of the 3 nodes in turn so that follower
	// writes are exercised. Every SET must succeed via forwarding.
	keys := []string{"routing-a", "routing-b", "routing-c"}
	for i, key := range keys {
		val := fmt.Sprintf("v%d", i)
		s := servers[i%len(servers)]
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		// Retry briefly in case the region leader has not been claimed yet.
		// Each attempt uses a freshly opened connection so a failed attempt's
		// stale response can never be consumed by a later retry.
		var setErr error
		for attempt := 0; attempt < 10; attempt++ {
			conn, err := connectTo(addr)
			if err != nil {
				t.Fatalf("connect node %d (%s): %v", s.id, addr, err)
			}
			_, setErr = binarySet(conn, key, val, 0)
			conn.Close()
			if setErr == nil {
				break
			}
			t.Logf("  node %d SET %s attempt %d: %v (retrying)", s.id, key, attempt+1, setErr)
			time.Sleep(300 * time.Millisecond)
		}
		if setErr != nil {
			t.Fatalf("node %d SET %s (forwarded to leader) failed: %v", s.id, key, setErr)
		}
		t.Logf("  node %d SET %s = %q (forwarded) OK", s.id, key, val)
	}

	t.Log("Waiting for replication to propagate...")
	time.Sleep(2 * time.Second)

	// D1: read-anywhere. Every node must return each forwarded write.
	for _, s := range servers {
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		conn, err := connectTo(addr)
		if err != nil {
			t.Fatalf("connect node %d (%s): %v", s.id, addr, err)
		}
		for i, key := range keys {
			val, msgType, err := binaryGet(conn, key)
			if err != nil {
				conn.Close()
				t.Fatalf("node %d GET %s: %v", s.id, key, err)
			}
			if msgType == 0x07 {
				conn.Close()
				t.Fatalf("node %d GET %s: NOT_FOUND (want forwarded value)", s.id, key)
			}
			want := fmt.Sprintf("v%d", i)
			if val != want {
				conn.Close()
				t.Fatalf("node %d GET %s = %q, want %q", s.id, key, val, want)
			}
			t.Logf("  node %d GET %s = %q (read-anywhere) OK", s.id, key, val)
		}
		conn.Close()
	}

	t.Log("=== MANUAL ROUTING TEST COMPLETE ===")
}

// TestManualPipeline is the manual end-to-end proof for phase 5 pipeline
// streams. It boots a real 3-node --cluster-mode Tellstone cluster and drives
// concurrent forwarded writes against every node: each non-leader node
// serializes its SET as a pipeline request/response multiplexed over the
// shared per-peer TCP connection to the region leader (mesh-X RAFT + pipeline
// streams, request-ID mux on one connection), so the run exercises
// concurrent in-flight pipeline calls, batch flushes, the per-peer
// worker-pool dispatch, and per-request error delivery. The test verifies
// every write replicated to all three replicas (read-anywhere, D1) and
// reports end-to-end throughput (the count is printed for comparison against
// the per-request gRPC baseline described in the phase 5 plan).
//
// Scale knobs (env):
//
//	TELLSTONE_PHASE5_OPS      total forwarded SETs            (default 2000)
//	TELLSTONE_PHASE5_WORKERS  concurrent client connections   (default 32)
//	TELLSTONE_PHASE5_VERIFY   GETs per node for D1 check      (default 200)
//
// Run with:
//
//	TELLSTONE_MANUAL_TEST=1 go test -v -count=1 \
//	    -run=TestManualPipeline ./internal/cluster/ -timeout=180s
func TestManualPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("manual cluster test requested only for non-short runs")
	}
	if os.Getenv("TELLSTONE_MANUAL_TEST") == "" {
		t.Skip("manual cluster test: set TELLSTONE_MANUAL_TEST=1 to run")
	}

	ops := 2000
	workers := 32
	verify := 200
	if v := os.Getenv("TELLSTONE_PHASE5_OPS"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &ops); err != nil || n != 1 || ops <= 0 {
			t.Fatalf("TELLSTONE_PHASE5_OPS=%q: want a positive integer", v)
		}
	}
	if v := os.Getenv("TELLSTONE_PHASE5_WORKERS"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &workers); err != nil || n != 1 || workers <= 0 {
			t.Fatalf("TELLSTONE_PHASE5_WORKERS=%q: want a positive integer", v)
		}
	}
	if v := os.Getenv("TELLSTONE_PHASE5_VERIFY"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &verify); err != nil || n != 1 || verify <= 0 {
			t.Fatalf("TELLSTONE_PHASE5_VERIFY=%q: want a positive integer", v)
		}
	}

	t.Log("=== MANUAL PIPELINE (PHASE 5) TEST ===")
	t.Logf("  plan: %d concurrent forwarded SETs, %d client connections, %d GETs/node", ops, workers, verify)
	bin := manualBuild(t)
	servers := manualStartCluster(t, 3, bin)
	defer manualStopCluster(t, servers)
	t.Log("Waiting for Raft election + region claim...")
	time.Sleep(3 * time.Second)

	// Phase 5D: drive concurrent pipelined forwarded writes against all nodes.
	// Workers keep one connection per node open and reuse it across SETs so the
	// pipeline's per-peer streams are genuinely multiplexed. A failed attempt
	// drops and reopens the connection (stale responses can never be consumed
	// by a later retry).
	t.Log("Driving concurrent forwarded writes over the pipeline...")
	jobs := make(chan int, ops)
	for i := 0; i < ops; i++ {
		jobs <- i
	}
	close(jobs)

	var (
		wg     sync.WaitGroup
		done   atomic.Int64
		failed atomic.Int64
	)

	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			addr := fmt.Sprintf("127.0.0.1:%d", servers[w%len(servers)].binaryPort)
			var conn net.Conn
			for idx := range jobs {
				key := fmt.Sprintf("phase5-%06d", idx)
				val := fmt.Sprintf("v%d", idx)
				for attempt := 0; ; attempt++ {
					if attempt == 30 {
						failed.Add(1)
						t.Errorf("worker %d: SET %s never succeeded after 30 attempts", w, key)
						conn = nil
						break
					}
					if conn == nil {
						c, err := connectTo(addr)
						if err != nil {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						conn = c
					}
					if _, err := binarySet(conn, key, val, 0); err != nil {
						conn.Close()
						conn = nil
						time.Sleep(100 * time.Millisecond)
						continue
					}
					break
				}
				done.Add(1)
			}
			if conn != nil {
				conn.Close()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if failed.Load() > 0 {
		t.Fatalf("%d forwarded SETs permanently failed", failed.Load())
	}
	if got := done.Load(); got != int64(ops) {
		t.Fatalf("completed %d SETs, want %d", got, ops)
	}
	rate := float64(ops) / elapsed.Seconds()
	t.Logf("  %d forwarded SETs in %s (%.0f ops/sec end-to-end)", ops, elapsed, rate)
	t.Logf("  (compare against the per-request gRPC baseline in docs/MULTI-CLUSTER-PLAN.md phase 5)")

	// D1: read-anywhere. Sample verify keys and GET each on all three nodes.
	t.Log("Waiting for replication to propagate...")
	time.Sleep(2 * time.Second)
	stride := ops / verify
	if stride < 1 {
		stride = 1
	}
	checked := 0
	for idx := 0; idx < ops && checked < verify; idx += stride {
		key := fmt.Sprintf("phase5-%06d", idx)
		want := fmt.Sprintf("v%d", idx)
		for _, s := range servers {
			addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
			conn, err := connectTo(addr)
			if err != nil {
				t.Fatalf("connect node %d (%s): %v", s.id, addr, err)
			}
			val, msgType, err := binaryGet(conn, key)
			if err != nil {
				conn.Close()
				t.Fatalf("node %d GET %s: %v", s.id, key, err)
			}
			if msgType == 0x07 {
				conn.Close()
				t.Fatalf("node %d GET %s: NOT_FOUND (want forwarded value)", s.id, key)
			}
			if val != want {
				conn.Close()
				t.Fatalf("node %d GET %s = %q, want %q", s.id, key, val, want)
			}
			conn.Close()
			t.Logf("  node %d GET %s = %q (read-anywhere) OK", s.id, key, val)
		}
		// Count a key only after every node has returned it, so verify is a
		// count of distinct keys (each checked on all nodes).
		checked++
	}
	t.Log("=== MANUAL PIPELINE (PHASE 5) TEST COMPLETE ===")
}

// GEO_SENTINEL_4242 marker

// TestManualGeoRouting is the manual end-to-end proof for Phase 6 geo routing.
// It boots a real 3-node --cluster-mode Tellstone cluster, each node in a
// distinct availability zone, pushes an operator geo policy into the embedded
// PD etcd, and asserts:
//
//  1. Zone registration: every node's /tellstone/nodes/<id> entry carries
//     the zone passed via --zone.
//
//  2. Policy convergence: the policy stored at /tellstone/geo/rules matches
//     the expected rules and PreferredZoneOf resolves the right zones.
//
//  3. Read-anywhere: a SET on every node followed by a GET on every node
//     succeeds across all three zones after geo routing is active.
//
// Run with:
//
//	TELLSTONE_MANUAL_TEST=1 go test -v -count=1 \
//	    -run=TestManualGeoRouting ./internal/cluster/ -timeout=180s
func TestManualGeoRouting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manual test in short mode")
	}
	if os.Getenv("TELLSTONE_MANUAL_TEST") == "" {
		t.Skip("manual geo routing test only runs with TELLSTONE_MANUAL_TEST=1")
	}

	t.Log("=== MANUAL GEO ROUTING (PHASE 6) TEST ===")
	bin := manualBuild(t)

	zones := []string{"us-east-1", "eu-west-1", "ap-southeast-1"}
	servers := manualStartZonedCluster(t, 3, bin, zones)
	defer manualStopCluster(t, servers)

	// Wait for leader election + geo registration + region bootstrap.
	t.Log("Waiting for leader election + geo registration...")
	time.Sleep(5 * time.Second)

	// --- Step 1: verify zone registration in the embedded PD etcd ---
	t.Log("")
	t.Log("=== STEP 1: ZONE REGISTRATION ===")
	leaderIdx := -1
	for i, s := range servers {
		pdAddr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort+10000)
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{pdAddr},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Logf("  node %d: dial PD at %s: %v (may still be starting)", s.id, pdAddr, err)
			continue
		}
		// Check that this node has registered its zone.
		resp, err := cli.Get(context.Background(), NodesKeyPrefix(), clientv3.WithPrefix())
		cli.Close()
		if err != nil {
			t.Logf("  node %d: get nodes: %v", s.id, err)
			continue
		}
		if resp.Count == 0 {
			t.Logf("  node %d: no nodes registered yet", s.id)
			continue
		}
		t.Logf("  node %d PD has %d node registrations:", s.id, resp.Count)
		registeredZones := make(map[string]int)
		for _, kv := range resp.Kvs {
			info, ok := decodeNodeInfo(kv.Value)
			if !ok {
				t.Errorf("  node %d: corrupt NodeInfo at %s", s.id, string(kv.Key))
				continue
			}
			t.Logf("    %s → zone=%s addr=%s", string(kv.Key), info.Zone, info.Addr)
			registeredZones[info.Zone]++
		}
		for _, z := range zones {
			if registeredZones[z] == 0 {
				t.Errorf("  node %d: zone %q not registered (got zones: %v)", s.id, z, registeredZones)
			}
		}
		if leaderIdx == -1 {
			leaderIdx = i
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no PD client responded; cannot continue")
	}
	t.Logf("  zone registration: PASS (all %d zones present)", len(zones))

	// --- Step 2: push a geo policy and verify convergence ---
	t.Log("")
	t.Log("=== STEP 2: GEO POLICY ===")
	pdAddr := fmt.Sprintf("127.0.0.1:%d", servers[leaderIdx].binaryPort+10000)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial PD at %s: %v", pdAddr, err)
	}
	defer cli.Close()

	policy := GeoPolicy{Rules: []GeoRule{
		{Prefix: "user:eu:", Zone: "eu-west-1", Replicas: 3},
		{Prefix: "user:us:", Zone: "us-east-1", Replicas: 3},
		{Prefix: "", Zone: GeoZoneGlobal, Replicas: 3},
	}}
	if err := SetGeoPolicy(context.Background(), cli, policy); err != nil {
		t.Fatalf("SetGeoPolicy: %v", err)
	}

	// Verify by reading it back.
	got, err := GetGeoPolicy(context.Background(), cli)
	if err != nil {
		t.Fatalf("GetGeoPolicy: %v", err)
	}
	if len(got.Rules) != 3 {
		t.Fatalf("policy rules = %d, want 3", len(got.Rules))
	}
	t.Logf("  policy stored with %d rules (version %d)", len(got.Rules), got.Version)

	// Verify PreferredZoneOf for each prefix.
	type zoneTest struct {
		key  string
		want string
	}
	for _, zt := range []zoneTest{
		{"user:eu:alice", "eu-west-1"},
		{"user:us:bob", "us-east-1"},
		{"global:data", GeoZoneGlobal},
	} {
		got := PreferredZoneOf(got, []byte(zt.key))
		if got != zt.want {
			t.Errorf("  PreferredZoneOf(%q) = %q, want %q", zt.key, got, zt.want)
		}
		t.Logf("  PreferredZoneOf(%q) = %q ✓", zt.key, got)
	}
	t.Log("  geo policy: PASS")

	// --- Step 3: read-anywhere with zone routing active ---
	t.Log("")
	t.Log("=== STEP 3: READ-ANYWHERE (zone-pinned keys) ===")
	type writeOp struct {
		key   string
		value string
	}
	writes := []writeOp{
		{"user:eu:alice", "eu-data-1"},
		{"user:us:bob", "us-data-1"},
		{"global:shared", "shared-data"},
	}

	// Find leader for writes.
	var leaderConn net.Conn
	for i, s := range servers {
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		conn, err := connectTo(addr)
		if err != nil {
			t.Logf("  connect node %d: %v", s.id, err)
			continue
		}
		_, err = binarySet(conn, "__geo_probe__", "1", 0)
		if err == nil {
			t.Logf("  node %d is leader", s.id)
			leaderIdx = i
			leaderConn = conn
			break
		}
		conn.Close()
	}
	if leaderConn == nil {
		t.Fatal("no leader found for writes")
	}

	// Write through the leader.
	for _, w := range writes {
		resp, err := binarySet(leaderConn, w.key, w.value, 0)
		if err != nil {
			t.Fatalf("SET %s failed: %v", w.key, err)
		}
		t.Logf("  SET %s = %q → %s", w.key, w.value, resp)
	}
	leaderConn.Close()

	t.Log("  waiting for replication...")
	time.Sleep(2 * time.Second)

	// Read from ALL nodes (read-anywhere via the zone-aware GET path).
	for _, s := range servers {
		addr := fmt.Sprintf("127.0.0.1:%d", s.binaryPort)
		conn, err := connectTo(addr)
		if err != nil {
			t.Errorf("  node %d: connect failed: %v", s.id, err)
			continue
		}
		for _, w := range writes {
			val, msgType, err := binaryGet(conn, w.key)
			if err != nil {
				t.Errorf("  node %d GET %s: %v", s.id, w.key, err)
				continue
			}
			if msgType == 0x07 {
				t.Errorf("  node %d GET %s: NOT_FOUND", s.id, w.key)
				continue
			}
			if val != w.value {
				t.Errorf("  node %d GET %s = %q, want %q", s.id, w.key, val, w.value)
				continue
			}
			t.Logf("  node %d GET %s = %q ✓", s.id, w.key, val)
		}
		conn.Close()
	}

	t.Log("")
	t.Log("=== MANUAL GEO ROUTING (PHASE 6) TEST COMPLETE ===")
}

// manualStartZonedCluster starts n Tellstone processes with --cluster-mode,
// assigning each a distinct availability zone from the provided slice.
func manualStartZonedCluster(t *testing.T, n int, bin string, zones []string) []*manualServer {
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
		if i < len(zones) {
			servers[i].zone = zones[i]
		}
		peers[i] = fmt.Sprintf("%d@127.0.0.1:%d", i+1, servers[i].raftPort)
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
		dir := filepath.Join(os.TempDir(), fmt.Sprintf("tellstone-geo-%d", s.id))
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
		if s.zone != "" {
			args = append(args, "--zone", s.zone)
		}

		cmd := exec.Command(bin, args...)
		cmd.Dir = dir
		cmd.Stdout = nil
		cmd.Stderr = nil

		stdoutPipe, _ := cmd.StdoutPipe()
		stderrPipe, _ := cmd.StderrPipe()

		if err := cmd.Start(); err != nil {
			t.Fatalf("start node %d: %v", s.id, err)
		}
		s.cmd = cmd
		s.started = true

		go pipeToTestLog(t, fmt.Sprintf("geo%d-stdout", s.id), stdoutPipe)
		go pipeToTestLog(t, fmt.Sprintf("geo%d-stderr", s.id), stderrPipe)

		t.Logf("started node %d: zone=%s binary=127.0.0.1:%d raft=127.0.0.1:%d",
			s.id, s.zone, s.binaryPort, s.raftPort)
	}

	t.Log("waiting for geo cluster to elect leader...")
	time.Sleep(3 * time.Second)

	return servers
}
