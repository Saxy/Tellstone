/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: integration_test.go
Description: Integration tests for the cluster write path. Boots a real
multi-node Raft cluster on localhost, elects a leader, proposes writes
via ProposeAndWait, and verifies replication to follower nodes through
a shared in-memory dispatcher.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

// testDispatcher is a thread-safe in-memory dispatcher for integration tests.
// It stores all dispatched values in a map keyed by key name, with the dispatch
// operation type and count tracked for assertions.
type testDispatcher struct {
	mu      sync.Mutex
	store   map[string][]byte
	ops     []string
	opCount atomic.Int64
}

func newTestDispatcher() *testDispatcher {
	return &testDispatcher{store: make(map[string][]byte)}
}

func (d *testDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opCount.Add(1)
	switch op {
	case OpSet:
		d.ops = append(d.ops, fmt.Sprintf("SET:%s", key))
		v := make([]byte, len(value))
		copy(v, value)
		d.store[key] = v
	case OpDel:
		d.ops = append(d.ops, fmt.Sprintf("DEL:%s", key))
		delete(d.store, key)
	}
	return nil
}

func (d *testDispatcher) get(key string) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.store[key]
	return v, ok
}

func (d *testDispatcher) opCountVal() int64 {
	return d.opCount.Load()
}

// testLogger emits cluster logs to the test's log output.
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Enabled(_ log.Level) bool { return true }
func (l *testLogger) Log(_ log.Level, msg string, fields ...log.Field) {
	l.t.Logf("[cluster] %s %v", msg, fields)
}

// startTestCluster creates a 3-node Raft cluster on localhost. It waits
// for a leader to be elected and returns the nodes, dispatchers, and a
// cleanup function that stops all nodes.
func startTestCluster(t *testing.T) (nodes []*Node, dispatchers []*testDispatcher, cleanup func()) {
	t.Helper()

	const (
		nodeCount    = 3
		electionTick = 5
		hbTick       = 1
		tickInterval = 50 * time.Millisecond
	)

	peers := make([]Peer, nodeCount)
	for i := range peers {
		peers[i] = Peer{ID: uint64(i + 1)}
	}

	nodes = make([]*Node, nodeCount)
	dispatchers = make([]*testDispatcher, nodeCount)

	for i := 0; i < nodeCount; i++ {
		d := newTestDispatcher()
		dispatchers[i] = d

		cfg := NodeConfig{
			NodeID:        uint64(i + 1),
			PeerAddr:      "127.0.0.1:0",
			Peers:         peers,
			ElectionTick:  electionTick,
			HeartbeatTick: hbTick,
			TickInterval:  tickInterval,
			Dispatcher:    d,
			Logger:        &testLogger{t},
		}

		n, err := NewNode(cfg)
		if err != nil {
			t.Fatalf("NewNode(%d): %v", i+1, err)
		}
		nodes[i] = n
	}

	// Start all nodes — transport listeners bind to :0 and get real ports.
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatalf("Node.Start(%d): %v", n.cfg.NodeID, err)
		}
	}

	// Re-register peers with the real OS-assigned addresses. This must
	// happen after Start() because Addr() only returns the real port once
	// the listener is up.
	for _, n := range nodes {
		for _, other := range nodes {
			if n.cfg.NodeID != other.cfg.NodeID {
				n.transport.RegisterPeer(other.cfg.NodeID, other.transport.Addr())
			}
		}
	}

	// Wait for a leader to be elected.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader election")
		default:
		}
		leaderFound := false
		for _, n := range nodes {
			if n.IsLeader() {
				leaderFound = true
				t.Logf("leader elected: node %d", n.cfg.NodeID)
				break
			}
		}
		if leaderFound {
			break
		}
		time.Sleep(tickInterval * 3)
	}

	cleanup = func() {
		for _, n := range nodes {
			n.Stop()
		}
		t.Log("all nodes stopped")
	}
	return nodes, dispatchers, cleanup
}

// waitForLeader polls until one of the nodes reports leadership and returns
// it. Fails the test on timeout instead of leaving a nil leader (or a default
// index of 0) behind for the caller to trip over.
func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader election")
		default:
		}
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// waitForApplied waits until all dispatchers have processed at least n
// operations. Returns an error on timeout.
func waitForApplied(dispatchers []*testDispatcher, n int64, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout: only %d/%d ops applied", dispatchers[0].opCountVal(), n)
		default:
		}
		all := true
		for _, d := range dispatchers {
			if d.opCountVal() < n {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestClusterLeaderElection verifies that a 3-node cluster elects a leader.
func TestClusterLeaderElection(t *testing.T) {
	nodes, _, cleanup := startTestCluster(t)
	defer cleanup()

	leaderCount := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
}

// TestClusterProposeAndWaitSet tests a single SET write through ProposeAndWait.
func TestClusterProposeAndWaitSet(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)

	data, err := EncodeSet("hello", []byte("world"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.ProposeAndWait(ctx, data); err != nil {
		t.Fatalf("ProposeAndWait: %v", err)
	}

	if err := waitForApplied(dispatchers, 1, 5*time.Second); err != nil {
		t.Fatalf("waitForApplied: %v", err)
	}

	for i, d := range dispatchers {
		v, ok := d.get("hello")
		if !ok {
			t.Fatalf("node %d: key 'hello' not found", i+1)
		}
		if string(v) != "world" {
			t.Fatalf("node %d: value = %q, want %q", i+1, v, "world")
		}
	}
	t.Log("SET replicated to all 3 nodes")
}

// TestClusterProposeAndWaitDel tests DELETE through ProposeAndWait.
func TestClusterProposeAndWaitDel(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)

	// SET first.
	setData, err := EncodeSet("toDelete", []byte("val"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.ProposeAndWait(ctx, setData); err != nil {
		t.Fatalf("SET ProposeAndWait: %v", err)
	}
	if err := waitForApplied(dispatchers, 1, 5*time.Second); err != nil {
		t.Fatalf("waitForApplied SET: %v", err)
	}

	// DEL.
	delData, err := EncodeDel("toDelete")
	if err != nil {
		t.Fatalf("EncodeDel: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := leader.ProposeAndWait(ctx2, delData); err != nil {
		t.Fatalf("DEL ProposeAndWait: %v", err)
	}
	if err := waitForApplied(dispatchers, 2, 5*time.Second); err != nil {
		t.Fatalf("waitForApplied DEL: %v", err)
	}

	for i, d := range dispatchers {
		if _, ok := d.get("toDelete"); ok {
			t.Fatalf("node %d: key 'toDelete' should be deleted", i+1)
		}
	}
	t.Log("DEL replicated to all 3 nodes")
}

// TestClusterMultipleWrites tests multiple sequential writes are all
// replicated correctly.
func TestClusterMultipleWrites(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)

	const writeCount = 20
	for i := 0; i < writeCount; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("val-%d", i)
		data, derr := EncodeSet(key, []byte(value), 0)
		if derr != nil {
			t.Fatalf("EncodeSet key-%d: %v", i, derr)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := leader.ProposeAndWait(ctx, data); err != nil {
			cancel()
			t.Fatalf("ProposeAndWait key-%d: %v", i, err)
		}
		cancel()
	}

	if err := waitForApplied(dispatchers, writeCount, 10*time.Second); err != nil {
		t.Fatalf("waitForApplied: %v", err)
	}

	for i := 0; i < writeCount; i++ {
		key := fmt.Sprintf("key-%d", i)
		wantVal := fmt.Sprintf("val-%d", i)
		for ni, d := range dispatchers {
			v, ok := d.get(key)
			if !ok {
				t.Fatalf("node %d: key %q not found", ni+1, key)
			}
			if string(v) != wantVal {
				t.Fatalf("node %d: %q = %q, want %q", ni+1, key, v, wantVal)
			}
		}
	}
	t.Logf("all %d writes replicated to 3 nodes", writeCount)
}

// TestClusterNonLeaderRejectsPropose verifies that ProposeAndWait returns
// ErrNotLeader when called on a follower.
func TestClusterNonLeaderRejectsPropose(t *testing.T) {
	nodes, _, cleanup := startTestCluster(t)
	defer cleanup()

	var follower *Node
	for _, n := range nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	data, err := EncodeSet("nope", []byte("nope"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = follower.ProposeAndWait(ctx, data)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
	t.Log("follower correctly rejected propose")
}

// TestClusterProposalTimeout verifies that a proposal times out if the
// leader is stopped before the write can commit.
func TestClusterProposalTimeout(t *testing.T) {
	nodes, _, cleanup := startTestCluster(t)
	defer cleanup()

	leaderIdx := slices.Index(nodes, waitForLeader(t, nodes))
	if leaderIdx < 0 {
		t.Fatal("leader disappeared before index lookup")
	}

	// Stop the leader to prevent commit.
	nodes[leaderIdx].Stop()

	// The leader check may still return true for a brief moment. Wait for
	// the remaining nodes to elect a new leader.
	time.Sleep(2 * time.Second)

	// Attempt to propose on the stopped node — should fail.
	data, err := EncodeSet("timeout-test", []byte("val"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := nodes[leaderIdx].ProposeAndWait(ctx, data)
	if err == nil {
		t.Fatal("expected error from stopped node, got nil")
	}
	t.Logf("proposal on stopped node failed as expected: %v", err)
}

// TestClusterSetThenOverwrite verifies a key can be overwritten.
func TestClusterSetThenOverwrite(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	v1, eerr := EncodeSet("overwrite", []byte("v1"), 0)
	if eerr != nil {
		cancel()
		t.Fatalf("EncodeSet v1: %v", eerr)
	}
	if err := leader.ProposeAndWait(ctx, v1); err != nil {
		cancel()
		t.Fatalf("ProposeAndWait v1: %v", err)
	}
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	v2, eerr := EncodeSet("overwrite", []byte("v2"), 0)
	if eerr != nil {
		cancel2()
		t.Fatalf("EncodeSet v2: %v", eerr)
	}
	if err := leader.ProposeAndWait(ctx2, v2); err != nil {
		cancel2()
		t.Fatalf("ProposeAndWait v2: %v", err)
	}
	cancel2()

	if err := waitForApplied(dispatchers, 2, 5*time.Second); err != nil {
		t.Fatalf("waitForApplied: %v", err)
	}

	for i, d := range dispatchers {
		v, ok := d.get("overwrite")
		if !ok {
			t.Fatalf("node %d: key not found", i+1)
		}
		if string(v) != "v2" {
			t.Fatalf("node %d: value = %q, want %q", i+1, v, "v2")
		}
	}
	t.Log("overwrite replicated to all nodes")
}

// TestClusterConcurrentProposals verifies that concurrent writes on the
// leader are all committed and replicated.
func TestClusterConcurrentProposals(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)

	const goroutines = 10
	const writesPerGoroutine = 5
	totalWrites := int64(goroutines * writesPerGoroutine)

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for w := 0; w < writesPerGoroutine; w++ {
				key := fmt.Sprintf("g%d-w%d", gID, w)
				value := fmt.Sprintf("v%d-%d", gID, w)
				data, derr := EncodeSet(key, []byte(value), 0)
				if derr != nil {
					errCh <- fmt.Errorf("goroutine %d write %d encode: %w", gID, w, derr)
					cancel()
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := leader.ProposeAndWait(ctx, data); err != nil {
					errCh <- fmt.Errorf("goroutine %d write %d: %w", gID, w, err)
					cancel()
					return
				}
				cancel()
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent write failed: %v", err)
	}

	if err := waitForApplied(dispatchers, totalWrites, 15*time.Second); err != nil {
		t.Fatalf("waitForApplied: %v", err)
	}

	for i, d := range dispatchers {
		if d.opCountVal() != totalWrites {
			t.Fatalf("node %d: op count = %d, want %d", i+1, d.opCountVal(), totalWrites)
		}
	}
	t.Logf("all %d concurrent writes replicated to 3 nodes", totalWrites)
}

// TestClusterTTLReplication verifies TTL values survive the encode/decode
// round-trip through Raft log entries.
func TestClusterTTLReplication(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)

	data, err := EncodeSet("ttlkey", []byte("ttlval"), 30*time.Second)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.ProposeAndWait(ctx, data); err != nil {
		t.Fatalf("ProposeAndWait: %v", err)
	}

	if err := waitForApplied(dispatchers, 1, 5*time.Second); err != nil {
		t.Fatalf("waitForApplied: %v", err)
	}

	for i, d := range dispatchers {
		v, ok := d.get("ttlkey")
		if !ok {
			t.Fatalf("node %d: TTL key not found", i+1)
		}
		if string(v) != "ttlval" {
			t.Fatalf("node %d: value = %q, want %q", i+1, v, "ttlval")
		}
	}
	t.Log("TTL write replicated to all 3 nodes")
}
