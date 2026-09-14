/*
Package server
Tellstone Cloud-Native In-Memory Database
File: manual_split_test.go
Description: Runnable manual proof of the Phase 4 region split feature. It
boots one embedded PD, a 3-node region-1 Raft group, live-splits region 1 at
the key "m", hosts region 2 as a second Raft group on every node over the
shared transport, and verifies that both halves serve writes while every node
consistently reads any key afterwards. Execute with:

	go test ./server/ -run TestManualEndToEndSplit -v -count=1 -timeout 150s

Authors:

	Maximilian Hagen
*/
package server

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster"
	"github.com/Saxy/Tellstone/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// manualStore is the per-node lightweight data store used by the proof. It
// implements the cluster.Dispatcher seam (the Raft FSM writes committed
// entries into it) AND the localReader seam (clusterStore reads from it),
// sharing one map — mirroring the production arrangement where the router
// writes to the same local engine the store reads.
type manualStore struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func newManualStore() *manualStore { return &manualStore{store: make(map[string][]byte)} }

func (m *manualStore) Dispatch(key string, op byte, value []byte, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch op {
	case cluster.OpSet:
		v := make([]byte, len(value))
		copy(v, value)
		m.store[key] = v
	case cluster.OpDel:
		delete(m.store, key)
	}
	return nil
}

func (m *manualStore) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.store[key]
	return v, ok
}

// snapshot returns a copy of the store so a test can assert the exact key set.
func (m *manualStore) snapshot() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]byte, len(m.store))
	for k, v := range m.store {
		out[k] = v
	}
	return out
}

type manualTestLogger struct{ t *testing.T }

func (l *manualTestLogger) Enabled(_ log.Level) bool { return true }
func (l *manualTestLogger) Log(_ log.Level, msg string, fields ...log.Field) {
	l.t.Logf("[manual-split] %s %v", msg, fields)
}

// manualFreePort reserves an ephemeral TCP port for an embedded PD member.
func manualFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// waitFor polls cond until it holds or timeout elapses.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestManualEndToEndSplit is the executable Phase 4 proof:
//
//	PD (embedded etcd) + 3-node region-1 raft group
//	-> pre-split writes on the whole keyspace (incl. a value > 1 MiB)
//	-> live split of region 1 at "m"
//	-> region 2 hosted on every node as a second raft group (shared transport)
//	-> routing convergence (region 1 = [..,"m"), region 2 = ["m",..))
//	-> post-split writes on both halves + cross-node reads on every node
func TestManualEndToEndSplit(t *testing.T) {
	const nodeCount = 3

	// --- 1. Placement driver (embedded etcd, single member). ---
	cp, pp := manualFreePort(t), manualFreePort(t)
	pd, err := cluster.StartPD(cluster.PDConfig{
		NodeID:          1,
		DataDir:         t.TempDir(),
		ClientListenURL: "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(cp)),
		PeerListenURL:   "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(pp)),
		AllPeerURLs: map[uint64]string{
			1: "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(pp)),
		},
	})
	if err != nil {
		t.Fatalf("StartPD: %v", err)
	}
	defer pd.Stop()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{pd.ClientURL()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// --- 2. 3-node region-1 raft group. Every process owns one TCP transport,
	// shared by all regions hosted on that process. ---
	peers := make([]cluster.Peer, nodeCount)
	for i := range peers {
		peers[i] = cluster.Peer{ID: uint64(i + 1)}
	}
	nodes := make([]*cluster.Node, nodeCount)
	locals := make([]*manualStore, nodeCount)
	for i := 0; i < nodeCount; i++ {
		st := newManualStore()
		locals[i] = st
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:        uint64(i + 1),
			PeerAddr:      "127.0.0.1:0",
			Peers:         peers,
			ElectionTick:  5,
			HeartbeatTick: 1,
			TickInterval:  50 * time.Millisecond,
			Dispatcher:    st,
			Logger:        &manualTestLogger{t},
		})
		if err != nil {
			t.Fatalf("NewNode(%d): %v", i+1, err)
		}
		if err = n.Start(); err != nil {
			t.Fatalf("Node.Start(%d): %v", i+1, err)
		}
		nodes[i] = n
	}
	// Re-register peers with the real OS-assigned addresses (transports bind
	// to :0, so addresses are only known after Start).
	for _, n := range nodes {
		for _, other := range nodes {
			if n.NodeID() != other.NodeID() {
				n.Transport().RegisterPeer(other.NodeID(), other.Transport().Addr())
			}
		}
	}

	// --- 3. Bootstrap region 1 (whole keyspace) before the managers watch. ---
	peerIDs := []uint64{1, 2, 3}
	firstMgr := cluster.NewRegionManager(cli, 1, nodes[0], peerIDs)
	if err = firstMgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap default region: %v", err)
	}

	// --- 4. Per-node region manager, coordinator and routed store. The
	// coordinator hosts one raft node per region on the process's shared
	// transport and publishes per-region leadership for routing convergence. ---
	coords := make([]*RegionCoordinator, nodeCount)
	stores := make([]*clusterStore, nodeCount)
	for i, n := range nodes {
		nodeID := n.NodeID()
		m := cluster.NewRegionManager(cli, nodeID, n, peerIDs)
		tracker := cluster.NewRegionSizeTracker()
		resolver := func(key string) uint64 {
			if route := m.RoutingTable().Find([]byte(key)); route != nil {
				return route.ID
			}
			return 0
		}
		n.FSM().SetSizeTracker(tracker, resolver)
		coord := NewRegionCoordinator(cli, nodeID, n, m, locals[i], peers, ^uint64(0), &manualTestLogger{t}, tracker, resolver)
		m.SetLeadershipProvider(coord.LeadershipProvider())
		coords[i] = coord
		stores[i] = newClusterStore(locals[i], n, m.RoutingTable(), m, coord, &manualTestLogger{t})
		go func() { _ = m.Run(ctx) }()
		go coord.Run(ctx)
	}

	// --- 5. Wait for region 1 leadership to be published (region 1 leader
	// elected + Leader field converged in every node's routing table). ---
	t.Log("waiting for region 1 leader election + routing convergence")
	waitFor(t, "region 1 leader in routing table", 20*time.Second, func() bool {
		for _, s := range stores {
			if r := s.rt.Find([]byte("apple")); r == nil || r.Leader == 0 || r.ID != 1 {
				return false
			}
		}
		return true
	})

	// --- 6. Pre-split writes across the whole keyspace. Keys below "m" will
	// stay in region 1; keys >= "m" will move to region 2. Writes alternate
	// across nodes so both the direct-propose and the forward-to-leader paths
	// are exercised. ---
	preKeys := []string{"apple", "banana", "cherry", "fig", "mango", "orange", "pineapple", "zebra"}
	preVal := map[string][]byte{}
	for _, k := range preKeys {
		preVal[k] = []byte("pre:" + k)
	}
	bigKey := "big:blob"
	bigVal := bytes.Repeat([]byte("x"), cluster.ChunkMax+20000)
	preVal[bigKey] = bigVal // chunked: two entries, reassembled by the FSM

	t.Log("writing pre-split keys (direct + forwarded + chunked)")
	for _, k := range preKeys {
		w := stores[len(preVal)%nodeCount]
		if err := w.Set(k, preVal[k], 0); err != nil {
			t.Fatalf("pre-write %s: %v", k, err)
		}
	}
	if err := stores[0].Set(bigKey, bigVal, 0); err != nil {
		t.Fatalf("pre-write chunked %s: %v", bigKey, err)
	}
	verifyReads(t, stores, preVal, "pre-split")

	// --- 7. Live split of region 1 at "m" on the region-1 leader. ---
	var leaderCoord *RegionCoordinator
	for _, c := range coords {
		if n := c.NodeForRegion(1); n != nil && n.IsLeader() {
			leaderCoord = c
			break
		}
	}
	if leaderCoord == nil {
		t.Fatal("no region-1 raft leader found")
	}
	splitCtx, splitCancel := context.WithTimeout(ctx, 30*time.Second)
	result, err := leaderCoord.Split(splitCtx, cluster.SplitRequest{RegionID: 1, SplitKey: []byte("m")})
	splitCancel()
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	rightID := result.RightID
	if result.LeftID != 1 || rightID <= 1 {
		t.Fatalf("unexpected split result: %+v", result)
	}
	t.Logf("split complete: left=%d [..,\"m\") right=%d [\"m\",..)", result.LeftID, rightID)

	// --- 8. Wait for convergence: every process hosts region 2, the region-2
	// raft group elects a leader, and every routing table maps both halves. ---
	t.Log("waiting for region 2 hosting + election + routing convergence")
	waitFor(t, "region 2 hosted on all nodes", 20*time.Second, func() bool {
		for _, c := range coords {
			if c.NodeForRegion(rightID) == nil {
				return false
			}
		}
		return true
	})
	waitFor(t, "region 2 leader in routing table", 30*time.Second, func() bool {
		for _, s := range stores {
			right := s.rt.Find([]byte("mango"))
			left := s.rt.Find([]byte("apple"))
			if right == nil || right.ID != rightID || right.Leader == 0 {
				return false
			}
			if left == nil || left.ID != 1 {
				return false
			}
		}
		return true
	})

	// --- 9. Post-split writes on both halves + cross-node reads everywhere. ---
	postKeys := map[string][]byte{
		"airplane":    []byte("post:left:airplane"),
		"bicycle":     []byte("post:left:bicycle"),
		"motorcycle":  []byte("post:right:motorcycle"),
		"zeppelin":    []byte("post:right:zeppelin"),
		"big:after":   bytes.Repeat([]byte("y"), cluster.ChunkMax+100),
		"m~mixed~key": []byte("post:right:m-tilde"), // sanity: edge of the split
		"apple":       []byte("post:left:apple-overwrite"),
	}
	for k, v := range postKeys {
		w := stores[1] // every write from one node: forwards to the region that owns k
		if err := w.Set(k, v, 0); err != nil {
			t.Fatalf("post-write %s: %v", k, err)
		}
	}
	all := make(map[string][]byte, len(preVal)+len(postKeys))
	for k, v := range preVal {
		all[k] = v
	}
	for k, v := range postKeys {
		all[k] = v
	}
	verifyReads(t, stores, all, "post-split")

	// --- 10. Deletes cross both regions. ---
	for _, k := range []string{"cherry", "zebra", "motorcycle", "big:blob"} {
		if ok, err := stores[2].Delete(k); err != nil || !ok {
			t.Fatalf("delete %s (ok=%v): %v", k, ok, err)
		}
		delete(preVal, k)
		delete(postKeys, k)
	}
	final := make(map[string][]byte, len(preVal)+len(postKeys))
	for k, v := range preVal {
		final[k] = v
	}
	for k, v := range postKeys {
		final[k] = v
	}
	verifyReads(t, stores, final, "post-delete")

	t.Log("manual end-to-end split proof PASSED")
}

func verifyReads(t *testing.T, stores []*clusterStore, expected map[string][]byte, phase string) {
	t.Helper()
	for _, s := range stores {
		for k, want := range expected {
			got, ok := s.Get(k)
			if !ok || !bytes.Equal(got, want) {
				t.Fatalf("%s: node %d read %q = (found=%v len=%d), want (found=%v len=%d)",
					phase, s.nodeID(), k, ok, len(got), true, len(want))
			}
		}
		// Reject keys that should not exist (e.g. deleted or never written).
		if ms, ok := s.local.(*manualStore); ok {
			for k := range ms.snapshot() {
				if _, wantOK := expected[k]; !wantOK {
					t.Fatalf("%s: node %d has unexpected key %q", phase, s.nodeID(), k)
				}
			}
		}
	}
}
