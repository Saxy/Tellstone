/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: region_test.go
Description: Tests for Region (de)serialization and RegionManager bootstrap +
etcd Watch convergence (Phase 3).
*/
package cluster

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type stubLP struct{ leader bool }

func (s stubLP) IsLeader() bool { return s.leader }

// startRegionTest boots a single embedded PD member and returns a client plus a
// cancelable context for RegionManager tests.
func startRegionTest(t *testing.T) (*clientv3.Client, context.Context, context.CancelFunc) {
	t.Helper()
	cport, pport := freePort(t), freePort(t)
	pd, err := StartPD(PDConfig{
		NodeID:          1,
		DataDir:         t.TempDir(),
		ClientListenURL: joinHostPortURL("127.0.0.1", cport),
		PeerListenURL:   joinHostPortURL("127.0.0.1", pport),
		AllPeerURLs: map[uint64]string{
			1: joinHostPortURL("127.0.0.1", pport),
		},
	})
	if err != nil {
		t.Fatalf("StartPD: %v", err)
	}
	t.Cleanup(pd.Stop)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{pd.ClientURL()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return cli, ctx, cancel
}

func TestRegionEncodeDecode(t *testing.T) {
	r := Region{
		ID:       42,
		StartKey: []byte("user:aaa"),
		EndKey:   []byte("user:zzz"),
		Peers:    []uint64{1, 2, 3},
		Leader:   2,
		Epoch:    7,
	}
	got, ok := decodeRegion(encodeRegion(r))
	if !ok {
		t.Fatal("decode failed")
	}
	if got.ID != r.ID || got.Epoch != r.Epoch || got.Leader != r.Leader || len(got.Peers) != 3 ||
		string(got.StartKey) != "user:aaa" || string(got.EndKey) != "user:zzz" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

func TestRegionManagerBootstrapAndWatch(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1, 2, 3})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = mgr.Run(runCtx) }()

	// Leader keep-alive should set region 1's Leader to this node.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if r := mgr.RoutingTable().Find([]byte("x")); r != nil && r.Leader == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader did not claim region 1")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Stop the leader manager so its leadership loop no longer rewrites the
	// region's leader field; the external write below is the only writer.
	runCancel()

	// An external writer (a second client) bumping the region leader must
	// converge via Watch into a follower's local routing table. The follower
	// manager is non-leader, so its own leadership loop stays silent and the
	// external write is observed unchanged (D4 convergence on a follower).
	followerMgr := NewRegionManager(cli, 99, stubLP{leader: false}, []uint64{1, 2, 3})
	frunCtx, frunCancel := context.WithCancel(ctx)
	defer frunCancel()
	go func() { _ = followerMgr.Run(frunCtx) }()

	nr := Region{ID: 1, StartKey: []byte{}, EndKey: nil, Peers: []uint64{1, 2, 3}, Leader: 2, Epoch: 100}
	if _, err := cli.Put(ctx, regionKey(1), string(encodeRegion(nr))); err != nil {
		t.Fatalf("external put: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if r := followerMgr.RoutingTable().Find([]byte("x")); r != nil && r.Leader == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower watch did not converge to leader 2")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRegionWireLimit verifies the uint16 wire-format bounds: a region at the
// 65,535 limit encodes/decodes cleanly, while 65,536 peers or key bytes are
// rejected before they can silently truncate the encoding.
func TestRegionWireLimit(t *testing.T) {
	atLimit := Region{
		ID:       1,
		StartKey: make([]byte, maxRegionWireField),
		EndKey:   nil,
		Peers:    make([]uint64, maxRegionWireField),
		Leader:   1,
		Epoch:    1,
	}
	if regionExceedsWireLimit(atLimit) {
		t.Fatal("region at the 65535 limit should be within bounds")
	}
	if _, ok := decodeRegion(encodeRegion(atLimit)); !ok {
		t.Fatal("encode/decode at 65535 limit failed")
	}

	if !regionExceedsWireLimit(Region{ID: 1, Peers: make([]uint64, maxRegionWireField+1), Leader: 1, Epoch: 1}) {
		t.Fatal("65536 peers should exceed the wire limit")
	}
	if !regionExceedsWireLimit(Region{ID: 1, StartKey: make([]byte, maxRegionWireField+1), Leader: 1, Epoch: 1}) {
		t.Fatal("65536 start-key bytes should exceed the wire limit")
	}
	if !regionExceedsWireLimit(Region{ID: 1, EndKey: make([]byte, maxRegionWireField+1), Leader: 1, Epoch: 1}) {
		t.Fatal("65536 end-key bytes should exceed the wire limit")
	}
}

// TestRegionManagerCompactedWatch verifies that after an etcd compaction the
// RegionManager still converges on a subsequent update: Run reseeds from etcd
// and recreates the watch instead of silently dropping events (D4).
func TestRegionManagerCompactedWatch(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1, 2, 3})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = mgr.Run(runCtx) }()

	// Wait for the leader to claim region 1.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if r := mgr.RoutingTable().Find([]byte("x")); r != nil && r.Leader == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader did not claim region 1")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Compact the historical revisions so the in-flight watch is now stale.
	resp, err := cli.Get(ctx, regionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := cli.Compact(ctx, resp.Header.Revision); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// A post-compaction write must still converge (Run recovers from the
	// compacted watch and reseeds from etcd).
	nr := Region{ID: 1, StartKey: []byte{}, EndKey: nil, Peers: []uint64{1, 2, 3}, Leader: 2, Epoch: 5}
	if _, err := cli.Put(ctx, regionKey(1), string(encodeRegion(nr))); err != nil {
		t.Fatalf("external put: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if r := mgr.RoutingTable().Find([]byte("x")); r != nil && r.Leader == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watch did not recover from compaction to converge to leader 2")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
