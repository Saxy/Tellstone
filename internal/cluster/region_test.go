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

	// A concurrent writer (second client) bumping the leader must converge via
	// Watch into the local routing table.
	nr := Region{ID: 1, StartKey: []byte{}, EndKey: nil, Peers: []uint64{1, 2, 3}, Leader: 2, Epoch: 2}
	if _, err := cli.Put(ctx, regionKey(1), string(encodeRegion(nr))); err != nil {
		t.Fatalf("external put: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if r := mgr.RoutingTable().Find([]byte("x")); r != nil && r.Leader == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watch did not converge to leader 2")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
