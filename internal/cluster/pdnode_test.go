/*
Package cluster
Tellstone Placement Driver + TSO (Phase 2)
File: pdnode_test.go
Description: Tests for the per-process PD/TSO stack: hybrid boots an
embedded member and primes the pool, data dials an external PD and does
the same, and Stop tears everything down without hanging.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"fmt"
	"testing"
	"time"
)

// waitForAlloc polls the pool until it yields a timestamp or ctx elapses,
// covering the brief window between StartPDNode returning and the refill
// goroutine priming the first range.
func waitForAlloc(t *testing.T, pool *TSOPool) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ts, err := pool.TryAlloc(); err == nil {
			return ts
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("pool never primed")
	return 0
}

func TestStartPDNodeHybrid(t *testing.T) {
	base := freePort(t)
	// Leave room for the +20000 derived peer port; freePort can hand back
	// high ephemeral ports whose offset would overflow 65535.
	if base > 45535 {
		base -= 30000
	}
	m := Peer{ID: 1, Addr: fmt.Sprintf("127.0.0.1:%d", base)}

	pd, err := StartPDNode(StartPDNodeConfig{
		Role:     "hybrid",
		NodeID:   1,
		DataAddr: m.Addr,
		Members:  []Peer{m},
		PDDir:    t.TempDir(),
		TSO:      TSOPoolConfig{MinBatch: 1000, Headroom: 30 * time.Second, RefillThresholdPct: 20},
	})
	if err != nil {
		t.Fatalf("StartPDNode hybrid: %v", err)
	}
	defer pd.Stop()

	if pd.Role() != "hybrid" {
		t.Fatalf("role: got %q", pd.Role())
	}
	first := waitForAlloc(t, pd.Pool())
	if first == 0 {
		t.Fatal("hybrid pool should allocate after priming")
	}
	// The embedded member is the source of the watermark — allocation
	// proves the grant round-trip succeeded.
	got, err := pd.Pool().TryAlloc()
	if err != nil || got <= first {
		t.Fatalf("allocations should be strictly increasing: %d then (%d, %v)", first, got, err)
	}
}

func TestStartPDNodeDataDialsExternal(t *testing.T) {
	// Stand up a plain embedded member to act as the "external PD".
	extBase, extPeer := freePort(t), freePort(t)
	extPD, err := StartPD(PDConfig{
		NodeID:          1,
		DataDir:         t.TempDir(),
		ClientListenURL: joinHostPortURL("127.0.0.1", extBase),
		PeerListenURL:   joinHostPortURL("127.0.0.1", extPeer),
		AllPeerURLs:     map[uint64]string{1: joinHostPortURL("127.0.0.1", extPeer)},
	})
	if err != nil {
		t.Fatalf("external PD: %v", err)
	}
	defer extPD.Stop()

	node, err := StartPDNode(StartPDNodeConfig{
		Role:   "data",
		NodeID: 7, // distinct; the data node hosts no etcd of its own
		PDAddr: extPD.ClientURL(),
		TSO:    TSOPoolConfig{MinBatch: 1000, Headroom: 30 * time.Second, RefillThresholdPct: 20},
	})
	if err != nil {
		t.Fatalf("StartPDNode data: %v", err)
	}
	defer node.Stop()

	if node.Pool() == nil {
		t.Fatal("data role must still expose a pool")
	}
	_ = waitForAlloc(t, node.Pool())
}

func TestStartPDNodeRejectsDataWithoutPD(t *testing.T) {
	_, err := StartPDNode(StartPDNodeConfig{Role: "data", NodeID: 1, TSO: TSOPoolConfig{}})
	if err == nil {
		t.Fatal("data role without --pd-addr must error")
	}
}
