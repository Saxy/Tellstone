/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: routing_integration_test.go
Description: Phase 3 routing integration tests. Boots a real multi-node Raft
cluster and proves write-forwarding (a follower forwards a write to the region
leader resolved from the routing table, D2/D3) and read-anywhere (a follower
serves a linearizable read from its local replica, D1).

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestClusterWriteForwarding proves Phase 3 D2/D3: a non-leader node forwards a
// write to the region leader (resolved from the routing table) and the write
// replicates to every node. It also proves D1 (read-anywhere): after the write
// has been applied, a follower can serve a linearizable read from its local
// replica without contacting the leader.
func TestClusterWriteForwarding(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)
	var follower *Node
	var followerIdx int
	for i, n := range nodes {
		if n != leader {
			follower = n
			followerIdx = i
			break
		}
	}

	// Single default region covering all keys, owned by the current leader.
	rt := NewRoutingTable()
	rt.Update(Region{
		ID:       1,
		StartKey: nil,
		EndKey:   nil,
		Peers:    []uint64{1, 2, 3},
		Leader:   leader.cfg.NodeID,
		Epoch:    1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := EncodeSet("fkey", []byte("fval"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	route := rt.Find([]byte("fkey"))
	if route == nil {
		t.Fatal("routing table has no region for key")
	}
	if err := follower.ForwardWrite(ctx, route.Leader, data); err != nil {
		t.Fatalf("ForwardWrite from follower: %v", err)
	}

	// The write must replicate to all nodes.
	if err := waitForApplied(dispatchers, 1, 5*time.Second); err != nil {
		t.Fatalf("replication: %v", err)
	}
	for i, d := range dispatchers {
		v, ok := d.get("fkey")
		if !ok || string(v) != "fval" {
			t.Fatalf("node %d: fkey not replicated (ok=%v v=%q)", i+1, ok, v)
		}
	}

	// D1: read-anywhere. The follower performs a linearizable read (ensuring it
	// has applied up to the read index) and then reads the value locally.
	if err := follower.LinearizableRead(ctx); err != nil {
		t.Fatalf("LinearizableRead on follower: %v", err)
	}
	v, ok := dispatchers[followerIdx].get("fkey")
	if !ok || string(v) != "fval" {
		t.Fatalf("read-anywhere: follower could not read fkey (ok=%v v=%q)", ok, v)
	}
}

// TestClusterRegionLeaderChange proves D4: when the Raft leader changes, the
// routing table's region leader is updated so forwards keep targeting the node
// that can actually propose. We drive the routing table manually (the RegionManager
// Watch path is covered by TestRegionManagerBootstrapAndWatch) and assert that a
// write forwarded to the new leader still replicates.
func TestClusterRegionLeaderChange(t *testing.T) {
	nodes, dispatchers, cleanup := startTestCluster(t)
	defer cleanup()

	leader := waitForLeader(t, nodes)
	rt := NewRoutingTable()
	rt.Update(Region{
		ID:       1,
		StartKey: nil,
		EndKey:   nil,
		Peers:    []uint64{1, 2, 3},
		Leader:   leader.cfg.NodeID,
		Epoch:    1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Write through the initial leader.
	data1, _ := EncodeSet("k1", []byte("v1"), 0)
	if err := forwardToLeader(ctx, nodes, rt, "k1", data1); err != nil {
		t.Fatalf("initial forward: %v", err)
	}
	if err := waitForApplied(dispatchers, 1, 5*time.Second); err != nil {
		t.Fatalf("replication k1: %v", err)
	}

	// Kill the leader; a new one must be elected.
	stoppedIdx := -1
	for i, n := range nodes {
		if n == leader {
			stoppedIdx = i
			break
		}
	}
	leader.Stop()
	// We just wait for a new leader and point the region at it.
	newLeader := waitForLeader(t, nodes)
	rt.Update(Region{
		ID:       1,
		StartKey: nil,
		EndKey:   nil,
		Peers:    []uint64{1, 2, 3},
		Leader:   newLeader.cfg.NodeID,
		Epoch:    2,
	})

	// Write through the new leader must still replicate to survivors.
	data2, _ := EncodeSet("k2", []byte("v2"), 0)
	if err := forwardToLeader(ctx, nodes, rt, "k2", data2); err != nil {
		t.Fatalf("post-failover forward: %v", err)
	}

	// Only the surviving nodes are expected to apply k2.
	aliveDispatchers := make([]*testDispatcher, 0, len(dispatchers))
	for i, d := range dispatchers {
		if i == stoppedIdx {
			continue
		}
		aliveDispatchers = append(aliveDispatchers, d)
	}
	if err := waitForApplied(aliveDispatchers, 2, 5*time.Second); err != nil {
		t.Fatalf("replication k2: %v", err)
	}
	for i, d := range dispatchers {
		if i == stoppedIdx {
			continue
		}
		if _, ok := d.get("k2"); !ok {
			t.Fatalf("node %d: k2 not replicated after leader change", i+1)
		}
	}
}

// forwardToLeader picks the node that currently owns the region leader role and
// forwards the write to it (proving the routing-table-driven path, D2/D3).
func forwardToLeader(ctx context.Context, nodes []*Node, rt *RoutingTable, key string, data []byte) error {
	route := rt.Find([]byte(key))
	if route == nil {
		return fmt.Errorf("no region covers key %q", key)
	}
	for _, n := range nodes {
		if n.cfg.NodeID == route.Leader {
			return n.ForwardWrite(ctx, route.Leader, data)
		}
	}
	return fmt.Errorf("region leader %d is not running", route.Leader)
}
