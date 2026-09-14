/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: split_test.go
Description: Tests for the region split coordinator. Verifies that Split
allocates a new region ID, creates the right half in etcd, updates the left
half, and that the resulting regions are consistent. Also verifies quiesce
drains in-flight proposals and blocks new ones until Resume.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestSplitBasic(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1, 2, 3})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	sc := NewSplitCoordinator(mgr, &testLogger{t})
	splitKey := []byte("m")
	result, err := sc.Split(ctx, SplitRequest{
		RegionID: 1,
		SplitKey: splitKey,
	})
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if result.LeftID != 1 {
		t.Fatalf("LeftID = %d, want 1", result.LeftID)
	}
	if result.RightID <= 1 {
		t.Fatalf("RightID = %d, want > 1", result.RightID)
	}

	left, err := mgr.getRegion(ctx, result.LeftID)
	if err != nil {
		t.Fatalf("get left: %v", err)
	}
	if left == nil {
		t.Fatal("left region not found")
	}
	right, err := mgr.getRegion(ctx, result.RightID)
	if err != nil {
		t.Fatalf("get right: %v", err)
	}
	if right == nil {
		t.Fatal("right region not found")
	}

	if !bytes.Equal(left.EndKey, splitKey) {
		t.Fatalf("left.EndKey = %q, want %q", left.EndKey, splitKey)
	}
	if !bytes.Equal(right.StartKey, splitKey) {
		t.Fatalf("right.StartKey = %q, want %q", right.StartKey, splitKey)
	}
	if len(right.EndKey) != 0 {
		t.Fatalf("right.EndKey = %q, want empty (full end)", right.EndKey)
	}
	if left.Epoch <= right.Epoch {
		t.Fatalf("left.Epoch=%d <= right.Epoch=%d", left.Epoch, right.Epoch)
	}
	if len(left.Peers) != 3 || len(right.Peers) != 3 {
		t.Fatalf("peers lost: left=%d right=%d", len(left.Peers), len(right.Peers))
	}
}

func TestSplitAutoMidpoint(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	sc := NewSplitCoordinator(mgr, &testLogger{t})
	result, err := sc.Split(ctx, SplitRequest{RegionID: 1})
	if err != nil {
		t.Fatalf("Split (auto midpoint): %v", err)
	}

	left, _ := mgr.getRegion(ctx, result.LeftID)
	right, _ := mgr.getRegion(ctx, result.RightID)
	if !bytes.Equal(left.EndKey, right.StartKey) {
		t.Fatalf("split key mismatch: left.EndKey=%q, right.StartKey=%q", left.EndKey, right.StartKey)
	}
	if len(left.StartKey) != 0 || len(right.EndKey) != 0 {
		t.Fatalf("boundary mismatch: left.StartKey=%q right.EndKey=%q", left.StartKey, right.EndKey)
	}
}

func TestSplitRegionNotFound(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1})
	sc := NewSplitCoordinator(mgr, &testLogger{t})

	_, err := sc.Split(ctx, SplitRequest{RegionID: 999})
	if err == nil {
		t.Fatal("expected error for non-existent region")
	}
}

func TestSplitInvalidSplitKey(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Create a region with explicit boundaries [aa, zz) so we can test invalid split keys.
	alloc := NewRegionIDAllocator(cli)
	regionID, err := alloc.NextID(ctx)
	if err != nil {
		t.Fatalf("AllocID: %v", err)
	}
	explicitRegion := Region{
		ID:       regionID,
		StartKey: []byte("aa"),
		EndKey:   []byte("zz"),
		Peers:    []uint64{1},
		Epoch:    1,
	}
	raw := encodeRegion(explicitRegion)
	if _, err := cli.Put(ctx, regionKey(regionID), string(raw)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	sc := NewSplitCoordinator(mgr, &testLogger{t})

	// Split key at or before start: "aa" equals StartKey.
	_, err = sc.Split(ctx, SplitRequest{RegionID: regionID, SplitKey: []byte("aa")})
	if err == nil {
		t.Fatal("expected error for split key equal to region start")
	}

	// Split key at or after end: "zz" equals EndKey.
	_, err = sc.Split(ctx, SplitRequest{RegionID: regionID, SplitKey: []byte("zz")})
	if err == nil {
		t.Fatal("expected error for split key equal to region end")
	}

	// Split key before start.
	_, err = sc.Split(ctx, SplitRequest{RegionID: regionID, SplitKey: []byte("`")})
	if err == nil {
		t.Fatal("expected error for split key before region start")
	}

	// Split key after end.
	_, err = sc.Split(ctx, SplitRequest{RegionID: regionID, SplitKey: []byte{0xff, 0xff, 0xff, 0xff}})
	if err == nil {
		t.Fatal("expected error for split key at region end")
	}
}

func TestSplitIncreasesEpoch(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	sc := NewSplitCoordinator(mgr, &testLogger{t})
	res, err := sc.Split(ctx, SplitRequest{RegionID: 1, SplitKey: []byte("z")})
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	left, _ := mgr.getRegion(ctx, res.LeftID)
	right, _ := mgr.getRegion(ctx, res.RightID)
	if left.Epoch != right.Epoch+1 {
		t.Fatalf("left.Epoch=%d, right.Epoch=%d; want left = right+1", left.Epoch, right.Epoch)
	}
}

func TestMidpointKey(t *testing.T) {
	tests := []struct {
		name  string
		start []byte
		end   []byte
	}{
		{"full keyspace", nil, nil},
		{"a to z", []byte("a"), []byte("z")},
		{"prefix range", []byte("user:"), []byte("user;")},
		{"adjacent bytes", []byte{0x01}, []byte{0x02}},
		{"long prefix", []byte("key:0000"), []byte("key:9999")},
		{"prefix true midpoint", []byte(":"), []byte(":\x80")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mid := midpointKey(tt.start, tt.end)
			if len(mid) == 0 {
				t.Fatal("midpointKey returned empty key")
			}
			// The midpoint must be strictly after start (or any key <= start).
			if len(tt.start) > 0 && bytes.Compare(mid, tt.start) <= 0 {
				t.Errorf("midpoint %q not after start %q", mid, tt.start)
			}
			// The midpoint must be strictly before end — but nil end means
			// "positive infinity" in the key space, so any non-maximal key
			// is valid.
			if len(tt.end) > 0 && bytes.Compare(mid, tt.end) >= 0 {
				t.Errorf("midpoint %q not before end %q", mid, tt.end)
			}
		})
	}
}

func TestQuiesceResume(t *testing.T) {
	nodes, _, cleanup := startTestCluster(t)
	defer cleanup()
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	var leader *Node
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	setData, err := EncodeSet("k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	if err := leader.ProposeAndWait(context.Background(), setData); err != nil {
		t.Fatalf("ProposeAndWait: %v", err)
	}

	// Quiesce: drain + block.
	if !leader.Quiesce(time.Second) {
		t.Fatal("Quiesce did not drain")
	}
	if !leader.IsQuiesced() {
		t.Fatal("expected quiesced")
	}

	setData2, _ := EncodeSet("k2", []byte("v2"), 0)
	if err := leader.ProposeAndWait(context.Background(), setData2); err != ErrQuiesced {
		t.Fatalf("expected ErrQuiesced, got %v", err)
	}

	// Resume: proposals accepted again.
	leader.Resume()
	if leader.IsQuiesced() {
		t.Fatal("expected not quiesced")
	}
	setData3, _ := EncodeSet("k3", []byte("v3"), 0)
	if err := leader.ProposeAndWait(context.Background(), setData3); err != nil {
		t.Fatalf("ProposeAndWait after Resume: %v", err)
	}
}
