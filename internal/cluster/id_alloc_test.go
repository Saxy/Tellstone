/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: id_alloc_test.go
Description: Tests for the Phase 4 region ID allocator.
*/
package cluster

import (
	"sync"
	"testing"
)

func TestRegionIDAllocatorMonotonic(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	alloc := NewRegionIDAllocator(cli)
	for i := 0; i < 10; i++ {
		id, err := alloc.NextID(ctx)
		if err != nil {
			t.Fatalf("NextID #%d: %v", i, err)
		}
		if id != uint64(i+2) { // counter starts at 1 (bootstrap region)
			t.Fatalf("NextID #%d = %d, want %d", i, id, i+2)
		}
	}
}

func TestRegionIDAllocatorConcurrent(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	alloc := NewRegionIDAllocator(cli)
	const goroutines = 8
	const perGoroutine = 20
	seen := make(chan uint64, goroutines*perGoroutine)
	fail := make(chan error, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id, err := alloc.NextID(ctx)
				if err != nil {
					fail <- err
					return
				}
				seen <- id
			}
		}()
	}
	ids := make(map[uint64]bool)
	for i := 0; i < goroutines*perGoroutine; i++ {
		select {
		case err := <-fail:
			t.Fatalf("NextID: %v", err)
		case id := <-seen:
			if ids[id] {
				t.Fatalf("duplicate region ID %d allocated", id)
			}
			ids[id] = true
		}
	}
	wg.Wait()
	want := goroutines * perGoroutine
	if len(ids) != want {
		t.Fatalf("got %d unique IDs, want %d", len(ids), want)
	}
}

func TestRegionIDAllocatorSeedFromRegions(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	// Create region metadata with a high ID (e.g. 42) directly.
	reg := Region{ID: 42, StartKey: []byte{}, EndKey: nil, Peers: []uint64{1, 2, 3}, Leader: 1, Epoch: 1}
	if _, err := cli.Put(ctx, regionKey(42), string(encodeRegion(reg))); err != nil {
		t.Fatalf("put region 42: %v", err)
	}

	alloc := NewRegionIDAllocator(cli)
	if err := alloc.SeedFromRegions(ctx); err != nil {
		t.Fatalf("SeedFromRegions: %v", err)
	}
	id, err := alloc.NextID(ctx)
	if err != nil {
		t.Fatalf("NextID after seed: %v", err)
	}
	if id != 43 {
		t.Fatalf("NextID after seed = %d, want 43", id)
	}
}
