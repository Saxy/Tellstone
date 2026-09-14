/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: region_size_test.go
Description: Tests for the Phase 4 per-region byte tracker.
*/
package cluster

import (
	"testing"
	"time"
)

func TestRegionSizeTrackerSetAndDel(t *testing.T) {
	tr := NewRegionSizeTracker()

	tr.TrackSet(1, "key1", []byte("0123456789"))
	if got := tr.GetSize(1); got != 14 { // 4 + 10 key+value bytes
		t.Fatalf("GetSize after set = %d, want 14", got)
	}

	tr.TrackSet(1, "x", []byte("y"))
	if got := tr.GetSize(1); got != 16 {
		t.Fatalf("GetSize after second set = %d, want 16", got)
	}

	tr.TrackDel(1, "key1", []byte("0123456789"))
	if got := tr.GetSize(1); got != 2 {
		t.Fatalf("GetSize after del = %d, want 2", got)
	}

	// Deleting more bytes than tracked must not underflow.
	tr.TrackDel(1, "x", []byte("99999999999999999999"))
	if got := tr.GetSize(1); got != 0 {
		t.Fatalf("GetSize after oversized delete = %d, want 0", got)
	}

	// A region never written reports zero.
	if got := tr.GetSize(99); got != 0 {
		t.Fatalf("GetSize for unvisited region = %d, want 0", got)
	}
}

func TestRegionSizeTrackerSetSizeOverwrite(t *testing.T) {
	tr := NewRegionSizeTracker()
	tr.TrackSet(1, "k", []byte("v"))
	tr.SetSize(1, 777)
	if got := tr.GetSize(1); got != 777 {
		t.Fatalf("GetSize after SetSize = %d, want 777", got)
	}
}

func TestRegionSizeTrackerParallel(t *testing.T) {
	tr := NewRegionSizeTracker()
	const goroutines = 8
	const perGoroutine = 1000
	done := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perGoroutine; i++ {
				tr.TrackSet(1, "key", []byte("value"))
			}
		}()
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}
	want := uint64(goroutines * perGoroutine * 8) // "key"=3 + "value"=5
	if got := tr.GetSize(1); got != want {
		t.Fatalf("parallel tracked size = %d, want %d", got, want)
	}
}

// TestRegionSizeTrackerReportToEtcd verifies the report loop persists tracked
// sizes into the region metadata in etcd.
func TestRegionSizeTrackerReportToEtcd(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1, 2, 3})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	tr := NewRegionSizeTracker()
	tr.TrackSet(1, "key1", []byte("0123456789"))

	go tr.ReportLoop(ctx, cli, 20*time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := getRegion(ctx, cli, 1)
		if err != nil {
			t.Fatalf("getRegion: %v", err)
		}
		if r != nil && r.SizeBytes == 14 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("region size never reported; last=%+v", r)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
