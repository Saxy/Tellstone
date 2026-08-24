/*
Package cluster
Tellstone Timestamp Oracle (Phase 2)
File: tso_test.go
Description: Tests for timestamp packing, pool allocation semantics
(exhaustion, blocking, concurrency, refill thresholds, batch sizing) and
watermark-CAS range granting against a live embedded etcd member.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"sort"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPackTimestamp(t *testing.T) {
	ts := PackTimestamp(1000, 42)
	if got := TimestampMillis(ts); got != 1000 {
		t.Fatalf("millis: got %d, want 1000", got)
	}
	if ts >= PackTimestamp(1001, 0) {
		t.Fatal("timestamps must increase with physical ms")
	}
	if PackTimestamp(1000, tsLogicalMask) >= PackTimestamp(1001, 0) {
		t.Fatal("logical overflow must roll into the next millisecond")
	}
}

func TestPoolTryAllocSequence(t *testing.T) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 10, Headroom: time.Second, RefillThresholdPct: 20})
	if _, err := p.TryAlloc(); err == nil {
		t.Fatal("empty pool must refuse allocation")
	}
	if err := p.Adopt(5, 8); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	want := []uint64{5, 6, 7, 8}
	for _, w := range want {
		got, err := p.TryAlloc()
		if err != nil || got != w {
			t.Fatalf("alloc: got (%d, %v), want %d", got, err, w)
		}
	}
	if _, err := p.TryAlloc(); err != ErrTSOExhausted {
		t.Fatalf("exhausted pool: got %v, want ErrTSOExhausted", err)
	}
}

// TestPoolConcurrentMonotonic hammers the hot path from many goroutines:
// every timestamp must be unique and the full budget consumed exactly.
func TestPoolConcurrentMonotonic(t *testing.T) {
	const (
		goroutines = 8
		budget     = 100000
	)
	p := NewTSOPool(TSOPoolConfig{MinBatch: 1, Headroom: time.Second, RefillThresholdPct: 20})
	if err := p.Adopt(1, budget); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var mu sync.Mutex
	seen := make(map[uint64]bool, budget)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ts, err := p.TryAlloc()
				if err == ErrTSOExhausted {
					return
				}
				if err != nil {
					t.Errorf("TryAlloc: %v", err)
					return
				}
				mu.Lock()
				if seen[ts] {
					t.Errorf("duplicate timestamp %d", ts)
				}
				seen[ts] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != budget {
		t.Fatalf("allocated %d unique timestamps, want %d", len(seen), budget)
	}
}

func TestPoolBlockingAllocWakesOnAdopt(t *testing.T) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 1, Headroom: time.Second, RefillThresholdPct: 20})

	ctx := context.Background()
	done := make(chan struct{})
	var got uint64
	go func() {
		defer close(done)
		ts, err := p.Alloc(ctx)
		if err != nil {
			t.Errorf("blocking Alloc: %v", err)
			return
		}
		got = ts
	}()

	time.Sleep(50 * time.Millisecond)
	if err := p.Adopt(100, 110); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	select {
	case <-done:
		if got != 100 {
			t.Fatalf("first post-refill alloc: got %d, want 100", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Alloc did not wake on Adopt")
	}
}

func TestPoolBlockingAllocHonorsContext(t *testing.T) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 1, Headroom: time.Second, RefillThresholdPct: 20})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.Alloc(ctx); err == nil {
		t.Fatal("expected cancellation error on empty pool")
	}
}

func TestPoolNeedsRefill(t *testing.T) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 100, Headroom: time.Second, RefillThresholdPct: 20})
	if !p.NeedsRefill() {
		t.Fatal("fresh pool needs refill")
	}
	if err := p.Adopt(1, 100); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if p.NeedsRefill() {
		t.Fatal("full pool must not need refill")
	}
	// Burn down to 15 remaining (<20% of 100).
	if err := p.Adopt(101, 185); err != nil { // replace range to control size cleanly below
		t.Fatalf("Adopt: %v", err)
	}
	p.cur.Store(185 - 15)
	if !p.NeedsRefill() {
		t.Fatal("pool under 20% must need refill")
	}
}

func TestNextBatchSize(t *testing.T) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 500, Headroom: 30 * time.Second, RefillThresholdPct: 20})
	if got := p.NextBatchSize(); got != 500 {
		t.Fatalf("idle rate: got %d, want min batch 500", got)
	}
	// Simulate an observed rate of 10000/s: feed identical per-second
	// samples until the EWMA converges (α=0.3 ⇒ ~50 rounds for 1-0.7ⁿ≈1),
	// then the sized batch must sit at rate × headroom.
	p.sampler.sample(p.total.Load(), time.Now())
	for i := 1; i <= 50; i++ {
		p.total.Store(uint64(i) * 10000)
		p.sampler.sample(p.total.Load(), time.Now().Add(time.Duration(i)*time.Second))
	}
	if got := p.NextBatchSize(); got < 290000 || got > 300000 {
		t.Fatalf("rate-sized batch: got %d, want ~300000 (10000/s x 30s)", got)
	}
}

// TestGrantRangeAgainstEtcd proves disjointness and persistence against a
// real embedded member: concurrent granters receive globally non-overlapping
// ranges, and a seeded future watermark is never reissued.
func TestGrantRangeAgainstEtcd(t *testing.T) {
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
	defer pd.Stop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd.ClientURL()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	defer cli.Close()
	g := NewEtcdGranter(cli)

	// Seed a watermark in the "future" (clock jumped back scenario): the
	// next grant must continue above it, never reissue it.
	ctx := context.Background()
	future := PackTimestamp(uint64(time.Now().Add(time.Hour).UnixMilli()), 7)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, future)
	if _, err := cli.Put(ctx, tsoWatermarkKey, string(buf)); err != nil {
		t.Fatalf("seeding watermark: %v", err)
	}

	const (
		granters   = 4
		grantsEach = 5
		rangeSize  = 1000
	)
	type rng struct{ lo, hi uint64 }
	var (
		mu   sync.Mutex
		rngs = make([]rng, 0, granters*grantsEach)
		wg   sync.WaitGroup
	)
	for i := 0; i < granters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < grantsEach; j++ {
				start, end, err := g.GrantRange(ctx, rangeSize)
				if err != nil {
					t.Errorf("granter %d grant %d: %v", i, j, err)
					return
				}
				mu.Lock()
				rngs = append(rngs, rng{start, end})
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if len(rngs) == 0 {
		t.Fatal("no PD grants collected; see prior grant errors")
	}

	sort.Slice(rngs, func(a, b int) bool { return rngs[a].lo < rngs[b].lo })
	for i := 1; i < len(rngs); i++ {
		if rngs[i].lo <= rngs[i-1].hi {
			t.Fatalf("overlapping grants: [%d,%d] and [%d,%d]",
				rngs[i-1].lo, rngs[i-1].hi, rngs[i].lo, rngs[i].hi)
		}
	}
	if rngs[0].lo <= future {
		t.Fatalf("grant %d reused seeded watermark %d", rngs[0].lo, future)
	}
	// Every range has exactly the requested size.
	for _, r := range rngs {
		if r.hi-r.lo+1 != rangeSize {
			t.Fatalf("range [%d,%d] size mismatch", r.lo, r.hi)
		}
	}
}

// TestGrantRangeSurvivesRestart closes the member, restarts it on the same
// data dir, and verifies the next grant continues above everything already
// handed out — the failover guarantee in miniature.
func TestGrantRangeSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cport, pport := freePort(t), freePort(t)
	cfg := PDConfig{
		NodeID:          1,
		DataDir:         dir,
		ClientListenURL: joinHostPortURL("127.0.0.1", cport),
		PeerListenURL:   joinHostPortURL("127.0.0.1", pport),
		AllPeerURLs: map[uint64]string{
			1: joinHostPortURL("127.0.0.1", pport),
		},
	}

	pd, err := StartPD(cfg)
	if err != nil {
		t.Fatalf("StartPD: %v", err)
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{pd.ClientURL()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	g := NewEtcdGranter(cli)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_, end, err := g.GrantRange(ctx, 500)
	cancel()
	if err != nil {
		t.Fatalf("first grant: %v", err)
	}
	pd.Stop()
	cli.Close()

	pd2, err := StartPD(cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer pd2.Stop()
	cli2, err := clientv3.New(clientv3.Config{Endpoints: []string{pd2.ClientURL()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("clientv3.New after restart: %v", err)
	}
	defer cli2.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	start2, end2, err := NewEtcdGranter(cli2).GrantRange(ctx2, 500)
	cancel2()
	if err != nil {
		t.Fatalf("post-restart grant: %v", err)
	}
	if start2 <= end {
		t.Fatalf("post-restart grant [%d,%d] overlaps pre-restart high water %d", start2, end2, end)
	}
}

// BenchmarkAlloc measures the request-path cost of a single timestamp: an
// atomic compare-and-swap loop over a pre-adopted range, no etcd in the
// loop. The pool is adopted with a span large enough that it never needs a
// refill during the benchmark, so this isolates the hot path. allocs/op
// must be 0.
func BenchmarkAlloc(b *testing.B) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 1, Headroom: time.Second, RefillThresholdPct: 20})
	if err := p.Adopt(1, 1<<40); err != nil {
		b.Fatalf("Adopt: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.TryAlloc(); err != nil {
			b.Fatalf("TryAlloc: %v", err)
		}
	}
}

// BenchmarkAllocParallel exercises the same hot path under contention from
// many goroutines, proving the single atomic cursor scales without per-call
// allocation.
func BenchmarkAllocParallel(b *testing.B) {
	p := NewTSOPool(TSOPoolConfig{MinBatch: 1, Headroom: time.Second, RefillThresholdPct: 20})
	if err := p.Adopt(1, 1<<45); err != nil {
		b.Fatalf("Adopt: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.TryAlloc(); err != nil {
				b.Fatalf("TryAlloc: %v", err)
			}
		}
	})
}
