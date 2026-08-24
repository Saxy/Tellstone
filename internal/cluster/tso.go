/*
Package cluster
Tellstone Timestamp Oracle (Phase 2)
File: tso.go
Description: Globally monotonic timestamp allocation (ADR-003, ADR-010).
The write-facing half is TSOPool: a local, pre-allocated range consumed by
a lock-free atomic increment — no network, no heap on the hot path. The
granting half advances a persistent watermark in etcd through CAS
transactions, which serializes concurrent refills without electing a
dedicated allocator: any member can extend the space, and a recovered or
freshly elected member resumes above everything already granted.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Timestamp layout: high 44 bits carry physical milliseconds (~558 years
// of range), low 20 bits a sub-millisecond logical counter (1,048,576
// timestamps per ms). Ordering is plain uint64 comparison.
const (
	tsLogicalBits = 20
	tsLogicalMask = (uint64(1) << tsLogicalBits) - 1

	// tsoWatermarkKey persists the highest granted timestamp. Its etcd
	// ModRevision drives the compare-and-swap below.
	tsoWatermarkKey = "/tellstone/pd/tso/watermark"

	// tsoRateAlpha weights the exponential moving average of the observed
	// allocation rate; 0.3 spans roughly a handful of samples.
	tsoRateAlpha = 0.3
)

// ErrTSOExhausted is returned by TryAlloc when the local pool has no
// timestamps left and PD contact is required before allocation can resume
// (ADR-003: the cluster degrades toward read-only).
var ErrTSOExhausted = errors.New("cluster: TSO pool exhausted")

// PackTimestamp combines physical milliseconds and a logical counter into
// one comparable uint64.
func PackTimestamp(physicalMS, logical uint64) uint64 {
	return physicalMS<<tsLogicalBits | logical&tsLogicalMask
}

// TimestampMillis extracts the physical component.
func TimestampMillis(ts uint64) uint64 {
	return ts >> tsLogicalBits
}

// TSOPool hands out timestamps from a locally owned range [start, end].
//
// Allocation is a single compare-and-swap on the hot path: two atomic
// loads, one CAS, zero heap. Exhaustion is the only slow path — callers
// either poll TryAlloc or block in Alloc until Adopt publishes a fresh
// range. All sizing knobs come from config defaults (ADR-010 §2).
type TSOPool struct {
	cur       atomic.Uint64
	end       atomic.Uint64
	total     atomic.Uint64
	mu        sync.Mutex
	cond      *sync.Cond
	lastSize  atomic.Uint64
	closed    bool
	minBatch  uint64
	headroom  time.Duration
	refillPct int
	sampler   rateSampler
}

// TSOPoolConfig carries the tuning knobs resolved from config flags.
type TSOPoolConfig struct {
	MinBatch           uint64
	Headroom           time.Duration
	RefillThresholdPct int
}

// NewTSOPool creates an empty pool; nothing is allocatable until the first
// Adopt.
func NewTSOPool(cfg TSOPoolConfig) *TSOPool {
	p := &TSOPool{
		minBatch:  cfg.MinBatch,
		headroom:  cfg.Headroom,
		refillPct: cfg.RefillThresholdPct,
	}
	if p.minBatch < 1 {
		p.minBatch = 1
	}
	if p.refillPct < 1 || p.refillPct > 99 {
		p.refillPct = 20
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// tryAlloc burns one timestamp; false signals exhaustion.
func (p *TSOPool) tryAlloc() (uint64, bool) {
	for {
		end := p.end.Load()
		cur := p.cur.Load()
		if cur >= end {
			return 0, false
		}
		if p.cur.CompareAndSwap(cur, cur+1) {
			p.total.Add(1)
			return cur + 1, true
		}
		// Lost the race; another goroutine claimed cur. Retry — the range
		// can only shrink, so this terminates.
	}
}

// TryAlloc returns the next timestamp or ErrTSOExhausted without waiting.
func (p *TSOPool) TryAlloc() (uint64, error) {
	if ts, ok := p.tryAlloc(); ok {
		return ts, nil
	}
	return 0, ErrTSOExhausted
}

// Alloc blocks until a timestamp is available, the pool is closed, or ctx
// is cancelled — the ADR-003 "writes block when pools run dry" semantic.
func (p *TSOPool) Alloc(ctx context.Context) (uint64, error) {
	if ts, ok := p.tryAlloc(); ok {
		return ts, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if ts, ok := p.tryAlloc(); ok {
			return ts, nil
		}
		if p.closed {
			return 0, errors.New("cluster: TSO pool closed")
		}
		stop := context.AfterFunc(ctx, p.cond.Broadcast)
		if ctx.Err() != nil {
			stop()
			return 0, fmt.Errorf("cluster: TSO allocation cancelled: %w", ctx.Err())
		}
		p.cond.Wait()
		stop()
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("cluster: TSO allocation cancelled: %w", err)
		}
	}
}

// Close releases every blocked Alloc caller.
func (p *TSOPool) Close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

// NeedsRefill reports whether the remaining budget dropped below the
// configured percentage of the last adopted range. An empty pool always
// needs a refill.
func (p *TSOPool) NeedsRefill() bool {
	remaining := p.end.Load() - p.cur.Load()
	if remaining == 0 {
		return true
	}
	size := p.lastSize.Load()
	if size == 0 {
		return true
	}
	return remaining*100 < uint64(p.refillPct)*size
}

// NextBatchSize sizes the next grant: max(min_batch, observed_rate ×
// headroom), per ADR-003.
func (p *TSOPool) NextBatchSize() uint64 {
	rate := p.sampler.sample(p.total.Load(), time.Now())
	size := uint64(rate * p.headroom.Seconds())
	if size < p.minBatch {
		size = p.minBatch
	}
	return size
}

// Adopt publishes a freshly granted range [start, end] and wakes every
// blocked allocator.
func (p *TSOPool) Adopt(start, end uint64) error {
	if start == 0 {
		return fmt.Errorf("cluster: invalid TSO range [%d, %d]: start must be non-zero", start, end)
	}
	if end < start {
		return fmt.Errorf("cluster: invalid TSO range [%d, %d]", start, end)
	}
	p.cur.Store(start - 1)
	p.end.Store(end)
	p.mu.Lock()
	p.lastSize.Store(end - start + 1)
	p.cond.Broadcast()
	p.mu.Unlock()
	return nil
}

// rateSampler maintains an EWMA of allocations per second from periodic
// totals. Sampling happens on the refill path only.
type rateSampler struct {
	mu        sync.Mutex
	prevTotal uint64
	prevAt    time.Time
	ewma      float64
	started   bool
}

func (s *rateSampler) sample(totalNow uint64, now time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		s.started = true
		s.prevTotal, s.prevAt = totalNow, now
		return 0
	}
	dt := now.Sub(s.prevAt).Seconds()
	if dt <= 0 {
		return s.ewma
	}
	instant := float64(totalNow-s.prevTotal) / dt
	if instant < 0 {
		instant = 0
	}
	s.ewma += tsoRateAlpha * (instant - s.ewma)
	s.prevTotal, s.prevAt = totalNow, now
	return s.ewma
}

// EtcdGranter grants timestamp ranges by CAS-advancing the persistent
// watermark. Concurrent granters are serialized by etcd transactions, so
// no dedicated leader election is required for correctness: ranges are
// globally disjoint by construction and survive member restarts.
type EtcdGranter struct {
	cli *clientv3.Client
}

// NewEtcdGranter wraps an established etcd client.
func NewEtcdGranter(cli *clientv3.Client) *EtcdGranter {
	return &EtcdGranter{cli: cli}
}

// GrantRange reserves the next n timestamps, retrying the CAS on conflict.
// The returned interval [start, end] never overlaps anything previously
// granted — including by a predecessor process that died mid-flight.
func (g *EtcdGranter) GrantRange(ctx context.Context, n uint64) (uint64, uint64, error) {
	if n == 0 {
		return 0, 0, errors.New("cluster: TSO grant size must be positive")
	}
	for {
		getResp, err := g.cli.Get(ctx, tsoWatermarkKey)
		if err != nil {
			return 0, 0, fmt.Errorf("cluster: reading TSO watermark: %w", err)
		}
		var watermark uint64
		var rev int64
		if len(getResp.Kvs) == 1 {
			watermark = binary.LittleEndian.Uint64(getResp.Kvs[0].Value)
			rev = getResp.Kvs[0].ModRevision
		}
		start := watermark + 1
		if nowMS := uint64(time.Now().UnixMilli()); nowMS > TimestampMillis(watermark) {
			start = PackTimestamp(nowMS, 0)
		}
		end := start + n - 1

		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, end)
		txn, err := g.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(tsoWatermarkKey), "=", rev)).
			Then(clientv3.OpPut(tsoWatermarkKey, string(buf))).
			Commit()
		if err != nil {
			return 0, 0, fmt.Errorf("cluster: committing TSO watermark: %w", err)
		}
		if txn.Succeeded {
			return start, end, nil
		}
	}
}
