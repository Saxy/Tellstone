/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: region_size.go
Description: Per-region byte-size tracker for Phase 4 region splitting. Every
committed SET/DEL is reflected via atomic counters so the PD can monitor region
sizes and trigger splits when a threshold is exceeded. The tracker also runs a
background loop that pushes sizes to etcd so all nodes stay informed.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// RegionSizeTracker maintains per-region byte counters and periodically pushes
// them to etcd. It is safe for concurrent use by the FSM apply path and the
// background report loop.
//
// The counters reflect the *current* stored bytes per region, not cumulative
// writes: every key's live total (key + value bytes) is tracked so a SET
// overwriting an existing key adjusts by the net delta and a DEL removes
// exactly the stored bytes. Without this bookkeeping, repeated SETs to the
// same key would double-count and DEL would under-subtract (the DEL log entry
// carries no value bytes), inflating region sizes and triggering false splits.
type RegionSizeTracker struct {
	mu    sync.RWMutex
	sizes map[uint64]*atomic.Uint64 // regionID → current byte count
	keys  map[regionSizeKey]uint64  // (regionID, key) → live stored bytes
}

// regionSizeKey identifies one key within one region for size bookkeeping.
type regionSizeKey struct {
	regionID uint64
	key      string
}

// NewRegionSizeTracker creates a tracker ready for use.
func NewRegionSizeTracker() *RegionSizeTracker {
	return &RegionSizeTracker{
		sizes: make(map[uint64]*atomic.Uint64),
		keys:  make(map[regionSizeKey]uint64),
	}
}

// TrackSet adjusts the byte counter for region by the net change of storing
// key with value: total bytes for a new key, or the delta replacing an
// existing value.
func (t *RegionSizeTracker) TrackSet(regionID uint64, key string, value []byte) {
	total := uint64(len(key) + len(value))
	ek := regionSizeKey{regionID: regionID, key: key}
	t.mu.Lock()
	oldTotal, existed := t.keys[ek]
	t.keys[ek] = total
	c := t.sizes[regionID]
	if c == nil {
		c = &atomic.Uint64{}
		t.sizes[regionID] = c
	}
	// Apply the counter delta while mu is still held so the map mutation and
	// the byte counter stay atomic with respect to concurrent TrackDel: an
	// interleaved delete must not observe the new keys entry with the old
	// counter (or vice versa), which would over- or under-count bytes.
	if existed {
		if oldTotal > total {
			subCounter(c, oldTotal-total)
		} else if total > oldTotal {
			c.Add(total - oldTotal)
		}
	} else {
		c.Add(total)
	}
	t.mu.Unlock()
}

// TrackDel removes the tracked bytes for key from the region counter.
func (t *RegionSizeTracker) TrackDel(regionID uint64, key string, _ []byte) {
	ek := regionSizeKey{regionID: regionID, key: key}
	t.mu.Lock()
	oldTotal, existed := t.keys[ek]
	if existed {
		delete(t.keys, ek)
	}
	c := t.sizes[regionID]
	if existed && c != nil {
		subCounter(c, oldTotal)
	}
	t.mu.Unlock()
}

// subCounter decrements c by n, flooring at zero.
func subCounter(c *atomic.Uint64, n uint64) {
	for {
		old := c.Load()
		if old < n {
			c.Store(0)
			return
		}
		if c.CompareAndSwap(old, old-n) {
			return
		}
	}
}

// GetSize returns the current tracked byte count for region.
func (t *RegionSizeTracker) GetSize(regionID uint64) uint64 {
	t.mu.RLock()
	c, ok := t.sizes[regionID]
	t.mu.RUnlock()
	if !ok {
		return 0
	}
	return c.Load()
}

// SetSize overwrites the tracked byte count for region. Used when reconciling
// after a split or on startup when sizes are loaded from etcd.
func (t *RegionSizeTracker) SetSize(regionID uint64, size uint64) {
	t.counterFor(regionID).Store(size)
}

// counterFor returns the atomic counter for region, creating it on first access.
func (t *RegionSizeTracker) counterFor(regionID uint64) *atomic.Uint64 {
	t.mu.RLock()
	c, ok := t.sizes[regionID]
	t.mu.RUnlock()
	if ok {
		return c
	}
	t.mu.Lock()
	c, ok = t.sizes[regionID]
	if !ok {
		c = &atomic.Uint64{}
		t.sizes[regionID] = c
	}
	t.mu.Unlock()
	return c
}

// ReportLoop pushes region sizes to etcd at the given interval until ctx is
// cancelled. It is non-blocking and designed to run as a background goroutine.
func (t *RegionSizeTracker) ReportLoop(ctx context.Context, cli *clientv3.Client, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reportAll(ctx, cli)
		}
	}
}

// reportAll writes every tracked region size to etcd. A write is performed
// only when the size has changed since the last report (tracked locally).
func (t *RegionSizeTracker) reportAll(ctx context.Context, cli *clientv3.Client) {
	t.mu.RLock()
	snapshot := make(map[uint64]uint64, len(t.sizes))
	for id, c := range t.sizes {
		snapshot[id] = c.Load()
	}
	t.mu.RUnlock()

	for id, size := range snapshot {
		r, err := getRegion(ctx, cli, id)
		if err != nil || r == nil {
			continue
		}
		r.SizeBytes = size
		if _, err := cli.Put(ctx, regionKey(id), string(encodeRegion(*r))); err != nil {
			continue
		}
	}
}

// getRegion reads a single region from etcd. Exported for use by the report
// loop and test helpers.
func getRegion(ctx context.Context, cli *clientv3.Client, id uint64) (*Region, error) {
	resp, err := cli.Get(ctx, regionKey(id))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	r, ok := decodeRegion(resp.Kvs[0].Value)
	if !ok {
		return nil, nil
	}
	return &r, nil
}
