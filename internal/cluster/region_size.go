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
type RegionSizeTracker struct {
	mu    sync.RWMutex
	sizes map[uint64]*atomic.Uint64 // regionID → current byte count
}

// NewRegionSizeTracker creates a tracker ready for use.
func NewRegionSizeTracker() *RegionSizeTracker {
	return &RegionSizeTracker{
		sizes: make(map[uint64]*atomic.Uint64),
	}
}

// TrackSet increments the byte counter for region by the entry size.
func (t *RegionSizeTracker) TrackSet(regionID uint64, key string, value []byte) {
	t.counterFor(regionID).Add(uint64(len(key) + len(value)))
}

// TrackDel decrements the byte counter for region by the entry size.
func (t *RegionSizeTracker) TrackDel(regionID uint64, key string, value []byte) {
	c := t.counterFor(regionID)
	n := uint64(len(key) + len(value))
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
