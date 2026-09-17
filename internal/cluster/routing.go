/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: routing.go
Description: Node-local routing table for Phase 3 (read/write-anywhere). Maps
key ranges to the leader node of the owning region. Lookups are an O(log n)
binary search over a slice sorted by StartKey; updates arrive from the PD via
etcd Watch (see region.go) and are epoch-guarded so stale entries are dropped.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bytes"
	"sort"
	"sync"
)

// RegionRoute is one entry in a node's routing table: the key range it covers
// and the node currently leading that region's Raft group.
type RegionRoute struct {
	ID       uint64 // region ID, used by the size tracker resolver
	StartKey []byte
	EndKey   []byte
	Leader   uint64 // node ID of the current region leader
	Epoch    uint64
	// Peers are the region's Raft member node IDs, used for zone-aware read
	// routing (prefer a same-zone replica) and diagnostics.
	Peers []uint64
	// PreferredZone is the region's geo-pinned zone (empty = none, "*" =
	// global). Used by zone-aware read routing to prefer leader/replicas in
	// the client's zone (Phase 6).
	PreferredZone string
}

// RoutingTable is a concurrency-safe, node-local cache of region metadata.
// It is the lookup structure that enables read/write-anywhere: any node can
// resolve a key to the region leader it must forward writes to.
type RoutingTable struct {
	mu      sync.RWMutex
	regions []RegionRoute
}

// NewRoutingTable returns an empty routing table.
func NewRoutingTable() *RoutingTable { return &RoutingTable{} }

// FindInZone returns the route covering key, preferring a route whose leader
// sits in clientZone when the region is zone-pinned. With no pinning or an
// unknown client zone it behaves exactly like Find. This lets a node route a
// read to the leader closest to the client that issued it (Phase 6, ADR-006
// §Read Routing).
func (rt *RoutingTable) FindInZone(key []byte, clientZone string, zones *NodeZones) *RegionRoute {
	route := rt.Find(key)
	if route == nil || clientZone == "" || zones == nil {
		return route
	}
	if route.PreferredZone != "" && route.PreferredZone != GeoZoneGlobal &&
		ZoneOf(zones, route.Leader) != clientZone {
		// The current leader is in the wrong zone for this client. Prefer a
		// same-zone member of the same region.
		for _, peer := range route.Peers {
			if ZoneOf(zones, peer) == clientZone {
				cp := *route
				cp.Leader = peer
				return &cp
			}
		}
	}
	return route
}

// Find returns the route whose [StartKey, EndKey) contains key. An empty
// StartKey matches negative infinity; an empty EndKey matches positive
// infinity. Returns nil if no region covers the key.
func (rt *RoutingTable) Find(key []byte) *RegionRoute {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	// First region with StartKey > key; the candidate is the one before it.
	i := sort.Search(len(rt.regions), func(i int) bool {
		return bytes.Compare(rt.regions[i].StartKey, key) > 0
	})
	if i == 0 {
		return nil
	}
	r := &rt.regions[i-1]
	if len(r.EndKey) > 0 && bytes.Compare(key, r.EndKey) >= 0 {
		return nil
	}
	cp := *r
	return &cp
}

// Update merges region metadata into the table. Stale updates (epoch not
// strictly greater than the existing entry for the same StartKey) are dropped
// unless the region is new. This prevents a delayed Watch event from rolling
// back a split or move (ADR-004 §Routing Table).
func (rt *RoutingTable) Update(meta Region) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	idx := sort.Search(len(rt.regions), func(i int) bool {
		return bytes.Compare(rt.regions[i].StartKey, meta.StartKey) >= 0
	})
	if idx < len(rt.regions) && bytes.Equal(rt.regions[idx].StartKey, meta.StartKey) {
		if meta.Epoch <= rt.regions[idx].Epoch {
			return // stale (lower or equal epoch)
		}
		rt.regions[idx] = regionRouteOf(meta)
		return
	}
	// Insert in sorted order (regions slice is small; copy is cheap).
	rt.regions = append(rt.regions, RegionRoute{})
	copy(rt.regions[idx+1:], rt.regions[idx:])
	rt.regions[idx] = regionRouteOf(meta)
}

// Snapshot returns a copy of the current routes, sorted by StartKey. Used by
// tests and diagnostics.
func (rt *RoutingTable) Snapshot() []RegionRoute {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := make([]RegionRoute, len(rt.regions))
	copy(out, rt.regions)
	return out
}

func regionRouteOf(m Region) RegionRoute {
	return RegionRoute{
		ID:            m.ID,
		StartKey:      m.StartKey,
		EndKey:        m.EndKey,
		Leader:        m.Leader,
		Epoch:         m.Epoch,
		Peers:         m.Peers,
		PreferredZone: m.PreferredZone,
	}
}
