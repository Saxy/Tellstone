/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: geo.go
Description: Geo-aware placement (Phase 6, ADR-006). Nodes self-declare their
availability zone at startup and register it in etcd under /tellstone/nodes/<id>.
Operators define a GeoPolicy mapping key prefixes to zones; the PD places new
regions according to the policy and leader election prefers same-zone replicas.
Unmatched keys default to the global zone "*" (replicated everywhere).

Wire format for NodeInfo (compact binary, no protobuf):

	[8B ID][2B zone_len][zone][2B addr_len][addr]

Wire format for GeoPolicy:

	[8B version][2B rule_count][rule...]
	rule: [2B prefix_len][prefix][2B zone_len][zone][8B replicas]

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
)

// GeoZoneGlobal is the wildcard zone used by rules that replicate a key
// range everywhere. It is also the default for keys that match no rule:
// unknown data is replicated globally rather than silently pinned to one
// zone with cross-ocean latency for readers elsewhere (ADR-006 §Negative).
const GeoZoneGlobal = "*"

// GeoDefaultReplicas is the replica count assumed when a rule does not
// specify one.
const GeoDefaultReplicas = 3

const nodesKeyPrefix = "/tellstone/nodes/"

const geoPolicyKey = "/tellstone/geo/rules"

func nodesKey(id uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return nodesKeyPrefix + string(b[:])
}

// NodeInfo is the metadata a node self-registers in etcd at startup so every
// other node (and the PD) can resolve node IDs to availability zones.
type NodeInfo struct {
	ID   uint64
	Zone string
	Addr string
}

// encodeNodeInfo serializes a NodeInfo into compact binary form.
func encodeNodeInfo(n NodeInfo) []byte {
	buf := make([]byte, 0, 12+len(n.Zone)+len(n.Addr))
	buf = appendUint64(buf, n.ID)
	buf = appendBytesField(buf, []byte(n.Zone))
	buf = appendBytesField(buf, []byte(n.Addr))
	return buf
}

// decodeNodeInfo parses a NodeInfo value. Returns ok=false on truncated data.
func decodeNodeInfo(b []byte) (NodeInfo, bool) {
	var n NodeInfo
	if len(b) < 12 { // 8 + 2 + 2
		return n, false
	}
	n.ID = binary.BigEndian.Uint64(b[0:8])
	b = b[8:]
	var ok bool
	b, z, ok := readBytesField(b)
	if !ok {
		return n, false
	}
	n.Zone = string(z)
	b, a, ok := readBytesField(b)
	if !ok {
		return n, false
	}
	n.Addr = string(a)
	return n, true
}

// GeoRule pins keys with a matching prefix to a zone. Zone "*" means the
// range is replicated across all zones; Replicas defaults to
// GeoDefaultReplicas when zero.
type GeoRule struct {
	Prefix   string
	Zone     string
	Replicas int
}

// GeoPolicy is the operator-defined placement policy. Rules are matched by
// longest prefix; a key with no matching rule defaults to GeoZoneGlobal.
type GeoPolicy struct {
	Version uint64
	Rules   []GeoRule
}

// encodeGeoPolicy serializes a policy into compact binary form. It returns an
// error when the rule count exceeds the uint16 wire field so an oversized
// policy is never persisted with a wrapped count.
func encodeGeoPolicy(p GeoPolicy) ([]byte, error) {
	if len(p.Rules) > math.MaxUint16 {
		return nil, fmt.Errorf("cluster: geo policy has %d rules, wire format supports at most %d", len(p.Rules), math.MaxUint16)
	}
	buf := make([]byte, 0, 16)
	buf = appendUint64(buf, p.Version)
	buf = appendUint16(buf, uint16(len(p.Rules)))
	for _, r := range p.Rules {
		buf = appendBytesField(buf, []byte(r.Prefix))
		buf = appendBytesField(buf, []byte(r.Zone))
		buf = appendUint64(buf, uint64(r.Replicas))
	}
	return buf, nil
}

// decodeGeoPolicy parses a policy value. Returns ok=false on truncated data.
func decodeGeoPolicy(b []byte) (GeoPolicy, bool) {
	var p GeoPolicy
	if len(b) < 10 { // 8 + 2
		return p, false
	}
	p.Version = binary.BigEndian.Uint64(b[0:8])
	nRules := binary.BigEndian.Uint16(b[8:10])
	b = b[10:]
	for i := 0; i < int(nRules); i++ {
		var r GeoRule
		var prefix, zone []byte
		var ok bool
		b, prefix, ok = readBytesField(b)
		if !ok {
			return p, false
		}
		r.Prefix = string(prefix)
		b, zone, ok = readBytesField(b)
		if !ok {
			return p, false
		}
		r.Zone = string(zone)
		if len(b) < 8 { // replicas
			return p, false
		}
		r.Replicas = int(binary.BigEndian.Uint64(b[0:8]))
		b = b[8:]
		p.Rules = append(p.Rules, r)
	}
	return p, true
}

// DefaultGeoPolicy returns the bootstrap policy: a single catch-all rule
// replicating everything globally. Applied when no policy exists in etcd.
func DefaultGeoPolicy() GeoPolicy {
	return GeoPolicy{
		Version: 1,
		Rules: []GeoRule{
			{Prefix: "", Zone: GeoZoneGlobal, Replicas: GeoDefaultReplicas},
		},
	}
}

// Match returns the zone a key should be pinned to. Longest matching prefix
// wins; keys matching none default to GeoZoneGlobal.
func (p GeoPolicy) Match(key []byte) string {
	best := ""
	bestLen := -1
	for _, r := range p.Rules {
		if len(r.Prefix) <= bestLen {
			continue
		}
		if len(r.Prefix) == 0 {
			// Catch-all rule: a candidate, but only wins if nothing longer
			// matches (bestLen guards this below).
			if bestLen < 0 {
				bestLen = 0
				best = r.Zone
			}
			continue
		}
		if len(key) >= len(r.Prefix) && string(key[:len(r.Prefix)]) == r.Prefix {
			bestLen = len(r.Prefix)
			best = r.Zone
		}
	}
	if bestLen < 0 {
		return GeoZoneGlobal
	}
	return best
}

// ReplicaCount returns the target replica count for a key, or
// GeoDefaultReplicas when no rule (or no requested count) matches.
func (p GeoPolicy) ReplicaCount(key []byte) int {
	bestPrefix := ""
	bestLen := -1
	for _, r := range p.Rules {
		if len(r.Prefix) <= bestLen {
			continue
		}
		matched := len(r.Prefix) == 0
		if !matched && len(key) >= len(r.Prefix) && string(key[:len(r.Prefix)]) == r.Prefix {
			matched = true
		}
		if matched {
			bestLen = len(r.Prefix)
			bestPrefix = r.Prefix
		}
	}
	if bestLen < 0 {
		return GeoDefaultReplicas
	}
	for _, r := range p.Rules {
		if r.Prefix == bestPrefix {
			if r.Replicas > 0 {
				return r.Replicas
			}
		}
	}
	return GeoDefaultReplicas
}

// GeoPolicyProvider is implemented by *GeoManager (and test doubles). It
// supplies the current operator policy to placement/leader-election code.
type GeoPolicyProvider interface {
	Policy() GeoPolicy
}

// PreferredZoneOf computes the zone a region covering startKey should be
// pinned to. The policy is applied to the region's start key; a range that
// spans multiple rule zones inherits the zone of its lowest key, and future
// splits at rule boundaries refine it. Global ("*") means the replicas are
// spread across all zones (no pinning).
func PreferredZoneOf(policy GeoPolicy, startKey []byte) string {
	return policy.Match(startKey)
}

// PickZonePeers orders candidate node IDs so that nodes in the preferred
// zone come first, followed by all remaining nodes. The global zone "*"
// means no ordering preference (all nodes are treated equally). Nodes not in
// the registry sort after every known node. Used for placement and read
// routing when a region's replicas must be preferred by zone.
func PickZonePeers(zones *NodeZones, preferred string, candidates []uint64) []uint64 {
	out := make([]uint64, 0, len(candidates))
	if preferred == "" || preferred == GeoZoneGlobal {
		// No pinning: preserve the candidate order (all equal).
		return candidates
	}
	preferredSet := make(map[uint64]struct{})
	for _, id := range zones.NodesInZone(preferred) {
		preferredSet[id] = struct{}{}
	}
	for _, id := range candidates {
		if _, ok := preferredSet[id]; ok {
			out = append(out, id)
		}
	}
	for _, id := range candidates {
		if _, ok := preferredSet[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// ZoneOf best-effort resolves a node's zone from the registry for logging and
// latency metrics. Returns "" when unknown.
func ZoneOf(zones *NodeZones, id uint64) string {
	if zones == nil {
		return ""
	}
	return zones.Zone(id)
}

// NodeZones tracks the node → zone registry watched from
// /tellstone/nodes/<id>. It is a concurrency-safe cache used by the PD for
// placement decisions and by nodes for zone-aware routing.
type NodeZones struct {
	mu    sync.RWMutex
	zones map[uint64]string
}

// NewNodeZones returns an empty node→zone registry.
func NewNodeZones() *NodeZones {
	return &NodeZones{zones: make(map[uint64]string)}
}

// Update upserts a node's zone. Lower-or-equal registrations are ignored so a
// stale watch event cannot roll back a zone change.
func (z *NodeZones) Update(n NodeInfo) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if cur, ok := z.zones[n.ID]; ok && cur != "" && n.Zone == "" {
		return // never downgrade a known zone to unknown
	}
	z.zones[n.ID] = n.Zone
}

// Remove drops a node entry (etcd deletion).
func (z *NodeZones) Remove(id uint64) {
	z.mu.Lock()
	defer z.mu.Unlock()
	delete(z.zones, id)
}

// Replace atomically swaps the registry contents, preserving the receiver so
// callers holding the pointer observe the new state. Used by the seed path to
// rebuild the cache from a full registry read so nodes omitted from the read
// are dropped.
func (z *NodeZones) Replace(nodes []NodeInfo) {
	repl := make(map[uint64]string, len(nodes))
	for _, n := range nodes {
		repl[n.ID] = n.Zone
	}
	z.mu.Lock()
	z.zones = repl
	z.mu.Unlock()
}

// Zone returns the zone for a node ID, or "" when unknown.
func (z *NodeZones) Zone(id uint64) string {
	z.mu.RLock()
	defer z.mu.RUnlock()
	return z.zones[id]
}

// NodesInZone returns the node IDs registered in the given zone.
func (z *NodeZones) NodesInZone(zone string) []uint64 {
	z.mu.RLock()
	defer z.mu.RUnlock()
	var out []uint64
	for id, zz := range z.zones {
		if zz == zone {
			out = append(out, id)
		}
	}
	return out
}

// Snapshot returns a copy of the registry keyed by node ID.
func (z *NodeZones) Snapshot() map[uint64]string {
	z.mu.RLock()
	defer z.mu.RUnlock()
	out := make(map[uint64]string, len(z.zones))
	for id, zone := range z.zones {
		out[id] = zone
	}
	return out
}
