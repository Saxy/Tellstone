/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: region.go
Description: Region metadata and the RegionManager (Phase 3). A Region is the
authoritative map of a key range to its Raft group (ADR-004). The RegionManager
stores region metadata in the embedded etcd KV, watches it so every node's
RoutingTable converges, and — on the Raft leader — keeps the region's Leader
field pointed at itself (D1: etcd Watch, not a gRPC push).

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Region is the authoritative metadata for one key-range Raft group. It is
// serialized and stored in etcd under /tellstone/regions/<id>.
type Region struct {
	ID        uint64
	StartKey  []byte
	EndKey    []byte
	Peers     []uint64 // node IDs of the region's Raft members
	Leader    uint64   // current leader node ID (0 until first leader claims it)
	Epoch     uint64   // bumped on every split/move/leadership change
	SizeBytes uint64   // tracked byte count for split decisions (Phase 4)
	// PreferredZone is the geo zone the region should be pinned to by the
	// routing table and leader election (Phase 6, ADR-006). Empty means no
	// pinning; GeoZoneGlobal ("*") means replicated everywhere.
	PreferredZone string
}

const regionKeyPrefix = "/tellstone/regions/"

func regionKey(id uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return regionKeyPrefix + string(b[:])
}

// LeadershipProvider reports whether this node is the current Raft leader.
// Satisfied by *Node.
type LeadershipProvider interface {
	IsLeader() bool
}

// RegionManager owns region metadata for one node: it bootstraps the default
// region, watches etcd for updates, and (on the leader) keeps the leader field
// current. It is safe to run one per node.
type RegionManager struct {
	cli    *clientv3.Client
	nodeID uint64
	// lpMu guards lp: it is set once during startup (SetLeadershipProvider)
	// after the manager is constructed, while the leadershipLoop may already
	// be reading it.
	lpMu  sync.RWMutex
	lp    LeadershipProvider
	peers []uint64
	rt    *RoutingTable
	// geoMu guards geo; it is set once during startup (SetGeoPolicyProvider)
	// after the manager is constructed, while the leadershipLoop may already
	// be reading it. When nil, region PreferredZone is never reconciled.
	geoMu sync.RWMutex
	geo   GeoPolicyProvider
}

// NewRegionManager creates a manager bound to the PD etcd client and the local
// node's leadership signal. It allocates its own RoutingTable.
func NewRegionManager(cli *clientv3.Client, nodeID uint64, lp LeadershipProvider, peers []uint64) *RegionManager {
	return &RegionManager{
		cli:    cli,
		nodeID: nodeID,
		lp:     lp,
		peers:  peers,
		rt:     NewRoutingTable(),
	}
}

// RoutingTable returns the node-local table fed by the watcher.
func (m *RegionManager) RoutingTable() *RoutingTable { return m.rt }

// BootstrapDefaultRegion creates the single initial region (ID 1, whole
// keyspace) if it does not already exist. Safe to call on every node; only the
// first writer wins (D4: one default region to start).
func (m *RegionManager) BootstrapDefaultRegion(ctx context.Context) error {
	key := regionKey(1)
	nr := Region{
		ID:       1,
		StartKey: []byte{},
		EndKey:   nil,
		Peers:    m.peers,
		Leader:   0,
		Epoch:    1,
	}
	if regionExceedsWireLimit(nr) {
		return fmt.Errorf("region %d exceeds wire limit: peers=%d startKey=%d endKey=%d",
			nr.ID, len(nr.Peers), len(nr.StartKey), len(nr.EndKey))
	}
	val := encodeRegion(nr)
	txn := m.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(val)))
	_, err := txn.Commit()
	return err
}

// Refresh re-reads the full region set from etcd and updates the local routing
// table. It is used by the write path to recover quickly from a stale table
// (e.g. the Watch has not yet delivered a leader change) instead of failing the
// write. It is safe to call concurrently with Run.
func (m *RegionManager) Refresh(ctx context.Context) error {
	resp, err := m.cli.Get(ctx, regionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		if r, ok := decodeRegion(kv.Value); ok {
			m.rt.Update(r)
		}
	}
	return nil
}

// seed reads the full region set from etcd, merges it into the local routing
// table, and returns the next revision to watch from.
func (m *RegionManager) seed(ctx context.Context) (int64, error) {
	resp, err := m.cli.Get(ctx, regionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	for _, kv := range resp.Kvs {
		if r, ok := decodeRegion(kv.Value); ok {
			m.rt.Update(r)
		}
	}
	return resp.Header.Revision + 1, nil
}

// Run seeds the routing table from etcd, then watches for changes until ctx is
// cancelled. The leadership loop runs concurrently. If the watch is interrupted
// (e.g. etcd compaction returning ErrCompacted) or the channel closes, Run
// re-reads the latest metadata and recreates the watch so no updates are
// missed (D4 convergence across compaction).
func (m *RegionManager) Run(ctx context.Context) error {
	rev, err := m.seed(ctx)
	if err != nil {
		return err
	}
	go m.leadershipLoop(ctx)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ch := m.cli.Watch(ctx, regionKeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
		recreate := false
		for !recreate {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case wresp, ok := <-ch:
				if !ok {
					// Watch channel closed (compaction or client teardown).
					recreate = true
					break
				}
				if werr := wresp.Err(); werr != nil {
					// Terminal watch error (e.g. rpctypes.ErrCompacted):
					// re-read the latest metadata before recreating the watch.
					recreate = true
					break
				}
				for _, ev := range wresp.Events {
					if ev.Type == clientv3.EventTypeDelete {
						continue
					}
					if r, ok := decodeRegion(ev.Kv.Value); ok {
						m.rt.Update(r)
					}
				}
			}
		}
		// Reconnect: pull the latest metadata and continue with a fresh watch.
		newRev, rerr := m.seed(ctx)
		if rerr != nil {
			return rerr
		}
		rev = newRev
	}
}

// RegionLeadershipProvider is an optional extension to LeadershipProvider for
// multi-region (Phase 4) deployments: per-region leaders are reported for the
// region's own Raft group. Nodes hosting only the bootstrap group can keep the
// plain LeadershipProvider; leadership for any other region is then reported
// as not-leader.
type RegionLeadershipProvider interface {
	IsLeaderFor(regionID uint64) bool
}

// SetLeadershipProvider swaps the node's leadership signal after creation.
// Used by the server when per-region Raft group nodes are hosted by a separate
// coordinator that owns the leadership query, avoiding a construction cycle.
func (m *RegionManager) SetLeadershipProvider(lp LeadershipProvider) {
	m.lpMu.Lock()
	m.lp = lp
	m.lpMu.Unlock()
}

// isLeaderFor reports whether this node is the current Raft leader of the
// region's group. A plain LeadershipProvider only knows the bootstrap group
// (region 1); a RegionLeadershipProvider resolves every region.
func (m *RegionManager) isLeaderFor(regionID uint64) bool {
	m.lpMu.RLock()
	lp := m.lp
	m.lpMu.RUnlock()
	if rlp, ok := lp.(RegionLeadershipProvider); ok {
		return rlp.IsLeaderFor(regionID)
	}
	if lp == nil {
		return false
	}
	return regionID == 1 && lp.IsLeader()
}

// SetGeoPolicyProvider wires the region manager to a geo policy source so the
// leadership loop can pin region zones (Phase 6). Optional: without it regions
// keep whatever PreferredZone they were created with.
func (m *RegionManager) SetGeoPolicyProvider(g GeoPolicyProvider) {
	m.geoMu.Lock()
	m.geo = g
	m.geoMu.Unlock()
}

// geoPolicy returns the current geo policy and whether a provider is wired.
// The bool is false when no provider is set, letting callers skip zone
// reconciliation: without a real policy they would otherwise re-pin every
// region to the default global zone and bump its epoch for no reason.
func (m *RegionManager) geoPolicy() (GeoPolicy, bool) {
	m.geoMu.RLock()
	g := m.geo
	m.geoMu.RUnlock()
	if g == nil {
		return GeoPolicy{}, false
	}
	return g.Policy(), true
}

// reconcileZone computes the region's PreferredZone from the geo policy and
// reports whether it differs, so the leadership loop can pin (or re-pin) it
// when the operator changes the rules.
func reconcileZone(cur Region, policy GeoPolicy) (string, bool) {
	want := PreferredZoneOf(policy, cur.StartKey)
	return want, want != cur.PreferredZone
}

// leadershipLoop, on each region's Raft leader, ensures the region's Leader
// field points at this node. It scans all region metadata and only writes when
// the field is stale, so non-leaders are silent and each region's single
// leader wins quickly after an election. Without this, routed writes to a
// split region would never learn the leader (route.Leader == 0).
func (m *RegionManager) leadershipLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := m.cli.Get(ctx, regionKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				continue
			}
			policy, hasGeo := m.geoPolicy()
			for _, kv := range resp.Kvs {
				cur, ok := decodeRegion(kv.Value)
				if !ok {
					continue
				}
				if !m.isLeaderFor(cur.ID) {
					continue
				}
				cur, changed := m.reconcileLeadership(cur, policy, hasGeo)
				if !changed || regionExceedsWireLimit(cur) {
					continue
				}
				m.persistLeadership(ctx, cur, kv.ModRevision)
			}
		}
	}
}

// reconcileLeadership applies the current geo policy and this node's
// leadership to a region record. The PreferredZone is reconciled only when a
// real geo provider is wired: the provider-less default policy would otherwise
// pin every region to the global zone and bump its epoch for no reason.
// It reports whether the record actually changed.
func (m *RegionManager) reconcileLeadership(cur Region, policy GeoPolicy, hasGeo bool) (Region, bool) {
	changed := false
	if zone, zchange := reconcileZone(cur, policy); hasGeo && zchange {
		cur.PreferredZone = zone
		cur.Epoch++
		changed = true
	}
	if cur.Leader != m.nodeID {
		cur.Leader = m.nodeID
		cur.Epoch++
		changed = true
	}
	return cur, changed
}

// persistLeadership conditionally writes a reconciled region record. The etcd
// txn compares the ModRevision seen by the leadership scan so a competing
// leader (or any other writer) that updated the record in between can never be
// overwritten: on conflict the record is reloaded and reconciliation is
// retried against the fresh revision.
func (m *RegionManager) persistLeadership(ctx context.Context, cur Region, modRev int64) {
	for {
		txn := m.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(regionKey(cur.ID)), "=", modRev)).
			Then(clientv3.OpPut(regionKey(cur.ID), string(encodeRegion(cur))))
		tresp, err := txn.Commit()
		if err != nil || tresp.Succeeded {
			return
		}
		fresh, freshRev, err := m.getRegionRev(ctx, cur.ID)
		if err != nil || fresh == nil {
			return
		}
		policy, hasGeo := m.geoPolicy()
		reconciled, changed := m.reconcileLeadership(*fresh, policy, hasGeo)
		if !changed || regionExceedsWireLimit(reconciled) {
			return
		}
		cur = reconciled
		modRev = freshRev
	}
}

// getRegionRev reads a single region from etcd along with its ModRevision, so
// a conditional leader write can compare against the very record it read.
func (m *RegionManager) getRegionRev(ctx context.Context, id uint64) (*Region, int64, error) {
	resp, err := m.cli.Get(ctx, regionKey(id))
	if err != nil {
		return nil, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, nil
	}
	r, ok := decodeRegion(resp.Kvs[0].Value)
	if !ok {
		return nil, 0, nil
	}
	return &r, resp.Kvs[0].ModRevision, nil
}

// getRegion reads a single region from etcd.
func (m *RegionManager) getRegion(ctx context.Context, id uint64) (*Region, error) {
	resp, err := m.cli.Get(ctx, regionKey(id))
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

// UpdateRegionSize writes a region's byte count to etcd so the PD can monitor
// it for split decisions. Only the SizeBytes field is updated; all other
// metadata is preserved. This is called by the leader's size report loop.
func (m *RegionManager) UpdateRegionSize(ctx context.Context, id uint64, sizeBytes uint64) error {
	r, err := m.getRegion(ctx, id)
	if err != nil {
		return err
	}
	if r == nil {
		return nil // region does not exist yet; skip
	}
	r.SizeBytes = sizeBytes
	_, err = m.cli.Put(ctx, regionKey(id), string(encodeRegion(*r)))
	return err
}

// RegionKeyPrefix returns the etcd key prefix under which region metadata is
// stored. Exported for server-side coordinators that scan all regions.
func RegionKeyPrefix() string { return regionKeyPrefix }

// DecodeRegion decodes a region stored in etcd. Exported for server-side
// consumers (the region coordinator) that read raw etcd values.
func DecodeRegion(data []byte) (Region, bool) { return decodeRegion(data) }

// --- serialization (compact binary, no protobuf) ---

func encodeRegion(r Region) []byte {
	buf := make([]byte, 0, 40+len(r.StartKey)+len(r.EndKey)+len(r.PreferredZone))
	buf = appendUint64(buf, r.ID)
	buf = appendUint64(buf, r.Epoch)
	buf = appendUint64(buf, r.Leader)
	buf = appendUint16(buf, uint16(len(r.Peers)))
	for _, p := range r.Peers {
		buf = appendUint64(buf, p)
	}
	buf = appendBytesField(buf, r.StartKey)
	buf = appendBytesField(buf, r.EndKey)
	buf = appendUint64(buf, r.SizeBytes)
	// PreferredZone is appended after SizeBytes for Phase 6. Older readers
	// stop at SizeBytes and ignore the trailer; the zone only affects new
	// placement decisions.
	buf = appendBytesField(buf, []byte(r.PreferredZone))
	return buf
}

func decodeRegion(b []byte) (Region, bool) {
	var r Region
	if len(b) < 26 { // 3*8 + 2
		return r, false
	}
	r.ID = binary.BigEndian.Uint64(b[0:8])
	r.Epoch = binary.BigEndian.Uint64(b[8:16])
	r.Leader = binary.BigEndian.Uint64(b[16:24])
	nPeers := binary.BigEndian.Uint16(b[24:26])
	b = b[26:]
	for i := 0; i < int(nPeers); i++ {
		if len(b) < 8 {
			return r, false
		}
		r.Peers = append(r.Peers, binary.BigEndian.Uint64(b[0:8]))
		b = b[8:]
	}
	var ok bool
	b, r.StartKey, ok = readBytesField(b)
	if !ok {
		return r, false
	}
	b, r.EndKey, ok = readBytesField(b)
	if !ok {
		return r, false
	}
	// SizeBytes is appended after EndKey for Phase 4. Older encoded regions
	// without this field are still valid — they decode as SizeBytes=0.
	if len(b) >= 8 {
		r.SizeBytes = binary.BigEndian.Uint64(b[0:8])
		b = b[8:]
	}
	// PreferredZone is appended after SizeBytes for Phase 6. Older encoded
	// regions without this trailer are still valid — they decode as
	// PreferredZone="". A trailer whose field length cannot be decoded is
	// corrupt and must be rejected, not silently treated as an empty zone.
	if len(b) >= 2 {
		if _, z, ok := readBytesField(b); ok {
			r.PreferredZone = string(z)
		} else {
			return r, false
		}
	}
	return r, true
}

// maxRegionWireField is the largest length encodable in the uint16 wire format
// used for peer-count and key-byte-length fields.
const maxRegionWireField = 65535

// regionExceedsWireLimit reports whether r cannot be encoded without truncating
// its peer count or key fields: the wire format stores these lengths as uint16,
// so a larger value would silently corrupt the encoding.
func regionExceedsWireLimit(r Region) bool {
	return len(r.Peers) > maxRegionWireField ||
		len(r.StartKey) > maxRegionWireField ||
		len(r.EndKey) > maxRegionWireField ||
		len(r.PreferredZone) > maxRegionWireField
}

func appendUint64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

func appendUint16(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

func appendBytesField(buf, v []byte) []byte {
	buf = appendUint16(buf, uint16(len(v)))
	return append(buf, v...)
}

func readBytesField(buf []byte) ([]byte, []byte, bool) {
	if len(buf) < 2 {
		return nil, nil, false
	}
	n := int(binary.BigEndian.Uint16(buf[0:2]))
	buf = buf[2:]
	if len(buf) < n {
		return nil, nil, false
	}
	return buf[n:], buf[:n], true
}
