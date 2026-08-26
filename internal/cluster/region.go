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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Region is the authoritative metadata for one key-range Raft group. It is
// serialized and stored in etcd under /tellstone/regions/<id>.
type Region struct {
	ID       uint64
	StartKey []byte
	EndKey   []byte
	Peers    []uint64 // node IDs of the region's Raft members
	Leader   uint64   // current leader node ID (0 until first leader claims it)
	Epoch    uint64   // bumped on every split/move/leadership change
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
	lp     LeadershipProvider
	peers  []uint64
	rt     *RoutingTable
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
	val := encodeRegion(Region{
		ID:       1,
		StartKey: []byte{},
		EndKey:   nil,
		Peers:    m.peers,
		Leader:   0,
		Epoch:    1,
	})
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

// Run seeds the routing table from etcd, then watches for changes until ctx is
// cancelled. The leadership loop runs concurrently.
func (m *RegionManager) Run(ctx context.Context) error {
	resp, err := m.cli.Get(ctx, regionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		if r, ok := decodeRegion(kv.Value); ok {
			m.rt.Update(r)
		}
	}
	go m.leadershipLoop(ctx)

	rev := resp.Header.Revision + 1
	ch := m.cli.Watch(ctx, regionKeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case wresp, ok := <-ch:
			if !ok {
				return nil
			}
			if werr := wresp.Err(); werr != nil {
				continue
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
}

// leadershipLoop, on the Raft leader, ensures region 1's Leader field points at
// this node. It only writes when the field is stale, so non-leaders are silent
// and the single leader wins quickly after an election.
func (m *RegionManager) leadershipLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.lp.IsLeader() {
				continue
			}
			cur, err := m.getRegion(ctx, 1)
			if err != nil {
				continue
			}
			if cur != nil && cur.Leader == m.nodeID {
				continue
			}
			epoch := uint64(1)
			if cur != nil {
				epoch = cur.Epoch + 1
			}
			nr := Region{
				ID:       1,
				StartKey: []byte{},
				EndKey:   nil,
				Peers:    m.peers,
				Leader:   m.nodeID,
				Epoch:    epoch,
			}
			if _, err := m.cli.Put(ctx, regionKey(1), string(encodeRegion(nr))); err != nil {
				continue
			}
		}
	}
}

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

// --- serialization (compact binary, no protobuf) ---

func encodeRegion(r Region) []byte {
	buf := make([]byte, 0, 32+len(r.StartKey)+len(r.EndKey))
	buf = appendUint64(buf, r.ID)
	buf = appendUint64(buf, r.Epoch)
	buf = appendUint64(buf, r.Leader)
	buf = appendUint16(buf, uint16(len(r.Peers)))
	for _, p := range r.Peers {
		buf = appendUint64(buf, p)
	}
	buf = appendBytesField(buf, r.StartKey)
	buf = appendBytesField(buf, r.EndKey)
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
	return r, true
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
