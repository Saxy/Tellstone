/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: geo_manager.go
Description: Geo registry lifecycle (Phase 6). The GeoManager owns one node's
view of the geo world: it registers the local node's zone in etcd, watches the
node registry (/tellstone/nodes/…) so every node can resolve peer zones, and
watches the operator-defined GeoPolicy (/tellstone/geo/rules) so placement and
routing decisions always act on the current rules. It follows the same
etcd-Watch delivery pattern as RegionManager (D1, ADR-003).

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// GeoManager owns the node's zone registration, the peer zone registry, and
// the cached geo policy. Safe to run one per node.
type GeoManager struct {
	cli    *clientv3.Client
	nodeID uint64
	zones  *NodeZones
	// policyMu guards policy; read paths (routing) take RLock.
	policyMu sync.RWMutex
	policy   GeoPolicy
}

// GeoPolicyKey returns the etcd key under which the operator-defined geo
// policy is stored. Exported for the PD and diagnostics.
func GeoPolicyKey() string { return geoPolicyKey }

// NodesKeyPrefix returns the etcd key prefix under which nodes register
// their metadata. Exported for the PD and diagnostics.
func NodesKeyPrefix() string { return nodesKeyPrefix }

// GetGeoPolicy reads the operator policy from etcd. When no policy has been
// stored, it returns the default policy (everything global). Used by the PD.
func GetGeoPolicy(ctx context.Context, cli *clientv3.Client) (GeoPolicy, error) {
	resp, err := cli.Get(ctx, geoPolicyKey)
	if err != nil {
		return GeoPolicy{}, err
	}
	if len(resp.Kvs) == 0 {
		return DefaultGeoPolicy(), nil
	}
	p, ok := decodeGeoPolicy(resp.Kvs[0].Value)
	if !ok {
		return GeoPolicy{}, fmt.Errorf("cluster: stored geo policy is corrupt")
	}
	return p, nil
}

// SetGeoPolicy writes the operator policy to etcd, bumping its version so
// every node's watcher converges on the new rules. The version is taken from
// the current stored policy (or the default when none exists) plus one, so
// concurrent writers cannot roll rules back.
func SetGeoPolicy(ctx context.Context, cli *clientv3.Client, p GeoPolicy) error {
	cur, err := GetGeoPolicy(ctx, cli)
	if err != nil {
		return err
	}
	p.Version = cur.Version + 1
	_, err = cli.Put(ctx, geoPolicyKey, string(encodeGeoPolicy(p)))
	return err
}

// BootstrapGeoPolicy ensures a policy exists in etcd, creating the default
// (everything global) when the key is absent. Safe to call concurrently from
// every node; the first writer wins. Returns the effective policy.
func BootstrapGeoPolicy(ctx context.Context, cli *clientv3.Client) (GeoPolicy, error) {
	// Fast path: a policy already exists.
	if p, err := GetGeoPolicy(ctx, cli); err != nil {
		return GeoPolicy{}, err
	} else if len(p.Rules) > 0 {
		return p, nil
	}
	def := DefaultGeoPolicy()
	txn := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(geoPolicyKey), "=", 0)).
		Then(clientv3.OpPut(geoPolicyKey, string(encodeGeoPolicy(def))))
	if _, err := txn.Commit(); err != nil {
		return GeoPolicy{}, err
	}
	return def, nil
}

// NewGeoManager creates a geo manager bound to the PD etcd client. policy
// starts at the default (everything global) until the watcher applies the
// operator policy.
func NewGeoManager(cli *clientv3.Client, nodeID uint64) *GeoManager {
	return &GeoManager{
		cli:    cli,
		nodeID: nodeID,
		zones:  NewNodeZones(),
		policy: DefaultGeoPolicy(),
	}
}

// Zones returns the peer zone registry fed by the node watcher.
func (m *GeoManager) Zones() *NodeZones { return m.zones }

// Policy returns the current cached geo policy.
func (m *GeoManager) Policy() GeoPolicy {
	m.policyMu.RLock()
	defer m.policyMu.RUnlock()
	return m.policy
}

// RegisterNode writes the local node's zone into etcd so the PD and all
// peers can resolve it. Addr is the node's data address. Safe to call from
// every node; the write is idempotent.
func (m *GeoManager) RegisterNode(ctx context.Context, zone, addr string) error {
	n := NodeInfo{ID: m.nodeID, Zone: zone, Addr: addr}
	_, err := m.cli.Put(ctx, nodesKey(m.nodeID), string(encodeNodeInfo(n)))
	if err == nil {
		m.zones.Update(n)
	}
	return err
}

// seed reads the full node registry and geo policy from etcd and populates
// the local caches, returning the next revision to watch from.
func (m *GeoManager) seed(ctx context.Context) (int64, error) {
	// Node registry.
	nresp, err := m.cli.Get(ctx, nodesKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	for _, kv := range nresp.Kvs {
		if n, ok := decodeNodeInfo(kv.Value); ok {
			m.zones.Update(n)
		}
	}
	// Geo policy (single key).
	presp, err := m.cli.Get(ctx, geoPolicyKey)
	if err != nil {
		return 0, err
	}
	for _, kv := range presp.Kvs {
		if p, ok := decodeGeoPolicy(kv.Value); ok {
			m.applyPolicy(p)
		}
	}
	rev := nresp.Header.Revision
	if presp.Header.Revision > rev {
		rev = presp.Header.Revision
	}
	return rev + 1, nil
}

// applyPolicy stores a policy if it is newer than the current cache.
func (m *GeoManager) applyPolicy(p GeoPolicy) {
	m.policyMu.Lock()
	defer m.policyMu.Unlock()
	if p.Version > m.policy.Version {
		m.policy = p
	}
}

// Run seeds the caches from etcd, then watches the node registry and geo
// policy until ctx is cancelled. Two watchers run on the same channel fan-in:
// one on /tellstone/nodes/…, one on the single /tellstone/geo/rules key.
// Watches are recreated on interruption so no updates are missed
// (compaction-safe, same as RegionManager).
func (m *GeoManager) Run(ctx context.Context) error {
	rev, err := m.seed(ctx)
	if err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Fan in both watch sources into one channel.
		nodeCh := m.cli.Watch(ctx, nodesKeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
		policyCh := m.cli.Watch(ctx, geoPolicyKey, clientv3.WithRev(rev))
		recreate := false
		for !recreate {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case wresp, ok := <-nodeCh:
				if !ok {
					recreate = true
					break
				}
				if werr := wresp.Err(); werr != nil {
					recreate = true
					break
				}
				for _, ev := range wresp.Events {
					m.applyNodeEvent(ev)
				}
			case wresp, ok := <-policyCh:
				if !ok {
					recreate = true
					break
				}
				if werr := wresp.Err(); werr != nil {
					recreate = true
					break
				}
				for _, ev := range wresp.Events {
					m.applyPolicyEvent(ev)
				}
			}
		}
		newRev, rerr := m.seed(ctx)
		if rerr != nil {
			return rerr
		}
		rev = newRev
	}
}

// applyNodeEvent applies one node-registry watch event.
func (m *GeoManager) applyNodeEvent(ev *clientv3.Event) {
	if ev.Type == clientv3.EventTypeDelete {
		// etcd delete events carry the previous value in PrevKv.
		if ev.PrevKv != nil {
			if n, ok := decodeNodeInfo(ev.PrevKv.Value); ok {
				m.zones.Remove(n.ID)
			}
		}
		return
	}
	if n, ok := decodeNodeInfo(ev.Kv.Value); ok {
		m.zones.Update(n)
	}
}

// applyPolicyEvent applies one geo-policy watch event. Deletions reset the
// cache to the default (everything global).
func (m *GeoManager) applyPolicyEvent(ev *clientv3.Event) {
	if ev.Type == clientv3.EventTypeDelete {
		m.policyMu.Lock()
		m.policy = DefaultGeoPolicy()
		m.policyMu.Unlock()
		return
	}
	if p, ok := decodeGeoPolicy(ev.Kv.Value); ok {
		m.applyPolicy(p)
	}
}