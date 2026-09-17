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
// concurrent writers cannot roll rules back. Version allocation and the write
// happen in a read-modify-write transaction comparing the observed revision of
// the policy key; when another writer lands first the compare fails and the
// operation retries from the updated value, so no two writers reuse a version.
func SetGeoPolicy(ctx context.Context, cli *clientv3.Client, p GeoPolicy) error {
	for {
		resp, err := cli.Get(ctx, geoPolicyKey)
		if err != nil {
			return err
		}
		cur := DefaultGeoPolicy()
		var modRev int64
		if len(resp.Kvs) > 0 {
			if decoded, ok := decodeGeoPolicy(resp.Kvs[0].Value); ok {
				cur = decoded
			}
			modRev = resp.Kvs[0].ModRevision
		}
		p.Version = cur.Version + 1
		enc, err := encodeGeoPolicy(p)
		if err != nil {
			return err
		}
		tresp, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(geoPolicyKey), "=", modRev)).
			Then(clientv3.OpPut(geoPolicyKey, string(enc))).
			Commit()
		if err != nil {
			return err
		}
		if tresp.Succeeded {
			return nil
		}
		// Another writer committed first; re-read and retry so the version
		// keeps incrementing instead of being reused.
	}
}

// BootstrapGeoPolicy ensures a policy exists in etcd, creating the default
// (everything global) when the key is absent. Safe to call concurrently from
// every node; the first writer wins. Returns the effective policy. The
// presence of the key is decided by a raw-key read, not by whether the stored
// rules are non-empty: an existing policy with zero rules is preserved instead
// of being mistaken for an absent key.
func BootstrapGeoPolicy(ctx context.Context, cli *clientv3.Client) (GeoPolicy, error) {
	// Existence check: a stored policy (even one with zero rules) is final.
	resp, err := cli.Get(ctx, geoPolicyKey)
	if err != nil {
		return GeoPolicy{}, err
	}
	if len(resp.Kvs) > 0 {
		p, ok := decodeGeoPolicy(resp.Kvs[0].Value)
		if !ok {
			return GeoPolicy{}, fmt.Errorf("cluster: stored geo policy is corrupt")
		}
		return p, nil
	}
	def := DefaultGeoPolicy()
	enc, err := encodeGeoPolicy(def)
	if err != nil {
		return GeoPolicy{}, err
	}
	txn := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(geoPolicyKey), "=", 0)).
		Then(clientv3.OpPut(geoPolicyKey, string(enc)))
	tresp, err := txn.Commit()
	if err != nil {
		return GeoPolicy{}, err
	}
	if !tresp.Succeeded {
		// Another node bootstrapped first; reuse whatever it stored.
		return GetGeoPolicy(ctx, cli)
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
// the local caches, returning the next revision to watch from for each source
// individually. Separate revisions are required: node registrations and the
// policy are read in two Gets, and a node write landing between them must not
// be skipped by the node watcher just because the policy Get returned a newer
// revision (the classic etcd get-then-watch gap).
func (m *GeoManager) seed(ctx context.Context) (nodeRev, policyRev int64, err error) {
	// Node registry.
	nresp, err := m.cli.Get(ctx, nodesKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, 0, err
	}
	// Rebuild the registry from the full read: nodes omitted from the
	// response are gone and must be dropped, not merged on top of the cache.
	nodes := make([]NodeInfo, 0, len(nresp.Kvs))
	for _, kv := range nresp.Kvs {
		if n, ok := decodeNodeInfo(kv.Value); ok {
			nodes = append(nodes, n)
		}
	}
	m.zones.Replace(nodes)
	// Geo policy (single key).
	presp, err := m.cli.Get(ctx, geoPolicyKey)
	if err != nil {
		return 0, 0, err
	}
	// Replace the cached policy; when no policy has been stored the default
	// (everything global) applies.
	m.policyMu.Lock()
	m.policy = DefaultGeoPolicy()
	if len(presp.Kvs) > 0 {
		if p, ok := decodeGeoPolicy(presp.Kvs[0].Value); ok {
			m.policy = p
		}
	}
	m.policyMu.Unlock()
	return nresp.Header.Revision + 1, presp.Header.Revision + 1, nil
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
	nodeRev, policyRev, err := m.seed(ctx)
	if err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Fan in both watch sources into one channel. Each watcher starts at
		// the revision of its own seed Get so a write landing between the two
		// Gets is never skipped.
		nodeCh := m.cli.Watch(ctx, nodesKeyPrefix, clientv3.WithPrefix(), clientv3.WithRev(nodeRev), clientv3.WithPrevKV())
		policyCh := m.cli.Watch(ctx, geoPolicyKey, clientv3.WithRev(policyRev))
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
		var rerr error
		nodeRev, policyRev, rerr = m.seed(ctx)
		if rerr != nil {
			return rerr
		}
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
