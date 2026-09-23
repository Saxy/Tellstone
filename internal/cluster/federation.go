/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: federation.go
Description: Federation policy (Phase 7, ADR-011). A federation policy maps
key prefixes to the home cluster that owns them, so a cluster can tell whether
a key is local or must be forwarded through a gateway to another cluster.
The policy lives in the local PD etcd under /tellstone/federation/rules and is
watched by the FederationManager on every node — the same etcd-Watch delivery
pattern as RegionManager (Phase 3 D1) and GeoManager (Phase 6).

Wire format for FederationPolicy (compact binary, no protobuf):

	[8B version][2B rule_count][rule...]
	rule: [2B prefix_len][prefix][8B cluster]

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// federationPolicyKey is the etcd key holding the operator federation policy.
const federationPolicyKey = "/tellstone/federation/rules"

// FederationPolicyKey returns the etcd key under which the operator
// federation policy is stored. Exported for the server and diagnostics.
func FederationPolicyKey() string { return federationPolicyKey }

// FederationRule pins keys with a matching prefix to a home cluster. The
// empty Prefix is a catch-all rule.
type FederationRule struct {
	Prefix  string
	Cluster uint64
}

// FederationPolicy is the operator-defined home-cluster policy. Rules are
// matched by longest prefix; a key with no matching rule is local (falls
// back to this cluster's own ID).
type FederationPolicy struct {
	Version uint64
	Rules   []FederationRule
}

// encodeFederationPolicy serializes a policy into compact binary form. It
// returns an error when the rule count exceeds the uint16 wire field so an
// oversized policy is never persisted with a wrapped count.
func encodeFederationPolicy(p FederationPolicy) ([]byte, error) {
	if len(p.Rules) > math.MaxUint16 {
		return nil, fmt.Errorf("cluster: federation policy has %d rules, wire format supports at most %d", len(p.Rules), math.MaxUint16)
	}
	buf := make([]byte, 0, 16)
	buf = appendUint64(buf, p.Version)
	buf = appendUint16(buf, uint16(len(p.Rules)))
	for _, r := range p.Rules {
		if len(r.Prefix) > math.MaxUint16 {
			return nil, fmt.Errorf("cluster: federation policy rule prefix length %d exceeds the %d-byte wire field", len(r.Prefix), math.MaxUint16)
		}
		buf = appendBytesField(buf, []byte(r.Prefix))
		buf = appendUint64(buf, r.Cluster)
	}
	return buf, nil
}

// decodeFederationPolicy parses a policy value. Returns ok=false on truncated
// data.
func decodeFederationPolicy(b []byte) (FederationPolicy, bool) {
	var p FederationPolicy
	if len(b) < 10 { // 8 + 2
		return p, false
	}
	p.Version = binary.BigEndian.Uint64(b[0:8])
	nRules := binary.BigEndian.Uint16(b[8:10])
	b = b[10:]
	for i := 0; i < int(nRules); i++ {
		var r FederationRule
		var prefix []byte
		var ok bool
		b, prefix, ok = readBytesField(b)
		if !ok {
			return p, false
		}
		r.Prefix = string(prefix)
		if len(b) < 8 {
			return p, false
		}
		r.Cluster = binary.BigEndian.Uint64(b[0:8])
		b = b[8:]
		p.Rules = append(p.Rules, r)
	}
	return p, true
}

// DefaultFederationPolicy returns the bootstrap policy: a single catch-all
// rule pinning every key to the local cluster. Applied when no policy exists
// in etcd, which reproduces pre-federation behavior (everything local).
func DefaultFederationPolicy(local uint64) FederationPolicy {
	return FederationPolicy{
		Version: 1,
		Rules:   []FederationRule{{Prefix: "", Cluster: local}},
	}
}

// Match returns the home cluster for a key and whether any rule matched. It
// is an exact duplicate of the geo policy's longest-prefix semantics so both
// policies behave identically to operators.
func (p FederationPolicy) Match(key []byte) (uint64, bool) {
	bestLen := -1
	bestCluster := uint64(0)
	matched := false
	for _, r := range p.Rules {
		if len(r.Prefix) <= bestLen {
			continue
		}
		if len(r.Prefix) == 0 {
			// Catch-all rule: a candidate, but only wins if nothing longer
			// matches (bestLen guards this below).
			if bestLen < 0 {
				bestLen = 0
				bestCluster = r.Cluster
				matched = true
			}
			continue
		}
		if len(key) >= len(r.Prefix) && string(key[:len(r.Prefix)]) == r.Prefix {
			bestLen = len(r.Prefix)
			bestCluster = r.Cluster
			matched = true
		}
	}
	return bestCluster, matched
}

// Home returns the home cluster for a key. Unmatched keys fall back to the
// given local cluster ID, so a policy with no catch-all rule still keeps
// unknown data local.
func (p FederationPolicy) Home(key []byte, local uint64) uint64 {
	if c, ok := p.Match(key); ok {
		return c
	}
	return local
}

// GetFederationPolicy reads the operator policy from etcd. When no policy has
// been stored, it returns the default policy (everything local) for the given
// local cluster ID. Used by diagnostics.
func GetFederationPolicy(ctx context.Context, cli *clientv3.Client, local uint64) (FederationPolicy, error) {
	resp, err := cli.Get(ctx, federationPolicyKey)
	if err != nil {
		return FederationPolicy{}, err
	}
	if len(resp.Kvs) == 0 {
		return DefaultFederationPolicy(local), nil
	}
	p, ok := decodeFederationPolicy(resp.Kvs[0].Value)
	if !ok {
		return FederationPolicy{}, errors.New("cluster: stored federation policy is corrupt")
	}
	return p, nil
}

// SetFederationPolicy writes the operator policy to etcd, bumping its version
// so every node's watcher converges on the new rules. The version is taken
// from the currently stored policy (or 0 when none exists) plus one, and the
// write is guarded by a read-modify-write transaction comparing the observed
// revision so concurrent writers cannot roll a version back.
func SetFederationPolicy(ctx context.Context, cli *clientv3.Client, p FederationPolicy) error {
	for {
		resp, err := cli.Get(ctx, federationPolicyKey)
		if err != nil {
			return err
		}
		cur := FederationPolicy{}
		var modRev int64
		if len(resp.Kvs) > 0 {
			decoded, ok := decodeFederationPolicy(resp.Kvs[0].Value)
			if !ok {
				return errors.New("cluster: stored federation policy is corrupt; refusing to overwrite")
			}
			cur = decoded
			modRev = resp.Kvs[0].ModRevision
		}
		p.Version = cur.Version + 1
		enc, err := encodeFederationPolicy(p)
		if err != nil {
			return err
		}
		tresp, err := cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(federationPolicyKey), "=", modRev)).
			Then(clientv3.OpPut(federationPolicyKey, string(enc))).
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

// BootstrapFederationPolicy ensures a policy exists in etcd, creating the
// default (everything local) when the key is absent. Safe to call
// concurrently from every node; the first writer wins. Returns the effective
// policy.
func BootstrapFederationPolicy(ctx context.Context, cli *clientv3.Client, local uint64) (FederationPolicy, error) {
	resp, err := cli.Get(ctx, federationPolicyKey)
	if err != nil {
		return FederationPolicy{}, err
	}
	if len(resp.Kvs) > 0 {
		p, ok := decodeFederationPolicy(resp.Kvs[0].Value)
		if !ok {
			return FederationPolicy{}, errors.New("cluster: stored federation policy is corrupt")
		}
		return p, nil
	}
	def := DefaultFederationPolicy(local)
	enc, err := encodeFederationPolicy(def)
	if err != nil {
		return FederationPolicy{}, err
	}
	txn := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(federationPolicyKey), "=", 0)).
		Then(clientv3.OpPut(federationPolicyKey, string(enc)))
	tresp, err := txn.Commit()
	if err != nil {
		return FederationPolicy{}, err
	}
	if !tresp.Succeeded {
		// Another node bootstrapped first; reuse whatever it stored.
		return GetFederationPolicy(ctx, cli, local)
	}
	return def, nil
}

// FederationManager owns one node's view of the federation policy. It caches
// the policy and keeps it converged via an etcd Watch, exactly like
// GeoManager's policy watch but with a single source (no node registry).
type FederationManager struct {
	cli   *clientv3.Client
	local uint64
	// policyMu guards policy; read paths (the store's routing) take RLock.
	policyMu sync.RWMutex
	policy   FederationPolicy
}

// NewFederationManager creates a federation manager bound to the PD etcd
// client. initial seeds the cache — callers pass the bootstrapped (or
// default) policy so the node routes correctly before its first watch
// delivery, and the watcher replaces it on the next version.
func NewFederationManager(cli *clientv3.Client, local uint64, initial FederationPolicy) *FederationManager {
	return &FederationManager{
		cli:    cli,
		local:  local,
		policy: initial,
	}
}

// Policy returns the current cached federation policy.
func (m *FederationManager) Policy() FederationPolicy {
	m.policyMu.RLock()
	defer m.policyMu.RUnlock()
	return m.policy
}

// Home returns the home cluster for a key using the cached policy, falling
// back to this cluster's ID when the key matches no rule.
func (m *FederationManager) Home(key []byte) uint64 {
	m.policyMu.RLock()
	defer m.policyMu.RUnlock()
	return m.policy.Home(key, m.local)
}

// Local returns this manager's local cluster ID.
func (m *FederationManager) Local() uint64 { return m.local }

// applyPolicy stores a policy if it is newer than the current cache.
func (m *FederationManager) applyPolicy(p FederationPolicy) {
	m.policyMu.Lock()
	defer m.policyMu.Unlock()
	if p.Version >= m.policy.Version {
		m.policy = p
	}
}

// seed reads the policy from etcd, replaces the cache (default when absent),
// and returns the next revision to watch from.
func (m *FederationManager) seed(ctx context.Context) (int64, error) {
	resp, err := m.cli.Get(ctx, federationPolicyKey)
	if err != nil {
		return 0, err
	}
	m.policyMu.Lock()
	m.policy = DefaultFederationPolicy(m.local)
	if len(resp.Kvs) > 0 {
		p, ok := decodeFederationPolicy(resp.Kvs[0].Value)
		if !ok {
			m.policyMu.Unlock()
			return 0, errors.New("cluster: stored federation policy is corrupt")
		}
		m.policy = p
	}
	m.policyMu.Unlock()
	return resp.Header.Revision + 1, nil
}

// Run seeds the policy from etcd, then watches it until ctx is cancelled.
// The watch is recreated on interruption (compaction) so no update is missed,
// re-reading the latest policy first — same pattern as RegionManager.Run and
// GeoManager.Run.
func (m *FederationManager) Run(ctx context.Context) error {
	rev, err := m.seed(ctx)
	if err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ch := m.cli.Watch(ctx, federationPolicyKey, clientv3.WithRev(rev))
		recreate := false
		for !recreate {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case wresp, ok := <-ch:
				if !ok {
					recreate = true
					break
				}
				if werr := wresp.Err(); werr != nil {
					recreate = true
					break
				}
				for _, ev := range wresp.Events {
					m.applyEvent(ev)
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

// applyEvent applies one policy watch event. A deletion resets the cache to
// the default (everything local).
func (m *FederationManager) applyEvent(ev *clientv3.Event) {
	if ev.Type == clientv3.EventTypeDelete {
		m.policyMu.Lock()
		m.policy = DefaultFederationPolicy(m.local)
		m.policyMu.Unlock()
		return
	}
	if p, ok := decodeFederationPolicy(ev.Kv.Value); ok {
		m.applyPolicy(p)
	}
}
