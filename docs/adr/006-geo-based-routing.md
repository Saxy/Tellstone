# ADR-006: Geo-Based Routing

Status: Accepted
Date: 2026-08-20

## Context

Tellstone targets worldwide deployments. A cluster spanning multiple
data centers (us-east, eu-west, ap-south) needs to place data close to
its users. Without geo-awareness, a write to `user:europe:alice` from
us-east-1 might land on a US region leader, adding cross-ocean latency
to every subsequent read.

Two approaches:

1. **Uniform placement** — regions are placed purely by load balancing,
   ignoring geography. Simple, but cross-ocean latency for geo-distributed
   data.
2. **Geo-pinned placement** — regions are pinned to geographic zones via
   policy rules. The PD respects these rules when placing regions and
   choosing leaders.

## Decision

**Geo-pinned placement with policy rules.** Operators define rules that
map key prefixes to geographic zones. The PD places regions and chooses
leaders to satisfy these rules.

### Policy Definition

```go
type GeoPolicy struct {
    Rules []GeoRule
}

type GeoRule struct {
    Prefix   string  // key prefix to match
    Zone     string  // preferred zone (e.g., "eu-west-1")
    Replicas int     // number of replicas (default: 3)
}
```

Example policy:

```yaml
geo:
  rules:
    - prefix: "user:europe:"
      zone: eu-west-1
      replicas: 3
    - prefix: "user:us:"
      zone: us-east-1
      replicas: 3
    - prefix: "global:"
      zone: "*"           # replicated everywhere
      replicas: 5
```

### How It Works

**Region placement.** When the PD places a new region (after split or
rebalance), it checks the geo policy:

1. Find the rule matching the region's key prefix
2. Place region leaders in the preferred zone
3. Place followers in the same zone (preferred) or nearby zones
4. If `zone: "*"` (global), distribute replicas across all zones

**Leader election.** When a region's leader dies, the Raft election
prefers a follower in the same zone as the geo policy. This keeps
leaders close to their data's "home" zone.

**Write forwarding.** A write to `user:europe:alice` from a node in
us-east-1 is forwarded to the region leader in eu-west-1. The routing
table knows the leader's zone. The client pays the cross-ocean latency
for this one write, but subsequent reads from eu-west-1 replicas are
fast.

**Read routing.** For stale/follower reads, the routing table can
prefer replicas in the client's zone. A read from us-east-1 for
`user:europe:alice` is forwarded to the nearest EU replica, not the
US replica.

### Zone Discovery

Nodes self-declare their zone at startup:

```sh
tellstone --cluster-mode --zone eu-west-1 --node-id node3
```

The zone is stored in the node's metadata and reported to the PD. The
PD uses it for region placement decisions.

### Inter-Zone Replication

Replicas in different zones are connected via the same pipeline streams.
Cross-zone traffic is:

- **Raft replication** — log entries are sent to followers in other
  zones. Latency: 50-200ms depending on distance.
- **Routing updates** — PD pushes to all nodes, including other zones.
  Latency: irrelevant (background).
- **TSO allocation** — timestamps come from the PD leader. If the leader
  is in us-east-1 and a node is in eu-west-1, each TSO request adds
  ~100-200ms. Pre-allocation mitigates this.

## Consequences

### Positive

- **Low-latency reads.** Data is close to its users. A read for
  `user:europe:alice` from eu-west-1 hits a local replica.
- **Predictable latency.** Operators know exactly where data lives.
  No surprise cross-ocean hops.
- **Flexible policies.** Rules can be as simple (everything in one
  zone) or as complex (per-prefix pinning) as needed.
- **Global mode.** `zone: "*"` replicates everywhere for data that
  needs worldwide availability.

### Negative

- **Cross-zone writes.** A write from the "wrong" zone pays cross-ocean
  latency. This is unavoidable for geo-pinned data.
- **Policy complexity.** Overlapping rules, policy changes during
  traffic, and zone failures need careful handling.
- **Replication lag.** Cross-zone Raft replication adds 50-200ms to
  commit latency. This affects write throughput for geo-distributed
  regions.

### Mitigations

- **Pre-allocated TSO pools** eliminate cross-zone TSO latency for
  normal writes.
- **Pipeline batching** amortizes cross-zone connection overhead.
- **Zone failure handling:** If a zone goes down, the PD can promote
  followers in other zones to leader, accepting the latency tradeoff
  for availability.
- **Policy versioning:** Policies are versioned. The PD checks policy
  version on each decision to avoid acting on stale rules.

## Alternatives Considered

### Uniform placement (no geo-awareness)

Simpler, but adds cross-ocean latency to all geo-distributed data.
Unacceptable for worldwide deployments.

### Client-side routing

Clients choose which node to connect to based on their zone. This
pushes complexity to clients and doesn't help when the client is in
the wrong zone.

### Automatic zone detection (via latency probing)

Nodes discover their zone by measuring latency to other nodes. This is
fragile (latency varies, network topology changes) and adds complexity.
Explicit zone declaration is simpler and more reliable.

## References

- [TiKV placement rules](https://docs.pingcap.com/tidb/stable/configure-placement-rules)
- [Spanner challenge-response](https://research.google/pubs/pub39966/)
- [CockroachDB zone configs](https://www.cockroachlabs.com/docs/stable/zone-configs.html)
