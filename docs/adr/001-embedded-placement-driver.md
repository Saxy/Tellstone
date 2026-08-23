# ADR-001: Embedded Placement Driver

Status: Accepted
Date: 2026-08-20

## Context

Tellstone needs a Placement Driver (PD) to manage region metadata, allocate
timestamps (TSO), and coordinate cluster operations (split, rebalance).
Two deployment models were considered:

1. **Separate binary** — PD runs as its own process, data nodes connect
   to it via `--pd-addr`. This is TiKV's model.
2. **Embedded** — every Tellstone node runs an embedded etcd cluster.
   One node is elected PD leader dynamically.

## Decision

**Embedded PD.** Every Tellstone node participates in the etcd cluster.
The PD leader is elected automatically. An optional `--pd-addr` flag
allows data-only nodes to connect to an external PD for large-scale
deployments.

## Consequences

### Positive

- **Single binary deployment.** Operators run N Tellstone instances and
  the cluster forms automatically. No separate PD component to deploy,
  monitor, or scale.
- **No extra infrastructure.** Small-to-medium clusters (3-10 nodes)
  need zero additional components.
- **Automatic failover.** If the PD leader dies, etcd leader election
  picks a new one from surviving nodes. No manual intervention.
- **Escape hatch.** `--pd-addr` allows data-only nodes to connect to an
  external PD when the embedded model no longer scales.

### Negative

- **Resource contention.** Every node runs etcd, consuming ~50MB RAM and
  occasional CPU for Raft heartbeats. In data-heavy nodes, this competes
  with the storage engine.
- **Scaling coupling.** Adding a data node also adds an etcd member. At
  very large scale (100+ nodes), etcd cluster membership becomes
  unwieldy.
- **Blast radius.** A bug in the etcd layer affects all nodes, not just
  PD nodes.

### Mitigations

- Etcd overhead is negligible for clusters under 50 nodes.
- For larger deployments, `--pd-addr` allows separating PD from data
  nodes.
- Etcd is a mature, well-tested dependency (used by Kubernetes, TiKV,
  etc.).

## Alternatives Considered

### Separate PD binary (TiKV model)

Cleaner separation, but requires operators to deploy and manage two
clusters (PD + data). This doubles operational complexity and contradicts
Tellstone's "single binary" philosophy.

### Gossip-based coordination (like Cassandra)

No central authority, but makes global timestamp allocation impossible
without additional protocol complexity. Gossip is eventually consistent,
which conflicts with the linearizability requirement for TSO.

## References

- [TiKV PD architecture](https://docs.pingcap.com/tikv/stable/tikv-architecture#pd-placement-driver)
- [etcd raft library](https://github.com/etcd-io/raft)
