# ADR-002: Raft Per Region

Status: Accepted
Date: 2026-08-20

## Context

Tellstone needs consensus for replicated writes. Two models exist:

1. **Single Raft group** — one Raft group replicates the entire keyspace.
   Simple, but doesn't scale (all writes go through one log).
2. **Raft per region** — each region (contiguous key range) is an
   independent Raft group. Regions operate in parallel.

TiKV uses Raft per region. CockroachDB uses Raft per range (equivalent).
Both have proven this scales to thousands of regions per cluster.

## Decision

**Raft per region.** Each region is an independent Raft group with its
own log, its own leader, and its own set of peers. Regions operate
in parallel — writes to different regions don't contend on a single
Raft log.

## Consequences

### Positive

- **Horizontal scaling.** Writes to different regions proceed in
  parallel. Throughput scales with the number of regions.
- **Independent failure domains.** A Raft group failure affects only
  one region, not the entire cluster.
- **Independent rebalancing.** Regions can be moved between nodes
  independently. A hot region can be migrated without affecting others.
- **Clean split/merge.** Region splitting is a local operation — create
  a new Raft group for the upper half, update routing table.
- **Leverages etcd-io/raft.** Battle-tested consensus library. No need
  to implement Raft from scratch.

### Negative

- **Raft group overhead.** Each region maintains its own Raft state
  (log, leader election, heartbeats). With thousands of regions, this
  adds up.
- **Cross-region operations.** Writes that touch multiple regions need
  distributed coordination (future Phase 8 transactions).
- **Routing complexity.** Every node needs a routing table to know which
  region leader to forward to.

### Mitigations

- Raft heartbeats are lightweight (empty messages at configurable
  intervals). 1000 regions × 1 heartbeat/100ms = 10K msgs/sec — well
  within network capacity.
- Cross-region transactions are deferred to Phase 8. Single-region
  operations are the common case.
- Routing table is a sorted array with binary search — O(log n) lookup,
  cached locally, updated by PD push.

## How Raft Integrates with Existing Code

The current Tellstone WAL (`internal/persistence`) is append-only and
local. In cluster mode, the WAL serves a different role:

```
Single-node mode:    Write → WAL → Engine (local)
Cluster mode:        Write → Raft proposal → Raft log (replicated)
                     → Apply → Engine (local)
```

The Raft log replaces the local WAL as the source of truth. The local
WAL still exists for crash recovery of the Raft state machine (replaying
committed entries that weren't applied before the crash).

Each region's Raft group manages its own:
- Raft log (in-memory, backed by disk)
- Committed index tracking
- Leader election
- Log replication to followers

## Alternatives Considered

### Single Raft group

Simpler, but doesn't scale. All writes serialize through one log.
A 100K writes/sec workload on a single Raft group is feasible but
leaves no headroom for growth.

### Multi-Raft (per-shard, not per-region)

The current Tellstone sharding model (per-GOMAXPROCS) could map to
Raft groups. But shards are fixed at startup and don't represent
contiguous key ranges. Regions are more natural for range-based
operations (scan, split, geo-routing).

## References

- [TiKV multi-Raft design](https://docs.pingcap.com/tikv/stable/tikv-architecture#multi-raft)
- [etcd-io/raft documentation](https://github.com/etcd-io/raft)
- [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf)
