# ADR-010: Phase 2 PD + TSO Implementation Decisions

Status: Accepted
Date: 2026-08-23

## Context

Phase 2 of the multi-cluster architecture adds the Placement Driver (PD)
and Timestamp Oracle (TSO) on top of the Phase 1 Raft regions
(ADR-008). Before implementation, five areas required explicit
decisions: the PD substrate, the phase scope, how deeply timestamps
integrate with the write path, transport constraints, and deployment
topology.

## Decisions

### 1. PD Substrate: Embedded etcd

**Decision:** Embed a real etcd server in every Tellstone process that
participates in the PD, exactly as prescribed by ADR-001. We do not
build a bespoke consensus layer for PD metadata.

**Rationale:** etcd provides three primitives phase 2 needs — replicated
KV (timestamp watermark), failure-detected leader election, and cluster
membership — all battle-tested at Kubernetes scale. Self-developing
these on top of our region Raft groups would duplicate years of edge-case
handling (discovery, snapshotting, defragmentation, auth) for zero
product differentiation. We already depend on `go.etcd.io/raft/v3`; the
server dependency extends the same ecosystem.

**Dependency tradeoff (AGENTS.md stdlib-first rule):** Accepted
deliberately. Cost: larger binary, ~50MB RSS per embedded member,
gRPC underneath the control plane. Benefit: correctness and operations
we would otherwise own. The dependency is justified in the PR carrying
the go.mod change with measured build/RAM deltas.

### 2. Scope: Membership + Election + TSO Only

**Decision:** The phase 2 PD tracks cluster membership, elects a PD
leader, and allocates timestamps. Nothing else.

Region metadata, routing tables (phase 3), splitting (phase 4), load
balancing, and geo placement (phase 6) remain out of scope. The etcd
key space is namespaced (`/tellstone/…`) so later phases extend without
migration.

### 3. TSO Integration Depth: Standalone Service

**Decision:** The write path is untouched in this phase. SET/DEL
proposals continue through Raft exactly as phase 1 built them. The TSO
exists as a correct, tested service whose timestamps are consumed only
by tests and benchmarks until the transaction phases (12+) wire them
into Raft entries and MVCC snapshots.

**Rationale:** RESP compatibility is scheduled for removal (ADR-007)
and cross-region transactions need globally ordered versions; building
the ordering source now, decoupled from the codec we just stabilized,
lets the integration land once instead of twice.

### 4. Transport Policy: No gRPC on Any Client-Facing or Hot Path

**Decision:** gRPC appears only inside the embedded-etcd control plane.
Two hard constraints:

- **Client → server data path:** binary protocol and RESP over plain
  TCP. No gRPC, unchanged.
- **Per-write hot path:** timestamp allocation is a lock-free local
  pool increment — no network call, zero heap allocations. gRPC-based
  traffic (election heartbeats, pool refills ≈ one request per node per
  ~30 s) is confined to the control plane and never sits between a
  client request and its response.

**Rationale:** Performance goal. The refill channel runs four orders of
magnitude less often than the allocation path; optimizing it with
custom framing would buy nothing measurable while bypassing etcd's
supported client contract.

### 5. Deployment Modes and Flags

**Decision:** Single binary, role selected by flag:

```text
--node-role=hybrid   # default: data shards + embedded PD member
--node-role=pd       # PD member only, no data shards
--node-role=data     # data shards only, connects external PD via --pd-addr
```

`--node-role=data` requires `--pd-addr`. This preserves ADR-001's
escape hatch for large deployments while keeping the default
three-command bootstrap of the multi-cluster plan.

### 6. Embedded etcd Addressing

**Decision:** Deterministic derivation from the existing data address;
explicit override flags for special topologies:

| Channel | Derived from | Rule | Override |
|---------|--------------|------|----------|
| Data (existing) | `--addr` | as configured | — |
| etcd client | data addr | data port + 10000 | `--pd-client-addr` |
| etcd peer | data addr | data port + 20000 | `--pd-peer-addr` |

Example: `--addr node1:9988` → client `node1:19988`, peer
`node1:29988`. Collisions are a validation error at startup.

### 7. Failure Semantics

**Decision:** As specified in ADR-003: pools ride out PD outages
(default 30 s headroom), exhaustion blocks new timestamps
(`ErrTSOExhausted`), stale reads unaffected, automatic recovery on PD
return. No deviation.

## Consequences

- go.mod gains `go.etcd.io/etcd/server/v3`, `client/v3`, and transitive
  gRPC — largest dependency jump in the project's history; measured and
  recorded in the implementation PR.
- Every hybrid/pd node runs an etcd member: RAM and heartbeat overhead
  accepted per ADR-001 mitigations (clusters ≤ 50 nodes; `--pd-addr`
  beyond).
- The TSO API surface shipped in this phase is internal
  (`internal/cluster/tso.go`) — public exposure waits for the phases
  that consume timestamps.

## References

- ADR-001: Embedded Placement Driver (substrate, escape hatch)
- ADR-003: TSO Graceful Degradation (pools, failure timeline)
- ADR-008: Phase 1 Implementation Decisions (Raft foundation)
- `docs/PHASE2-PD-TSO-DESIGN.md`: detailed design and status tracking
