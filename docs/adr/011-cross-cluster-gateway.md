# ADR-011: Cross-Cluster Gateway

Status: Accepted
Date: 2026-09-22

## Context

Phase 7 of the multi-cluster plan federates independent Tellstone clusters so
a key can be read/written from any cluster while living in one home cluster.
Prior to implementation, five areas required explicit decisions: cluster
identity, the transport for cross-cluster traffic, how "home cluster" is
determined, how reads/writes cross the boundary, and timestamp ordering.

See [PHASE7-GATEWAY-DESIGN](../PHASE7-GATEWAY-DESIGN.md) for the build plan
and deliverables.

## Decisions

### 1. Cluster Identity: `--cluster-id` (uint64)

Every node declares the ID of its cluster. Raft node IDs remain
cluster-scoped; a gateway is addressed by its cluster ID.

**Rationale:** Node IDs collide between clusters; no existing concept names a
federation. A single uint64 is the minimal namespace that does.

### 2. Transport: dedicated gateway port, reusing the pipeline

Cross-cluster traffic runs on a **separate `network.Transport` + Pipeline**
per federation-enabled node, on `--gateway-addr`. Peer addressing keys the
transport's connection map by **cluster ID**.

**Rationale:** The existing codec, batching, keepalive/reconnect, and
request-ID dispatch are reused verbatim. A separate transport avoids raft
node-ID collisions and keeps cross-cluster traffic purely application-level.
Per-request TCP or gRPC are rejected: we already amortized connection
overhead in Phase 5 and do not spend it twice.

### 3. Home-cluster resolution: federation policy in each PD etcd

`FederationPolicy{Prefix→ClusterID}` stored at `/tellstone/federation/rules`
in each cluster's embedded PD etcd, watched by a per-node `FederationManager`
(same Watch pattern as `RegionManager`/`GeoManager`). Longest prefix wins;
unmatched keys default to the local cluster.

**Rationale:** Consistency with the Phase 3 D1 decision (etcd Watch, not a
custom gRPC push). The default policy makes unfederated deployment a no-op.
Policy reuses the proven, compaction-safe watch plumbing.

### 4. Cross-cluster ops: forwarded, one hop, then the home body

Three new pipeline op kinds:

- `OpXWrite` — write forwarded to the home cluster (single entry or chunk
  chain, reusing the intra-cluster chunk encoding).
- `OpXRead` — linearizable read forwarded to the home cluster.
- `OpXResp` — response carrying data or an error.

The receiving gateway decodes the key and executes the **home cluster's own
`clusterStore` write/read path**. Reads return `[present][value]` so a
missing key differs from an empty value.

**Rationale:** This is "read/write anywhere" extended one level: the client
cluster only adds a gateway hop; correctness lives in the already-shipped
local routing and Raft code. Reinventing cross-cluster consensus was never in
scope; the home cluster's Raft remains the single writer.

### 5. Timestamp ordering: deferred to Phase 12

Cross-cluster writes are ordered by the **home cluster's Raft log** exactly
like intra-cluster writes. No TSO is stamped yet; ADR-010 keeps the write
path TSO-free until the MVCC/transaction phase.

**Rationale:** Consistency with the standing write-path policy (ADR-010 §3).
Stamping timestamps before the consume side exists would change the entry
codec twice. The structural guarantee — the write commits in the home cluster
and is ordered there — already holds.

### 6. Failure semantics

A down gateway/remote cluster fails x-cluster ops with a `CLUSTERDOWN`-style
error; no buffering, no stale reads in v1. The pipeline keepalive drops the
dead connection and redials lazily, so operations resume on availability.

**Rationale:** Predictable behavior for the first federation cut; queuing or
async replication is a later product decision (the multi-cluster plan's
"async replication for eventual consistency" mitigation).

## Consequences

- go.mod: no new dependency — reuses `network.Transport`, `Pipeline`, etcd
  client of the existing PD substrate.
- New flags: `--cluster-id`, `--gateway-addr`, `--federation-clusters`.
- Symmetric `--federation-clusters` on federated clusters is required so
  responses route back by cluster ID.
- The cluster store gains a federation branch before the existing
  routing-table path; unfederated nodes are unaffected (default policy).

## References

- `docs/MULTI-CLUSTER-PLAN.md` (Phase 7)
- ADR-003 (watch/graceful degradation), ADR-005 (pipelined networking),
  ADR-006 (geo policy pattern), ADR-010 (PD/TSO substrate and write-path policy)