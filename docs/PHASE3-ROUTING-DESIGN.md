# Phase 3: Routing Table + Forwarding (Design & Decisions)

Status: Accepted
Date: 2026-08-26
Branch: `feat/raft-phase-3`

## Context

Phase 1 (`raft cluster mode`, #55) and Phase 2 (`embedded PD + TSO`, #56) are
merged. Today there is **one Raft group over the whole keyspace**. A non-leader
node rejects writes (`server/cluster.go` returns `ErrNotLeader`); reads read the
local store. There is no routing table and no forwarding.

Phase 3 makes the cluster **read/write-anywhere**: any node serves any key.
This doc records the decisions so they are not re-litigated, then the build
plan. Source of truth: `docs/MULTI-CLUSTER-PLAN.md` (Phase 3) and `ADR-002` /
`ADR-004`. (ROADMAP.md is intentionally not used — it describes an unrelated
Security & Transport phase.)

## Decisions (locked)

### D1 — Routing distribution: etcd Watch, not a gRPC push stream
Region metadata is stored in the embedded etcd KV (`/tellstone/regions/<id>`).
Every node **`Watch`es** that prefix and refreshes its local `RoutingTable`.
This reuses the PD substrate shipped in Phase 2 instead of building a custom
gRPC streaming push service. The doc's "PD pushes via gRPC stream" is deferred
to Phase 5 (pipelines) if ever wanted.

### D2 — Read Anywhere via Raft `ReadIndex`; writes forwarded to leader
- **Leader** serves linearizable reads from its local applied state.
- **Followers** serve reads locally too, but only after a `ReadIndex` round
  trip proves their applied index has caught up to the read's committed index.
  No read is ever forwarded over the network (every node already holds the full
  replicated dataset for the single default region).
- **Writes** must reach the region leader (Raft only proposes from the leader).
  A non-leader node **forwards** the write to the leader; the leader applies it
  through Raft and replies. This is symmetric, so "write anywhere" is already
  satisfied by the same path.

### D3 — No protobuf / gRPC `ClusterService` for forwarding
Forwarding reuses the **existing `network.Transport`** (`internal/cluster/network`,
a custom binary-framed TCP transport). We add application-level message types
(`MsgForwardWrite` / `MsgForwardRead` / `MsgForwardResp`) carried as
`raftpb.Message` values with out-of-range type codes, intercepted in
`Node.handleMessage` *before* `raftNode.Step`. No protobuf schema, no codegen,
no new client library. (A dedicated internal app-transport reusing `codec.go`
remains a later option, not required here.)

### D4 — Single default region to start
Phase 3 introduces the Region/routing machinery with **one** region covering
`["", "")` = the current Raft group. True multi-region splitting is Phase 4.
Phase 3 ships the structure so Phase 4 is a small addition.

### D5 — TSO stays off the write path
Writes go through Raft exactly as today. TSO pre-allocated pools exist but are
not wired into the write path yet (deferred to the SQL/MVCC phase per ADR-010).

## Architecture

```text
             ┌─────────────┐  etcd Watch (/tellstone/regions/*)  ┌──────────────┐
            │   PD etcd   │ ───────────────────────────────────▶│ RoutingTable │
            │  (region    │                                      │  (per node)  │
            │  metadata)  │ ◀── RegionManager (leader rewrites  │              │
            └─────────────┘      leader field on leadership)    └──────┬───────┘
                                                                      │ Find(key)
                                                                      ▼
   client ──▶ Node A ──(not leader)──▶ forward MsgForwardWrite ──▶ Node C (leader)
                  │                                                      │ ProposeAndWait
                  │ (follower read) ReadIndex ─▶ local store ◀──────────┘ applied to all
```

- `RegionManager` runs on every node, holds a `clientv3.Client` to the PD, and:
  - bootstraps the default region (if absent) from the raft peer list;
  - `Watch`es region metadata and feeds `RoutingTable.Update`;
  - if this node is the raft leader, keeps the region's `Leader` field = self
    (epoch-bumped on change) so all nodes converge.
- `Node` gains `LeaderID()`, `AppliedIndex()`, `PeerAddr(id)`, `ReadIndex` /
  `LinearizableRead`, and `ForwardWrite` / `ForwardRead` (pending-map + reply).
- `clusterStore` (`server/cluster.go`): `Set`/`Delete` → if local node leads the
  key's region, `ProposeAndWait` (current path); else `Node.ForwardWrite` to the
  leader. `Get` → `LinearizableRead` (leader or follower) then read local store.

## Deliverables

| # | File | Responsibility |
|---|------|----------------|
| 1 | `internal/cluster/routing.go` | `RegionRoute`, `RoutingTable` (sorted slice, binary-search `Find`, epoch-guarded `Update`). |
| 2 | `internal/cluster/routing_test.go` | Unit tests: `Find` boundaries, stale-epoch rejection. |
| 3 | `internal/cluster/region.go` | `Region` type, binary (de)serialization, `RegionManager` (bootstrap + `Watch` + leadership keep-alive). |
| 4 | `internal/cluster/region_test.go` | Integration with embedded etcd: bootstrap, watch converges, leader rewrite. |
| 5 | `internal/cluster/node.go` | `LeaderID`, `AppliedIndex`, `PeerAddr`, `ReadIndex`/`LinearizableRead`, forward msg types + `ForwardWrite`/`ForwardRead` + `handleMessage` routing + pending map. |
| 6 | `internal/cluster/pdnode.go` | Expose `Client()` so `RegionManager` gets a `clientv3.Client`. |
| 7 | `server/cluster.go` | `clusterStore` uses `RoutingTable` + `Node.Forward` (writes) / `LinearizableRead` (reads). |
| 8 | `server/server.go` | Wire `RoutingTable` + `RegionManager` in `initCluster`. |
| 9 | `internal/cluster/manual_test.go` | Gated `TestManualRouting`: 3-node, write on non-leader, read on another, kill leader → converge. |

## Verification

- **Unit:** `RoutingTable.Find` + stale-epoch; region (de)serialization.
- **Integration:** 3-node, kill leader → `Watch` converges, follower becomes
  leader, forwarded writes/reads keep working.
- **Manual (gated `TELLSTONE_MANUAL_TEST=1`):** read/write-anywhere across nodes.

## Out of scope (deferred)

- Multi-region / splitting (Phase 4).
- TSO on the write path (SQL/MVCC phase).
- Protobuf/gRPC `ClusterService` (D3 — not needed).
