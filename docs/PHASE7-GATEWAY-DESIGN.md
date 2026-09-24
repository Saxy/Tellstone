# Phase 7: Cross-Cluster Gateway (Design & Decisions)

Status: Accepted
Date: 2026-09-22
Branch: `feat/raft-phase-7`

## Context

Phases 1-6 (Raft regions, embedded PD + TSO, routing/forwarding, per-region
splitting, pipelined networking, geo routing) are merged. A single cluster is
self-contained: one keyspace replicated within one Raft membership, one
embedded PD, one TSO pool.

Phase 7 federates independent clusters into a worldwide deployment. A key has
a **home cluster**; clients may connect to *any* cluster and still read/write
that key. This doc records the locked decisions (ADR-011), then the build
plan. Source of truth: `docs/MULTI-CLUSTER-PLAN.md` (Phase 7). (ROADMAP.md is
intentionally not used — it describes an unrelated Security & Transport phase.)

## Decisions (locked)

### D1 — Cluster identity: `--cluster-id`

Every node in a federation declares the ID of the cluster it belongs to
(`--cluster-id`, uint64). Raft node IDs remain unique *within* a cluster and
say nothing across clusters. A gateway is addressed as its **cluster ID** —
the one thing all member nodes of that cluster share.

### D2 — Dedicated gateway transport (new port), not shared with Raft

Each federation-enabled node runs a **separate `network.Transport` + platform
"Pipeline"** on its own `--gateway-addr` port. Rationale:

- Cross-cluster peer addressing keys on **cluster ID**, which would collide
  with intra-cluster raft node IDs on a shared transport.
- Cross-cluster traffic is purely application-level (forwarded ops); no raft
  frames ever cross a gateway connection, so demuxing by region group ID is
  meaningless there.
- The existing codec, batching sender, keepalive detection, redial backoff,
  and request-ID response matching are reused verbatim — the gateway
  transport differs only in what it addresses.

One outbound gateway connection per *configured remote cluster* (keyed by the
remote cluster ID). `--federation-clusters "<clusterid>@<gateway-addr>,…"`.
**Symmetric configuration is required**: each cluster lists its peers' gateway
addresses, so responses can be routed back (`Pipeline.respond` dials `from`,
which is the sender's cluster ID).

### D3 — Federation policy in each PD etcd, longest-prefix match

A `FederationPolicy` maps key prefixes to home clusters:

```go
type FederationRule struct {
    Prefix  string  // key prefix ("" = catch-all)
    Cluster uint64  // home cluster ID
}
type FederationPolicy struct {
    Version uint64
    Rules   []FederationRule
}
```

Stored in the local PD etcd at `/tellstone/federation/rules`, watched by a
`FederationManager` on every node (same etcd-Watch delivery pattern as
`RegionManager` / `GeoManager`, D1 of Phase 3). Longest prefix wins; a key
with no matching rule falls back to the **local cluster**. The bootstrap
policy is a single catch-all rule pinning everything to the local cluster
(unfederated behavior is a no-op).

The policy is stored per-cluster; operators must keep federated clusters
consistent. Cross-cluster policy distribution through the gateways themselves
is future work.

### D4 — New pipeline op kinds (data-returning reads)

The Phase 5 pipeline carries no read op. Phase 7 adds:

- `OpXWrite` (6) — forward a write (single or chunk chain) to the home cluster.
  Payload reuses the existing chunk-chain encoding: `[4B count][per: 4B
  len][data]` prefixed by the key.
- `OpXRead` (7) — forward a linearizable read. Payload: `[2B keylen][key]`.
- `OpXResp` (8) — response for both. Reuses `encodePipeResp` (flags byte:
  error vs. data); read data carries a `[1B present][value]` body so "missing"
  is distinguishable from an empty value.

The receiving gateway decodes the key, then runs its **own cluster write/read
body** (`clusterStore.routeWrite` / `clusterStore.Get`) — forwarding is
one hop to the home cluster's already-proven read/write path.

### D5 — TSO: deferred to Phase 12 (unchanged write-path policy)

ADR-010 keeps the TSO off the write path until the MVCC/transaction phases;
Phases 3-6 all respect that. An x-cluster write is therefore ordered by the
**home cluster's Raft log** (the forwarded op is proposed/committed there),
which is the same ordering guarantee intra-cluster writes already have. The
"cross-cluster writes require TSO from the home cluster's PD" line in the
multi-cluster plan is honored structurally — the write lands in the home
cluster and is ordered by its consensus — but no timestamp is stamped into
the entry yet. Wire the home TSO when Phase 12 lands.

### D6 — Failure semantics

- **Gateway/down remote cluster:** x-cluster writes fail with a
  `CLUSTERDOWN`-style error (no buffering); x-cluster reads fail (no stale
  fallback in v1). The transport keepalive drops dead gateway connections and
  redials lazily, so ops resume once the link returns. Intra-cluster traffic
  is never affected.
- **Local gateway nil:** a node without federation configured never forwards;
  keys are all local by the default policy.

## Architecture

```text
 Cluster A (cluster-id=1)                  Cluster B (cluster-id=2)
 ┌─────────────────────────┐               ┌─────────────────────────┐
 │ Node A1  A2  A3          │               │ Node B1  B2  B3          │
 │  └── gateway transport ──┼─── pipeline ──┼── gateway transport ───┘ │
 │  (dedicated pipe, peer=2)│   (batched)   │  (dedicated pipe, peer=1)│
 │   └── intra raft ────────┘               └── intra raft ────────────┘
 └─────────────────────────┘               └─────────────────────────┘

 client ─▶ B2: GET user:eu:alice   (fed policy: prefix "user:eu:" → cluster 1)
   B2: Home(user:eu:alice)=1 ≠ 2 → Gateway.Call(1, OpXRead, key)
   ─────────────────────────────────────────────────────────▶ A3 gateway
   A3: decode key → local Get (LinearizableRead + engine) → [present][value]
   ◀───────────────────────────────────────────────────────── OpXResp
   B2: decode → return value

 client ─▶ A1: SET user:eu:bob ... (home = 1 = local)  → normal intra path
```

## Deliverables

| # | File | Responsibility |
|---|------|----------------|
| 1 | `internal/cluster/federation.go` | `FederationRule`/`FederationPolicy` (binary encode/decode, `Match`), `FederationManager` (bootstrap + etcd Watch, mirrors `GeoManager`). |
| 2 | `internal/cluster/gateway.go` | `Gateway`: dedicated transport+pipeline on `--gateway-addr`, cluster-ID addressing, inbound op executor hook, outbound `Call`, lifecycle. |
| 3 | `internal/cluster/network/pipeline.go` | `OpXWrite`/`OpXRead`/`OpXResp` kinds; gateway handler dispatch. |
| 4 | `internal/cluster/gateway_test.go` + `federation_test.go` | Unit: `Match` boundaries, policy converge (embedded etcd), x-op encode/decode, gateway round-trip over in-process transports. |
| 5 | `config/config.go` | `--cluster-id`, `--gateway-addr`, `--federation-clusters` (+ env `TSD_CLUSTER_ID`/`TSD_GATEWAY_ADDR`/`TSD_FEDERATION_CLUSTERS`) + validation (gateway requires cluster-id; federation entries cannot include the local cluster). |
| 6 | `server/server.go` | Start `FederationManager` + `Gateway` in `initCluster`; wire gateway handler after `clusterStore` construction; shutdown ordering. |
| 7 | `server/cluster.go` | `clusterStore` routes `Get/Set/Delete` through the federation policy first: home ≠ local → gateway; local otherwise. |

## Verification

- **Unit:** federation `Match` + stale-revision policy hydration; x-op
  encoding round-trips; gateway Call replies over two in-process transports.
- **Manual (gated `TELLSTONE_MANUAL_TEST=1`):** `TestManualFederation` boots
  two real 3-node clusters (6 processes). Federation policy pins
  `user:na:`→cluster A and `user:eu:`→cluster B. Steps:
  1. Write `user:eu:alice` through a **B** process → read it back through an
     **A** process (and vice-versa); values consistent.
  2. Intra-cluster keys stay local (no gateway hop) — reads on any node.
  3. Kill one cluster's gateway → x-cluster ops fail cleanly; restart → ops
     resume on the reconnected pipe.

## Known limitations (tracks to a later phase)

- **Cluster-raft member restart (base architecture).** A cluster-raft member
  always boots from an empty in-memory log and must catch up from the live
  quorum. `go.etcd.io/raft/v3` can panic (`tocommit out of range`) when a
  heartbeat carrying the quorum's commit index arrives between a snapshot
  restore and the catch-up appends; the manual test therefore *skip-verifies*
  the gateway-host-restart claim when the restarted node's raft rejoin dies,
  rather than failing the phase. Deterministic member-restart recovery is a
  platform concern (storage-raft persistence/rejoin) to fix separately; the
  Phase 7 gateway plumbing itself (re-bind with `SO_REUSEADDR`, pipeline
  redial, resumed forwarding) is verified on every healthy rejoin.

## Out of scope (deferred)

- TSO on the (x-cluster) write path — Phase 12, per D5.
- Gateway TLS/auth; gateway metrics counters; policy sync *through* gateways;
  multi-gateway per cluster / load balancing; stale remote reads.