# Phase 2 Design: Placement Driver + Timestamp Oracle

Status: Draft — implementation tracking
Date: 2026-08-23
Decisions: [ADR-010](adr/010-phase2-pd-tso-decisions.md)

This document is the engineering reference for phase 2: concrete
formats, key space, pool mechanics, and the live implementation status.
Update the status table as work lands.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│ Tellstone node (--node-role=hybrid, default)               │
│                                                            │
│  ┌──────────┐  ┌─────────────────┐  ┌───────────────────┐ │
│  │ Data      │  │ TSO local pool   │  │ Embedded etcd     │ │
│  │ shards    │  │ atomic cur/end   │◄─┼─ member           │ │
│  │ (Raft)    │  │ Alloc(): lock-free│ │ client :data+10000│ │
│  └──────────┘  └────────▲────────┘  │ peer   :data+20000│ │
│                         │ refill     └─────────┬─────────┘ │
│                         │ ~1 req / ~30s        │ etcd Raft │
└─────────────────────────┼──────────────────────┼───────────┘
                          │                      │
                 control plane (clientv3/gRPC)   │
                          │                      │
                 ┌──────────────────────────────────────┐
                 │ PD member set (embedded etcd among    │
                 │ members; leader reserved for future   │
                 │ singleton duties only). Any member    │
                 │ grants disjoint ranges via            │
                 │ CAS-advanced watermark in etcd KV.    │
                 └──────────────────────────────────────┘
```

Roles:

| `--node-role` | Data shards | Embedded etcd | Needs |
|---------------|-------------|---------------|-------|
| `hybrid` (default) | yes | yes | `--peers`, `--node-id` |
| `pd` | no | yes | `--peers`, `--node-id` |
| `data` | yes | no | `--pd-addr` (+ data flags) |

## Timestamp Format

Single uint64, monotonic across the cluster:

```
 63          20 19            0
┌───────────────┬───────────────┐
│ physical_ms   │ logical       │
│ (44 bits)     │ (20 bits)     │
└───────────────┴───────────────┘
```

- `physical_ms`: wall clock milliseconds — 44 bits ≈ 558 years
- `logical`: counter within one millisecond — 20 bits = 1,048,576
  timestamps/ms ceiling; overflow rolls into the next ms

Ordering: plain uint64 comparison. No two granted ranges overlap
(enforced by the watermark CAS below).

## etcd Key Space

| Key | Value | Writer |
|-----|-------|--------|
| `/tellstone/pd/tso/watermark` | last granted upper bound (uint64 LE) | any member, via txn CAS (decentralized) |
| `/tellstone/pd/leader` | election slot (concurrency.Election) | any member |
| `/tellstone/nodes/<node-id>` | {role, addr, last-seen} with lease | each node |

## Range Grant Protocol

1. Node's pool drops below the refill threshold → request a grant of
   size `n`.
2. **Any** member advances the watermark through an etcd transaction:
   `if ModRevision(key) == rev then watermark = w_prev + n` (CAS; retry
   on conflict). The key's own revision is pinned (not the store-wide
   revision), so concurrent granters serialize correctly and a fresh key
   is created when absent. No dedicated leader hop is needed — etcd's
   transaction serializes the space, so ranges are globally disjoint by
   construction.
3. The granted range `[w_prev + 1, w_prev + n]` is adopted into the
   local pool atomically.

Failover safety: because the watermark advances only through committed
CAS transactions, a recovered or freshly elected member reads the stored
value and never reissues consumed timestamps. A crashed granter that
advanced the watermark but lost its range in flight simply leaves a gap
that is never reused.

> Design note (deviation from the original plan): the plan described a
> "PD leader" allocator. The implemented grant is decentralized — every
> member can CAS-extend the watermark via etcd. Behavior and the failover
> guarantee are identical; the leader election still exists for future
> singleton PD duties but is not on the grant path.

## Local Pool Mechanics (`internal/cluster/tso.go`)

```go
type Pool struct {
    cur  atomic.Uint64 // next timestamp to hand out
    end  atomic.Uint64 // exclusive upper bound of owned range
    sampler rateSampler // EWMA of allocations/sec
}
```

- **Alloc:** `tryAlloc` runs a compare-and-swap loop on `cur` while `cur <
  end` — one atomic op on the hot path, zero heap allocations. Slow path
  (pool empty) blocks per ADR-003 or returns `ErrTSOExhausted` in
  non-blocking callers.
- **Refill trigger:** remaining < 20 % of current range.
- **Batch sizing:** `max(min_batch, EWMA(rate) × headroom_seconds)`,
  defaults `min_batch = 1000`, `headroom = 30 s` (ADR-003).
- Rate estimator updates on allocation, exponential decay α chosen so
  the window spans seconds, not hours.

## Addressing Rules

Derived ports (ADR-010 §6), override via flags:

```
data addr "n1:9988" → etcd client "n1:19988", etcd peer "n1:29988"
```

Validation at startup: derived/override addresses must not collide with
any configured data address; duplicates across peers are rejected with
the phase-1 panic style.

## Failure Timeline (acceptance scenario)

The watermark-CAS grant protocol is decentralized: any member can extend the
timestamp space, so there is no grant-gating PD leader to elect. The failure
mode the suite covers is losing the entire PD member set (the embedded etcd
quorum):

```
T+0s      all PD members (etcd quorum) stopped
T+0–30s   nodes allocate from pools (writes would continue;
          this phase verifies via harness consumption)
T+30s     pools exhausted → Alloc blocks / ErrTSOExhausted
recovery  any PD member restarted → quorum reforms, pools refill,
          allocation resumes above everything previously granted
```

> Leader-only failover (stopping a single member while the quorum survives) is
> not covered by the current suite and must not be marked complete.

## Verification Plan

| Check | Method |
|-------|--------|
| 1- and 3-node embedded clusters form and elect | unit tests, ephemeral ports |
| Global monotonicity under concurrency | `-race` test, N goroutines × M allocations vs sorted set |
| Failover never reissues | stop entire PD quorum mid-test (3-member), restart → resumes above old watermark (decentralized CAS, no grant-gating leader) |
| Pool exhaustion semantics | harness drains pool, asserts block/ErrTSOExhausted, restart, resume |
| Hot-path cost | `go test -bench=Alloc -benchmem`: target ns/op sub-50, `allocs/op = 0` |
| Role wiring | `pd` role starts no shards; `data` role starts no etcd, dials `--pd-addr` |

## Implementation Status

| Item | Status | Notes |
|------|--------|-------|
| ADR-010 decisions recorded | ✅ Done | this doc + ADR-010 |
| Dependency intake (etcd server/client) | ✅ Done | etcd v3.7.1; binary +14.2 MB projected (19.9 → ~34 MB, scratch-link measurement); idle embedded member RSS ≈ 26 MB |
| Config surface (`--node-role`, `--pd-addr`, TSO tuning, port overrides) | ✅ Done | role/PD rules + endpoint resolution validated at flag time; regression tests in `config_test.go` |
| Embedded etcd bootstrap (`internal/cluster/pd.go`) | ✅ Done | concurrent member bootstrap, derived/override endpoints, bounded stop; 1- and 3-member in-process tests pass with `-race` |
| TSO core (`internal/cluster/tso.go`) | ✅ Done | uint64 packing, lock-free pool (TryAlloc/Alloc/Adopt), EWMA batch sizing, watermark-CAS granter; tests: concurrency-monotonic, blocking-wakes, live-etcd disjoint grants, restart safety |
| PD/TSO node stack + refill loop (`internal/cluster/pdnode.go`) | ✅ Done | `StartPDNode` per role (hybrid/pd boot embedded member; data dials `--pd-addr`), `TSOManager` primes + tops up the pool; wired into `server.go` initCluster/Stop |
| Acceptance failover test | ✅ Done | `internal/cluster/failover_test.go`: 3-member PD, kill whole quorum → pools drain → `Alloc` blocks → restart quorum → resumes (decentralized CAS, no single leader) |
| Benchmarks (hot path proof) | ✅ Done | `BenchmarkAlloc`: 7.3 ns/op single / 41 ns/op parallel, **0 allocs/op** (`-benchmem`) |
| Manual end-to-end PD/TSO proof | ✅ Done | `TestManualPDTSO` (gated `TELLSTONE_MANUAL_TEST=1`): boots a real 3-node `--cluster-mode` server, dials each node's embedded etcd client port, asserts 15 grants across the cluster are globally disjoint |
| Close-out status update | ✅ Done | all code + tests land; tracked in this doc |

Deferred by decision (not part of phase 2):

- **write-path timestamp integration** — RESP is being dropped in favour
  of a Postgres wire protocol + SQL parser (later phase). TSO wiring moves
  to that frontend instead of the old RESP path; phase 2 ships the oracle
  standalone.
- routing/metadata services (phases 3–5)
- transport peer-auth hardening for the etcd peer channel (evaluate with
  phase 3 security review)
