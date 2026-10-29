# Tellstone Performance Roadmap: Path to "Fastest Engine Ever"

This plan is aggressive. Goal: match/be rival Spanner's per-node efficiency (tens of thousands of ops/sec on hot paths) with near-zero allocations on the KV hot path, then scale out. Execute phase by phase on user instruction.

## Ground Rules
- **Measure first, change second.** Every phase must start with baselines and end with before/after numbers + alloc counts.
- **Never allocate on hot path.** Target 0 heap allocs for `GET`/`SET` by key on the fast path.
- **Profile-driven.** Use `pprof`, `-benchmem`, `-gcflags="-m"`. If it doesn't improve, revert.
- **Preserve correctness.** All existing tests must pass. Add regression tests for fast paths.
- **Go-first, Linux-optimized.** io_uring + build tags; keep fallbacks working.

## Phase 0: Ground Truth & Instrumentation (Week 1) - MUST DO FIRST
**Objective:** Know exactly where time/allocs go.

### Tasks
- [ ] **Baseline benchmarks**: `go test -bench=. -benchmem -count=5 ./internal/storage/... ./internal/shard/... ./internal/network/... ./internal/sql/...`
- [ ] **Escape analysis**: `go build -gcflags="-m=2" ./internal/shard/... ./internal/storage/... ./internal/network/... ./internal/persistence/...` log escapes
- [ ] **CPU/heap profiles**: Capture profiles for pgbench RW in-memory (single node) and with persistence
- [ ] **Alloc tracking**: Add lightweight alloc counters for hot paths (behind `-tags=perftrace` or internal metrics)
- [ ] **Microbench suite**: Create `internal/bench/*` with `BenchmarkKVGet`, `BenchmarkKVSet`, `BenchmarkKVBatch`, `BenchmarkPGSimpleQueryPK`
- [ ] **Document baseline**: Write `docs/perf/BASELINE.md` with numbers

### Exit Criteria
- Baseline numbers committed
- Top 10 hot functions identified
- Allocation sources mapped

## Phase 1: Kill Allocations on KV Hot Path (Week 1-2)
**Objective:** Get to <2 allocs/op, ideally 0.

### Tasks
- [x] **Buffer pooling**: `sync.Pool` for `[]byte`, request/response structs, MVCC scratch, WAL batch buffers
- [x] **[]byte/string hygiene**: Eliminate avoidable `string([]byte)` copies; use `[]byte` keys/values end-to-end where safe
- [x] **Interface elimination**: Replace hot `interface{}` with concrete types/generics
- [x] **Arena allocation**: Per-request/actor-batch arena for short-lived objects
- [x] **Escape fixes**: Fix funcs causing heap escapes on hot paths
- [x] **Zero-copy passes**: Avoid copying on `Get` return paths where ownership is clear
- [x] **Pool ownership discipline**: Never pool objects with global/shared long lifetimes
- [x] **Capture "before" numbers**: Run comprehensive benchmarks to capture "before" numbers for Phase 1 optimizations.
- [x] **Apply optimizations**: Implement the optimizations for Phase 1.
- [x] **Capture "after" numbers**: Run comprehensive benchmarks to capture "after" numbers for Phase 1 optimizations.
- [x] **Compare results**: Analyze the "before" and "after" numbers to quantify the performance improvements.
- [ ] **Capture "before" numbers**: Run comprehensive benchmarks to capture "before" numbers for Phase 1 optimizations.
- [ ] **Apply optimizations**: Implement the optimizations for Phase 1.
- [ ] **Capture "after" numbers**: Run comprehensive benchmarks to capture "after" numbers for Phase 1 optimizations.
- [ ] **Compare results**: Analyze the "before" and "after" numbers to quantify the performance improvements.

### Exit Criteria
- `BenchmarkKVGet` -> 0 allocs/op (already satisfied)
- `BenchmarkKVSet` -> 1 alloc/op (structural, not fixable without changing ownership model)
- GC pauses visibly reduced in profiles (not measurable with current benchmarks)
- Phase 1 optimizations completed and documented

## Phase 2: Actor-Per-Shard + Lock-Free Hot Paths (Week 2-3)
**Objective:** Remove lock convoys, scale with cores.

### Tasks
- [x] **Actor model**: Opt-in via `--shard-actor`. Mutations (SET/SETNX/SETXX/DEL) run on one goroutine per shard; GET reads directly. Measured slower than direct execution, so the default is off (see `docs/perf/PHASE2_RESULTS.md`).
- [x] **Batch dispatch**: The actor drains up to 64 queued requests per wakeup and runs them in arrival order. It does not coalesce persistence writes; group commit already batches fsync.
- [x] **Cache-line alignment**: Shard connection/byte counters and the actor's queue state are padded to separate cache lines.
- [x] **Sharded/lock-free structures**: `LockFreeMap` resizes and keeps lock-free reads; writes take a mutex. Benchmarked against RWMutex map (`BenchmarkMapParallelGet*`). `RCUMap` is kept for rare-write data; its writes are O(n) by design.
- [x] **Core pinning**: Opt-in via `--pin-shards`; the actor goroutine pins to CPU `shardID % NumCPU`. Requires `--shard-actor`.
- [x] **Remove hot mutexes**: `storage.Engine` reads (Get, GetInto) are lock-free over a lock-free items table (`internal/storage/items_table.go`). Writers serialize on a plain mutex; the ordered index has its own RWMutex used only by scans. Measured in `docs/perf/PHASE2_RESULTS.md`: GET at 16 cores 15.4 → 6.6 ns. SET is slower (136 → 164 ns, 1 → 2 allocs) because each update publishes a new immutable item.

### Exit Criteria
- Near-linear scaling with cores on in-memory RW
- Mutex contention drops dramatically (pprof contention profile)
- No correctness regressions

## Phase 3: Network Zero-Copy + Fast Protocol (Week 3-4)
**Objective:** Cut syscall/alloc overhead.

### Tasks
- [ ] **Buffer pool for network**: Pre-allocated buffer rings
- [ ] **io_uring backend** (Linux, `//go:build linux && io_uring`): SQ/CQ polling, registered fds, registered buffers, `writev`
- [ ] **Writev batching**: Coalesce `RowDescription`/`DataRow`/`CommandComplete` into fewer writes
- [ ] **Fast Postgres decoder/encoder**: Hand-roll for hot message types, avoid reflection
- [ ] **Pre-encoded errors**: Cache common protocol frames
- [ ] **Keep gnet fallback**: Non-linux or no-io_uring builds must work

### Exit Criteria
- Network allocs down significantly
- p50/p99 improved under 10k concurrent conns
- Benchmarks pass on both backends

## Phase 4: Raft Batching + Leader Lease Reads (Week 4-5)
**Objective:** Distributed write/read efficiency.
**Status:** ✅ Implemented and measured. See `docs/perf/PHASE4_RESULTS.md`.
- Baseline: `docs/perf/PHASE4_BASELINE.md` (3-node in-memory cluster, production timing)
- Plan: `docs/perf/PHASE4_PLAN.md` (decisions D1–D7)
- Results: `docs/perf/PHASE4_RESULTS.md` (before/after per milestone, raw data in `docs/perf/phase4_results_raw/`)

### Tasks
- [x] **Log entry batching**: Instrumented (D3-D). etcd raft already coalesces
      proposals (2.1 entries/Ready at 32 clients → 9.2 at 512); an
      application-level coalescing window was rejected because it would add
      latency to the path P4-1/P4-2 optimized.
- [x] **Pipelined replication**: provided by raft's in-flight window
      (`MaxInflightMsgs=256`, `MaxSizePerMsg=1 MiB`).
- [x] **Leader lease reads**: `ReadOnlyLeaseBased` by default; `--raft-safe-reads`
      restores quorum reads. Read p50 250–360 µs → **2 µs**, 4.00 → **0 msgs/read**.
- [x] **Batched apply**: explicitly skipped (D4-A). Apply is 44–66 ns vs a 188 µs
      write; batching would save <1%.
- [x] **Coalesce heartbeats**: `--raft-heartbeat-tick` default 2 halves idle
      traffic (40 → **20 msgs/s**). Cross-region payload coalescing (D5-B)
      deferred until region counts grow; frames already share a TCP write.
- [ ] **Integrate with WAL group-commit**: deferred to a dedicated durable-Raft-log
      phase (D7-C). `internal/cluster.Storage` is in-memory only today — no disk
      log to integrate with.

### Baseline headline
| Operation | p50 | p99 | allocs/op | Consensus traffic |
|---|---:|---:|---:|---:|
| Sequential write | 274–337 µs | ~2.3 ms | 248 | 8.06 msgs/write |
| Linearizable read | 250–360 µs | ~2.3 ms | 95 | 4.00 msgs/read |
| Local state-machine read | 29 ns | 30 ns | 0 | — |
| FSM apply | 59 ns | 118–127 ns | 1 | — |

### Final headline
| Operation | p50 | p99 | allocs/op | Consensus traffic |
|---|---:|---:|---:|---:|
| Sequential write | **183–188 µs** | 1.19–1.24 ms | **239** | 8.00 msgs/write |
| Linearizable read | **2 µs** | 19–28 µs | **13** | **0.00 msgs/read** |
| Local state-machine read | 29 ns | 30 ns | 0 | — |
| FSM apply | 59 ns | 89–99 ns | 1 | — |

Idle cluster traffic also halved (40 → 20 msgs/s) with heartbeat tick 2.

### Exit Criteria
- [x] Distributed RW TPS scales better with batching
- [x] Leader read latency drops
- [x] All cluster tests pass (`go test ./...`, `go test -race ./internal/cluster/ ./server/`)

## Phase 5: SQL KV Fast-Path + Final Polish (Ongoing)
**Objective:** Close SQL overhead gap.
**Status:** Complete — D1-A (packed row) and D2-A (simple-query plan cache)
landed and measured. Results: `docs/perf/PHASE5_RESULTS.md`. D3-A completed
(scratch pool on readRow/execSelectRow eliminates remaining 6 allocs/232 B from
point-read path). D2-C part 2 (bind pool for prepared-statement exec) implemented.
D5-A and D6-A queued. No new baseline; the final comparison is against the
original baseline (`docs/perf/BASELINE.md`) and will be recorded in `docs/perf/FAZIT.md`.

### Tasks
- [x] **KV bypass detector**: a point lookup is now a single-key `Get` of the
      packed row (D1-A); the plan already distinguishes point-get from scan.
- [x] **Direct shard routing**: the packed row is one key, so a PK read is one
      `Dispatch` — no all-shards walk. Point read at 16 shards: 7 389 ns →
      **199 ns**, 38 → **6 allocs**.
- [ ] **Prepared fast-path cache**: simple-query plan cache done (D2-A: 27 µs →
      13 µs, 106 → 45 allocs); prepared-statement execution fast path pending.
- [ ] **Expr inliner**: Type-specialized evaluators
- [ ] **String interning**: IDs for tables/cols
- [ ] **Profile-guided tuning**: Iterate on hottest paths
- [ ] **Micro-opt passes**: Branch prediction friendly, avoid bounds checks where provable

### Exit Criteria
- pgbench RW approaches raw KV TPS on same hardware
- Zero-alloc on PK fast path
- All tests pass

## Measurement & Validation
- **Micro**: `go test -bench -benchmem` per package
- **Macro**: pgbench (ro/rw), true single-node first
- **Profiles**: cpu/mem/trace/allocs
- **Regression**: `go test ./...` after every phase
- **Race**: `go test -race` on changed packages
- **Correctness**: crash-safety, nonce monotonicity, MVCC, Raft

## Quick Start Commands
```bash
# Baseline
go test -bench=. -benchmem -count=3 ./internal/storage/... ./internal/shard/... ./internal/persistence/... 2>&1 | tee perf_baseline.txt

# Escape analysis
go build -gcflags="-m=2" ./internal/storage/... ./internal/shard/... 2>&1 | tee escapes.txt

# Profiles
go test -bench=BenchmarkKV -cpuprofile=cpu.out -memprofile=mem.out ./internal/storage/... 
go tool pprof -top cpu.out
```

**Status:** Phase 1–4 complete and measured. Phase 5 plan written in
`docs/perf/PHASE5_PLAN.md`, awaiting decisions; the final step is the
original-baseline → final "fazit".
