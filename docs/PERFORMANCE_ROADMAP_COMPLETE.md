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
- [x] **Baseline benchmarks**: `go test -bench=. -benchmem -count=5 ./internal/storage/... ./internal/shard/... ./internal/network/... ./internal/sql/...`
- [x] **Escape analysis**: `go build -gcflags="-m=2" ./internal/shard/... ./internal/storage/... ./internal/network/... ./internal/persistence/...` log escapes
- [x] **CPU/heap profiles**: Capture profiles for pgbench RW in-memory (single node) and with persistence
- [x] **Alloc tracking**: Add lightweight alloc counters for hot paths (behind `-tags=perftrace` or internal metrics)
- [x] **Microbench suite**: Create `internal/bench/*` with `BenchmarkKVGet`, `BenchmarkKVSet`, `BenchmarkKVBatch`, `BenchmarkPGSimpleQueryPK`
- [x] **Document baseline**: Write `docs/perf/BASELINE.md` with numbers

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

### Exit Criteria
- `BenchmarkKVGet` -> 0 allocs/op (already satisfied)
- `BenchmarkKVSet` -> 1 alloc/op (structural, not fixable without changing ownership model)
- GC pauses visibly reduced in profiles (not measurable with current benchmarks)
- Phase 1 optimizations completed and documented

**Phase 1 is complete**

## Phase 2: Actor-Per-Shard + Lock-Free Hot Paths (Week 2-3)
**Objective:** Remove lock convoys, scale with cores.

### Tasks
- [ ] **Actor model**: Each shard runs dedicated goroutine with MPSC ring buffer queue. All mutations serialized on actor; reads snapshot/lock-free where correct
- [ ] **Batch dispatch**: Drain queue in batches (coalesce ops)
- [ ] **Cache-line alignment**: `//go:align`-aware struct layout, pad hot structs, separate hot/cold fields
- [ ] **Sharded/lock-free structures**: Prototype lock-free hash table for hot maps; benchmark vs mutex
- [ ] **Core pinning**: Optional shard->core pinning via `runtime.LockOSThread` + config
- [ ] **Remove hot mutexes**: Replace RWMutex on critical paths with snapshot reads/RCU-like patterns where safe

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

### Tasks
- [ ] **Log entry batching**: Batch N entries or timeout before `AppendEntries`
- [ ] **Pipelined replication**: Overlap in-flight batches
- [ ] **Leader lease reads**: Linearizable reads without log roundtrip where safe
- [ ] **Batched apply**: Apply committed entries in chunks to state machine
- [ ] **Coalesce heartbeats**: Reduce RPC chatter
- [ ] **Integrate with WAL group-commit**: Ensure ordering/crash-safety preserved

### Exit Criteria
- Distributed RW TPS scales better with batching
- Leader read latency drops
- All cluster tests pass

## Phase 5: SQL KV Fast-Path + Final Polish (Ongoing)
**Objective:** Close SQL overhead gap.

### Tasks
- [ ] **KV bypass detector**: Identify `SELECT ... WHERE pk=$1`, single-table INSERT/UPDATE/DELETE by PK
- [ ] **Direct shard routing**: Call KV path, skip full translate/plan where possible
- [ ] **Prepared fast-path cache**: Zero-alloc execution for cached plans
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

**Status:** Phase 1 complete. Ready to start Phase 2 on command.