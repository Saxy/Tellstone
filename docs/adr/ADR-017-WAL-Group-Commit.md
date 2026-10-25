# ADR-017: WAL Group Commit to Eliminate Synchronous fsync Bottleneck

- **Status:** Proposed
- **Date:** 2026-10-24
- **Authors:** Maximilian Hagen
- **Component:** Storage Engine / Persistence
- **PR:** feat/sql-comp (ongoing)

## Context

Benchmarks against a true single-node Tellstone (no `--cluster-mode`) with the new MVCC + transaction layer showed a dramatic write-throughput collapse once WAL persistence was enabled:

| Mode | RW TPS (c=32) | RW TPS (c=64) | Latency (c=64) |
|------|---------------|---------------|----------------|
| Single node, persistence disabled | ~3,909 | ~3,758 | ~17 ms |
| Single node, persistence enabled (current) | ~207 | ~211 | ~302 ms |

Key observations:
- Throughput flatlines at ~200 TPS regardless of concurrency (c=8, 32, 64)
- Latency scales linearly with queue depth
- Failed transactions drop to 0% once `pgbench --max-tries` is corrected, confirming the issue is I/O serialization, not retry logic

This is the textbook symptom of synchronous `fsync()` per transaction commit with no batching. Each `COMMIT` currently writes its WAL record and calls `fsync()`/`Sync()` under a per-shard mutex. With many concurrent clients all serializing on that path and blocking on disk flushes, the system hits the physical `fsync()` rate limit of the underlying storage (often 200-500 ops/sec) and becomes I/O-bound.

The in-memory engine, MVCC, and SQL execution are performing well (~3.7-3.9k TPS). The bottleneck is exclusively the durability path.

## Decision

Implement **Group Commit** in `internal/persistence/` to decouple log append from durability sync. 

### Design

1. **Per-shard group committer**: Each opened shard gets a dedicated goroutine that owns all file writes and `fsync()`/`fdatasync()` calls. No other goroutine performs disk flushes.
2. **Synchronous API preserved**: `(*Storage).Write()` and `(*Storage).Delete()` remain blocking from the caller's perspective. Callers (e.g. `internal/shard/runner.go`) do not need to change - they will block until their transaction's batch has been durably flushed and acked.
3. **Batching**: The flusher collects pending `commitReq` entries from a queue, waiting up to `WAL_GROUP_TIMEOUT` (default `500µs`) or until `WAL_GROUP_BATCH_SIZE` (default `128`) entries are accumulated. All entries in a batch are written with `write()`/`writev()` and a single `fdatasync()` is issued.
4. **Strict ordering**: Commits are acked in enqueue order. Nonce assignment for encrypted WALs happens at enqueue time under serialization to guarantee nonce monotonicity across crashes.
5. **Flush barriers**: Add `Flush(shardID)` to force pending batches to disk before `TruncateWALTo`, snapshots, and `CloseShard()`. `CloseShard()` must drain and flush before closing files and persisting nonce sidecars.
6. **Configurable durability**: Support modes `group_commit` (default), `per_commit_sync` (legacy behavior for debugging/regression tests), and `async` (opt-in, best-effort durability). Prefer `fdatasync` over `fsync` where available.

### Data structures (sketch)
```go
type commitReq struct {
    data []byte        // pre-serialized record (plaintext header+key+value or encrypted frame)
    done chan error    // ack channel, buffered size 1
}

type groupCommitter struct {
    h       *shardHandle
    ch      chan *commitReq
    stopCh  chan struct{}
    wg      sync.WaitGroup
    batchSz int
    timeout time.Duration
}
```

The serialization of record bytes and (for encrypted WALs) nonce assignment remains under ordering control before enqueue. The flusher goroutine is the only actor that calls `Write()` and `Sync()`/`Fdatasync()` on the file descriptor.

## Consequences

### Positive
- **Massive throughput gain**: Expect RW TPS to increase from ~200 to potentially 1k-4k with persistence enabled, much closer to the in-memory ~3.9k baseline.
- **Better concurrency scaling**: Additional clients will scale throughput until CPU/execution becomes the bottleneck again, rather than hitting a fixed `fsync()` ceiling.
- **Lower tail latency under load**: Batching amortizes the expensive `fsync()` cost across many transactions.
- **Minimal API churn**: `Write`/`Delete` semantics stay synchronous; `internal/shard/runner.go` and callers require minimal/no changes.
- **Tunable**: Batch size/timeout/mode configurable for different hardware and durability requirements.

### Negative
- **Increased complexity**: Adds goroutines, channels, batching logic, and flush barriers to the persistence layer.
- **Slightly higher commit latency for single-threaded workloads**: A lone commit may wait up to `batchTimeout` before flushing (configurable down to lower values). Can be mitigated by flushing immediately when batch reaches size or on certain conditions.
- **More complex shutdown/recovery paths**: Must ensure all pending batches are drained on `CloseShard()`, `TruncateWALTo()`, and process exit.
- **Platform differences**: `fdatasync` availability varies (Windows needs fallback to `FlushFileBuffers`/`Sync()`).

### Risks
- **Crash safety regression**: Must never ack a transaction before its batch is durably synced. Requires careful ordering in flush/ack logic.
- **Nonce reuse**: Encrypted WAL nonces must remain strictly monotonic. Assignment must not move into the unsynchronized flusher.
- **Truncation races**: Snapshots call `TruncateWALTo()` which must flush pending batches before truncating to avoid losing unacknowledged data or corrupting WAL boundaries.

## Implementation Plan

### Phase 0: Investigation (Complete)
- [x] Identify `fsync`/`Sync()` call sites in `internal/persistence/`
- [x] Trace write path through `appendRecord`/`appendRecordEncrypted`
- [x] Confirm integration in `internal/shard/runner.go`
- [x] Benchmark: persistence on vs off, true single node vs cluster (showed fsync bottleneck)

### Phase 1: Group Commit Core
- [ ] Create `internal/persistence/group_commit.go` with `commitReq`, `groupCommitter`, batch loop
- [ ] Add config/env plumbing: `TSD_WAL_SYNC_MODE`, `TSD_WAL_GROUP_BATCH_SIZE`, `TSD_WAL_GROUP_TIMEOUT`, `TSD_WAL_USE_FDATASYNC`
- [ ] Modify `shardHandle` to hold group committer; initialize on `OpenShard()`, stop on `CloseShard()`

### Phase 2: Refactor Write Path
- [ ] Pre-serialize records in `appendRecord`/`appendRecordEncrypted` (including nonce assignment under order)
- [ ] Enqueue `commitReq` to group committer and block on `done` channel
- [ ] Add `Flush(shardID uint32)` API
- [ ] Update `TruncateWALTo()` to call `Flush()` before truncation
- [ ] Update `CloseShard()` to drain/flush group committer before closing file and persisting nonce sidecar
- [ ] Preserve `per_commit_sync` legacy mode for comparison

### Phase 3: Testing & Tuning
- [ ] Unit tests: batching, ordering, error propagation, flush on shutdown/truncate, encrypted WAL nonce monotonicity
- [ ] Race tests: `go test -race ./internal/persistence/...`
- [ ] Integration tests: WAL replay correctness after group commit
- [ ] Benchmarks: pgbench RW on true single node with persistence enabled (group commit vs per-commit sync). Target: restore throughput towards in-memory levels while maintaining durability.
- [ ] Tune batch size/timeout for optimal latency/throughput

### Phase 4: Documentation
- [ ] Update `internal/persistence/README.md` with group commit behavior, sync modes, and tuning knobs
- [ ] Add comments explaining ordering/nonces/flush barriers

## Validation Criteria
- [ ] `go test ./internal/persistence/...` passes
- [ ] `go test -race ./internal/persistence/...` passes
- [ ] `go test ./...` passes (no regressions)
- [ ] With `TSD_WAL_SYNC_MODE=group_commit` and persistence enabled, pgbench RW shows throughput scaling with concurrency (no flatline at ~200 TPS)
- [ ] Crash-safety preserved: no committed transaction lost, no nonce reuse on restart
- [ ] `TruncateWALTo` and snapshot paths continue to work correctly

## References
- PostgreSQL: Group Commit (`synchronous_commit`, commit delay/batching)
- SQLite: WAL mode commit groupings
- RocksDB: Write batching / group commit
- Tellstone benchmarks: single node RW with/without persistence (2026-10-24)
