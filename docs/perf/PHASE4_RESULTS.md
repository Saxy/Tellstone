# Phase 4 Results

Baseline: `docs/perf/PHASE4_BASELINE.md`. Plan: `docs/perf/PHASE4_PLAN.md`.
Method is the same for every milestone: 3-node in-memory cluster over loopback,
benchmarks pinned with `taskset -c 0-15`, same harness and same machine (Ryzen
9950X / WSL2). Raw output: `docs/perf/phase4_results_raw/`; pre-change numbers:
`docs/perf/phase4_baseline_raw/`.

## Headline

Go-level benchmarks (`-benchmem -benchtime=3s`, `-16`):

| Benchmark | Before | After |
|---|---:|---:|
| `ProposeSequential` | 719 363 ns, 10 987 B, 248 allocs | **230 215 ns, 9 055 B, 239 allocs** |
| `ProposeParallel` (16) | 29 139 ns, 17 762 B, 197 allocs | **23 173 ns, 7 815 B, 179 allocs** |
| `LinearizableRead` | 648 937 ns, 3 616 B, 95 allocs | **2 459 ns, 841 B, 13 allocs** |
| `LinearizableReadParallel` (16) | 32 663 ns, 3 741 B, 72 allocs | **10 388 ns, 885 B, 12 allocs** |
| `LocalRead` | 11 ns, 0 B, 0 allocs | 10.8 ns, **0 B, 0 allocs** |
| `FSMApply` | 46 ns, 16 B, 1 alloc | 45.6 ns, 16 B, 1 alloc |

## M1: Leader-lease reads — DONE

### Change
- `raft.Config.ReadOnlyOption` now defaults to `raft.ReadOnlyLeaseBased`.
  `CheckQuorum` was already enabled, which raft requires for lease reads, so the
  leader answers `ReadIndex` from its local commit index while its lease holds
  and no longer broadcasts a heartbeat per read.
- `NodeConfig.SafeReads` + `--raft-safe-reads` (env `TSD_RAFT_SAFE_READS`) force
  the old `ReadOnlySafe` behaviour. The knob is wired to the bootstrap node and
  to every region group node (`RegionCoordinator.safeReads`), because region
  nodes are created after the bootstrap node.
- `TestPhase4LeaseReadStopsOnQuorumLoss` isolates the leader's transport and
  asserts CheckQuorum makes it step down within the election timeout, after
  which linearizable reads fail instead of serving stale data.

### Go-level benchmarks (`-benchmem`, `-benchtime=3s`)

| Benchmark | Before (`ReadOnlySafe`) | After (lease) | `--raft-safe-reads` |
|---|---:|---:|---:|
| `LinearizableRead` | 648 937 ns, 3 616 B, 95 allocs | **2 459 ns, 841 B, 13 allocs** | 114 244 ns, 3 768 B, 95 allocs |
| `LinearizableReadParallel` (16) | 32 663 ns, 3 741 B, 72 allocs | **10 388 ns, 885 B, 12 allocs** | — |

### Report (500 samples, 3-node cluster)

| Metric | Before | After | `--raft-safe-reads` |
|---|---:|---:|---:|
| Read p50 | 250–360 µs | **2 µs** | 107 µs |
| Read p95 | 1.33–2.26 ms | 7 µs | 187 µs |
| Read p99 | 2.28–2.36 ms | 19–28 µs | 240 µs |
| Messages per read | 4.00 | **0.00** | 4.00 |
| Reads/s, 32 clients | 74–80 k | **124–180 k** | 94 k |

The fallback reproduces the old quorum round trip exactly (4.00 msgs/read), so
operators who cannot assume bounded clock drift can opt out. Lease reads assume
single-datacenter / NTP synchronization; this is the same trade etcd and TiKV
make.

## M2: Adaptive transport flush — DONE

### Change
- `batchConn.run` keeps the 100 µs burst timer but flushes immediately when a
  message arrives on an idle sender: no pending batch and at least
  `batchFlushIntervalSparse` (20 µs) since the last flush. Under load, messages
  arrive inside that window, so the timer still aggregates them into one frame.
- The idle check is anchored on the last flush, not the previous batch size. A
  first attempt that re-armed the timer to 20 µs after single-message flushes
  regressed sequential writes to 1 026 300 ns/op (it defeated aggregation on
  empty ticks) and was discarded.

### Sequential write latency / throughput

| Metric | Before | After |
|---|---:|---:|
| Sequential write mean | 527–753 µs | **219 µs** |
| Sequential write p50 | 274–337 µs | **183 µs** |
| Sequential write p95 | 1.34–2.24 ms | 346 µs |
| Sequential write p99 | 2.25–2.35 ms | **1.24 ms** |
| Concurrent write, 32 clients | 63 152 ops/s | 66–75 k ops/s |
| Frames per write, sequential | 4.65 | 7.38 |
| Frames per write, 32 clients | 0.41 | 0.54–0.57 |

### Trade-off at high concurrency

| Writers | With idle flush | Idle flush disabled (A/B) |
|---:|---:|---:|
| 32 | 66–75 k ops/s | ~63 k ops/s |
| 128 | 84–88 k ops/s | ~73 k ops/s (baseline) |
| 512 | 72–86 k ops/s | 94–98 k ops/s |
| Frames/write at 512 | 0.21–0.36 | 0.20 |

At ≤128 clients throughput is flat or better; at 512 clients the extra frames
cost ~10–15%. We keep it because p50 latency is the stated priority for the
sequential path and 512-client throughput still exceeds the baseline 32-client
rate. The 512-client frame count is run-dependent, so it is reported as a range.

## M3: Cached leadership + write-path allocations — DONE

### Change
- `IsLeader`/`LeaderID` return atomics refreshed from `Ready.SoftState` instead
  of calling `raftNode.Status()`, which deep-copies the progress maps on every
  write. `leaderLost` is closed on step-down and selected by `ProposeAndWait` /
  `ProposeChunked`, so a proposal queued against a stale cache is released with
  `ErrNotLeader` instead of blocking to the caller's deadline. `Stop()` clears
  the cache and signals the channel.
- `Storage.Entries` returns a sub-slice of the stored log instead of copying the
  window. `Append` now copies the kept prefix before truncating, because a
  queued transport frame may still reference the old window (copy-on-truncate,
  not copy-on-read).
- `proposalTracker` replaces its `sync.Map` with 16 mutex-guarded typed maps,
  removing one interface boxing and one map store per proposal. Remove-on-timeout
  and unknown-ID-is-a-no-op are unchanged.

### `ProposeSequential` progression

| Step | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Baseline | 719 363 | 10 987 | 248 |
| After M2 (flush) | 223 346 | 10 946 | 257 |
| After leadership cache + `Entries` | 223 375 | 9 080 | 241 |
| After sharded tracker | 216 318 | 9 039 | 239 |
| Final run | 230 215 | 9 055 | **239** |

Allocation exit criteria met (allocs < 240, bytes < 10 500). The remaining top
sites are raft/transport scratch (`varintptr`, `encodeGroupFrame`, `raft.send`,
`decodeMsg`) — see `phase4_results_raw/write_alloc_top.txt`.

`TestClusterRegionLeaderChange` caught a stale cached leader after `Stop()`
during this milestone; clearing the cache and signalling `leaderLost` in `Stop()`
fixed it.

## M4: Heartbeat / election / tick knobs — DONE

### Change
- New flags and `TSD_*` envs: `--raft-heartbeat-tick` (default 2),
  `--raft-election-tick` (default 10), `--raft-tick-interval` (default 100 ms).
- Validation rejects non-positive ticks and `heartbeat >= election`.
- Region group nodes use `heartbeatTickFor`, clamping the configured heartbeat
  below the region's zone-biased election timeout (10 or 20 ticks at 50 ms), so
  a value valid for the bootstrap node cannot break region hosting.

### Idle traffic and stable operation

| Metric | heartbeat 1 (old) | heartbeat 2 (new) |
|---|---:|---:|
| Idle cluster traffic | 40 msgs/s | **20 msgs/s** |
| Idle frames | 46/s | 26/s |
| Sequential write p50 | 191 µs | 184 µs |
| Concurrent write, 32 clients | 72 314 ops/s | 72 176 ops/s |

At 100 ms per tick, heartbeat 2 refreshes the lease every 200 ms against a 1 s
election timeout — five heartbeats per timeout, above the 3–5 minimum — so M1's
lease reads remain valid. Election/failover tests still elect a new leader within
the configured timeout.

## M5: Batching instrumentation — DONE (instrument only)

### Change
- `Node.BatchingStats()` exposes four atomics updated by the ready loop:
  `ReadyBatches`, `CommittedEntries`, `AppendMsgs`, `AppendEntriesEntries`.
- `TestPhase4BaselineReport` prints three ratios: entries per Ready (proposal
  coalescing), entries per AppendEntries (log batching), appends per write.

### Concurrency sweep (1.5 s window)

| Writers | ops/s | entries/Ready | entries/AppendEntries | appends/write |
|---:|---:|---:|---:|---:|
| 32 | 66 936 | 2.16 | 0.50 | 4.00 |
| 128 | 84 517 | 3.93 | 0.50 | 3.97 |
| 512 | 72 201 | 9.82 | 1.29 | 1.55 |

etcd raft already coalesces proposals: 2.2 entries per Ready at 32 clients
rising to 9.8 at 512. At 32 clients a write still costs ~4 AppendEntries (two
followers × replicate + commit-advance) and each carries ~0.5 entries, i.e. half
of those messages are empty commit-advance heartbeats — protocol ping-pong, not a
missing batching layer. An application-level coalescing window (D3-B) would add
latency to the path M1/M2 just optimized for a throughput gain that the measured
curve does not need, so **D3-B is not taken**. Revisit only if a workload shows
a ceiling below 32 clients.

## M6: Batched apply — SKIPPED (D4-A)

FSM apply is 45.6 ns/op (59 ns p50) with one decode allocation, against a
183 µs write and a 2 µs read. Batching the dispatcher API would save well under
1% and add ordering plumbing. The measurement is recorded here so the decision is
explicit; revisit if the state machine becomes remote or persistent.

## M7: Raft log durability — DEFERRED (D7-C)

`internal/cluster.Storage` is in-memory only: `processReady` writes slices, there
is no fsync and no recovery path, so a restart loses the consensus log. Phase 4's
wins (lease reads, flush latency, write-path allocations) are independent of
durability, and mixing in a disk log would make these numbers incomparable. A
durable Raft log (HardState + entries + snapshot, group-commit, replay, crash
tests) is tracked as its own phase with its own baseline.

## Verification

- `go build ./...` — clean.
- `go test ./...` — clean.
- `go test -race ./internal/cluster/ ./server/` — clean.
- New tests: `TestReadOnlyOptionMapping`,
  `TestPhase4LeaseReadStopsOnQuorumLoss`.
- Correctness suites exercised: leader election, failover, split, federation,
  forwarded writes, proposal timeout, non-leader rejection.

## Reproduction

```sh
# Go-level benchmarks (pinned cores).
taskset -c 0-15 go test -run '^$' -bench 'BenchmarkPhase4' -benchmem -benchtime=3s ./internal/cluster/

# Latency / throughput report.
PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/
PHASE4_WRITERS=128 PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/
PHASE4_WRITERS=512 PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/

# Fallback and heartbeat comparisons.
PHASE4_SAFE_READS=1 PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/
PHASE4_HEARTBEAT_TICK=1 PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/
```
