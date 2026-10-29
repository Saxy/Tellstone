# Phase 4 Baseline: Raft batching + leader lease reads

Baseline captured before any Phase 4 change. Raw data in
`docs/perf/phase4_baseline_raw/`.

## Method

- Real 3-node Raft cluster on `127.0.0.1`, one process, in-memory state machine,
  production timing from `server.initCluster`: `ElectionTick 10`,
  `HeartbeatTick 1`, `TickInterval 100 ms`.
- Production transport settings: per-peer batching sender, 100 µs flush
  interval, 64 KiB batch limit.
- Server pinned to cores 0–15 (`taskset -c 0-15`), matching the Phase 3 M4
  hygiene rule.
- Harness: `internal/cluster/phase4_bench_test.go`
  - `BenchmarkPhase4ProposeSequential`, `...ProposeParallel`
  - `BenchmarkPhase4LinearizableRead`, `...LinearizableReadParallel`
  - `BenchmarkPhase4LocalRead`, `...FSMApply`
  - `PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/`
    prints percentiles, messages per write/read, apply cost and idle chatter.
- `PHASE4_WRITERS=N` varies the concurrent client count in the report.

## Headline numbers

`go test -bench BenchmarkPhase4 -benchmem -benchtime=3s`, 3-node cluster,
GOMAXPROCS 16:

| Benchmark | ns/op | B/op | allocs/op | What it measures |
|---|---:|---:|---:|---|
| `ProposeSequential` | 719 363 | 10 987 | 248 | One client, full propose→commit→apply round trip |
| `ProposeParallel` (16 goroutines) | 29 139 | 17 762 | 197 | Concurrent proposals to the leader |
| `LinearizableRead` | 648 937 | 3 616 | 95 | `ReadIndex` + applied-index wait |
| `LinearizableReadParallel` (16) | 32 663 | 3 741 | 72 | Concurrent linearizable reads |
| `LocalRead` | **11** | 0 | 0 | Applied state machine, no consensus |
| `FSMApply` | **46** | 16 | 1 | Decode + dispatch one committed entry |

Latency percentiles (`TestPhase4BaselineReport`, 500 samples; repeated runs):

| Operation | mean | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Sequential write | 527–753 µs | 274–337 µs | 1.34–2.24 ms | 2.25–2.35 ms |
| Linearizable read | 551–804 µs | 250–360 µs¹ | 1.33–2.26 ms | 2.28–2.36 ms |
| Local state-machine read | 29 ns | 29 ns | 30 ns | 30 ns |
| FSM apply | 46–72 ns | 59 ns | 68–79 ns | 118–127 ns |

¹ One run produced a 1.1 ms read p50 (the applied-index poll can sleep 1 ms if
the leader is momentarily behind); the p99 is stable around 2.3 ms.

## Write path

| Metric | Value |
|---|---:|
| Raft messages per write | **8.06** |
| TCP frames per write, synchronous | 4.65 |
| TCP frames per write, 32 concurrent clients | 0.41 |
| Bytes per write | 333 |
| Concurrent write throughput, 32 clients | 63 152 ops/s |
| Concurrent write throughput, 128 clients | 72 882 ops/s |
| Concurrent write throughput, 512 clients | 89 239 ops/s |

Eight messages per write is four one-way messages per follower: replicate,
ack, propagate the new commit index, ack. That is two network round trips per
write. Transport batching already collapses the *frames* (0.41/write at 32
clients), but not the *messages*: msgs/write stays at 8.00 until concurrency
reaches 512 clients, where it finally drops to 3.19.

CPU profile for the sequential write benchmark:

| Bucket | Share |
|---|---:|
| `Syscall6` (write path) | 32% |
| `runtime.futex` (wakeups/channels) | 28% |
| `mallocgc` + friends | ~9% |
| Raft/transport code | remainder |

So the single-client write path is dominated by kernel/scheduler time, not
userspace work. The 100 µs transport flush timer is a large part of the
latency: with the interval temporarily lowered to 20 µs, `ProposeSequential`
fell from ~600–720 µs to **203 µs**; with it raised to 250 µs it rose to
1 127 µs (see `phase4_baseline_raw/experiments.txt`).

## Read path

| Metric | Value |
|---|---:|
| Raft messages per read | **4.00** |
| TCP frames per read | 0.16–0.27 |
| Concurrent read throughput, 32 clients | 74–80 k ops/s |
| Concurrent read throughput, 128 clients | 111 k ops/s |
| Local state-machine read | 29 ns |

Every read costs a `MsgHeartbeat` to each follower plus each response, because
`ReadIndex` runs in `ReadOnlySafe` mode. Reads are never coalesced: 4.00
msgs/read at 32, 128 and 512 concurrent clients. The local state machine serves
the same value in 29 ns, so ~650 µs of the read is consensus paperwork.

A temporary prototype setting `ReadOnlyOption = raft.ReadOnlyLeaseBased`
(`CheckQuorum` is already enabled) removed the round trip entirely:

| Metric | Baseline | Lease prototype |
|---|---:|---:|
| Read p50 | 250–360 µs | **2 µs** |
| Read p99 | 2.3 ms | **19 µs** |
| `LinearizableRead` | 648 937 ns/op, 95 allocs | **2 388 ns/op, 13 allocs** |
| Concurrent reads, 32 clients | 74–78 k ops/s | **181 k ops/s** |
| Messages per read | 4.00 | **0.00** |
| Sequential write | 8.04 msgs/write | unchanged |

The prototype was reverted; it is not part of this baseline.

## Apply and idle traffic

- `FSMApply` is 46–72 ns with one small allocation (the decoded key string).
  Even a 64-entry committed batch is only a few microseconds of dispatch, which
  is negligible next to a ~600 µs consensus round trip. Batched apply has very
  little to win in a single-region cluster.
- An idle 3-node cluster sends **40 msgs/s** and 45 frames/s: the leader's
  2 heartbeats per 100 ms plus 2 responses. That is harmless for one region,
  but ADR-002 targets thousands of regions per node; at 1 000 regions the same
  configuration would be ~40 000 msgs/s of pure heartbeat chatter.

## Allocation hot spots (sequential write)

From `phase4_baseline_raw/write_alloc_top.txt`:

| Source | Share of objects |
|---|---:|
| `network.varintptr` (frame encode) | 11.5% |
| `network.encodeGroupFrame` | 10.1% |
| `Storage.Entries` (result slice copy) | 7.7% |
| `raft.(*raft).send` | 7.7% |
| `raft.handleAppendEntries` | 6.2% |
| `raft.hardState` | 5.7% |
| `raft.appendEntry` | 4.1% |
| `proposalTracker.add` | 3.8% |
| `network.DecodeBatch` (cumulative) | 19.8% |

`proposalTracker.add` is one channel + one `sync.Map` store per write.
`Status()` inside the per-write `IsLeader()` check deep-copies the Raft progress
maps (`getProgressCopy`, `Inflights.Clone`) and accounts for roughly 11% of
allocated bytes.

## Durability note

`internal/cluster.Storage` is an **in-memory** Raft log: `processReady` writes
hard state and entries into a slice, with no fsync and no restart recovery.
The roadmap's “integrate with WAL group-commit” item therefore has no disk
commit to integrate with yet. This is a correctness gap that is larger than the
performance work in Phase 4 and deserves its own decision.

## Raw artifacts

- `phase4_baseline_raw/bench.txt` — benchmark numbers.
- `phase4_baseline_raw/report.txt` — latency percentiles and traffic counters.
- `phase4_baseline_raw/write_alloc_top.txt`, `write_cpu_top.txt` — write profiles.
- `phase4_baseline_raw/read_alloc_top.txt`, `read_cpu_top.txt` — read profiles.
- `phase4_baseline_raw/experiments.txt` — concurrency sweep, flush-interval and
  leader-lease prototypes.
