# Phase 4 Plan: Raft batching + leader lease reads

Status: **implemented and measured — see `docs/perf/PHASE4_RESULTS.md`.**
Baseline: `docs/perf/PHASE4_BASELINE.md` (3-node, in-memory, production timing).
Scope of this document: the decisions that shape Phase 4, the options for each,
their measured or expected impact, and the order to implement them in.

## Decisions at a glance

| # | Decision | Recommended | Expected impact | Effort | Risk |
|---|---|---|---|---|---|
| D1 | Leader-lease reads | **A. `ReadOnlyLeaseBased` + safe fallback knob** | Read p50 250–360 µs → **~2 µs**; p99 2.3 ms → 19 µs; read TPS 78 k → 181 k; 4 msgs/read → 0 | S | M (clock assumptions; mitigated by CheckQuorum) |
| D2 | Transport flush latency | **A. Adaptive flush** | Sequential write ~600–720 µs → **~200 µs** at low concurrency, batching preserved under load | M | M |
| D3 | Write batching | **D. Instrument first, then B only if needed** | Batching already appears at 512 clients (3.19 msgs/write); a coalescing window trades latency for throughput | S→L | M |
| D4 | Batched apply | **A. Skip** | Apply is 46–60 ns; batching saves <1% of a write | — | — |
| D5 | Heartbeat/election tuning | **A. Config knobs now, B later** | Idle 40 msgs/s → ~20 msgs/s for 3 nodes; matters at 1 000 regions | S | L |
| D6 | Cheap write-path wins | **A. Do all three** | ~11% of write alloc bytes from `Status()`; one channel + map store per write; an `Entries` slice copy per AppendEntries | S–M | L |
| D7 | Raft log durability | **C. Separate phase** | No measured TPS change; closes a real correctness gap | L | H |

Effort: S ≤ 1 day, M ≤ 1 week, L > 1 week. “Risk” is risk to correctness or to
regressing existing behaviour.

---

## D1 — Leader-lease reads (highest value)

**Today:** `Node.LinearizableRead` → `raftNode.ReadIndex` in `ReadOnlySafe`
mode. The leader sends a heartbeat to every follower and waits for a quorum, so
every read pays 4 messages and a network round trip. Measured: p50 250–360 µs,
4.00 msgs/read, 74–80 k reads/s at 32 clients, while the local state machine
answers in 29 ns.

### Options

- **A. Switch `raft.Config.ReadOnlyOption` to `raft.ReadOnlyLeaseBased`.**
  `CheckQuorum` is already `true`. The leader answers `ReadIndex` from its local
  commit index while its lease (derived from heartbeat acknowledgements) is
  valid. A `--raft-safe-reads` config flag can force `ReadOnlySafe` for operators
  who do not accept lease assumptions.
  - Prototype measured: p50 **2 µs**, p99 **19 µs**, 13 allocs/op,
    **0 msgs/read**, 181 k reads/s at 32 clients.
  - Caveat: lease reads assume bounded clock drift between nodes. This is the
    standard etcd/TiKV trade and is safe inside a single datacenter; it is not
    appropriate for unbounded clock skew.
- **B. Implement an explicit lease in `cluster.Node`** (last quorum-ack time +
  election-timeout window) and serve reads directly from `appliedIndex` when
  valid. Slightly faster than A (skips the raft goroutine hop), more code, same
  clock assumption, and duplicates logic etcd raft already has.
- **C. Keep `ReadOnlySafe` and coalesce concurrent `ReadIndex` calls** into one
  heartbeat round trip. Safe under any clock model, but single-reader latency
  stays ~250 µs; it only helps concurrency. More machinery than A.
- **D. Do nothing.**

### Recommendation
**A**, with the fallback knob. It is a ~10-line change with a measured 100–200×
latency win and removes all per-read consensus traffic. If the deployment
tolerates neither clock assumptions nor safe-mode latency, revisit C as a second
step rather than replacing A.

### Exit criteria
- `LinearizableRead` p50 ≤ 10 µs, p99 ≤ 100 µs on the 3-node benchmark.
- Read messages/op = 0 at ≤128 clients.
- Cluster tests (including failover and split) pass; a leader that loses quorum
  stops serving lease reads within the election timeout.
- `--raft-safe-reads` reproduces the old behaviour exactly.

---

## D2 — Transport flush latency vs batching

**Today:** `batchConn` accumulates outbound raft messages and flushes every
`batchFlushInterval = 100 µs` (or when 64 KiB / 255 messages accumulate). This
aggregates packets well under load (0.41 frames/write at 32 clients) but adds up
to 100 µs twice per synchronous write.

Measured by temporarily changing the interval:

| Interval | `ProposeSequential` |
|---:|---:|
| 20 µs | **203 µs** |
| 100 µs (current) | 594–719 µs |
| 250 µs | 1 127 µs |

### Options

- **A. Adaptive flush.** Flush immediately when the sender has been idle and the
  pending batch is a single message; keep the 100 µs timer while batches are
  accumulating. Goal: low-concurrency latency of the 20 µs setting without
  giving up packet aggregation under load.
- **B. Lower the fixed interval** (e.g. 20–50 µs). Simplest, but multiplies write
  syscalls at high request rates — exactly what Phase 3 fixed for the client
  network path.
- **C. Flush on every completed `Ready` batch** instead of on a timer. Removes
  the timer from the critical path but may flush partial batches while raft is
  still producing messages.
- **D. Leave it.** Latency is acceptable for a consensus store; batching is the
  priority.

### Recommendation
**A.** Measure syscalls/frame counts under high concurrency before and after;
reject if frames/write rises materially. If A proves too fiddly, B with 50 µs is
the safe fallback.

### Exit criteria
- Sequential write p50 ≤ 150 µs (from 274–337 µs) on loopback.
- Concurrent write throughput at 32 clients does not regress; frames/write stays
  under ~1.
- Syscall share in the write CPU profile does not rise above the baseline 32%.

---

## D3 — Write batching

**Today:** etcd raft batches proposals into `Ready` and the transport batches
frames, but each client write still costs 8 messages (four per follower: append,
ack, commit advance, ack) until concurrency reaches 512 clients:

| Writers | ops/s | msgs/write |
|---:|---:|---:|
| 32 | 63 219 | 8.00 |
| 128 | 72 882 | 7.96 |
| 512 | 89 239 | 3.19 |

The roadmap asks for “batch N entries or timeout before AppendEntries” and
“pipelined replication”. Reimplementing etcd raft's batching would be wrong;
the question is whether to add an **application-level** coalescing layer.

### Options

- **A. Rely on etcd raft.** Do nothing beyond D6 and D2. At high concurrency the
  existing batching already reduces messages; throughput scales 63 k → 89 k.
- **B. Proposal coalescing window.** A per-node queue holds incoming proposals
  for a short window (e.g. 50–200 µs) or until N accumulate, then steps them
  into raft together. This lowers msgs/write at moderate concurrency but adds
  the window to every write's latency. Attractive only if the target is
  throughput at 64–256 clients more than tail latency.
- **C. Multi-op log entries.** Encode several client operations into one raft
  entry, with a new opcode and per-op results in the apply completion. Cuts both
  messages and log entries, but changes the FSM wire format, the proposal
  tracker (N waiters per entry), and crash/replay semantics.
- **D. Instrument first.** Add counters for entries per `Ready`, `Ready` batches
  per second, and messages per `AppendEntries` (cheap atomics), run the
  concurrency sweep, then choose A/B/C with data.

### Recommendation
**D now → B only if the target workload shows a real ceiling below 32 clients.**
The baseline says throughput still rises to 512 clients and apply is negligible;
the case for C is weak. The batched-apply sibling task should not be coupled to
this decision.

### Exit criteria (if B is chosen)
- msgs/write at 32 clients drops below 5 without raising p50 beyond the D2
  target.
- No proposal ordering regressions: one key's writes remain serialized; the
  proposal tracker still correlates each entry to its waiter.

---

## D4 — Batched apply

**Today:** `processReady` applies committed entries one by one; each
`FSM.Apply` decodes and calls `Dispatcher.Dispatch`. Measured cost: **46–72 ns**,
1 alloc/op (the decoded key string). A 64-entry batch is a few microseconds,
against a ~600 µs write.

### Options
- **A. Skip.** Apply is not the bottleneck. Revisit only if a future
  persistent/remote state machine makes dispatch expensive.
- **B. Batch the dispatcher API** (one call per shard per ready batch). More
  plumbing; preserves per-key order only if the batching keeps global order or
  groups by key.
- **C. Apply shards in parallel.** Violates the Raft “apply in log order”
  contract unless carefully restricted to independent keys.

### Recommendation
**A. Skip for Phase 4.** Record the measurement so the decision is explicit;
revisit after D1/D2 when the write path is re-profiled.

---

## D5 — Heartbeat and election tuning

**Today:** `HeartbeatTick 1`, `ElectionTick 10`, `TickInterval 100 ms` → the
leader sends 2 heartbeats per 100 ms to the other nodes; measured idle traffic is
**40 msgs/s** for a 3-node cluster. ADR-002 targets thousands of regions per
node, where that becomes tens of thousands of messages per second of pure
chatter.

### Options
- **A. Expose `--raft-heartbeat-tick`, `--raft-election-tick`,
  `--raft-tick-interval`.** Default heartbeat 2 (200 ms) with election 10
  (1 s) gives 5 heartbeats per election timeout and halves idle chatter.
- **B. Coalesce heartbeats across regions.** Raft requires one heartbeat per
  group, but their frames already share a TCP write. True coalescing would need
  a multi-group payload format; defer until multi-region is real.
- **C. Leave production defaults, tune per deployment.**

### Recommendation
**A now, B when region counts grow.** Verify D1's lease reads still hold under
the longer heartbeat interval: the lease is refreshed by the same heartbeats, so
keep at least 3–5 heartbeat intervals per election timeout.

### Exit criteria
- Idle msgs/s for a 3-node cluster drops from 40 to ~20 with the new defaults.
- No election instability under load; failover test still elects a new leader
  within the configured timeout.

---

## D6 — Cheap write-path wins

These are independent, low-risk, and address measured allocation sources.

1. **Cache leadership instead of calling `Status()` per write.**
   `ProposeAndWait` calls `IsLeader()` → `raftNode.Status()`, which deep-copies
   the progress map (`getProgressCopy`, `Inflights.Clone`) — ~11% of write alloc
   bytes. Update an atomic `isLeader` from the `Ready.SoftState` / leadership
   transitions in `processReady`, or from a single status poll on state changes.
   Keep the authoritative check for `ProposeAndWait` if the cache can be stale;
   the proposal still fails safely with `ErrNotLeader`.
2. **Stop copying the log window in `Storage.Entries`.** Return a sub-slice of
   `s.entries` when the size limit does not truncate, matching etcd
   `MemoryStorage`. The caller only reads it. Saves the 7.7% `Entries` slice
   allocation per AppendEntries.
3. **Reduce the proposal tracker's per-write cost.** One channel allocation and
   one `sync.Map` store per proposal (`proposalTracker.add`, 3.8% of objects).
   Options: a sharded map, a completion ring indexed by ID, or completing a whole
   `Ready` batch's proposals in one pass. Must preserve the
   remove-on-timeout semantics and the “unknown ID is a no-op” contract.

### Options
- **A. Do all three.**
- **B. Do (1) and (2), defer (3).**
- **C. Skip; accept the allocation profile.**

### Recommendation
**A**, with (3) last and measured separately so it can be reverted if it does not
move the numbers.

### Exit criteria
- Sequential write allocs/op < 240 (from 248) and B/op < 10 500.
- No change in proposal timeout/cancel behaviour; `-race` passes on the cluster
  package.

---

## D7 — Raft log durability (scope decision)

`internal/cluster.Storage` is currently **in-memory**: `processReady` calls
`storage.Append`/`SetHardState`, which write slices, with no fsync and no
recovery path. A process restart loses the consensus log. The roadmap's “WAL
group-commit” item assumes a disk log that does not exist yet.

### Options
- **A. Keep Phase 4 non-durable, document the gap.** No performance work, but the
  roadmap's crash-safety claim must be corrected and the limitation made
  explicit.
- **B. Add a disk-backed Raft log now** (HardState + entries + snapshot,
  fsync/group-commit, replay on start). This is the prerequisite for real
  durability and a substantial project on its own.
- **C. Treat durability as its own phase** (recommended): keep Phase 4 focused on
  latency/throughput, and open a dedicated “Phase 4b / Phase 5: durable Raft
  log” with its own baseline and crash tests.

### Recommendation
**C.** Phase 4's measured wins (lease reads, flush latency) are independent of
durability, and the current in-memory log already makes “WAL group-commit”
unenforceable. Do not mix a correctness project with the performance phase.

---

## Proposed sequence

1. **P4-1 (D1):** `ReadOnlyLeaseBased` + `--raft-safe-reads` fallback. Measure
   the same report/benchmarks; target p50 ≤ 10 µs, 0 msgs/read.
2. **P4-2 (D2):** adaptive transport flush. Re-run sequential and concurrent
   write numbers plus the CPU profile; reject if frames or syscalls regress.
3. **P4-3 (D6):** leadership cache, `Entries` sub-slice, proposal tracker.
   Measure allocs/op after each sub-change.
4. **P4-4 (D5):** heartbeat/election knobs and new defaults; re-run the idle
   traffic and failover tests.
5. **P4-5 (D3):** add the batching instrumentation, run the concurrency sweep,
   then decide whether proposal coalescing is worth it.
6. **P4-6 (D4/D7):** record the explicit “skip batched apply” and durability
   decisions; open a follow-up for the durable Raft log if wanted.

Every step records before/after numbers in a `docs/perf/PHASE4_RESULTS.md` and
reverts anything that does not move the metric.

## Verification

- `go test ./...`, `go test -race ./internal/cluster/... ./server/...`.
- Cluster correctness suites: leader election, failover, split, federation,
  forwarded writes, `ProposalTimeout`, `NonLeaderRejectsPropose`.
- Benchmark gates: `BenchmarkPhase4*` and `TestPhase4BaselineReport` with pinned
  cores; compare on the same machine, same GOMAXPROCS.
- A new failover test for lease reads: partition the leader, confirm reads stop
  being served as linearizable once the lease expires.

## Open questions for the decision

1. **Clock model:** is a single-datacenter, NTP-synchronized deployment the
   target? If yes, D1-A is safe. If nodes may drift arbitrarily, choose D1-C.
2. **Latency priority:** optimize p50 at low concurrency (D2-A) or raw throughput
   at high concurrency (D3-B/C)? They trade against each other.
3. **Durability:** is a non-durable Raft log acceptable for this phase, or should
   the durable log jump ahead of the batching work?
4. **Multi-region:** is the next target one region per node or thousands? This
   determines whether D5-B (heartbeat coalescing) is urgent.
5. **API stability:** may the FSM entry format change (D3-C), or must the current
   single-op entries stay compatible across rolling upgrades?
