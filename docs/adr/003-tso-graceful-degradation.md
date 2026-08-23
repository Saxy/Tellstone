# ADR-003: TSO Graceful Degradation

Status: Accepted
Date: 2026-08-20

## Context

Tellstone uses a centralized Timestamp Oracle (TSO) hosted on the PD
leader to allocate globally monotonic timestamps. When the PD leader
dies, the TSO is unavailable. The question is: what happens to writes?

Three options were considered:

1. **Hard stop** — all writes block immediately when PD is unreachable.
2. **Pre-allocation only** — each node holds a pool of pre-allocated
   timestamps. Writes continue until the pool is exhausted, then block.
3. **Raft-only ordering** — single-region writes use Raft log index as
   the version, bypassing TSO entirely. Only cross-region operations
   need TSO.

Option 3 was rejected to keep the design simple and consistent with
TiKV's proven model. Options 1 and 2 were combined.

## Decision

**Pre-allocation with graceful degradation.** The PD leader distributes
batches of pre-allocated timestamps to each node. Writes use local
pools. When the pool is exhausted (PD still down), writes block and the
cluster enters read-only mode. When PD recovers, writes resume.

## How It Works

### Timestamp Format

```
Timestamp = (physical_ms, logical_counter)
```

- `physical_ms`: wall clock in milliseconds (NTP-synced)
- `logical_counter`: atomic uint64, increments within each millisecond

The PD leader allocates timestamps as `(now_ms, next_counter)`. The
counter is monotonically increasing across all nodes — no two nodes
receive overlapping ranges.

### Pre-Allocation Flow

```
1. Node starts, requests initial batch from PD
   PD → Node: [ts 1,000,000 — 2,000,000]  (1M timestamps)

2. Node allocates timestamps from local pool
   Write → atomically increment local counter → ts = pool[0]

3. Pool drops below 20% → node requests refill from PD
   PD → Node: [ts 2,000,001 — 3,000,001]

4. PD dies → node continues allocating from remaining pool
   Pool has 200K timestamps → at 100K writes/sec → 2 seconds headroom
```

### Batch Sizing

The PD sizes batches based on each node's observed write rate:

```
batch_size = max(min_batch, write_rate × target_headroom_seconds)
```

Default `target_headroom_seconds = 30`. A node doing 10K writes/sec
gets a batch of 300K timestamps (~30 seconds of headroom).

### Failure Timeline

```
T+0s     PD leader dies
T+0-3s   etcd leader election in progress
T+0-30s  Nodes continue writing from pre-allocated pools
T+30s    First node's pool exhausted → writes block
T+30s+   Remaining nodes exhaust pools one by one → write-only mode
T+3-5s   New PD leader elected (but nodes may not know yet)
T+5s     Nodes discover new PD leader, request refill
T+5s+    Pools refilled, writes resume
```

### Node Behavior by State

| State | Writes | Reads (strong) | Reads (stale) |
|-------|--------|----------------|---------------|
| PD available, pool full | OK | OK | OK |
| PD unavailable, pool has budget | OK | OK | OK |
| PD unavailable, pool empty | BLOCKED | BLOCKED | OK (from cache) |
| PD available, refilling | OK | OK | OK |

## Consequences

### Positive

- **Write availability during PD outages.** Pre-allocation provides
  seconds-to-minutes of continued writes, covering the typical
  etcd election window (3-5 seconds) with large margin.
- **No data loss.** Raft consensus ensures committed writes are durable
  regardless of PD state. The PD is only needed for *new* timestamps.
- **Automatic recovery.** When PD comes back, pools are refilled and
  writes resume. No manual intervention.
- **Predictable degradation.** Operators know exactly how long writes
  will survive (pool_size / write_rate). They can tune batch sizes
  for their tolerance.

### Negative

- **Eventually read-only.** If PD is down long enough, writes stop.
  This is a fundamental tradeoff of centralized TSO.
- **Timestamp waste.** Idle nodes consume pre-allocated timestamps that
  are never used. Mitigated by dynamic batch sizing.
- **Pool exhaustion race.** Two nodes could exhaust their pools at
  different times, causing partial write availability. This is
  acceptable — the cluster is degraded, not dead.

## Alternatives Considered

### Raft-only ordering (no TSO for single-region)

Single-region writes use Raft log index as the version. No PD
dependency. Cross-region operations use a reconciliation protocol.

Rejected because:
- Adds complexity to the version model (two different version schemes)
- Makes MVCC implementation harder (Raft index ≠ wall clock time)
- Diverges from TiKV's proven model without clear benefit at this stage
- Can be reconsidered in Phase 8 if cross-region transactions prove
  simpler with Raft-based ordering

### HLC (Hybrid Logical Clocks) everywhere

Each node has a local HLC. No PD dependency for timestamps.

Rejected because:
- Physical clock drift can cause timestamp conflicts between nodes
- Requires strict NTP enforcement and maximum drift rejection
- Adds fragility (cloud VMs can have clock jumps)
- Less predictable than centralized TSO

## References

- [TiKV Timestamp Oracle](https://docs.pingcap.com/tikv/stable/tikv-architecture#tsoservice)
- [Spanner: TrueTime](https://research.google/pubs/pub39966/)
- [Hybrid Logical Clocks](https://.cse.buffalo.edu/tech-reports/2014-04.pdf)
