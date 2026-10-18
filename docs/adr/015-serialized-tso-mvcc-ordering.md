# ADR-015: Serialized TSO for MVCC Ordering (Phase 12)

Status: Accepted
Date: 2026-10-14

## Context

Phase 12 ([MULTI-CLUSTER-PLAN §Phase 12](../MULTI-CLUSTER-PLAN.md)) adds MVCC
snapshot isolation. Its first hard requirement is an ordering property:
**if transaction A's commit is observed before transaction B starts, then A's
commit timestamp must be lower than B's start timestamp.** A tombstone must
never be stamped below the live version it deletes, or `Resolve` will keep
returning the live frame and the row will be undeletable.

ADR-003 chose a **pre-allocated timestamp pool** per node: the PD leader hands
each node a disjoint batch, and each node increments its own local counter.
[ADR-010](010-phase2-pd-tso-decisions.md) §3 already anticipated that these
timestamps would feed MVCC at Phase 12, and **ADR-003 §"Timestamp Format" claims
the counter is "monotonically increasing across all nodes"**. That claim is not
true of the pool as built.

### Why the pool breaks MVCC

Disjointness (no two nodes share a timestamp — required for uniqueness) is
*not* the same property as real-time monotonicity (a later allocation is
always greater). A node hands out the values in its own batch in local order,
but the **batches themselves are granted out of order, and a node keeps serving
its old batch while another node has already moved to a newer range**. A later
statement routed to a different node can therefore receive a lower timestamp
than an earlier statement.

This is not a clock-skew edge case; it reproduces with NTP-synced nodes. In one
traced run, after a port leaked a read-back loop onto the wrong node, a
sequential insert-then-delete on the same row gave:

```
node-1  INSERT meta id=1   ts=...314347010   (earlier statement, higher ts)
node-3  DELETE meta id=1   ts=...301764101   (later statement,  lower ts)
```

`mvcc.Resolve` saw the live frame as newest, the row survived the delete, and
`DROP TABLE` was refused with:

```
sql: table is not empty: table "meta" still has rows
```

The failure surfaced as an intermittent (~7% of runs) regression in
`scripts/manual-ddl-test.sh`; the root cause is deterministic, the *trigger*
(two statements landing on two nodes) was not.

## Decision

**MVCC timestamps are allocated one at a time through the etcd watermark, whose
compare-and-swap is the cluster's single ordering point. DDL keeps the
pre-allocated pool.**

Concretely, the SQL timestamp oracle is wired to a new
`PDNode.NextTimestamp(ctx)`, which calls the granter's `GrantRange(ctx, 1)` — a
single-timestamp grant straight off the shared watermark — instead of the
node-local pool. This bypasses ADR-003's pre-allocation for every statement
that stamps a version:

- autocommit `INSERT` / `UPDATE` / `DELETE`: one write timestamp per statement,
- `BEGIN`: one start timestamp,
- `COMMIT`: one commit timestamp.

DDL (`CREATE TABLE`, `DROP TABLE`) continues to draw from the pool: the catalog
is resolved by raw value in Raft-log order, not by timestamp, so it does not
need the global ordering property and benefits from the outage headroom.

Autocommit `SELECT`s take no timestamp at all — they resolve at
`math.MaxUint64` (read the newest visible frame) and add no round-trip.

## How It Works

The granter writes the watermark through a CAS on the etcd key's
`ModRevision`. `NextTimestamp` grants exactly one value, so:

```
watermark ---------watervalue--------- key
start = max(watermark + 1, PackTimestamp(now_ms, 0))
CAS(key, start, prev_revision)     // exactly one caller wins
watermark = start
```

Because the grant is size 1, `end == start` and the next CAS builds directly on
the returned value. Under concurrency:

- every successful CAS observes the previous winner's value and returns a
  strictly greater one, and
- a caller only returns *after* its CAS succeeds, so **allocation return order
  equals value order equals real-time order**.

Two callers racing cannot receive the same value, and the one that returns
later always holds the larger timestamp. This is exactly the MVCC ordering
requirement, and it holds across all regions and all SQL nodes.

The version codec in `internal/mvcc` (`Resolve` / `Latest` / `Append` /
`Versioned`) is unchanged; only the source of the timestamp changes. MVCC still
applies to catalog-backed rows only (`plan.Schema != nil || plan.Join != nil`);
the implicit `tellstone` table and the binary frontend remain raw.

## Consequences

### Positive

- **The ordering property is guaranteed, not probabilistic.** Drop-after-delete
  across nodes can no longer invert: a delete's tombstone is always stamped
  above any insert whose commit was observed earlier.
- **One ordering point.** Writes, read-write conflict detection
  (`keyCommittedAfter`) and snapshot reads all share the same monotone clock,
  so "observed before" and "lower timestamp" coincide.
- **Regression coverage.** The harness now exercises the exact bug: a row
  inserted on one node and deleted on a different node, in both directions. A
  temporary revert to the pool path fails it deterministically on every run
  (5 checks), while the serialized path passes (0).
- **No change to the on-disk format or the codec.** Existing MVCC envelopes are
  untouched.

### Negative

- **A control-plane round-trip on the write hot path.** Every versioned write
  now pays one etcd CAS latency, where the pool was a local atomic increment.
  This directly contradicts ADR-003's rationale that pre-allocation keeps PD
  off the hot path. Throughput is bounded by the serialized watermark, not by
  per-node allocation.
- **No writer headroom through a PD outage for versioned writes.** Unlike DDL,
  an autocommit write or `BEGIN`/`COMMIT` fails (or blocks, within the 5s
  client timeout) when the watermark is unreachable. This narrows ADR-003's
  graceful degradation: MVCC writes are effectively "hard stop" while DDL keeps
  the "pre-allocation" behavior.
- **Clock component still present.** The value is still `max(watermark+1,
  physical_ms<<bits)`, so a node clock jumping forward can advance the
  watermark; this only widens gaps, it cannot invert order.

## Alternatives Considered

### Hybrid Logical Clocks (no PD on the hot path)

Each node keeps a local HLC and the maximum observed timestamp. Rejected in
ADR-003 and rejected again here: it moves the ordering guarantee onto clock
sync and max-drift assumptions, which is precisely the fragility MVCC cannot
tolerate.

### Keep the pool, hand batches out in grant order

Have the PD grant one batch at a time and wait for a node to exhaust it before
granting the next, so batch ranges are consumed in order. Rejected because it
is a serialized allocation with *more* coordination and less availability than
the direct CAS, while still leaving the "old batch served after a newer grant"
window unless nodes are forced to give up short batches — at which point it is
the direct grant with extra machinery.

### Raft-log-index versions for single-region writes

Version by per-region Raft index, TSO only for cross-region. Rejected for the
same reasons as ADR-003: two version schemes, and a Raft index is not a
comparable clock for snapshot reads.

### Extend serialized ordering to DDL as well

Simpler mental model (one allocator). Rejected for now — catalog entries are
ordered by Raft-log application, not timestamp, so DDL gains no correctness and
would lose its outage headroom. Can be revisited if 2PC needs a single
allocator for catalog and row writes together.

## References

- [MULTI-CLUSTER-PLAN §Phase 12](../MULTI-CLUSTER-PLAN.md) — transactions,
  MVCC, write intents, conflict detection, 2PC.
- [ADR-003](003-tso-graceful-degradation.md) — pre-allocated timestamp pool;
  this ADR narrows its monotonicity claim for MVCC.
- [ADR-010](010-phase2-pd-tso-decisions.md) §3 — batch allocation decisions;
  anticipated wiring timestamps into MVCC at Phase 12.
- [ADR-013](013-table-namespaces-phase9.md) §7 — row-id write-intent anchor
  (still unimplemented).
- `internal/cluster/pdnode.go` — `NextTimestamp`; `internal/cluster/tso.go` —
  `TSOPool` / granter `GrantRange`.
- `server/server.go` — `tsoOracle` wiring; `internal/sql/exec.go` — `mvccStore`.
- `scripts/manual-ddl-test.sh` — cross-node ordering regression test.
