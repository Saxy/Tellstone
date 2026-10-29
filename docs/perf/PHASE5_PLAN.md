# Phase 5 Plan: SQL KV Fast-Path + Final Polish

Status: **proposal — no Phase 5 code has been changed.**
Reference baseline: the original baseline in `docs/perf/BASELINE.md` plus the
Phase 1–4 results. **Phase 5 does not take a new baseline**: it is the last
phase, and its exit measurement is the final comparison of everything against
the original baseline (the "fazit").

Scope: close the gap between a SQL statement executed through the real
planner/executor/wire and the raw KV hot path underneath it. The roadmap's
Phase 5 tasks (KV bypass detector, direct shard routing, prepared fast-path
cache, expr inliner, string interning, micro-opt passes) map onto the decisions
below.

## What the gap is made of

A primary-key `SELECT` (the pgbench read statement) currently costs:

1. **Parse + translate** — `pg_query.Parse` (cgo/protobuf) then `translateNode`
   (AST → `Plan`). From the Phase 3 profile this is ~40% parse + ~56% translate
   (cum) + ~15% catalog lookup of the allocation budget. The simple-query path
   (`TranslateAll`) does this on every `Q` message; the extended protocol
   (`handleParse`) does it once per `Parse` and caches the plan in `c.stmts`.
2. **A point read is not one key.** A row is one key *per column*
   (`ColumnKey(db, table, rowID, col)`, `keyspace.go`), and `Router.Dispatch`
   hashes the **whole** key, so a row's columns scatter across every shard
   (`router.go`). `readRow` (`rows.go`) therefore runs `rowExists` (one `Get`)
   **plus** `Router.ScanPrefix`, which walks **every shard's** ordered index,
   clones each value, materializes per-shard runs, and k-way merges them. A
   3-column row over 16 shards is ~16 shard walks + a heap merge for one lookup.
3. **Point-read executor is not on the arena.** The Phase 11 scan/join operators
   run on an `ExecCtx` arena (`executor.go`), but `execSelectRow` (`exec.go`)
   predates it: it allocates a `[][]byte` row, a `[]Column` header, and a
   `wireCell` result per column, then `finalizeResult` builds the result set.
4. **Wire/value decode** — `wireCell` and `EncodeValue` are not type-specialized;
   table/column names are re-allocated strings.
5. **No end-to-end harness** measures "pgbench RW vs raw KV TPS", which is the
   phase's own exit criterion.

## Decisions at a glance

| # | Decision | Recommended | Expected impact | Effort | Risk |
|---|---|---|---|---|---|
| D1 | Point-read storage access | **C. direct per-column `Get` fast path** (A as the end-state) | Kill the all-shards `ScanPrefix`+merge per PK read; point read → N direct gets | M | M |
| D2 | Parse/translate overhead | **A. simple-query plan cache** (B is already there) | Repeated SQL skips cgo parse + translate, the two largest alloc sources | S–M | L |
| D3 | Point-read executor allocations | **A. migrate `execSelectRow` to the arena + direct wire render** | `SELECT pk` toward 0 allocs; matches the Phase 11 contract | M | M |
| D4 | `Router.ScanPrefix` allocations | **A. stream the merge, don't materialize/clone** | Scans (`BenchmarkScanSelect`, range reads) stop cloning per key | S–M | L |
| D5 | Wire/value micro-optimizations | **A. cheap wins, measure each** | Pre-encoded frames, specialized codecs, name interning | S | L |
| D6 | End-to-end comparison harness | **A. pgbench-RW vs raw-KV TPS + original-baseline fazit** | Produces the phase's exit number | S | L |

Effort: S ≤ 1 day, M ≤ 1 week, L > 1 week. "Risk" is risk to correctness or to
regressing existing behaviour.

---

## D1 — Point-read storage access (highest value)

**Today:** `readRow` = `rowExists` (1 `Get` on the PK key) + `ScanPrefix` over
every shard, cloned and merged. The number of shards, not the number of columns,
sets the cost of a point lookup.

### Options

- **A. Pack the row into one value.** Store the whole row under a single key
  (PK key → packed cells). A point read becomes one `Get`/`Dispatch`. Bonus:
  it removes the partial-row crash window documented in `rows.go` (a row would
  be written and read atomically). Cost: a wire/storage format migration, the
  write path rewrites, and full-table/range scans must decode packed rows. It is
  the correct end-state, but it is a storage-format project, not a micro-opt.
- **B. Prefix-aware sharding.** Hash by `RowPrefix` instead of the full key, so
  a row's columns co-locate on one shard and `ScanPrefix` becomes a single-shard
  walk. Keeps the scan shape, improves locality, but still does N column keys +
  clones, and changes redistribution semantics (a whole row lives on one shard).
- **C. Direct per-column `Get` fast path.** For a plan whose point key is a
  proven single equality, issue one `Get` per declared column instead of
  `ScanPrefix`. No format or sharding change; the walk+clone+merge disappears.
  Still N round trips (one per column), each a cheap `Dispatch`.
- **D. Do nothing.**

### Recommendation
**C now** (small, low-risk, no format change; removes the all-shards scan that
dominates `BenchmarkPGSimpleQueryPK` at `shards=16`), then treat **A** as the
end-state to reach after the fast path is proven. B is a sharding-semantics
change best decided with the placement/region work, not inside a perf phase.

### Exit criteria
- `BenchmarkPGSimpleQueryPK` (shards=16) no longer scales cost with shard count;
  allocs/op drop from the current per-scan clone level to N small gets.
- Scan-based SELECTs (full table, range) are unchanged and still pass.
- No change in row semantics: null columns, extra/leftover keys, and
  non-declared column keys still behave as today.

---

## D2 — Parse/translate overhead

**Today:** every simple-query `Q` message runs `pg_query.Parse` + `translateNode`
(`TranslateAll`). Prepared statements (`Parse`/`Bind`/`Execute`) already cache
the plan in `c.stmts` and re-execute it, so the extended protocol is largely
there; the simple path is not.

### Options

- **A. Simple-query plan cache.** Cache `TranslateAll` output keyed by the raw
  (or lightly normalized) SQL text, bounded and invalidated by DDL/`search_path`
  changes. Repeated identical `SELECT ... WHERE pk=$1` skips cgo parse and
  translate entirely.
- **B. Prepared-statement execution fast path.** For a cached plan, bind and
  execute with minimal per-call allocation (params slice reuse, no re-derivation
  of formats). Partially exists; tighten it.
- **C. Both** (A for simple, B for extended).
- **D. Skip** and rely on clients to use prepared statements.

### Recommendation
**C.** A is the single biggest win in the profile (parse + translate together
are the top alloc source). B is cheap and makes the pgbench extended-protocol
path zero-alloc in steady state. Measure both; revert either if it does not move
the numbers.

### Exit criteria
- `BenchmarkPGWireSimpleSelectPK` allocs/op drop from ~106 to a small constant
  on the second and later iterations of a repeated query.
- A statement that is semantically affected by a concurrent DDL must not be
  served from a stale cached plan (cache key/invalidation covers it).

---

## D3 — Point-read executor allocations

**Today:** `execSelectRow` allocates a row slice, a column header slice, and one
`wireCell` per returned column, then `finalizeResult`. The scan operators already
run zero-alloc on the `ExecCtx` arena; the point path does not.

### Options

- **A. Migrate to the arena + direct wire render.** Reuse per-statement (or
  per-connection) buffers for the projection and result set; where a stored value
  is already in wire format, pass it through without an intermediate `wireCell`.
- **B. Leave it**, since D1/D2 may remove more cost than this.
- **C. Skip.**

### Recommendation
**A**, after D1. It brings the point read under the same zero-allocation contract
the Phase 11 operators already follow, which is the phase's stated goal.

### Exit criteria
- `BenchmarkPGSimpleQueryPK` approaches 0 allocs/op on the read path (excluding
  the one-time harness row-id encoding, which the benchmark already hoists).
- Correctness suites for projection, hidden ORDER BY/DISTINCT columns, and CASE
  expressions still pass.

---

## D4 — `Router.ScanPrefix` allocations

**Today:** `ScanPrefix` clones every value into a `scanKV`, builds one slice per
shard, and merges with a `container/heap`. Full scans and range reads pay a
clone + string per key.

### Options

- **A. Stream the merge.** Walk shards in key order and deliver directly to the
  callback without materializing runs (or reuse pre-sized buffers), so values are
  handed out owned-and-cloned only when the caller retains them.
- **B. Buffer reuse only.** Keep the merge shape but pool the `scanKV` slices.
- **C. Skip.**

### Recommendation
**A** if the point read still uses a scan after D1; otherwise this matters only
to `BenchmarkScanSelect` and can be a smaller follow-up within the phase. Either
way measure `BenchmarkRouterScanPrefix` before/after.

### Exit criteria
- `BenchmarkScanSelect` and `BenchmarkRouterScanPrefix` allocs/op drop without
  changing key order or the return-false early-stop contract.

---

## D5 — Wire/value micro-optimizations (final polish)

Cheap, independent, profile-driven. Do each behind a measurement; keep only the
ones that move numbers.

1. **Pre-encoded frames** for the most common command tags and
   `ReadyForQuery`/`CommandComplete` shapes (Phase 3 already did the row
   description/DataRow scratch; extend to tags).
2. **Type-specialized `EncodeValue`/`wireCell`** for `TypeBigInt`/`TypeVarchar`
   (the pgbench column types), avoiding reflection and interface dispatch.
3. **String interning** for table/column names and result column headers, so the
   header is a pointer rather than a fresh string per row.

### Recommendation
**A**, in this order, each measured. Revert anything that does not move the
benchmark.

### Exit criteria
- No single micro-opt is kept without a before/after number.
- `-race` and the full test suite stay green.

---

## D6 — End-to-end comparison harness ("fazit")

**Today:** there are micro benchmarks (`BenchmarkKVGet`, `BenchmarkRouterGet`,
`BenchmarkPGWireSimpleSelectPK`, `BenchmarkPGSimpleQueryPK`, `BenchmarkScanSelect`)
but no single repeatable number that says "pgbench RW approaches raw KV TPS" or
compares the finished engine against the original baseline.

### Options

- **A. Build the harness.** A pinned-core, in-process pgbench-RW loop (a fixed
  mix of PK select/insert/update/delete through the real pgwire server) reported
  alongside the raw KV TPS micro benchmarks, plus a final
  `docs/perf/PHASE5_RESULTS.md` and `docs/perf/FAZIT.md` that table original
  baseline → final for every phase's headline metric.
- **B. Reuse the existing benchmarks only.** No new harness; the fazit tables
  the micro numbers that already exist.
- **C. Skip the fazit.**

### Recommendation
**A.** It is the phase's exit criterion and the "compare everything to the
original baseline" deliverable the user asked for.

### Exit criteria
- pgbench-RW throughput is reported on the same machine/core-pinning as the
  original baseline, so the comparison is apples-to-apples.
- `docs/perf/FAZIT.md` carries the original-baseline → final table for Phases 1–5.

---

## Proposed sequence

1. **P5-1 (D2):** simple-query plan cache + prepared-statement execution fast
   path. Measure `BenchmarkPGWireSimpleSelectPK`.
2. **P5-2 (D1-C):** direct per-column `Get` point-read fast path. Measure
   `BenchmarkPGSimpleQueryPK` at shards 1/4/16.
3. **P5-3 (D3):** migrate `execSelectRow` to the arena + direct wire render.
   Measure allocs/op on the point read.
4. **P5-4 (D4):** stream `Router.ScanPrefix` (if a scan path remains hot).
5. **P5-5 (D5):** the three cheap wire/value wins, each measured.
6. **P5-6 (D6):** the pgbench-RW harness, then the original-baseline fazit.

Each step records before/after in `docs/perf/PHASE5_RESULTS.md` and reverts
anything that does not move the metric. The final `docs/perf/FAZIT.md` compares
every phase against the original baseline.

## Verification

- `go test ./...`, `go test -race` on changed packages.
- Correctness suites: `translate`, `executor`, `integration`, `stagea/b/c`,
  `dml_catalog`, `introspection`, `transcript`, `expr`.
- Benchmarks: `BenchmarkPGWireSimpleSelectPK`, `BenchmarkPGSimpleQueryPK`,
  `BenchmarkScanSelect`, `BenchmarkHashJoin`, `BenchmarkRouterScanPrefix`,
  `BenchmarkRouterGet`/`Set`, `BenchmarkKVGet`/`Set` — pinned cores, same
  machine as the original baseline.

## Open questions for the decision

1. **Row storage shape:** adopt the low-risk fast path (D1-C) only, or also plan
   the single-key packed-row end-state (D1-A) this phase? A also fixes the
   partial-row crash window, but it is a format migration.
2. **Scan priority:** after the point-read fast path lands, is `ScanPrefix`
   streaming (D4) worth doing now, or is the point path the whole story?
3. **Harness scope:** is an in-process pgbench-RW loop sufficient for the fazit,
   or must the final number come from the real `pgbench` binary?
4. **Cache invalidation:** for D2, is DDL/`search_path` invalidation enough, or
   must the cache also key on the current database/user?
