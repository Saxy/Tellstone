# Phase 5 Results

Baseline: the original baseline in `docs/perf/BASELINE.md` and the Phase 1–4
results. Plan: `docs/perf/PHASE5_PLAN.md` (decisions D1–D6). Method is the same
as every phase: benchmarks pinned with `taskset -c 0-15`, same machine (Ryzen
9950X / WSL2). Raw output: `docs/perf/phase5_before_raw/` (before) and
`docs/perf/phase5_results_raw/` (after).

Status: **D1-A and D2-A landed and measured.** D3-A, D5-A and D6-A are queued.

## M1: Packed row storage (D1-A) — DONE

### Change
A catalog row is now a **single key holding one packed value** instead of one key
per column. The key is the row prefix (`db/table/<row-id>/`; the empty column
segment can never collide with a real column name), and the value is every
column's encoded bytes in schema order with a per-column presence flag
(`internal/sql/rows.go`: `packRow`/`unpackRowInto`).

- `insertRow` is one `SetIfAbsent` — the row is written and read atomically, so
  the old partial-write compensation (`sweepRow`/`compensateRow`) is gone and the
  crash window it documented is closed.
- `readRow` is one `Get` + unpack; `updateRow` is one read-modify-write;
  `deleteRow`/`deleteRowGroup` are one `Delete`.
- The scan assembler (`scan.go`) decodes each packed key as a whole row instead
  of grouping per-column keys, and ignores leftover per-column keys.
- The virtual catalog relations (`pgcat_store.go`) project packed rows, so they
  flow through the same read path.
- DDL invalidation, ANALYZE counts, EXPLAIN bounds, and the cluster split rule
  all still hold: a packed row key is its own row boundary, so a split snaps to
  it exactly as before.

The keyspace grammar itself is unchanged; `RowPrefix`/`RowPrefixOf` now describe
the packed key, which is a strict simplification for region splitting (there is
no longer a multi-key range to cut).

### Point read (`BenchmarkPGSimpleQueryPK`)

| Shards | Before | After | |
|---:|---:|---:|---|
| 1 | 996.5 ns, 1 232 B, 20 allocs | **182.7 ns, 232 B, 6 allocs** | −82% ns, −96% allocs |
| 4 | 2 324 ns, 3 500 B, 25 allocs | **183.2 ns, 232 B, 6 allocs** | −92% ns, −97% allocs |
| 16 | 7 389 ns, 12 256 B, 38 allocs | **198.9 ns, 232 B, 6 allocs** | **−97% ns, −98% allocs** |

The point read no longer scales with shard count: at 16 shards it was ~37× the
single-shard cost (the old row was an all-shards `ScanPrefix` + clone + k-way
merge per lookup); it is now flat at ~180–200 ns across 1/4/16 shards. The
remaining 6 allocs are `readRow`'s scratch (rowCells + owned buffers + per-column
copies) and the row-key string — D3's target.

### Scans (`BenchmarkScanSelect`)

Unchanged within noise (rows=1024: 244 µs before → 192 µs after), confirming the
packed layout does not regress the scan path.

## M2: Simple-query plan cache (D2-C, part 1) — DONE

### Change
`handleQuery` now caches `TranslateAll` output per connection, keyed by the exact
query text (`pgwire.go`). A repeated `Q` message skips `pg_query.Parse` +
`translateNode`; the cached plans are re-resolved against the live catalog on
every execute, the same idempotent resolve prepared statements already rely on.

Correctness guard: `Server.schemaGen` is bumped on every successful CREATE/DROP
TABLE, and a connection's cache is dropped and rebuilt when it is stale, so a
cached plan is never served against a schema a later DDL removed
(`TestSimpleQueryPlanCacheInvalidatedByDDL`).

### Wire round trip (`BenchmarkPGWireSimpleSelectPK`)

| Metric | Before | After |
|---|---:|---:|
| Latency | 27 003 ns | **13 070 ns** |
| Bytes/op | 4 932 B | **1 617 B** |
| Allocs/op | 106 | **45** |

The remaining 45 allocs are the executor + wire rendering, not translation.

## Remaining (queued)

- **D2-C part 2**: prepared-statement execution fast path (the plan is already
  cached; tightening the Bind/Execute allocation).
- **D3-A**: migrate `execSelectRow` / `readRow` to a reusable scratch so the
  point read's remaining 6 allocs/232 B go to zero.
- **D5-A**: pre-encoded command tags, type-specialized `EncodeValue`/`wireCell`,
  column-name interning — each measured.
- **D6-A**: the pgbench-RW vs raw-KV-TPS harness and the original-baseline
  `docs/perf/FAZIT.md`.

## Verification

- `go build ./...` — clean.
- `go test ./...` — clean.
- `go test -race ./internal/sql/ ./internal/cluster/ ./server/` — clean.
- New/updated tests: `TestRowIsOneKey`, `TestSimpleQueryPlanCacheInvalidatedByDDL`;
  `TestRowExistenceIsThePrimaryKey`, `TestRowInsertLeavesNothingOnFailure`,
  `TestAnalyzeCountsRowsNotKeys`, `TestRowUpdateCreatesAnAbsentColumn` updated
  for the packed layout; RBAC/join/param-description seeds moved to the packed
  key.
