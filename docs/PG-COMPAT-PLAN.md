# Tellstone PostgreSQL Client Compatibility Plan (DataGrip milestone)

Status: Draft
Date: 2026-10-19
Related: [MULTI-CLUSTER-PLAN.md](MULTI-CLUSTER-PLAN.md) Phase 16,
[Phase 13 (deferred)](MULTI-CLUSTER-PLAN.md#phase-13-secondary-indexes),
[ADR-007](adr/007-drop-redis-compatibility.md)

This plan supersedes the Phase 15/16 sequencing for the immediate
roadmap. Phase 12 (transactions/MVCC) is complete. The decision recorded
here is to **defer Phase 13 (secondary indexes)** and concentrate on
PostgreSQL client compatibility so that DataGrip can be used against
Tellstone.

## 1. Goal

Make DataGrip (JetBrains, the **stock PostgreSQL data source, no custom
driver**) able to:

- connect over the PostgreSQL wire protocol,
- complete introspection with no errors in the client log,
- show every user table and column in the database tree,
- run SELECT / INSERT / UPDATE / DELETE / DDL from the query console.

This is "basic support" in the sense TiDB, CockroachDB and YugabyteDB
reach it: a correct, error-free tree and console backed by a
`pg_catalog`/`information_schema` view of the real schema. Rich
completion, code inspections and other "full support" features are a
JetBrains-side decision and are explicitly out of scope.

## 2. What is already in place

- **Parser.** Full PostgreSQL grammar via
  `github.com/pganalyze/pg_query_go/v6` (`internal/sql/translate.go:178`).
  The grammar is not the gap; the translator and executor are.
- **Extended query protocol.** Parse/Bind/Execute, parameter binding,
  RowDescription with type OIDs, NULL handling
  (`internal/sql/pgwire.go`; exercised in `integration_test.go` and
  `dml_catalog_test.go`). Most of Phase 14 is effectively done.
- **Execution.** Inner hash join (`internal/sql/join.go`), multi-row
  UPDATE/DELETE, schema catalog (`internal/sql/schema.go`, `Catalog`),
  MVCC transactions (Phase 12), optional TLS on the PG wire.
- **Type mapping.** Column type → PostgreSQL OID
  (`internal/sql/wiretype.go`).

## 3. What blocks DataGrip today

1. **Connect-time SQL.** GUIs run session setup before anything else.
   `SET`/`SHOW` and FROM-less `SELECT` fall through to the unsupported
   default (`internal/sql/translate.go:214`); multi-statement is refused
   (`translate.go:182`); `SELECT` without `FROM` is refused
   (`translate.go:543`). The connection is marked broken at the first
   statement.
2. **Introspection SQL surface.** `translate.go:488` refuses `GROUP BY`,
   `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET` and `DISTINCT`; target
   expressions must be plain column references (`selectTargets`,
   `translate.go:501`), so aggregates such as `count(*)` are refused.
   Introspection queries use all of these plus subqueries and outer
   joins.
3. **No system catalogs.** `pg_catalog` and `information_schema`
   relations do not exist.

## 4. Strategy

1. **Drive scope by observed client SQL, not speculation.** Record the
   exact SQL DataGrip and pgJDBC emit (startup and introspection) as
   golden transcripts, then implement only the constructs those
   transcripts actually use. This keeps the Phase 15 SQL surface from
   being built blindly.
2. **Project the catalogs, do not duplicate them.** `pg_catalog` and
   `information_schema` are read-only relations materialized at query
   time from the existing `Catalog` (`internal/sql/schema.go`), so they
   cannot diverge from the real schema.
3. **Land the independent connect work first.** Session/GUC and
   FROM-less `SELECT` are not blocked by the catalog work and unblock
   every client immediately.
4. **Use `psql` as the intermediate milestone.** `\dt`, `\l` and
   `\d <table>` need far less SQL than DataGrip and de-risk the catalog
   work before tackling a full IDE.

## 5. Stages

### Stage A — Connect handshake and session GUCs (independent, first)

Deliverables:

- `VariableSetStmt`: `SET` / `SET SESSION` / `SET LOCAL` / `RESET` /
  `RESET ALL` accepted for every GUC name; known names stored on the
  session, unknown names accepted and ignored, all replying `SET`.
  `SET LOCAL` is stored only for an active transaction and reverted at
  `COMMIT`/`ROLLBACK`; issued outside a block it changes nothing and
  emits the PostgreSQL `WARNING` (`no transaction in progress`, SQLSTATE
  `25P01`).
- `VariableShowStmt`: `SHOW <name>` and `SHOW ALL` return the session
  value or a sensible default.
- StartupMessage parameters captured into the session at connect:
  `application_name`, `search_path`, `client_encoding`, `DateStyle`,
  `TimeZone`, `extra_float_digits`, `standard_conforming_strings`.
- `ParameterStatus` emitted at connect and on change.
- FROM-less `SELECT`: `SELECT 1`, string/number literals, arithmetic,
  `current_timestamp`.
- Multi-statement simple query: iterate `res.Stmts` and emit one
  `CommandComplete`/`ResultSet` per parsed statement; answer an empty or
  `;`-only query with `EmptyQueryResponse`.

Code touchpoints: `internal/sql/translate.go` (accept the new node
types; stop splitting/refusing on `;`), `internal/sql/exec.go`
(FROM-less execution path), `internal/sql/pgwire.go` (session state on
`pgConn`, alongside `txn`).

Verification: `psql` connects; `SELECT 1` and `SELECT version()` return;
the pgJDBC startup transcript replays green.

### Stage B — Introspection transcript harness, then the SQL surface subset

Deliverables:

- A harness that captures the exact SQL from a DataGrip and a pgJDBC
  connect + introspection session and commits it as golden tests
  (`scripts/` plus Go tests). This must land **before** the SQL work, so
  the required constructs are known.
- Only the constructs those transcripts use. Expected set: `count` and
  the common aggregates, `GROUP BY`, `ORDER BY`, `LIMIT`/`OFFSET`,
  `DISTINCT`, output-column aliases, scalar-expressions in the target
  list, and the subqueries/outer joins the introspection queries need.

Code touchpoints: `internal/sql/translate.go` (`refuseSelectModifiers`,
`selectTargets`), `internal/sql/expr.go`, `internal/sql/optimizer.go`,
`internal/sql/executor.go`, and new operators as needed (aggregate,
sort, limit, distinct).

Verification: the recorded transcripts run against an empty schema and
return well-formed empty results rather than errors.

### Stage C — `pg_catalog` and `information_schema` emulation

Deliverables:

- A reserved, read-only `pg_catalog` schema searched implicitly, and
  `information_schema`, both projected at query time from the existing
  `Catalog.Tables()` / `Catalog.Get()` (`internal/sql/schema.go:459`,
  `:434`). No stored rows, so the catalogs never diverge.
- Minimum `pg_catalog` relations for DataGrip/pgJDBC: `pg_namespace`,
  `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_constraint`,
  `pg_index` (stub/empty), `pg_proc`, `pg_description`, `pg_database`,
  `pg_roles`/`pg_authid`, `pg_am`, `pg_tablespace`, `pg_inherits`,
  `pg_collation`, `pg_enum`, `pg_range`, `pg_settings`; plus views
  `pg_tables`, `pg_views`, `pg_matviews`, `pg_sequences`, `pg_stat_*`
  (may be static/empty).
- `information_schema` relations: `tables`, `columns`, `views`,
  `schemata`, `table_constraints`, `key_column_usage`,
  `referential_constraints`, `check_constraints`, `routines`,
  `parameters`, `sequences`, `domains`, `character_sets`,
  `table_privileges`, `column_privileges`, `enabled_roles`,
  `applicable_roles`.
- Stable OIDs from a persisted catalog counter, stored with the object;
  rename keeps the OID, drop retires it, re-create gets a fresh one.
  A name-derived hash is rejected (collides; not rename-stable).
- The catalogs describe themselves, as real PostgreSQL does.
- Reuse and extend `internal/sql/wiretype.go` for the `pg_type` ↔
  Tellstone-type mapping.

Code touchpoints: a new `internal/sql/pgcat*.go` (virtual relation
provider wired into name resolution and the scan/plan path), plus a
persisted OID counter in the existing schema catalog.

Verification: `psql` `\dt`, `\l`, `\d <table>` succeed; a fresh
`CREATE TABLE` appears in them.

#### Stage C status

The virtual catalogs and the SQL surface a client needs to read them are
in place. The relations are projected at query time from the live catalog
(40 relations across `pg_catalog` and `information_schema`), and the
statements a client actually issues are supported:

| Statement shape | Where |
|---|---|
| comma join over two or three relations | `internal/sql/translate.go` (`translateThreeCommaSelect`) |
| left-deep chain of joins | `internal/sql/translate.go` (`translateChainedJoinSelect`) |
| compound `ON` (an equality plus a remainder) | `internal/sql/join.go` (`JoinPlan.MatchFilter`) |
| column-to-column comparison | `internal/sql/expr.go` (`ColCmpExpr`) |
| `CASE`, nested `CASE`, `CASE` with an expression operand | `internal/sql/translate.go` (`translateCaseTarget`) |
| constant in the target list (`NULL`, `''`, `'x'`) | `internal/sql/translate.go` (`selectTargets`) |
| regular expression (`~`, `!~`) | `internal/sql/expr.go` (`RegexExpr`) |
| boolean column as a predicate | `internal/sql/expr.go` (`BoolColExpr`) |
| `::regclass`, `::oid` casts | `internal/sql/translate.go` (`castToOID`) |
| `current_schemas(bool)` returning `name[]` over the session `search_path` | `internal/sql/selectexpr.go` (`currentSchemasValue`) |
| extended-protocol empty query: Parse accepted, Execute answers `EmptyQueryResponse` | `internal/sql/pgwire.go` (`handleParse`, `StmtEmpty`) |
| portal `Describe` row-shape for SHOW / FROM-less SELECT / EXPLAIN | `internal/sql/pgwire.go` (`producesRows`) |

pgJDBC's `DatabaseMetaData.getTables` statement runs as the driver spells
it, and answers with the table, its schema and its type
(`internal/sql/introspection_test.go`).

**Milestone M2 is reached.** A live DataGrip 2026.1.2 session (stock
PostgreSQL data source) was recorded with `pgrecord` through TLS
termination; the capture -- init sequence, empty-query validation
round-trip, schema queries, `select * from instances` / `select * from
test` previews and `SHOW TRANSACTION ISOLATION LEVEL` -- is committed as
`internal/sql/testdata/transcripts/datagrip-introspection.jsonl` and
replays green. Every statement in it runs against a rebuilt node without
error; the wire bugs the capture exposed (suppressed `RowDescription`
after `Describe`, refused empty query) are pinned by regression tests.
The gaps that remain are the function calls a client makes
(`pg_get_userbyid`, `has_schema_privilege`), the derived table pgJDBC's
`getColumns` reads, and the array subscript that `getSchemas` uses
(`(pg_catalog.current_schemas(true))[1]` -- the function itself is
supported).

### Stage D — Wire hardening

Deliverables:

- Binary result format (`resultFormat = 1`) either implemented or
  explicitly negotiated down to text.
- Correct `Describe` / `NoData` / `ParameterDescription` for the new
  statement classes.

Code touchpoints: `internal/sql/pgwire.go`.

Verification: a client requesting binary still works (text negotiated or
binary delivered).

## 6. Milestones

| # | Milestone | Proves |
|---|-----------|--------|
| M0 | `psql` connects; `SELECT 1`, `SELECT version()` | Stage A |
| M1 | `psql` `\dt`, `\l`, `\d <table>` | Stages A+B+C (minimal) |
| M2 | DataGrip connects and lists tables/columns, no client-log errors | Stages A+B+C |
| M3 | DataGrip query console runs SELECT/INSERT/UPDATE/DELETE/DDL | all |
| M4 | `pg_dump --schema-only` succeeds | Stages C+D |

## 7. Decisions (fixed by ADR-016)

These decisions were made by the owner and are recorded in
[ADR-016](adr/016-postgresql-client-compatibility.md). The rejected
alternatives are listed there.

| # | Decision | Choice |
|---|----------|--------|
| D1 | First target client | `psql` first, then DataGrip |
| D2 | Catalog representation | virtual, projected at query time |
| D3 | OID scheme | persisted catalog counter |
| D4 | SQL surface scope | transcript-driven minimal |
| D5 | Phase 13 secondary indexes | deferred; `pg_index` is a stub |
| D6 | Session/GUC state location | on `pgConn` |
| D7 | `pg_stat_*` and exotic catalogs | static/empty stubs |

## 8. Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Catalog emulation larger than expected | schedule slip | implement only the relations the transcripts query; stub the rest |
| Introspection SQL uses unsupported constructs | blocked | transcript harness surfaces them before implementation |
| pgJDBC/DataGrip version drift | rework | golden transcripts are pinned; re-record on client upgrade |
| OID instability breaks client caches | broken tree | persisted counter, never reused |
| Binary result format requested | decode failure | negotiate text first, implement binary in Stage D |
| `SET LOCAL` semantics wrong | subtle client bugs | follow PostgreSQL: transaction-scoped, reverted on end |

## 9. Verification (end state)

- Recorded pgJDBC and DataGrip startup + introspection transcripts
  replay green in CI.
- A `scripts/` smoke test asserts a clean connect for each client.
- Manual: DataGrip with the stock PostgreSQL data source, no errors in
  the client log, the tree lists every user table and column, and the
  console runs CRUD and DDL.

## 10. Out of scope

- Byte-for-byte fidelity of every `pg_catalog` column.
- Extensions, replication, logical decoding.
- `pg_stat_*` beyond static/empty stubs.
- Rich DataGrip completion and inspections (JetBrains-side).
- Secondary indexes (Phase 13, deferred).
- The remainder of Phase 15 not required by the transcripts (e.g. CTEs,
  set operations, window functions) until after client compatibility.
