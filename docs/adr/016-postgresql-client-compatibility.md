# ADR-016: PostgreSQL Wire Protocol & Virtual System Catalogs for DataGrip Compatibility

Status: Accepted
Date: 2026-10-19

## Context

Phase 12 (transactions/MVCC) is complete. The next goal is to make
Tellstone usable from DataGrip over the PostgreSQL wire protocol, using
the **stock PostgreSQL data source with no custom driver**. This is the
"basic support" tier that TiDB, CockroachDB and YugabyteDB reach: a
client connects, completes introspection without a single error, and
browses the real schema.

Two gaps block that today:

1. **Connect-time SQL.** GUIs run session setup (`SET extra_float_digits`,
   `SHOW`, `SELECT 1`, `SELECT version()`) before anything else. `SET` /
   `SHOW` and FROM-less `SELECT` fall through to the unsupported default
   (`internal/sql/translate.go:214`); multi-statement payloads are
   refused (`translate.go:182`).
2. **Introspection.** Clients enumerate the schema through
   `pg_catalog` / `information_schema`. Those relations do not exist, and
   the queries that read them use `GROUP BY`, `ORDER BY`, `LIMIT`,
   `DISTINCT`, aggregates, subqueries and outer joins — several of which
   the translator refuses (`translate.go:488`, `translate.go:501`).

The parser is already the real PostgreSQL grammar
(`github.com/pganalyze/pg_query_go/v6`), and the extended query protocol
(Parse/Bind/Execute, RowDescription, type OIDs) and an inner hash join
already exist. The gap is in the translator, the executor, and the
absence of the system catalogs — not the grammar or the wire framing.

[PG-COMPAT-PLAN.md](../PG-COMPAT-PLAN.md) lays out the stages and
milestones. This ADR records the decisions that constrain that work.

## Decision

Implement PostgreSQL client compatibility for the DataGrip milestone
with the seven decisions below. Where a decision names an alternative, it
rejects it.

### D1 — First target client: `psql` first, then DataGrip

`psql` (`\dt`, `\l`, `\d <table>`) is the first milestone. It exercises
the same catalog and session machinery as DataGrip but needs far less of
the SQL surface, so it de-risks the catalog work before a full IDE is
attempted. DataGrip is the acceptance target (M2/M3), not the first
client.

### D2 — Catalog representation: virtual, projected at query time

`pg_catalog` and `information_schema` are **read-only virtual relations
projected at query time** from the existing schema catalog
(`internal/sql/schema.go`, `Catalog.Tables()` / `Catalog.Get()`). No
stored rows and no hidden materialized tables. This is chosen so the
catalogs cannot diverge from the real schema: there is one source of
truth, and the system relations are a view of it.

### D3 — OID scheme: persisted catalog counter

User tables, columns and types receive OIDs from a **persisted catalog
counter**, stored with the object. The allocation is collision-free,
stable across reconnects, and independent of the object's name.
System objects get fixed OIDs. Renaming an object keeps its OID; dropping
it retires the OID (never reused), so a later `CREATE` of the same name
gets a fresh OID — the rule PostgreSQL follows. A **name-derived hash is
rejected**: it can collide and it changes identity on `ALTER … RENAME`,
which breaks client caches and the joins between `pg_class`,
`pg_attribute` and `pg_type`.

### D4 — SQL scope: transcript-driven minimal

The SQL surface is driven by **recorded client transcripts**, not by
completing Phase 15 first. The exact SQL DataGrip and pgJDBC emit for
startup and introspection is captured as golden tests (Stage B), and only
the constructs those transcripts use are implemented. The remainder of
Phase 15 (CTEs, set operations, window functions, etc.) is deferred
until after client compatibility, unless a transcript proves a need.

### D5 — Phase 13 (Secondary Indexes): deferred

Secondary indexes are a performance feature, not a compatibility
requirement. `pg_index` is served as an empty or derived stub, which is
sufficient for the introspection queries. Phase 13 is therefore off the
critical path and is deferred until after client compatibility ships.

### D6 — Session/GUC state: on `pgConn`

Session and GUC state is stored on `pgConn`
(`internal/sql/pgwire.go`), alongside the existing `txn` field. This
matches where the transaction state already lives and keeps a session's
state with its connection. A separate server-side session map is
rejected as unnecessary indirection for the current single-writer-per-
connection model.

Semantics follow PostgreSQL: `SET` / `SET SESSION` are session-scoped;
`SET LOCAL` is stored only for the active transaction and reverted at
`COMMIT` / `ROLLBACK`, and issued outside a block it changes nothing and
emits the `25P01` warning (`no transaction in progress`). Unknown GUC
names are accepted and ignored; known names are stored. StartupMessage
parameters are captured into the session at connect, and
`ParameterStatus` is emitted at connect and on change.

### D7 — `pg_stat_*` and exotic catalogs: static or empty stubs

`pg_stat_*`, replication, and other exotic relations are served as
**static or empty stubs**, emitted only when a client hard-fails without
them. Byte-for-byte fidelity of every `pg_catalog` column is explicitly
not promised. Extensions and logical decoding are out of scope.

## Consequences

### Positive

- **Scope is bounded by observation.** Transcript-driven SQL work means
  no speculative operator is built; the golden transcripts are the spec.
- **One source of truth.** The virtual catalogs are a projection of the
  real schema, so they cannot drift.
- **Stable identity.** Persisted-counter OIDs survive reconnects and
  renames, which is what client caches and the catalog joins require.
- **`psql` de-risks the catalog.** A small SQL surface proves the catalog
  machinery before DataGrip's larger surface is attempted.
- **Phase 13 is unblocked from compatibility.** Index work can proceed on
  its own schedule without gating the milestone.

### Negative

- **The catalog emulation is the long pole.** A virtual relation
  provider must be wired into name resolution and the plan/scan path, and
  the relation set is large (`pg_class`, `pg_attribute`, `pg_type`,
  `pg_proc`, `pg_namespace`, `pg_attrdef`, `pg_constraint`, …).
- **A persisted OID counter adds state to the schema catalog** and must
  participate in drop/rename handling.
- **Some clients probe catalogs beyond the minimum.** They are covered
  only by the D7 stub policy, which may need revisiting per client
  version.
- **Deferring the rest of Phase 15** may mean revisiting it as soon as a
  new client (or a client upgrade) emits SQL outside the recorded set.

## Amendments

### A1 — D3 is not yet implemented as decided

The decision above is the right one, and the implementation does not yet
meet it. A user's object identifiers are currently derived from a hash of
the object's name (`objectOID`, `internal/sql/pgcat.go`), below a fixed
base that keeps them clear of the catalog's own. That is the alternative
this ADR rejected: a name-derived identifier changes when the object is
renamed, and two objects can collide.

The work is not scheduled, because the clients read the catalog
consistently within a session and so have not shown the breakage the
decision was written to prevent. It remains a deviation, not a change of
mind: the identifiers must come from a persisted counter before the
catalog can be called compatible with a client that caches them across
renames.

## Alternatives Considered

### Materialize the catalogs as hidden tables

Rejected (D2): a second copy of schema state that can diverge, and
write-path maintenance for data that is already derivable.

### Hash-derived OIDs

Rejected (D3): collisions and rename instability. A persisted counter is
strictly more correct for the modest cost of one counter.

### Build all of Phase 15, then attempt DataGrip

Rejected (D4): speculative and slow; most of Phase 15 is not emitted by
the target clients during connect/introspection.

### Gate compatibility on secondary indexes (Phase 13 first)

Rejected (D5): indexes do not affect whether a client can read the
catalog.

### Server-side session map

Rejected (D6): extra indirection with no benefit for the current
connection model.

### Promise full `pg_catalog` fidelity

Rejected (D7): unbounded scope. The target is a correct, error-free
"basic support" tree and console, not a byte-for-byte clone.

## References

- [PG-COMPAT-PLAN.md](../PG-COMPAT-PLAN.md) — stages, milestones, code
  touchpoints, and the decisions this ADR fixes (D1–D7).
- [MULTI-CLUSTER-PLAN.md §Phase 16](../MULTI-CLUSTER-PLAN.md) —
  PostgreSQL catalog and client compatibility.
- [MULTI-CLUSTER-PLAN.md §Phase 13](../MULTI-CLUSTER-PLAN.md) — secondary
  indexes, now deferred.
- [ADR-012](012-postgresql-wire-phase8.md) — PostgreSQL wire frontend
  (Phase 8).
- [ADR-013](013-table-namespaces-phase9.md) — schema catalog and table
  namespaces (Phase 9).
- `internal/sql/translate.go` — parser dispatch and the refusals for
  connect-time SQL and select modifiers.
- `internal/sql/pgwire.go` — session/`pgConn` state and `ParameterStatus`.
- `internal/sql/schema.go` — `Catalog`, the source of the projected
  system relations.
- `internal/sql/wiretype.go` — column type ↔ PostgreSQL OID mapping.
