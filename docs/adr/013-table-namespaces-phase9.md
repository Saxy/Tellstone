# ADR-013: Table Namespaces over the KV Engine (Phase 9)

Status: Accepted
Date: 2026-09-24

## Context

Phase 8 delivered the PG wire protocol with one implicit table mapping 1:1 onto
the KV engine (`CREATE TABLE tellstone (key TEXT PRIMARY KEY, value BYTEA)`,
ADR-012 §4). Phase 9 introduces real DDL — `CREATE TABLE`, column types, and
multiple user tables that outlive the implicit `tellstone` table.

The underlying engine is a sharded key/value store. Every frontend already
routes by **key prefix**: geo policy selects a region for a key range
(ADR-004, ADR-006) and RBAC grants operate on `~prefix` whitelists. When a
SQL table needs to live in this system, the open question is how a row maps
onto keys so that per-table geo policy, per-table RBAC, sharding, and
future index/scan support all fall out of mechanisms that already exist.

The motivating example:

```
CREATE TABLE users (name VARCHAR, lastname VARCHAR, age INT);

INSERT INTO users (name, lastname, age)
VALUES ('max', 'mustermann', 18);
```

must become physical writes that respect geo policy and namespaces.

See [MULTI-CLUSTER-PLAN §Phase 9-13](../MULTI-CLUSTER-PLAN.md).

## Decisions

### 1. The database name is the top-level namespace; a row is a set of column keys

All SQL-visible data lives under one well-known database subtree. With the
single database `tellstone`, a user table `users` maps each row onto keys of
the form

```
<db>/<table>/<row-id>/<column>   # e.g. tellstone/users/<id>/name
```

The database segment defaults to `tellstone`; multiple databases later only
add another sibling under the same scheme. The `INSERT` above materializes as

```
tellstone/users/<id>/name      = "max"
tellstone/users/<id>/lastname  = "mustermann"
tellstone/users/<id>/age       = 18 (INT, compact-encoded)
```

- **`tellstone/` is the database subtree.** The whole SQL surface shares one
  prefix, so geo routing, snapshots, and migrations operate on `tellstone/`
  as a unit.
- **`users/` is the table namespace** within the database. Geo-local table
  (ADR-004/006) is a `~prefix tellstone/users/` placement rule; RBAC grants
  `~prefix tellstone/users/` with the existing Get/Set/Delete command grants.
  Per-field geo/RBAC granularity is a free side effect of the prefix model.
- **`<row-id>` identifies the row.** It is either the table's declared primary
  key (see decision 2) or a generated value (see decision 3).
- **`<column>` is the last segment.** A column becomes one independently
  routable, shardable key, keeping kinds and encodings per column.

Row enumeration needs no separate registry key: a row exists iff at least one
of its column keys exists, and `tellstone/users/<id>` is the row-id index by
construction (`<id>` is the next segment after the table prefix). It is also
the deterministic write-intent anchor for Phase 12 transactions (decision 7).

**Rationale:** Routing, grant, and shard machinery is key-prefix based today.
Encoding a row as multiple keys under one well-known subtree means SQL tables
get geo policy, RBAC, and sharding with zero new placement code — only the key
layout is new.

### 2. Declared primary key becomes the row-id segment

For tables that declare a primary key:

```
CREATE TABLE users (user_id BIGINT PRIMARY KEY, name VARCHAR);

INSERT INTO users (user_id, name) VALUES (42, 'max');
-- tellstone/users/42/name = "max"
```

The PK column does **not** keep its own `<table>/<id>/<column>` location; it
is elevated to the `<row-id>` position. Point lookups then need only the raw
key `tellstone/users/<pk>`. Tables without a PK column are not rejected; they
use a generated id (decision 3) and are naturally append-heavy.

### 3. Rows without a primary key use a TSO-derived generated id

Tables without a PK (or with no PK value supplied) generate `<row-id>` from
the embedded PD/TSO monotonic timestamp (ADR-003, 60-bit timestamp component),
rendered as a fixed-width integer string so generated ids sort
chronologically. This is the same global-timestamp source the cluster already
uses for TSO; no separate sequence infrastructure is built in Phase 9.

**Rationale:** The engine has one monotonic id source already; `SERIAL`-style
sequences would be a parallel mechanism and a consensus pattern Phase 12
transactions can revisit without changing the key layout.

### 4. Column value encoding preserves orderability per type

Phase 9 column types (INT, BIGINT, FLOAT, VARCHAR, BYTES, TIMESTAMP, JSONB,
BOOL) are encoded so that a prefix/range scan over a column key returns rows
in the type's natural order:

- INT/BIGINT/TIMESTAMP → big-endian two's-complement (orderable)
- FLOAT → order-preserving IEEE-754 transform (invert sign/exponent)
- VARCHAR → UTF-8 bytes (collation-lite, byte orderable)
- BOOL → `0x00`/`0x01`
- BYTES, JSONB → canonical bytes (JSONB canonicalized before encoding)

**Rationale:** Phase 10's optimizer and Phase 11's executors will scan column
keys (`tellstone/users/*/name`). Order-preserving encodings make those scans index-safe
without rewriting encodings later and keep `ORDER BY <column>` cheap.

### 5. The implicit `tellstone` table joins the database subtree

The implicit table (ADR-012 §4) is folded under the same namespace: its
physical keys become `tellstone/tellstone/<key>` (database `tellstone`, table
`tellstone`) — the `value` column keeps its raw BYTES encoding, only the key
layout changes. ADR-012 §4's verbatim-key clause is superseded by this ADR.
Table names beginning with a reserved marker (such as `~` or `*`) are
rejected by the DDL executor.

**Rationale:** Every SQL-visible byte lives under `tellstone/`; one subtree is
snapshotable and routeable as a unit, and there is no second "raw" SQL surface
to keep compatible (Phase 8 has not shipped).

### 6. Phase 9 delivers point lookup and full-table scan only

Phase 9 supports:

- `WHERE <pk> = ?` point plans against `tellstone/users/<pk>/...` (reconstruct
  the row by reading the known column keys)
- `WHERE <column> = ?` / unfiltered selects as a **full prefix scan** over
  `tellstone/users/` / `tellstone/users/*/<column>`, with filters applied by
  the schema layer

Secondary indexes and index-scans are explicitly Phase 13; optimizer
planning is Phase 10; the index key layout (`users@<index>/...`) is out of
scope here and will be fixed by ADR for Phase 13.

### 7. The row id is the deterministic write-intent anchor

Phase 9 executes multi-column `INSERT`/`UPDATE` as batch writes,
statement-at-a-time (durability is per key). Under Phase 12, the row id itself
(`tellstone/users/<id>`) is the deterministic write-intent anchor: a
transaction writing a row places a write intent on the row id and versions the
column keys under it; every column key of the row shares the
`tellstone/users/<id>/` prefix, so any read of any column encounters the anchor
and resolves commit/abort against one key. No per-txn key registry is stored —
the anchor is derived from the row id, and commit/abort is a decision on that
single key.

**Rationale:** Row atomicity without a row-level serialization format or an
extra txn-record key. Phase 12 machinery is identical across storage models;
the advantage here is that a row codec (read-modify-write on single-column
updates) and a payload parse (projection scans) never appear in the steady
state, so the hottest operations stay zero-alloc slices of the wire buffer.

### 8. Performance: column keys keep the steady-state path codec-free

- **No row codec.** A single-column `UPDATE` is one `Set` of the wire/param
  bytes under its column key — never a read-modify-write of a whole serialized
  row.
- **Projection pushdown.** `SELECT name` scans only the `tellstone/users/*/name`
  keys; storage returns exactly the requested column, with no whole-row
  deserialize.
- **NULL = key absence.** No null bitmap and no storage cost for NULL fields;
  sparse rows are naturally dense on disk.
- **Zero-alloc reads.** Column values are `[]byte` slices forwarded from the
  buffer into the PG `DataRow` frames; typed encoders reuse pooled buffers.
  Rows are reconstructed from keys (each key carries its row id) without
  packing or copying.
- **Robust value identity.** One updated column rewrites one key; concurrent
  single-column writers conflict only on that key, not on a shared blob.

**Rationale:** For a KV engine the steady-state read/write path should move
raw bytes, not "row codecs". A single-blob row model (`t:<tid>:r:<pk>` →
serialized payload) buys id-stable renames and trivial row-atomic intents,
but pays RMW on every partial update and a payload parse on every scan.
Decision 7's intent anchor removes the row-blob's only decisive advantage
without touching the layout, so the column-scoped model remains the best
performance and transaction variant.

## Alternatives considered

- **Single key per row (`tellstone/users/<id>` → serialized record)**: rejected
  — one blob per row is atomic and scan-trivial, but loses per-column geo
  routing, per-prefix RBAC granularity, and makes column scans deserialize
  every record. The prefix model buys column-level policy for free from
  existing machinery.
- **Separate sequence namespace for generated ids**: rejected — the PD/TSO
  already provides monotonic ids (decision 3); a parallel sequence is extra
  consensus we can defer.
- **Keep the implicit `tellstone` table's keys verbatim**: rejected in
  decision 5 — leaves a second, unnamespaced key surface under the SQL
  interface and an escape hatch from the database subtree.

## Implementation guardrails

The column-key layout's advantages depend on two storage-layer properties
that are **absent in today's engine** and must be delivered with Phase 9:

1. **Prefix batch reads, not N point lookups.** A `SELECT * WHERE id = 42`
   reconstructs a row from C column keys. Issuing C independent `Get`s (through
   `command.Store`'s single-key seam, and in cluster mode up to C raft/gateway
   operations) makes point-lookup latency scale linearly with C. The store seam
   must grow a prefix-range read (seek `tellstone/users/42/` →
   `tellstone/users/42/\xff`) so one contiguous lookup yields a whole row, and
   the cluster store must batch it identically.

2. **An ordered, prefix-compressible index.** The layout only pays off if a
   prefix seek is O(log n) *and* column keys do not duplicate the
   `tellstone/users/<id>/` prefix in memory. Today the storage engine is an
   unordered `map[string]Item` with no seek and no key compression, so a "full
   table scan" would be full-map iteration. Phase 9 requires an ordered index
   (front-coded / prefix-compressed) backing prefix scans; otherwise the
   per-key memory and scan costs negate the projection advantage of decision 8.

## Phase placement

This design is implemented in **Phase 9 (Schema Layer)**:

- DDL + schema manager (`internal/sql/schema.go`), schema in the meta region,
  schema mutations through Raft — per MULTI-CLUSTER-PLAN §Phase 9
- Key layout, generated ids, type encodings, point lookups, full-table scans
  — Phase 9
- Write-intent anchors on row ids (decision 7) — Phase 12, using the row-id
  segment already fixed here
- Column-predicate acceleration and index key layout — Phase 13; optimizer
  choice — Phase 10. None change this key layout.

## References

- [MULTI-CLUSTER-PLAN](../MULTI-CLUSTER-PLAN.md) §Phase 9-13
- [ADR-012: PostgreSQL Wire Protocol](012-postgresql-wire-phase8.md) §4
- [ADR-004: Region-Based Data Model](004-region-based-data-model.md)
- [ADR-006: Geo-Based Routing](006-geo-based-routing.md)
- [ADR-003: TSO Graceful Degradation](003-tso-graceful-degradation.md)
- [ADR-011: Cross-Cluster Gateway](011-cross-cluster-gateway.md)