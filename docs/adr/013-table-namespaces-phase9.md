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
CREATE TABLE users (
  name     VARCHAR PRIMARY KEY,
  lastname VARCHAR,
  age      INT
);

INSERT INTO users (name, lastname, age)
VALUES ('max', 'mustermann', 18);
```

must become physical writes that respect geo policy and namespaces.

The `PRIMARY KEY` is load-bearing, not decoration: the key column *is* the row
id, so a table without one has no way to address a row. The original form of this
example omitted it, which would be rejected at DDL time for the same reason a
`CREATE TABLE users ()` would be.

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

> **Superseded in Phase 10 (ADR-014 decision 4).** An integer row id is now 16
> fixed-width hex digits of the sign-flipped value, so the key this statement
> writes is `tellstone/users/000000000000002a/name`. The change was required for
> range-scan correctness and is a breaking format change; see ADR-014 §5. The
> example is left as written when this ADR was accepted.

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

The column-key layout's advantages depend on five storage-layer properties.
The first two are **absent in today's engine**; the remaining three are
decisions this ADR fixes for the work that follows.

### Guardrail 1: prefix batch reads, not point lookups

A `SELECT * WHERE id = 42` reconstructs a row from C column keys. Issuing C
independent `Get`s (through `command.Store`'s single-key seam, and in cluster
mode up to C raft/gateway operations) makes point-lookup latency scale
linearly with C. The store seam must grow a prefix-range read (seek
`tellstone/users/42/` → `tellstone/users/42/\xff`) so one contiguous lookup
yields a whole row, and the cluster store must batch it identically.

### Guardrail 2: an ordered, prefix-compressible index

The layout only pays off if a prefix seek is O(log n) *and* column keys do not
duplicate the `tellstone/users/<id>/` prefix in memory. Today the storage
engine is an unordered `map[string]Item` with no seek and no key compression,
so a "full table scan" would be full-map iteration. Phase 9 requires an
ordered index (front-coded / prefix-compressed) backing prefix scans; otherwise
the per-key memory and scan costs negate the projection advantage of decision 8.

### Guardrail 3: region split boundaries must respect rows

Routing is by **lexicographic key range**, not by hash
(`internal/cluster/routing.go`), so a row's column keys already land in one
region together. That property holds only while no split boundary falls
inside `<row-id>/`.

`internal/cluster/split.go` picked its split point with
`midpointKey(cur.StartKey, cur.EndKey)`, an arbitrary byte midpoint, which can
cut a row in half and scatter its columns across regions — trading a single
local read for a per-column cross-region read and breaking the atomicity that
decision 5 assumes.

**Resolved.** The row boundary is now defined in exactly one place,
`internal/keyspace`, and the splitter consults it. `keyspace.RowPrefixOf`
reports the `<db>/<table>/<row-id>/` prefix owning a key, and `Split` snaps an
automatically chosen midpoint onto that prefix
(`SplitCoordinator.chooseSplitKey`). The package exists because the alternative
is a layering inversion: `internal/sql` depends on `internal/cluster`, so the
consensus layer cannot import the row layout. `internal/sql` now delegates its
key grammar to `keyspace`, so the writer and the splitter cannot disagree about
where a row ends.

Three properties of the snap are worth recording, because each is a place where
the obvious alternative is wrong:

- **Backward only.** A row prefix is a prefix of every key in its row and of no
  key outside it, so a boundary placed on it leaves the whole row on one side.
  The forward direction is *not* derivable from a key alone: the start of the
  next row depends on the lexicographic successor of the longest column name in
  the row, which is only knowable by reading the row. Appending `0xFF` is the
  tempting shortcut and it is wrong — that byte sorts above every legal column
  name but still lands *inside* the row, whose range extends past its last
  column key.
- **Absence of a row is not absence of a hazard.** A key outside the row grammar
  (a catalog key, a legacy flat-table key, any key with fewer than four
  segments) is left exactly where the midpoint landed. There is no row there to
  cut, so moving it would be unmotivated. `~meta` is excluded by the table
  segment being a reserved marker, which is what stops a split from snapping a
  catalog key onto a row boundary.
- **Refusing is correct, not a failure.** A region holding a single row, or one
  whose start is already mid-row from before this rule existed, has no legal
  boundary. `Split` returns `ErrSplitWouldCutRow` and leaves the region
  untouched rather than cutting a row, and the error is documented as "try again
  once the region spans two rows" so a scheduler can treat it as backpressure.

An explicitly requested split key is honoured verbatim, including mid-row, and
logged. That is deliberate: it is the operator's escape hatch for regions whose
boundaries are already wrong, and the coordinator cannot make progress on
exactly the regions that most need attention if it second-guesses them.

The invariant is verified exhaustively rather than by example
(`TestChooseSplitKeyNeverCutsARowExhaustively`): over every region formable from
a key set including escaped row ids, every accepted boundary is interior and
cuts no row, and every refusal is a genuine "no boundary available". The layout
hint on the region is therefore still deferred — it is not needed to keep rows
intact, only to keep split points from being *unnecessarily* uneven.

### Guardrail 4: TSO ids are allocated on the proposer, never in `FSM.Apply`

Raft replicas replay a log deterministically, so a `time.Now()`-derived or
node-local sequence read inside `FSM.Apply` would make every replica assign a
different row id to the same log entry. Generated ids must be allocated on the
**request path of the node proposing the entry** and carried **inside the
committed key** (decision 3), so `Apply` is a pure function of the log.

This also fixes the failure mode for `SETNX`-style idempotency: the anchor
write and the row share one id, so a retried proposal cannot mint a second id.

### Guardrail 5: primary keys are escaped at the layout boundary

`<row-id>/` is unambiguous only if a row id can never itself contain `/`. A
raw `VARCHAR` primary key containing `/` would forge a column boundary and
split one row's keys across two rows' address spaces.

Row ids are therefore **validated or escaped in the key-formatting layer**, at
the single point that builds keys, rather than at each call site. An `INT`/
`BIGINT` primary key needs no escaping; a textual key does, and the escaping
must be total (not merely "reject `/`") so that distinct primary keys always
produce distinct key prefixes.

> **Corrected in Phase 10 (ADR-014 decision 4).** "Needs no escaping" was true
> of the *value* encoding, not of the row id that was actually stored, and the
> difference was not cosmetic: escaping is not order-preserving, so a range
> bounded on an escaped integer row id omitted and included the wrong rows.
> Phase 10 renders integer row ids as hex, whose alphabet contains neither `/`
> nor `%`, which makes the claim true of the row id itself rather than
> incidentally true.

## Measured index cost

The Phase 9 ordered index (`internal/storage/btree.go`) is a front-coded
B+Tree with linked leaves, key suffixes packed into a per-node arena, and
values plus expirations duplicated into leaf entries so a scan never consults
the items map.

Measured on the column-key layout (100k rows x 4 columns, 33-byte keys):

| Property | Value |
|---|---|
| Index overhead | **67.4 B/key** |
| Front-coding ratio | **5.33x** (52.6 KB vs 280 KB flat) |
| `ScanPrefix` row read | **0 B/op, 0 allocs/op**, 340 ns at 100k rows |
| Point seek (row reconstruction) | ~365 ns, vs ~34 ms for a full engine scan |
| `set` | 2105 ns, 20 allocs |
| Comparison | items map ~240 B/key; skiplist prototype ~225 B/key |

Once the range read is wired end to end (`Engine.ScanPrefix` ->
`command.Store` -> `RouterStore` / `clusterStore`), a three-column row read at
100k rows goes from **33.3 ms / 21.6 MB / 200,010 allocs** for a full engine scan
to **529 ns / 2 B / 0 allocs**. The index is what makes that possible; the
remaining cost is one descent plus three front-coded decodes.

Two properties of the merged path are worth recording:

- **A scan runs under the engine's read lock.** The index restructures in place
  on insert, split and compaction, so it cannot be walked while that happens.
  A long table scan therefore blocks writers, which the older full-map `Scan`
  avoided by snapshotting. A snapshot is not an option here: copying every
  matched value would cost exactly the allocation the index exists to remove.
- **Crossing a shard or region boundary costs a copy.** The router hashes the
  complete key, so a row's columns live on different shards; each shard's walk
  hands out slices valid only for the duration of its own call, so the shards are
  merged from collected runs rather than interleaved. Within one engine a scan is
  genuinely zero-copy; across shards it is not. That is the measurable cost of
  the row-locality gap in the router's hash.

Three findings are worth recording, because each contradicts an obvious guess:

1. **~40 B/key was not reachable while values stay in the leaf.** The entry
   struct is 40 bytes, of which 24 is the `val` slice header. Removing it
   requires referencing values by offset into storage the map also owns, which
   is a larger change to `engine.go` and is rejected here: 67.4 B/key still
   removes **70%** of the skiplist's footprint.

2. **Capacity slack, not the entry layout, dominated the first result.** A
   naive arena conversion measured *worse* (139.7 B/key) than the slice-per-entry
   layout it replaced (109 B/key), because `keys` slices kept pre-split
   capacity (87.6 B/key of dead space) and arenas kept a transient insertion
   peak forever (4.75x overshoot). Both buffers must be reallocated when
   capacity is too **large**, not only when too small.

3. **Fanout trades memory for write throughput, not the reverse.** Raising
   `btreeMaxEntries` from 32 to 64 buys 10% memory (67.4 -> 60.4 B/key) but
   costs 49% on `set`, because every insert recodes the whole node. 32 is
   retained.

`ScanPrefix` reaches zero allocations through a **pooled** buffer pair, not a
stack buffer: the scan takes an unknown `func`, so Go's escape analysis must
assume its arguments escape and will heap-allocate a local buffer regardless of
how the traversal is written. Pooling is what makes the range read free, and it
is what lets a scan stream rows directly into a wire frame.

## Phase placement

This design is implemented in **Phase 9 (Schema Layer)**:

- DDL + schema manager (`internal/sql/schema.go`), schema in the meta region,
  schema mutations through Raft — per MULTI-CLUSTER-PLAN §Phase 9
- Key layout, generated ids, type encodings, point lookups, full-table scans
  — Phase 9
- Row boundary definition shared by the writer and the splitter
  (`internal/keyspace`) and row-safe split snapping (`internal/cluster/split.go`)
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
## Phase 9 DDL surface (decided with the catalog)

The catalog is one versioned binary blob at `tellstone/~meta/tables/<db>/<table>`,
so publishing a table is a single conditional store write rather than a sequence
that could be observed half-applied.

**The encoding is versioned, deterministic and hand-rolled.** JSON was rejected
because the blob is read on every DML statement against the table; the parser's
protobuf was rejected because reusing it would tie the catalog to the cgo parser
that `.goreleaser.yaml` already excludes. Determinism is a correctness property,
not a nicety: the blob is replicated, so a replay that re-encoded the same schema
differently would present as divergence.

**Every catalog read validates, and validation is not repair.** A blob that
decodes into a shape the writer should have rejected means the writer is a
different version, so it is reported as corruption rather than adopted. The
distinction from "no such table" matters: conflating them would let a DDL
statement overwrite a schema it merely failed to read.

**`DROP TABLE` refuses to orphan rows.** The catalog entry is only removed when
the table is empty. A later `CREATE TABLE` of the same name would otherwise
inherit rows written against a definition it knows nothing about.

**Rejected, with a message naming what is missing:** `UNIQUE`, `CHECK`, `FOREIGN
KEY`, `EXCLUDE`, `CREATE INDEX`, `CREATE VIEW`, `ALTER TABLE`, composite
`PRIMARY KEY`, `serial`/identity, generated columns, arrays, `SETOF`, table
inheritance, `CREATE TABLE ... OF`, table options, `USING` other than heap,
`TEMP`/`UNLOGGED`, and non-literal defaults.

Two of those rejections are load-bearing rather than incidental:

- **`UNIQUE` and `CHECK` cannot be recorded and ignored.** A catalog that stored
  them would be a record of constraints the engine does not apply, which is worse
  than refusing: the client believes the constraint holds.
- **Non-literal defaults break replication.** A default evaluated at write time
  is evaluated by the proposer, so it has to be deterministic. `DEFAULT now()`
  is not, and cannot be made so without recording the value in the committed key.

**NULL is the absence of a key, not a value.** A null column has no key, so the
value bytes never have to encode NULL, and an empty string stays an empty string
instead of collapsing into one. This is why `DecodeValue` has no zero-length
special case, and why the catalog encodes a default's presence with a length
sentinel instead of a flag bit: a flag plus a zero-length field cannot tell an
empty default from a truncated entry.

**The primary key is required and single-column.** It is the row id (decision 2),
so a table without one has no way to address a row. A composite key needs a
single row id encoding a tuple, and the order-preserving integer encoding is
defined for scalars; a default on the key column needs an id source, and the
proposer's TSO (guardrail 4) is the only one this phase has.

## Row identity: the primary key key *is* the row

A row is several keys, and the store offers atomic single-key conditional writes
with nothing that spans them. So the question of which key stands for the row is
not an optimization, it is what makes an insert atomic at all.

**A row exists exactly when its primary key column key exists.** That key is
written first and conditionally, and it is the row's commit point:

- Two concurrent inserts of the same row id cannot both report success, because
  exactly one wins the `SetIfAbsent`.
- A delete takes effect before its remaining columns are swept, so a reader sees
  no row rather than a truncated one.
- Existence is a one-key read, so `SELECT` and `UPDATE` never scan a row that is
  not there.

The ordering matters as much as the choice. Writing the other columns *first*
would be strictly worse than non-atomic: a losing insert would have already
overwritten the winner's columns before discovering the conflict. Writing last
would leave a window in which a partial row is visible. The primary key goes
first, always.

This refines an earlier claim that a row exists iff *any* of its column keys
exists. That was true of the layout and false of a concurrent write: with
several columns landing by several writes, "any" makes a partial row look
complete, and an insert has no single key to make its atomicity about.

### A scan callback may not write, and the row sweep depends on it

`Store.ScanPrefix` invokes its callback under the storage engine's read lock:
`Engine.ScanPrefix` holds `mu.RLock` for the whole walk, because the index
restructures under the cursor and cannot be mutated mid-scan. A write issued
from inside that callback needs the write lock the read lock is already holding,
so it blocks forever rather than failing.

`deleteRow` therefore does not sweep inside the callback. `sweepRow` collects
the row's keys during the scan and deletes them after it returns. This is not a
tidy-up — it is a correctness requirement, and one that happened to be masked:
the cluster and router stores materialize their sorted runs before invoking the
caller's callback, so the callback there is already outside the lock, and the
row code was safe only by accident of which store the SQL server was given. A
store that called back in place, including a direct engine-backed one, would
have deadlocked on the first row delete. Materializing the keys makes the
requirement local instead of depending on every `Store` implementation getting
it right; `internal/sql/engine_store_test.go` pins this against the real engine
rather than a double, since a test double would have inherited the same accident
and passed either way.

The sweep also no longer aborts on the first delete error. Stopping early left
more orphan column keys for no benefit, and the orphans are invisible either way
because the primary key is already gone. It now attempts every key and reports
the first error.

### What a row still cannot do

A row is several keys and the store has no transaction, so a crash *between* two
column writes leaves a row that exists without all of its columns. This is not
papered over:

- A **reported** store failure is compensated. `insertRow` removes what it
  already wrote, so a client that sees an error can retry against a row that is
  not there.
- A **crash** cannot be compensated by a process that is no longer running, and
  is left to a future transaction. A recovery pass that repaired rows lacking
  non-key columns is the honest fix, and it belongs with the TSO work in
  guardrail 4 rather than here.
- A `DELETE` sweep failure leaves orphan column keys. They are invisible,
  because the primary key is gone, and are reported rather than retried.

The general statement: this phase has row-atomic *existence*, not row-atomic
*content*. A single-key read tells you whether the row is there; it does not tell
you the row is complete.

### Reading a row

A row read is one range scan over the row's prefix, and the cells are placed by
**column name**, not by position. Key order is lexicographic, so the scan
returns columns sorted by name and in no particular relation to declaration
order -- `(id, age, name)` scans back as `age, id, name`. A reconstruction that
trusted scan position would silently swap `age` and `name`, which is why the
ordinal is looked up in the schema and the scan is only used to find values.

Values crossing the wire are re-encoded from the column's own order-preserving
encoding, and text-format parameters are parsed against the column type at the
boundary. That keeps the stored bytes well formed: a value that could not be
parsed is rejected on the way in, rather than stored and left to fail to decode
on the next read.

## DML over catalog tables: where the table is resolved

A DML statement has to answer one question the parse tree cannot: *does this
table exist?* The answer is not available at translation time, and the first
implementation got it wrong in the most damaging way available — it hardcoded
the implicit table's name and reported `42P01` for everything else. A table that
had just been created and was sitting in the catalog was described to the client
as absent. That is worse than a slow answer, because it is confidently wrong and
a client that trusts it will conclude its own `CREATE TABLE` failed.

So the name check is gone, and the question is asked at execution against the
live catalog (`resolvePlan`). The consequences are deliberate:

- **Translation is pure.** A plan carries a table name, never a schema. A
  prepared statement parsed before its table was created must still work after.
- **`42P01` is now earned.** It is produced by a lookup that found nothing, and
  the message names the table the client actually asked about. Reserved names and
  qualifiers naming a database that does not exist are still refused at
  translation, because those are decidable without the catalog.
- **Resolution happens after the RBAC decision.** A session without permission
  is refused first, so it cannot use `42P01` to probe which tables exist.
- **It is idempotent.** `Describe` runs before `Execute` and both need the
  schema — the projection's column count and the parameter types are part of the
  reply. Re-reading the catalog per call would make one statement's answer depend
  on when it was asked.

### One value, one row identity

The row id is derived from the primary key value *through the column's type*:
parse the text as the declared type, then render it back. Without that round
trip `1`, `01` and `+1` are three different keys and therefore three rows, which
a client considers one. It also means a value the type cannot hold is rejected
where it is named instead of at read time, and that the primary key's stored
bytes come from the same encoding as every other column rather than from a
re-encode of a Go string — which would have panicked on every type but text.

### What a catalog table's DML still refuses

Refused rather than approximated, because a wrong answer here is indistinguishable
from a right one:

- A filter on anything but the primary key. Only the key identifies a row; every
  other filter needs a scan whose answer would not mean what equality means.
- `ON CONFLICT`. `DO NOTHING` needs a check atomic with the write and `DO UPDATE`
  needs a read of the existing row; neither is available without a transaction,
  and the row path has one conditional write and nothing else.
- `RETURNING`, `ORDER BY`, `LIMIT`, `DISTINCT`, grouping, `UPDATE ... FROM`,
  `DELETE ... USING`, and output aliases.
- A `DROP TABLE` that still has rows, so that a drop cannot leave keys nothing
  describes. Delete the rows first; this is `0A000`, a limitation, not a fault.

The implicit table keeps its own narrower rules and its own two-column path. The
split is deliberate: the catalog path is a superset, and inheriting the implicit
table's restrictions by accident would have been the wrong direction.

## Replicated DDL: opcodes, timestamps, and the emptiness guard

Schema mutation is the one write whose replay must be *identical* on every
replica. A divergent replay means a table exists on some nodes and not others,
so the DDL path is specified separately from the KV path rather than reusing it
with a flag.

**DDL is its own opcode, not a SET.** `OpCreateTable = 0x06` and
`OpDropTable = 0x07` carry an 11-byte header of the same width as the KV
header — `[1B op][8B TSO][2B keyLen]` — so an entry stays one contiguous buffer,
but the 8-byte field is a **timestamp where a KV entry has a TTL**. Three
things follow, each of which is why a separate codec exists:

- A schema entry has no TTL. A KV entry that expires is unremarkable; a schema
  entry that expires deletes a table out from under its rows. Decoding a DDL
  entry with the KV decoder would read a TSO of ~1.7e17 as a TTL of millennia —
  harmless by accident, and a latent bug the moment anything shortens it.
- The timestamp is allocated on the **proposing** node (`DDLProposer`, wrapping
  the existing `TSOPool`) and committed inside the entry. Nothing in the apply
  path allocates one. A replica that is behind on the log applies the proposer's
  value rather than stamping its own, which is what keeps `Apply` a pure
  function of the log (guardrail 4).
- A create is applied as a **conditional** create (`OpSetNX` against the catalog
  key). A replayed or duplicated create cannot redefine a table that may already
  hold rows; the first creator wins and the duplicate is reported to the
  proposer.

`DecodeDDLEntry` bounds-checks the key length before slicing, and a key length
that does not fit is a malformed entry rather than a panic.

**The emptiness guard fails closed, and that is the whole point.** A drop is
only correct once the rows are gone, and the check must not be able to report
success it did not earn. Three outcomes are distinguished:

| Situation | Result |
|---|---|
| Rows found under the table prefix | `ErrTableNotEmpty` → SQLSTATE `0A000` |
| Guard cannot complete (a region unreadable, or no checker configured) | `ErrEmptinessUnverifiable` → SQLSTATE `XX000` |
| Prefix empty across every region | drop entry proposed, then deleted on apply |

`ErrEmptinessUnverifiable` is deliberately not folded into `ErrTableNotEmpty`.
"I could not check" and "your table has rows" are different answers to a
client, and collapsing them would either invent rows that do not exist or hide a
fault. They also get different SQLSTATEs for the same reason: a full table is a
feature limitation the client resolves by deleting rows first, whereas an
unreadable cluster is a fault the client resolves by retrying. The capability is
a separate interface (`RangeChecker`), so a store that cannot answer range
questions still replicates ordinary writes and schema creates; it is only drops
that are refused.

The refusal is also translated rather than passed through. The cluster layer
reports its own sentinel errors, and `internal/sql` needs them to satisfy its own
so the SQLSTATE does not depend on which store executed the statement —
otherwise the same non-empty `DROP TABLE` would be `0A000` in standalone mode and
an IO fault in cluster mode.

The row prefix is **derived from the catalog key**, never passed alongside it, so
the guard cannot be pointed at a different range than the one the drop removes.
The derivation validates that the database segment inside the catalog key agrees
with the one it is filed under: a key that disagrees is one the guard does not
understand, and guessing which segment to trust is how a guard ends up scanning
the wrong table.

**The scan is cross-shard and cross-region.** The router hashes whole keys, so a
table's rows are spread across every shard; a scan confined to the shard owning
the prefix would report an almost-always-empty table and the guard would pass
when it should fail. And a table's rows can span region boundaries, so the
cluster-wide check walks every region overlapping the prefix rather than
resolving one. It stops at the first key found, so a drop costs O(1) in keys
examined rather than O(table size).

### How SQL reaches it

`Store` has no schema methods, so DDL rides an **optional capability**
(`DDLStore`) rather than a widening of the seam every store must implement.
`Catalog.Create`/`Drop` type-assert for it and fall back to
`SetIfAbsent`/`Delete` when it is absent. That keeps standalone mode — where
there is no Raft log and no TSO — on the path it has always used, and it keeps
the guarantee that actually matters unchanged: the create is conditional either
way, so two concurrent `CREATE TABLE`s still cannot both win.

In cluster mode `server` wires the real pool at startup
(`cs.SetDDL(s.pdNode.Pool())`), so schema entries are timestamped by the
placement driver. The guard rides along with the drop proposer rather than
sitting in `Catalog.Drop`, because a check in the SQL layer followed by a
proposal would leave the same window the proposal-side guard has — running it in
both places shrinks that window without closing it, and duplicates the rule in
two languages.

### Where the guard runs, and why it moved out of Apply

The guard was first written inside `FSM.Apply`, scanning local state before the
catalog key was deleted. That was wrong, and the reason is worth recording
because the wrong version looks obviously safe.

A node's local engines hold the data of **every region that node hosts**, and
which regions a node hosts differs per node (`RegionCoordinator` shares one
dispatcher across all its region `Node`s). So two replicas of the *same* group,
applying the *same* committed entry, could answer the emptiness question
differently: one holds rows for the table under its regions, the other does not.
One replica deletes the catalog key, the other refuses, and the state machines
diverge permanently. `Apply` is required to be a pure function of the log
(guardrail 4), and a guard that consults local state is not.

The guard therefore runs on the **proposing** node, in `GuardedDropProposer`,
before the entry exists:

    propose:  guard across every region leader  ──►  entry enters the log
    apply:    delete the catalog key, unconditionally, on every replica

Two properties follow. The proposer can reach every region's leader, so "empty"
means empty across the cluster. And once an entry is in the log, applying it is
identical everywhere, so there is nothing left that can diverge.

A corollary worth stating: **the guard must not reuse the store's ordinary
`ScanPrefix`.** That path degrades to a best-effort local read when a region's
leader cannot be linearized, which is the right trade for listing rows and the
wrong one for deciding whether a table may be deleted — a region that has not
yet seen the last insert would report "empty", and the drop would orphan live
rows. `PrefixExists` therefore requires every region in the span to be read
strictly and fails the check if any of them cannot be read. Its method name
differs from `ScanPrefix` deliberately, so no future caller can reach for the
best-effort one by muscle memory.

### What the guard still cannot do

**The check-then-propose window.** A row inserted after the guard passes but
before the entry commits is not seen by anything. Closing this needs a table lock
or a two-phase commit, and Phase 9 has neither. `DROP TABLE` is therefore best
effort against a concurrent writer on a live table; deleting the rows first, as
the guard requires, is the sound path.

**The apply-time create is conditional, the drop is not.** A create applies as
`OpSetNX`, so a duplicate cannot redefine a table that may hold rows. A drop
applies as an unconditional delete, because that is what makes it identical on
every replica — and it is safe precisely because the emptiness proof already
happened before the entry was proposed.

**`RowCount` is still used outside the DDL path.** The catalog's table listing
and the standalone store's `Drop` still count rows through `ScanPrefix`. That is
acceptable for a listing and for a single-node store, where "local" is "all of
it", but it is not a substitute for the strict check on a multi-node cluster.
