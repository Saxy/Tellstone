# ADR-014: Query Planner and Predicate Pushdown (Phase 10)

Status: Accepted
Date: 2026-10-02

## Context

Phase 9 delivered the schema layer: catalog-backed tables, the
`<db>/<table>/<row-id>/<column>` key layout, order-preserving value encodings,
a front-coded B+Tree, and point lookups plus full-table scans (ADR-013).
Phase 10 is titled "SQL Parser + Optimizer" in
[MULTI-CLUSTER-PLAN §Phase 10](../MULTI-CLUSTER-PLAN.md). Two of its five
deliverables are already satisfied or already deferred, and one of its two
verification criteria cannot be met by the Phase 9 key layout. Recording the
corrections first is the point of this ADR.

**The parser is done.** The plan's first deliverable is "SQL parser via
`pganalyze/pg_query_go`". Phase 8 adopted exactly that
(ADR-012 decision 1, `go.mod`), specifically so that no throwaway translator
would need discarding. `internal/sql/translate.go` already consumes the
`pg_query` parse tree. Phase 10 is therefore an **optimizer** phase: predicates,
plan selection, and a cost estimate.

**Index scans are Phase 13.** The plan lists an "index scan" plan type, but
ADR-013 §6 already deferred it: *"Secondary indexes and index-scans are
explicitly Phase 13."* Phase 10 defines the plan type and leaves it unimplemented.

**A range scan over a non-key column is impossible today.** The layout puts the
row id *before* the column, so one column's values are not contiguous across
rows. Sorted, `age` keys for three rows are:

```
tellstone/users/1/age    tellstone/users/10/age    tellstone/users/2/age
```

Ordered by row id, not by value, and no prefix selects them — the prefix probe
`tellstone/users//age` matches nothing, because the row id is not a wildcard.
This **narrows ADR-013 §4**, which claimed the encodings make "a prefix/range
scan over a column key" order-preserving. The encodings are order-preserving
*within one row prefix*, which is the one-row case and cannot order rows against
each other. Ordering rows by a non-key column needs the `users@<index>/...`
layout that ADR-013 §6 defers to Phase 13.

**A range scan over a text primary key is impossible today.** `EscapeRowID`
escapes `%` and `/`, and the escape is **not order preserving**: `'&'` (0x26)
sorts between `'%'` (0x25) and `'/'` (0x2F) raw, but escapes to a string
beginning with `'%'`, which sorts *below* `'&'`. So `'&' < '/'` raw becomes
`'&' > '%2F'` escaped. This was confirmed exhaustively and over 500k random
full-byte pairs. The comment at `internal/keyspace/keyspace.go` is correct and
this ADR relies on it. `WHERE name > 'A'` on a `VARCHAR` primary key therefore
cannot be a range scan.

Consequently the plan's verification criterion — *`SELECT * FROM users WHERE
name LIKE 'A%'` plans as a range scan* — is **unsatisfiable** for a text
primary key, and ADR-013's motivating example table uses exactly
`name VARCHAR PRIMARY KEY`. What remains range-scannable is a single-column
`INT`/`BIGINT` primary key, whose encoding is fixed-width and order-preserving.

**The store seam cannot express a bounded range.** `Store.ScanPrefix(prefix,
fn)` (`internal/command/command.go`) takes a prefix and no upper bound.
`keyspace` already provides the successors (`TablePrefixEnd`, `RowPrefixEnd`),
but nothing can hand them to a scan. A range plan is inexpressible at the seam
until this widens, which makes it a guardrail rather than a detail.

**Predicates are one shape, and a stricter one than assumed.**
`rowPredicate` (`internal/sql/translate.go`) accepts only `column = value`;
`translateSelect` refuses grouping, ordering, limiting, and `DISTINCT`; and
`resolvePlan` (`internal/sql/exec.go`) refuses any filter that is not the primary
key, with a statement that only the primary key identifies a row. A statement
with no `WHERE` at all is refused at translation. So a catalog table today
answers **exactly one query shape**: `WHERE pk = value`.

**There is no table scan in the SQL layer.** Every `ScanPrefix` call in
`internal/sql` uses a single row's `RowPrefix` — for row reconstruction and for
the delete sweep. Nothing scans a table. This is narrower than ADR-013 §6, which
claims Phase 9 supports "`WHERE <column> = ?` / unfiltered selects as a full
prefix scan … with filters applied by the schema layer". That filtering layer was
not built; ADR-013 §6's "full-table scan" is not present in the SQL executor.

**The wire protocol cannot return more than one row.** `execOutcome` carries a
single `row [][]byte` — the cells of one row — and `emitOutcome` is documented as
"writes a single DataRow". Every `SELECT` answers `SELECT 0` or `SELECT 1`.
There is no result-set loop, and no portal suspend.

This is the binding constraint on Phase 10, and it is not visible in the plan
document. `PointLookup` returns at most one row and works today. **Every other
plan can match many rows** — `WHERE id > 42` over a thousand rows matches a
thousand — so both `RangeScan` and `FullScan` require multi-row delivery before
they can execute a single correct result. That delivery is the plan document's
Phase 14 work ("Portal suspend / cursor-based streaming"). Phase 10 therefore
cannot ship an executable `RangeScan` or `FullScan` without pulling it forward.

## Decisions

### 1. Phase 10 is a planner; it executes nothing new

Phase 10 produces a plan tree and a cost estimate. The iterator/pipeline engine
that *runs* a tree — sort, aggregate, join — is Phase 11. `ORDER BY`,
`LIMIT`/`OFFSET`, `GROUP BY`, and `HAVING` are **refused at translation** in
Phase 10 exactly as they are today, and are picked up in Phase 11 by the
executors that can answer them.

**Rationale:** none of the four is answerable without an executor that can hold
or consume a row stream. Planning them now would mean planning for an execution
model that does not exist, and the shapes would be rewritten in Phase 11 anyway.
Refusing them is not a regression — it is the current behaviour, kept
deliberately.

### 2. The planner is topology-agnostic

A plan is a function of the statement, the table's schema, and the table's
statistics. It knows nothing about regions, shards, or hops. The cost model
estimates **bytes scanned** and **rows examined**, not network round trips.

**Rationale:** the plan document's cost model lists "network hops" as an input,
but hop cost is not knowable at plan time on a cluster whose regions move, and
the thing that routes scan fragments is the Phase 11/12 distributed coordinator.
Encoding placement knowledge in the planner would make plans depend on cluster
topology, which would make them non-deterministic and unreproducible under
`EXPLAIN`. If hop penalties are ever needed, they belong as a cost *modifier*
applied where the coordinator binds the plan to regions — a change that does not
require rewriting the planner.

### 3. Three plan types, one executable today

| Plan | Chosen when | Scan shape | Phase 10 |
|---|---|---|---|
| `PointLookup` | equality on the primary key | one row prefix, one bounded walk | **executable** |
| `RangeScan` | inequality / `BETWEEN` on an `INT`/`BIGINT` primary key | `[RowPrefix(lo), RowIDPrefixEnd(hi)]` | planned + costed |
| `FullScan` + `Filter` | every other filter | `TablePrefix`, filter in the scan | planned + costed |
| `IndexScan` | Phase 13 | — | interface stub only |

`IndexScan` is **declared as an interface with no implementation** in Phase 10.
Declaring it now means Phase 13 adds a plan rather than retrofitting the
planner's type switch.

A planned `RangeScan` or `FullScan` is **refused at execution** with SQLSTATE
`0A000` and a message naming both the plan and the phase that will run it
(`plan FullScan cannot be executed: multi-row execution pipeline deferred to
Phase 11`). Naming the plan is deliberate: a client that sees `Range Scan` knows
to retry with an equality, and one that sees `Seq Scan` knows to wait. This
changed two existing client-visible errors from `42601` to `0A000`, since
translation no longer rejects these shapes before planning — see "Consequences".

### 4. An integer row id is 16 hex digits, and that is what makes ranges sound

`RangeScan` needs the row id to satisfy **two** properties at once:

1. **Order-preserving** — the row ids of `10` and `9` must sort in that order.
2. **Escape-free** — `EscapeRowID` must be a no-op, because escaping inverts
   order.

Neither the value encoding nor a decimal rendering supplies both:

| Rendering | Order-preserving? | Escape-free? |
|---|---|---|
| `EncodeOrderableInt` (8 raw bytes) | yes | **no** — contains `0x25`/`0x2F` often |
| decimal text (what `rowID` used to emit) | **no** — `"10" < "9"` | yes |
| **16 hex digits of `v ^ (1<<63)`** | **yes** | **yes** — only `0x30`–`0x39`, `0x61`–`0x66` |

The decimal rendering is what the code did before this ADR, and it fails the
first property outright: 29 order inversions in `[-30, 30]`. The raw byte
encoding fails the second — escaping `/` yields `%2F`, whose leading `0x25`
sorts *below* the byte it replaced, so a range bounded on the unescaped encoding
returns the wrong rows. Fixed-width hex satisfies both, and costs 16 bytes
instead of 8.

**Rationale:** a range that silently returns the wrong rows is worse than no
range plan at all, so the condition is checked in one place —
`rowIDIsOrderPreserving` — and only integer keys pass. **Text primary keys
remain unrangeable** and are planned as `FullScan`: their row ids are the text
itself, so they are neither fixed-width nor escape-free.

### 5. Storage incompatibility: integer row ids are no longer decimal

**This is a breaking change to the stored key format.** Integer primary keys are
written at `keyspace.EncodeIntRowID(v)` — 16 hex digits of `v ^ (1<<63)` —
whereas pre-Phase-10 builds wrote the row id as **variable-width decimal text**.

Rows written before this change are at decimal row ids and are **not reachable**
after it: a lookup for `id = 42` now addresses
`<db>/<table>/000000000000002a/` and will not find `<db>/<table>/42/`. There is
**no migration**, and none is planned.

**No migration is required because the format has not shipped.** ADR-013 §5
records that the raw SQL surface had not shipped, so no deployment holds data in
the old layout. That is the whole justification, and it is a precondition rather
than a preference: if a build containing decimal row ids ever reaches production,
this change needs a rewrite pass over every key, not a planner fix.

Note the two encodings are easy to confuse at a glance, because
`EncodeOrderableInt` still exists and is *not* what a row id is. It remains the
**column value** encoding (ADR-013 §4) and is used for the bytes stored *under*
`<row-id>/<column>`. The row id is `EncodeIntRowID`. Anyone changing key
construction should grep for both.

**Rationale:** with no secondary index there are at most two legal plans for any
query, so the "cost-based optimizer" is a comparison, not a search. `PointLookup`
is already the fast path (`resolvePlan` resolves `WhereCol` against the primary
key today); it becomes a named plan type rather than an implicit branch.

**`RangeScan` and `FullScan` are planned, costed, and refused at execution in
Phase 10.** They appear in `EXPLAIN` with a real cost so the planner is
observable, and the executor returns a feature-limitation error naming what is
missing. This is the same rule Phase 9 follows for `ON CONFLICT` and `RETURNING`:
a plan that cannot produce a correct answer is refused rather than approximated.
Building the first table scan and the first multi-row result path is real work,
and it belongs with the executors that consume a row stream (decision 1).

### 6. Predicates become an expression tree, and only the plannable ones

`rowPredicate` is replaced by a recursive translator over the `pg_query` AST
producing an expression tree:

- **Comparisons** `=`, `<`, `<=`, `>`, `>=`
- **Boolean** `AND`, `OR`, `NOT`
- **Null tests** `IS [NOT] NULL` — meaningful here because **NULL is the absence
  of a key** (ADR-013), so `IS NULL` is a key-existence question, not a value
  comparison
- **Prefix `LIKE`** `'abc%'` with `_` and `%` only in the final position;
  anything else is refused

`IS NULL` is the reason boolean structure is worth modelling rather than
short-circuiting to a single comparison: `a IS NULL OR b IS NULL` must not be
rewritten into an equality on either column.

### 7. `RangeScan` is restricted to integer primary keys

`WHERE id > 42` on an `INT`/`BIGINT` primary key is a range over
`[RowPrefix(hex(43)), TablePrefixEnd)`. A textual primary key is **not**
range-scannable and a non-key column is **not** range-scannable (decision 4).
Both are planned as `FullScan` + `Filter` — correct, just slower.

Exclusive bounds are converted to inclusive ones by **arithmetic on the value**
(`> 42` becomes `>= 43`), never by incrementing encoded bytes.

**This has one trap, and it is a correctness bug if missed:** `id > MaxInt64`
has no successor value, so incrementing would wrap and the range would silently
become a **full scan over the whole column** — a plausible-looking plan that
returns every row the client excluded. `EmptyRange` is recorded explicitly
instead, and `WHERE id > <MaxInt64>` estimates and costs zero.

Two further bounds traps, both found by asserting against real row keys rather
than against the bound values:

- An unbounded side must still be a **key**. Leaving `Lo`/`Hi` nil would hand the
  caller an absent bound with no contract, and reading nil as "scan from the
  table start" is right only by luck.
- A bound must be built through the **same rendering `rowID` uses**. Deriving it
  from the column's value encoding produces keys no row can have, and the range
  comes back empty while looking entirely well formed.

### 8. Statistics are row counts, and histograms are deferred

`ANALYZE` records a per-table row count. Nothing else.

**Rationale:** a histogram or a per-column selectivity estimate has exactly one
consumer — a choice between index scan and table scan. That choice does not
exist until Phase 13 creates an index. With three plans where two are never
competing, a histogram would be a number computed on every `ANALYZE` and read by
nothing. Row counts are kept because they are what makes the cost estimate
meaningful, and because the catalog already carries the table definition, so the
count is one more field on an existing blob rather than a new subsystem.
Histograms and `n_distinct` move to Phase 13, where they have a reader.

**The count is local to a node and is not replicated.** No FSM opcode is added,
and `internal/persistence` is untouched. This is safe because a row count is an
*estimate input*, never a correctness input: every plan stays correct when fed a
wrong count, and the plan *shape* does not depend on the count at all. The cost
is that two nodes can estimate the same table differently, so `EXPLAIN` output is
node-local. Replicated statistics, and a rule for which node's count wins, move
to Phase 11 with distributed routing.

The count is of **rows, not keys**. A four-column row is four keys under the
table prefix, so a key-tallying counter would over-report by the table's width
and inflate every cost that follows. `ANALYZE` deduplicates by row id.

Counts are dropped when a table is dropped, and not set to zero: zero rows is a
measured fact about a table that exists, whereas a dropped table is not a table
with no rows.

### 9. `EXPLAIN` is delivered with the planner

`EXPLAIN` renders the chosen plan tree as indented text with an estimated cost
per node, and **executes nothing**. It is the plan document's stated verification
criterion and the only way to observe planner behaviour without instrumenting
storage.

```
Index Scan on users  (cost=47.00..47.00 rows=1 width=47)
  Index Cond: (id = 7)

Range Scan on users  (cost=47.00..11750.00 rows=250 width=47)  (rows estimated, table never analyzed)
  Index Cond: id >= 10 AND id <= 20

Seq Scan on users  (cost=47.00..1880.00 rows=10 width=188)
  Filter: (name = n7)
  (statistics: analyzed 0s ago, 40 rows)
```

Two details are load-bearing. The `(rows estimated, table never analyzed)`
caveat means a guess is never printed in the same voice as a measurement, so
nobody treats a default as an observation. And the byte model prices a full
scan by the rows it **reads**, not the rows it returns — pricing it by the
output makes a selective scan look cheaper than the range it should lose to,
and the two estimates are then compared on different quantities.

## Implementation guardrails

The column-key layout's advantages depend on storage-layer properties. Two are
absent today; the rest are decisions this ADR fixes for the work that follows.

### Guardrail 1: the store seam grows a bounded range, as an optional capability

`ScanPrefix(prefix, fn)` cannot express `[lo, hi)`. Widening it on `Store`
would force every implementation — `engineStore`, the router store, the cluster
store, and any test double — to provide it, including the ones that cannot
serve a bounded range efficiently.

So a bounded scan is added as an **optional capability interface**, type-asserted
at the call site, matching the `DDLStore` / `RangeChecker` pattern ADR-013
already uses. A store that does not implement it still serves unbounded scans,
and a range plan over such a store falls back to `FullScan` + `Filter` rather
than failing.

**The fallback must be a correct scan, not an error.** A range plan that cannot
be served has to produce the same rows a full scan would, or the planner becomes
correctness-relevant instead of performance-relevant.

### Guardrail 2: bound construction goes through `keyspace`, never through string concatenation

An exclusive upper bound is `RowPrefix` / `TablePrefix` plus a successor byte
(`0xFF`). That byte sorts above every legal column name and below nothing else
that can appear in a key, and `keyspace` already encodes this reasoning in
`RowIDPrefixEnd` / `TablePrefixEnd`. Bounds must be built by those functions.

Appending `0x00` instead is the tempting shortcut and is weaker: a key equal to
the prefix itself is legitimate, and an exclusive upper bound must exclude it.
Hand-rolled bounds at the call site would also re-open the layering inversion
ADR-013 guardrail 3 closed by putting the row grammar in one package.

### Guardrail 3: a filter runs inside the scan callback, and it may not write

`Store.ScanPrefix` invokes its callback under the storage engine's read lock.
A residual `Filter` is read-only, so evaluating a predicate there is safe — but
the constraint is inherited, not gone, and any future pushdown that needs to
write (or to issue another store call) from inside the callback will deadlock
rather than fail. ADR-013 reached the same conclusion for `deleteRow`.

Consequence for this phase: **the filter must not re-enter the store.** A
predicate that needs a second lookup cannot be pushed into the scan callback; it
is evaluated by the caller after the walk, or it is not pushed at all.

### Guardrail 4: multi-row delivery is the prerequisite, and it is not in Phase 10

The executor cannot return two rows, so a plan that can match two rows is not
implementable today. The tempting shortcut is to execute the range and return
the first match, or the first N matches. Both are **wrong answers to a question
the client did not ask**, and both are the failure mode Phase 9 refused
`ON CONFLICT` and `RETURNING` to avoid.

So a `RangeScan` or `FullScan` reaching execution returns a feature-limitation
error, not a truncated result. Making them executable requires:

- a table-level scan in the SQL layer, which does not exist yet (context above),
  and which must respect the callback-may-not-write rule (guardrail 3);
- a result path that emits many `DataRow`s, which requires either buffering the
  whole result — trading away the streaming property the storage scan was built
  for (ADR-013 "Measured index cost") — or portal suspend.

The second is the plan document's Phase 14 item. **This is the critical path for
the rest of the SQL roadmap**, and it is worth naming plainly: phases 11, 13 and
15 all need multi-row delivery, and none of them can deliver a useful query
without it.

### Guardrail 5: cost estimates are estimates, and `EXPLAIN` must not imply otherwise

An `EXPLAIN` that prints a cost is a claim a client may act on. The cost model
here is a byte-count heuristic over row counts that are only as fresh as the
last `ANALYZE`, and it is deliberately topology-agnostic (decision 2). The
rendered output therefore labels estimates as estimates and does not report a
plan as *optimal*. Where only one plan is legal, the output should say so rather
than printing a cost that implies a choice was made and won.

### Guardrail 6: `LIKE` prefix rewriting must not widen a bound it cannot honour

`'abc%'` is a lower bound on a key range. `'abc'` with no wildcard is an
equality. Everything else — `'%abc'`, `'a%b'`, a `_` before the last `%` — is
**refused as a range**, not approximated. A `_` matches exactly one byte and is
not a prefix, and treating it as one would silently widen the range and return
rows the client did not ask for. Refusal falls back to `FullScan` + `Filter`,
which is slower and correct.

## Alternatives considered

- **Build the parser in Phase 10 as the plan document says**: rejected — it is
  done (ADR-012 decision 1). Building it again would fork the parse path ADR-012
  was written to prevent.
- **Ship index scan in Phase 10**: rejected — ADR-013 §6 defers the index key
  layout to Phase 13. An index scan without an agreed `users@<index>/...` layout
  would hard-code a format that Phase 13 then rewrites.
- **Make the planner region-aware so it can cost network hops**: rejected —
  decision 2. Placement knowledge would make plans depend on cluster topology.
- **Implement histograms now, per the plan document**: rejected — decision 8. No
  plan reads them until Phase 13.
- **Widen `Store.ScanPrefix` to take `(lo, hi)` instead of adding a capability
  interface**: rejected — guardrail 1. It forces bounded-range support on every
  store, including doubles and stores that cannot serve one, for a feature one
  plan type uses.
- **Plan `ORDER BY`/`LIMIT`/`GROUP BY` now against the current row-at-a-time
  executor**: rejected — decision 1. The Phase 9 executor materializes rows
  through the wire, and planning a pipeline over an executor that cannot stream
  would produce plans Phase 11 rewrites.
- **Treat `LIKE 'A%'` on a text primary key as a range scan and accept the
  escaping subtlety**: **adopted, as fixed-width hex** (decision 4). Ranging on
  the unescaped encoding is rejected — `EscapeRowID` does not preserve order,
  so the range would omit and include the wrong rows — but the layout was
  changed to make the range sound rather than dropping the criterion. Rejected
  alternatives: rejecting row ids containing the escape byte (turns a valid
  text key into an error), and a length-prefixed or byte-stuffed escape (a new
  format, and a wider rewrite than integers need).
- **Pull multi-row delivery forward into Phase 10 so `RangeScan` and `FullScan`
  can execute**: deferred, not rejected. It is the right fix and it is Phase 14's
  work; doing it inside the optimizer phase would put a wire-protocol rewrite
  inside a planning phase and leave the planner untestable against real
  multi-row output. Recorded as the critical path (guardrail 4) so it is
  scheduled deliberately rather than discovered in Phase 13.
- **Execute `RangeScan` and return the first matching row**: rejected — it is a
  silently wrong answer. A client asking for `id > 42` and receiving one row has
  been told something false, which is worse than a refusal.

## What Phase 10 deliberately does not deliver

- **Multi-row result delivery** — a table-level scan, and a wire path that emits
  more than one `DataRow` (Phase 11 executor / Phase 14 portal suspend). This is
  the prerequisite for every remaining plan type (guardrail 4).
- Execution of `RangeScan` and `FullScan`, which are planned and costed but
  refused with SQLSTATE `0A000` (decision 3)
- Replicated statistics — `ANALYZE` counts are node-local, and no FSM opcode is
  added (decision 8)
- `RangeScan` on a **text** primary key, which stays a `FullScan` + `Filter`
  (decision 4)
- Secondary indexes and index scans (Phase 13)
- `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`, `HAVING`, `DISTINCT` (Phase 11)
- Joins, subqueries, CTEs, set operations (Phase 11 / Phase 15)
- Column histograms, `n_distinct`, per-column selectivity (Phase 13)
- Network-hop or region-placement cost (coordinator, Phase 11/12)
- Any multi-key row atomicity (Phase 12, ADR-013 §7)

## Phase placement

This design is implemented in **Phase 10 (Optimizer)**:

- Expression-tree predicate translation replacing `rowPredicate` — Phase 10
- `PointLookup` / `RangeScan` / `FullScan` plans, `IndexScan` interface stub —
  Phase 10
- Bounded-range store capability — Phase 10
- Row-count statistics and `ANALYZE` — Phase 10
- `EXPLAIN` — Phase 10
- Table-level scan and multi-row result delivery — **Phase 11 / 14, and the
  critical path for both**
- Iterator/pipeline execution of a plan tree — Phase 11
- Secondary indexes, index scans, histograms — Phase 13
- Transactions, write intents — Phase 12 (ADR-013 §7)

## References

- [MULTI-CLUSTER-PLAN](../MULTI-CLUSTER-PLAN.md) §Phase 10-13
- [ADR-012: PostgreSQL Wire Protocol](012-postgresql-wire-phase8.md) §1
- [ADR-013: Table Namespaces over the KV Engine](013-table-namespaces-phase9.md) §4, §6, §7