# ADR-012: PostgreSQL Wire Protocol (Phase 8)

Status: Accepted
Date: 2026-09-24

## Context

Phase 7 (cross-cluster federation) is complete. Phase 8 of the multi-cluster
plan introduces the PostgreSQL wire protocol as Tellstone's SQL entry point:
clients connect with `psql`, ORMs, and BI tools. The roadmap splits the SQL
stack across Phases 8-13, so Phase 8 must decide how much of the SQL surface it
owns versus deferring to the schema (9), parser/optimizer (10), executor (11),
and transactions (12) phases.

ADR-007 previously committed to deprecating RESP in v2.0 and removing it in
v3.0. This ADR accelerates that decision and fixes the Phase 8 scope.

See [MULTI-CLUSTER-PLAN §Phase 8](../MULTI-CLUSTER-PLAN.md) for the roadmap
deliverables.

## Decisions

### 1. Real PostgreSQL parser now (`pganalyze/pg_query_go`), not a mini-SQL translator

Phase 8 uses PostgreSQL's actual C parser via `pg_query_go`. There is no
hand-rolled SQL-to-KV translator that Phase 10 later throws away.

**Rationale:** The roadmap's "SQL-to-KV translation" predates the parser phase,
but writing a throwaway tokenizer double-pays for SQL parsing and risks
behaving differently from PostgreSQL (quoting, escapes, statement variants).
The real parser produces the same parse tree Phase 10's optimizer and Phase
11's executor will consume, so no translator code is discarded and parser
behavior matches `psql` from day one.

### 2. Extended query protocol in addition to simple query

Phase 8 implements both the simple query (`Q`) protocol and the extended
query protocol (`Parse`/`Bind`/`Execute`/`Sync`/`Describe`).

**Rationale:** `psql` only needs the simple protocol, but `pgx`, psycopg2, and
essentially every ORM drive parameterized statements through the extended
protocol. "SQLAlchemy connects and runs basic queries" is an acceptance
criterion (MULTI-CLUSTER-PLAN §Phase 8), which the simple protocol alone
cannot satisfy. Prepared statement caching is out of scope; the extended
protocol stays single-shot and portal-less: `Parse` registers the parsed
statement (session-scoped, named, retained per connection), `Bind` creates a
named portal without executing, and each `Execute` re-runs the full statement.
No results are cached across `Execute` calls and no portal suspension is
supported, matching what stateless drivers require.

### 3. Authentication reuses the existing identity stack

PG wire auth is layered over the existing security model instead of
introducing a parallel one:

- **Identity source** is the existing `rbac.Store` (roles with bcrypt password
  hashes, key/command grants) and the existing `oauth.Provider` for SSO.
- **Wire mechanisms** are `cleartext` — offered only over TLS — plus, when
  SSO is configured, an OAuth token in the password field validated by
  `oauth.Provider`, reusing the exact token path the binary frontend already
  uses. The `password` message field is the role password validated against the
  bcrypt hash. PostgreSQL's `md5` and `SCRAM-SHA-256` exchanges are not
  implemented because roles store bcrypt verifiers, which cannot be reproduced
  as MD5 or SCRAM-SHA-256 verifiers; the listener refuses to fake an
  unsupported mechanism.
- **Authorization** (per-command, per-key grants) continues to run through the
  shared `command`/`rbac` path once a statement is mapped to a KV op.

**Rationale:** Tellstone already owns role + SSO authentication; a parallel
PG-role store would fork the security model for no benefit. Mapping the PG
startup password exchange onto `rbac.Store`/`oauth.Provider` keeps one source
of truth. Because roles store bcrypt hashes (not PostgreSQL MD5/SCRAM
verifiers), cleartext-over-TLS is the only protocol-level exchange that can
validate a bcrypt credential; the same constraint is why cleartext is refused
on an unencrypted connection.

### 4. Implicit `tellstone` schema over the KV engine

Phase 8 exposes one implicit table mapping 1:1 onto the storage engine:

```
CREATE TABLE tellstone (key TEXT PRIMARY KEY, value BYTEA);
```

SQL maps onto the existing `command.Store` (and therefore onto
`clusterStore` in cluster mode — federation and region routing come for free):

- `SELECT value FROM tellstone WHERE key = ?` → `GetErr` (linearizable local
  read / gateway read in cluster mode)
- `INSERT INTO tellstone (key, value) VALUES (?, ?)` → `SetIfAbsent`, so a
  losing writer gets `23505` instead of silently overwriting a row
- `UPDATE tellstone SET value = ? WHERE key = ?` → `SetIfPresent`, so the
  `UPDATE n` tag only counts rows that were actually there
- `ON CONFLICT DO NOTHING` / `DO UPDATE SET value = excluded.value` → the same
  two primitives, the first discarded and the second reused
- `DELETE FROM tellstone WHERE key = ?` → `Delete`

The precondition check and the write must be one operation at the engine. A
check-then-act split across a `Get` and a `Set` lets two concurrent writers both
observe "absent" and both write, which turns a primary key into a suggestion.

**Cluster mode.** Conditional writes are proposed as distinct Raft opcodes
(`OpSetNX`/`OpSetXX`) and evaluated during apply, so the check rides in the log
instead of racing a separate read. State is a pure function of the log here:
the FSM keeps no snapshot of its own and a restart replays committed entries.
That makes apply deterministic as long as replicas agree on state, which holds
for a replayed log but is sensitive to the two ways a replica can lose a key
early — TTL expiry straddling an apply, and memory-ceiling eviction. Eviction
is the pre-existing caveat; conditional writes make a divergence visible as a
wrong `INSERT`/`UPDATE` result rather than only as an unexpected key. A
clustered deployment that must tolerate eviction-driven divergence should route
conditional writes to the key's owning shard with linearizable reads, or move
the precondition into a per-key lock in Phase 9's schema layer.

Only equality predicates on `key` are planned; the schema layer (Phase 9)
generalizes real DDL, types, and columns, and the executor phases bring
`pg_query_go` parse trees to life beyond the implicit table.

**Rationale:** The engine is a key/value store; the implicit table is the
smallest honest SQL surface that satisfies `psql`/ORM acceptance while keeping
Phase 9's schema work unconstrained.

### 5. RESP is removed now; binary frontend removal deferred

RESP (the Redis-compatible listener) is deleted in this phase, not deprecated
in v2.0. That means: the `internal/resp` package, `--enable-resp`,
`--resp-addr`, `--resp-starttls`, `TSD_RESP_*`, the RESP shard in
`config`, the RESP server wiring in `server`, and all RESP tests are removed.

The binary protocol frontend (`internal/network`, the `client` package, and
`cmd/benchmark`) remains for the time being as the internal/admin surface;
removal is scheduled once the PG listener reaches parity, at the v2.0
boundary.

**Rationale:** ADR-007's deprecation window assumed the PG listener needed a
coexistence period and that RESP users would migrate before removal. In
practice Phase 8 lands the PG listener in the same release window, and the
dual-protocol maintenance cost ADR-007 already identified is not worth paying
for a listener we know is being removed. Removing it now means v2.0 ships with
a single SQL interface and none of the Redis baggage. See
[ADR-007](007-drop-redis-compatibility.md) for the original rationale, which
still holds.

### 6. Transport and TLS follow the existing wiring

The PG listener is a `net.Listener` with goroutine-per-connection handling
(each connection is a serial request/response stream), mirroring the
`|`-frontend pattern of `internal/resp` but without an event loop. TLS uses the
existing `tlslib.ConfigStore` + `--tls-cert`/`--tls-key`; the PG-native
`SSLRequest` (0x04d2162f) / `GSSENCRequest` (`0x...`) upgrade path replaces
RESP's STARTTLS. New flags:

- `--pg-addr` (`TSD_PG_ADDR`, default `""`) — PG listener bind; empty disables
  the PG frontend, so `psql` requires an explicit `--pg-addr` to enable it
- `--pg-tls` (`TSD_PG_TLS`) — require TLS on the PG listener

**Rationale:** PG clients negotiate encryption in-band at startup; supporting
`SSLRequest` is how `psql`'s `sslmode=require` and all drivers connect. No new
TLS machinery is needed — the transport upgrade is the same
`tlslib.Conn` wrap the binary listener uses.

## Alternatives considered

- **Keep RESP through v2.0** (ADR-007 original): rejected in decision 5 — the
  coexistence window exists only to force migrations we do not need.
- **Hand-rolled mini-SQL for Phase 8**: rejected in decision 1 — throwaway
  parser, divergent SQL behavior, and a second parse path to delete in Phase 10.
- **PG-role store separate from `rbac.Store`**: rejected in decision 3 —
  forks identity and SSO handling.
- **Simple query protocol only**: rejected in decision 2 — fails ORM/driver
  acceptance.

## References

- [MULTI-CLUSTER-PLAN](../MULTI-CLUSTER-PLAN.md) §Phase 8-13
- [ADR-007: Drop Redis Compatibility](007-drop-redis-compatibility.md)
- PostgreSQL wire protocol v3.0 (`src/backend/tcop/postgres.c`, protocol docs)
- [pganalyze/pg_query_go](https://github.com/pganalyze/pg_query_go)