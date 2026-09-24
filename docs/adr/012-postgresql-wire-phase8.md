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
cannot satisfy. Prepared statement caching is out of scope; `Parse` executes
immediately against the implicit schema and `Execute` replays the cached
result is not supported — each `Parse`/`Bind`/`Execute` round is treated as a
single-shot statement (portal-less), matching what stateless drivers require.

### 3. Authentication reuses the existing identity stack

PG wire auth is layered over the existing security model instead of
introducing a parallel one:

- **Identity source** is the existing `rbac.Store` (roles with bcrypt password
  hashes, key/command grants) and the existing `oauth.Provider` for SSO.
- **Wire mechanisms** are `cleartext`, `md5`, and `SCRAM-SHA-256` (SASL)
  negotiated in the startup exchange; cleartext is only offered over TLS.
  The `password` message field is the role password validated against the
  bcrypt hash, or — when SSO is configured — an OAuth token validated by
  `oauth.Provider`, reusing the exact token path the binary frontend already
  uses.
- **Authorization** (per-command, per-key grants) continues to run through the
  shared `command`/`rbac` path once a statement is mapped to a KV op.

**Rationale:** Tellstone already owns role + SSO authentication; a parallel
PG-role store would fork the security model for no benefit. Mapping the PG
startup password exchange onto `rbac.Store`/`oauth.Provider` keeps one source
of truth. SCRAM subsumes MD5 and forwards HTTP-digest-style security, but
because roles store bcrypt (not SCRAM verifiers), SCRAM is the protocol-level
negotiation while the stored credential stays bcrypt; MD5/cleartext cover
drivers that demand the older exchange.

### 4. Implicit `tellstone` schema over the KV engine

Phase 8 exposes one implicit table mapping 1:1 onto the storage engine:

```
CREATE TABLE tellstone (key TEXT PRIMARY KEY, value BYTEA);
```

SQL maps onto the existing `command.Store` (and therefore onto
`clusterStore` in cluster mode — federation and region routing come for free):

- `SELECT value FROM tellstone WHERE key = ?` → `GetErr` (linearizable local
  read / gateway read in cluster mode)
- `INSERT INTO tellstone (key, value) VALUES (?, ?)` / `UPDATE tellstone SET
  value = ? WHERE key = ?` → `Set`
- `DELETE FROM tellstone WHERE key = ?` → `Delete`

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

- `--pg-addr` (`TSD_PG_ADDR`, default `127.0.0.1:5432`) — PG listener bind
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