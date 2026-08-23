# ADR-007: Drop Redis Compatibility

Status: Accepted
Date: 2026-08-20

## Context

Tellstone began as a Redis-compatible in-memory key/value store. It
speaks RESP2 (Redis protocol) and supports a subset of Redis commands
(GET, SET, DEL, PING, AUTH, etc.). This was the right decision for
v1.x — it gave Tellstone instant adoption by leveraging the existing
Redis ecosystem.

The product is now evolving into a PostgreSQL-compatible NewSQL database.
The storage engine (Raft, regions, MVCC, TSO) and the SQL layer
(PostgreSQL wire protocol, schema, joins, transactions) make Redis
compatibility a liability, not an asset.

## Decision

**Deprecate RESP protocol in v2.0.0, remove in v3.0.0.** The binary
protocol is also removed. PostgreSQL wire protocol becomes the sole
interface.

## Rationale

### Redis compatibility constrains the design

The RESP protocol assumes a flat key/value model with no schema. This
conflicts with the SQL layer:

- **Schema vs. schemaless.** RESP has no concept of tables, columns,
  or types. The SQL layer needs schema. Maintaining both models
  simultaneously creates confusion — is `SET user:alice "data"` a
  string operation or a row insert?

- **Command semantics vs. SQL semantics.** `INCR counter` in Redis
  is atomic and returns the new value. `UPDATE tellstone SET value =
  value + 1 WHERE key = 'counter'` in SQL is a statement that returns
  a row count. These are different mental models.

- **No JOINs in RESP.** Redis clients have no concept of JOINs,
  foreign keys, or transactions. RESP users will never use these
  features. The RESP interface becomes a "lite" mode that doesn't
  showcase Tellstone's capabilities.

### Two protocols create maintenance burden

- Every new feature must be tested against both RESP and PG wire
- Bug fixes must be verified in both protocol paths
- Documentation must explain both interfaces
- Community support must handle both Redis and PostgreSQL questions

### The PostgreSQL ecosystem is larger

| Metric | Redis | PostgreSQL |
|---|---|---|
| GitHub stars | ~65K | ~16K (but PG ecosystem is vast) |
| ORMs | None (client libraries) | SQLAlchemy, ActiveRecord, TypeORM, Drizzle, Prisma |
| BI tools | None native | Grafana, Metabase, Tableau, Looker |
| Migration tools | None | pg_dump, pg_restore, Flyway, Liquibase |
| Client libraries | Good | Universal (every language has a PG driver) |
| Hosting | Redis Cloud, ElastiCache | Every cloud provider, every region |
| Standards | Proprietary protocol | SQL standard |

PostgreSQL compatibility gives Tellstone access to the entire SQL
tooling ecosystem. Redis compatibility gives access to... Redis client
libraries, which also speak PostgreSQL.

### Users who need Redis should use Redis

Tellstone's value proposition is not "faster Redis." It is
"PostgreSQL-compatible, globally distributed, in-memory NewSQL." Users
who need Redis semantics (simple key/value, pub/sub, streams) should
use Redis. Users who need distributed ACID transactions, global
distribution, and SQL should use Tellstone.

## What is preserved

- **Storage engine.** The in-memory sharded map, timing wheel eviction,
  WAL, snapshots, encryption — all unchanged. The engine is
  protocol-agnostic.
- **Data model.** Key/value pairs are still the underlying storage.
  SQL is a translation layer over KV.
- **Performance.** Sub-millisecond reads, high write throughput.
  Removing RESP removes protocol overhead.

## What is removed

- **RESP2 listener** (port 6379 default)
- **Binary protocol listener** (port 9988 default)
- **Redis command set** (GET, SET, DEL, PING, AUTH, COMMAND, ROLE, ACL,
  STARTTLS)
- **Redis-specific ACL** (replaced by PostgreSQL-style roles and
  grants)

## Migration path

Users currently using RESP:

1. **v2.0:** Deprecation warning. Start testing PG wire protocol
   alongside RESP. Both work.
2. **v2.x:** Migrate application code from Redis client libraries to
   PostgreSQL client libraries. Run `tellstone migrate` to convert
   existing data.
3. **v3.0:** Remove RESP. Application must use PG wire protocol.

The `tellstone migrate` tool handles data migration:

```sh
# Detect schema from Redis key patterns
tellstone migrate detect-schema --redis-addr localhost:6379

# Migrate data
tellstone migrate redis-to-tellstone \
  --redis-addr localhost:6379 \
  --tellstone-addr localhost:5432
```

## Alternatives Considered

### Keep RESP as a secondary protocol

Maintain RESP alongside PG wire indefinitely. RESP handles simple KV
operations, PG wire handles SQL.

Rejected because:
- Dual-protocol maintenance burden is permanent
- Users are confused about which protocol to use
- Features added to PG wire (joins, transactions) have no RESP equivalent,
  creating an inconsistent experience
- The "lite" RESP mode under-sells Tellstone's capabilities

### Keep RESP but mark it as "compatibility mode"

RESP works but is undocumented and unsupported.

Rejected because:
- Undocumented interfaces still generate support requests
- Users depend on "unofficial" features and get surprised when they break
- Better to remove cleanly than to let it rot

### Replace RESP with a custom KV protocol

Build a new binary protocol that speaks KV but isn't RESP.

Rejected because:
- Reinventing the wheel — PostgreSQL wire protocol already exists
- No existing client libraries for a custom protocol
- PostgreSQL protocol is well-documented and battle-tested

## References

- [TiDB drops MySQL protocol compatibility discussion](https://github.com/pingcap/tidb/issues)
- [CockroachDB: PostgreSQL wire protocol](https://www.cockroachlabs.com/docs/stable/postgresql-wire-protocol)
- [Redis protocol specification](https://redis.io/docs/reference/protocol-spec/)
