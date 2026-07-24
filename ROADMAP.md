# Tellstone Roadmap

> Where we're going, and how we plan to get there.

---

## Phase 2 — Security & Transport (current focus)

Production readiness hinges on transport encryption and authentication. Today Tellstone
runs plaintext TCP with no access control — a non-starter for any real deployment.

### 2a — TLS / mTLS Transport

- [ ] TLS 1.3 support for both binary and RESP2 listeners (`--tls-cert`, `--tls-key`, `--tls-ca`)
- [ ] mTLS (mutual TLS) for service-to-service authentication in Kubernetes
- [ ] Automatic certificate rotation via filesystem watcher or Kubernetes Secret projection
- [ ] STARTTLS upgrade path for RESP connections (graceful upgrade from plaintext)

### 2b — Authentication & Authorization

- [ ] `AUTH` command for RESP protocol (password-based, `AUTH <user> <password>`)
- [ ] `AUTH` handshake for binary protocol (challenge-response or token-based)
- [ ] API key system — per-key ACLs with configurable command/namespace restrictions
- [ ] OIDC / OAuth2 integration for SSO (optional, pluggable provider interface)
- [ ] `ACL SETUSER` / `ACL DELUSER` / `ACL LIST` for managing access rules at runtime
- [ ] Audit logging — who issued what command, when, from which connection

### 2c — Encryption Key Management

- [ ] Key rotation command — re-encrypt all values in-place with a new key (background, non-blocking)
- [ ] Key derivation from environment / Vault / KMS integration (bypass raw `--encryption-key`)
- [ ] Envelope encryption — a master key decrypts per-shard data encryption keys

---

## Phase 3 — Data Durability & Recovery

The WAL provides crash recovery but has no compaction, no portable snapshots, and no
off-site backup story. This phase closes those gaps.

### 3a — WAL Compaction & Optimization

- [ ] Background WAL compaction — merge and deduplicate records, truncate stale tombstones
- [ ] WAL file rotation — split by size or time, archive old segments
- [ ] Incremental WAL replay — track the last replayed offset to speed startup

### 3b — Full-Database Snapshots

- [ ] `BGSAVE` command — fork-free snapshot of the in-memory dataset to a compact binary format
- [ ] Snapshot integrity via xxHash / SHA-256 checksums
- [ ] `LASTSAVE` command — timestamp of the most recent successful snapshot
- [ ] Point-in-time recovery — combine a snapshot + WAL replay to restore to any moment
- [ ] Snapshot streaming — `SAVE TO STDOUT` for piping to external backup tools

### 3c — Backup Integration

- [ ] Pluggable backup backends (interface pattern): local filesystem, S3, GCS, Azure Blob
- [ ] Encrypted backups — extend at-rest encryption to snapshot files (same ChaCha20-Poly1305)
- [ ] Backup rotation policies — keep last N snapshots, or daily/weekly/monthly retention
- [ ] Restore command — `RESTORE <snapshot-file>` to rehydrate an empty instance

### 3d — Better Persistence Backend

- [ ] Optional write-through mode — persist to SQLite / BoltDB as a durable backing store
- [ ] LSM-tree-inspired tiered storage — hot data in RAM, cold data on disk
- [ ] `MIGRATE` command — move keys between shards or to external stores

---

## Phase 4 — Intelligence Layer (Vectors & AI)

The biggest differentiation play. Redis has RediSearch but it's bolted on. Tellstone can
design vector search into its shared-nothing architecture from the start.

### 4a — Vector Index Foundation

- [ ] New shard type: `vector` — stores fixed-dimension float32 embeddings alongside metadata
- [ ] `VADD <index> <key> <vector> [METADATA <json>]` — insert a vector with optional structured metadata
- [ ] `VGET <index> <key>` — retrieve a vector and its metadata
- [ ] `VDEL <index> <key>` — delete a vector
- [ ] `VCOUNT <index>` — count vectors in an index

### 4b — Approximate Nearest Neighbor Search

- [ ] `VSEARCH <index> <query-vector> KNN <k> [EF <ef>] [DISTANCE <cosine|l2|ip>]` — ANN query
- [ ] HNSW (Hierarchical Navigable Small World) index — high-recall, low-latency
- [ ] IVF (Inverted File Index) with product quantization — memory-efficient for large datasets
- [ ] Configurable distance metrics: cosine similarity, L2 (Euclidean), inner product
- [ ] Index creation options: `VCREATE <index> DIM <n> ALGORITHM <hnsw|ivf> DISTANCE <metric>`

### 4c — Vector Metadata Filtering

- [ ] SQL-like WHERE clause on metadata: `VSEARCH <index> <vector> KNN 10 WHERE category = 'docs'`
- [ ] Filtered HNSW — pre-filter at graph traversal time for efficiency
- [ ] Composite indexes — vector similarity + metadata filter in a single query

### 4d — AI Integration Points

- [ ] Bulk ingestion pipeline — `VIMPORT <index> FROM CSV/JSON` for batch loading embeddings
- [ ] Streaming ingestion — accept vectors from Kafka / NATS / gRPC streams
- [ ] Integration hooks for LLM pipelines — embedding generation callbacks
- [ ] Dimension auto-detection from first inserted vector

---

## Phase 5 — Ecosystem & Operations

### 5a — Client SDKs

- [ ] **Go client** — official, high-performance (binary protocol + RESP fallback)
- [ ] **Python client** — with async support (`asyncio`)
- [ ] **Node.js client** — TypeScript-first with full type definitions
- [ ] Connection pooling, automatic reconnection, pub/sub support in all SDKs

### 5b — Protocol Extensions

- [ ] RESP3 support (already planned in Phase 2 milestones)
- [ ] Memcached protocol support (binary and text)
- [ ] gRPC service definition for structured RPC

### 5c — Cluster Mode

- [ ] Consistent hashing across nodes for multi-instance deployments
- [ ] Peer-to-peer replication (Raft consensus or CRDTs for eventual consistency)
- [ ] `CLUSTER INFO` / `CLUSTER NODES` for topology discovery
- [ ] Automatic failover and rebalancing on node join/leave

### 5d — Write-Through to External DBs

- [ ] PostgreSQL, MariaDB, MSSQL write-through adapters (already in Phase 2 milestones)
- [ ] Configurable write policies: synchronous, asynchronous, batched
- [ ] Read-through caching — fetch from backing DB on cache miss

---

## Phase 6 — Observability & Developer Experience

### 6a — Built-in Dashboard

- [ ] Lightweight embedded web UI (single binary, no external deps)
- [ ] Real-time shard stats: keys, memory, hit rate, connections, ops/sec
- [ ] TTL histogram, eviction rate, WAL lag visualization
- [ ] Connection inspector — list active connections per shard

### 6b — Query Layer

- [ ] Wire the existing SQL parser (`internal/protocol`) into the server
- [ ] `SELECT * FROM store WHERE key LIKE 'user:*' AND ttl > 0` — full SQL-like queries
- [ ] Slow query logging with configurable threshold
- [ ] Query plan output (`EXPLAIN SELECT ...`)

### 6c — Operational Tooling

- [ ] `INFO` command — comprehensive server statistics (like Redis INFO)
- [ ] `CONFIG SET` / `CONFIG GET` — runtime configuration changes without restart
- [ ] `SLOWLOG` — track and query slow operations
- [ ] `MEMORY DOCTOR` — memory usage analysis and optimization suggestions
- [ ] Graceful shard migration for rolling upgrades

---

## Design Principles

These principles guide every decision on this roadmap:

1. **Zero-allocation hot path** — performance is the core value prop; never regress the hot path
2. **Shared-nothing first** — avoid shared state; coordinate only when absolutely necessary
3. **Optional everything** — every feature is opt-in, disabled by default, zero cost when off
4. **Single binary** — no external dependencies; embed what you can, link what you must
5. **Security by default in production** — TLS and auth should be trivially deployable, not an afterthought
6. **Cloud-native** — Kubernetes-native config, graceful shutdown, Prometheus metrics, OTLP tracing

---

## Status

| Phase | Status |
|-------|--------|
| Phase 1 — Core Engine | ✅ Done |
| Phase 1.5 — Persistence | ✅ Done |
| Phase 2 — Security & Transport | 🚧 In progress |
| Phase 3 — Data Durability | 📋 Planned |
| Phase 4 — Vectors & AI | 📋 Planned |
| Phase 5 — Ecosystem | 📋 Planned |
| Phase 6 — Observability | 📋 Planned |

---

*"A contest of focus. Keep yours made of steel."* — **Tellstone**
