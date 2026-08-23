# Tellstone Multi-Cluster Architecture Plan

Status: Draft
Date: 2026-08-20

## Vision

Tellstone evolves from a single-node in-memory key/value store into a
worldwide, PostgreSQL-compatible NewSQL database. The storage engine is
a distributed, Raft-replicated, MVCC key/value store. The interface is
PostgreSQL wire protocol with full SQL support.

Redis compatibility (RESP protocol) is maintained during the transition
but deprecated and ultimately removed. See [ADR-007](adr/007-drop-redis-compatibility.md)
and the [Deprecation Plan](#redis-compatibility-deprecation-plan).

**Product positioning:** Tellstone is not a Redis alternative. It is a
PostgreSQL-compatible, globally distributed, in-memory NewSQL database
built on a key/value storage engine.

## Design Principles

1. **Single binary.** One Tellstone executable runs data nodes, embedded
   PD, or both. No separate components to deploy.
2. **Raft per region.** Each region is an independent Raft group. Consensus
   is local, not global.
3. **PD as TSO.** The Placement Driver (embedded etcd) allocates globally
   monotonic timestamps. This is the single source of truth for ordering.
4. **Graceful degradation.** PD failure degrades to read-only, not crash.
   Pre-allocated timestamp pools extend write availability.
5. **Read/write anywhere.** Every node caches a routing table that maps
   key ranges to region leaders. Any node can serve any request by
   forwarding.
6. **Geo-aware.** Regions can be pinned to geographic zones. Cross-zone
   traffic is explicit and measurable.
7. **Pipelined networking.** Inter-node communication uses multiplexed
   gRPC streams to amortize connection overhead and mitigate SDN limits.
8. **PostgreSQL compatible.** The primary interface is PostgreSQL wire
   protocol. Full SQL support: joins, transactions, indexes, schema.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Placement Driver (PD)                     │
│              Embedded etcd cluster (3 or 5 nodes)            │
│                                                             │
│   ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│   │ PD Follower │  │ PD Leader  │  │ PD Follower │           │
│   │   Node 1    │  │   Node 2   │  │   Node 3    │           │
│   └────────────┘  └─────┬──────┘  └────────────┘           │
│                         │                                   │
│                    ┌────▼────┐                              │
│                    │   TSO   │                              │
│                    └─────────┘                              │
└────────────────────────┬────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
   ┌──────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
   │ Region G1   │ │ Region G2│ │ Region G3  │
   │ (Raft group)│ │ (Raft)   │ │ (Raft)     │
   │ ┌───┐┌───┐  │ │ ┌──┐┌──┐ │ │ ┌──┐┌──┐  │
   │ │Ldr││Fol│  │ │ │LD││FL│ │ │ │LD││FL│  │
   │ └───┘└───┘  │ │ └──┘└──┘ │ │ └──┘└──┘  │
   └─────────────┘ └──────────┘ └───────────┘
```

## Components

### 1. Placement Driver (PD)

**Decision:** Embedded in every Tellstone node via embedded etcd.
See [ADR-001](adr/001-embedded-placement-driver.md).

The PD is the cluster brain. It runs as an embedded etcd cluster where
every Tellstone node participates. One node is elected PD leader and
serves as the Timestamp Oracle (TSO).

**Responsibilities:**

- **Region metadata.** Authoritative map of which key range lives where.
  Stored in etcd's KV store, replicated to all PD members.
- **TSO.** The PD leader allocates globally monotonic timestamps. Every
  write receives a timestamp before Raft proposal.
- **Region splitting.** When a region exceeds 64MB, the PD instructs the
  leader to split at the midpoint. Two new regions are created, the
  routing table is updated.
- **Load balancing.** The PD moves regions between nodes based on load,
  disk usage, and geo-proximity. Balancing runs on a configurable
  interval.
- **Cluster membership.** Nodes join/leave the etcd cluster. The PD
  tracks all data nodes and their health.

**Failure modes:**

- PD leader dies: etcd leader election (~1-3s), TSO resumes, writes
  continue after election. Pre-allocated timestamp pools extend write
  availability during the election window.
- PD follower dies: no impact if minority survives.
- Entire PD dies: writes block (pre-allocated pools provide headroom),
  stale reads continue, no data loss. Automatic recovery on restart.

See [ADR-003](adr/003-tso-graceful-degradation.md).

### 2. Regions

A region is a contiguous key range managed by a Raft group. Regions are
the unit of replication, migration, and splitting.

See [ADR-002](adr/002-raft-per-region.md) and
[ADR-004](adr/004-region-based-data-model.md).

```
Region {
    ID       uint64
    StartKey []byte    // inclusive
    EndKey   []byte    // exclusive
    Peers    []Peer    // Raft members
    Leader   uint64    // current leader node ID
    Epoch    uint64    // incremented on split/move
}
```

**Within a region**, the storage engine is Tellstone's existing
sharded in-memory map. The Raft log replaces the local WAL as the source
of truth. Writes go through Raft consensus before being applied.

**Region splitting:**

```
Before: [user:aaa ─────────── user:zzz]  (Region 42, 80MB)
After:  [user:aaa ─── user:fff] [user:fff ─── user:zzz]  (42, 43)
```

Split is a two-phase operation:
1. Leader creates a new region with the upper half of the key range
2. Leader proposes a conf change to add the new region to the routing
   table. The PD confirms and updates all nodes.

**64MB value chunking:**

Large values are split into 64MB chunks within a region:

```
SET bigkey <100MB value>
→ Entry 0: key = "bigkey\x00chunk:0", value = first 64MB
→ Entry 1: key = "bigkey\x00chunk:1", value = remaining 36MB
```

This keeps Raft log entries small and atomic operations fast. The
chunking is transparent to clients — GET reassembles, DEL removes all
chunks.

### 3. Routing Table

Every node caches a routing table that maps key ranges to region leaders.
This is the lookup table that enables read/write anywhere.

See [ADR-004](adr/004-region-based-data-model.md).

```go
type RoutingTable struct {
    mu      sync.RWMutex
    regions []regionRoute  // sorted by StartKey, binary search
}

type regionRoute struct {
    StartKey []byte
    EndKey   []byte
    Leader   string   // "node3:9988"
    Epoch    uint64
}
```

**Lookup:** `rt.Find(key)` → binary search → returns leader address.

**Updates:** The PD pushes routing table updates to all nodes via a
long-lived gRPC stream. The `Epoch` field prevents stale entries.

**Why this works for read/write anywhere:**

1. Client connects to any node (Node A)
2. Node A's routing table says key "user:123" lives on Node C
3. For reads: Node A forwards to Node C
4. For writes: Node A forwards to Node C, which proposes through Raft

### 4. Timestamp Oracle (TSO)

The TSO lives on the PD leader. It allocates globally monotonic
timestamps used for:

- Ordering writes across regions
- Future MVCC transactions (Phase 8)
- Snapshot isolation

**Timestamp format:** `(physical_ms, logical_counter)`

- `physical_ms`: wall clock (NTP-synced)
- `logical_counter`: atomic increment within each millisecond

**Pre-allocation:** Each node holds a pool of pre-allocated timestamps.
The PD refills the pool periodically based on the node's write rate.
This extends write availability during PD outages.

```
Normal:  PD → Node 1: [ts 1,000,000 — 2,000,000]
PD dies: Node 1 burns through pool at write rate
         → seconds to minutes of continued writes
```

**Batching:** Instead of one TSO request per write, nodes batch
timestamp requests. This reduces PD RPC overhead by 10-100x.

See [ADR-003](adr/003-tso-graceful-degradation.md).

### 5. Pipelined Cross-Node Communication

Inter-node communication uses bidirectional gRPC streams that batch
requests and responses. This amortizes connection overhead and mitigates
SDN packet limits.

See [ADR-005](adr/005-pipelined-networking.md).

```
Node A ──── gRPC stream ──── Node B
         ┌──────────────┐
  write  │  req1 (R42)  │
  write  │  req2 (R43)  │   batched into one TCP connection
  read   │  req3 (R42)  │
         └──────────────┘
         ┌──────────────┐
  resp1  │  resp (R42)  │
  resp2  │  resp (R43)  │   responses tagged with request IDs
  resp3  │  resp (R42)  │
         └──────────────┘
```

**Pipeline structure:**

```go
type Pipeline struct {
    stream  pb.Tellstone_StreamClient
    sendCh  chan *pb.Request
    recvCh  chan *pb.Response
    pending map[uint64]chan *pb.Response
}
```

**For worldwide:** Each cluster has a gateway node that maintains
pipelines to gateway nodes in other clusters. Cross-cluster traffic
flows through these pipes.

### 6. Geo-Based Routing

Regions can be pinned to geographic zones via policy rules.

See [ADR-006](adr/006-geo-based-routing.md).

```go
type GeoRule struct {
    Prefix   string  // "user:europe:"
    Zone     string  // "eu-west-1"
    Replicas int     // 3
}
```

The PD respects geo policies when placing regions and choosing leaders.
A write to `user:europe:alice` from us-east-1 gets forwarded to the EU
region leader.

## Write Path (end to end)

```
1.  Client → Node A: SET user:alice {"name": "Alice"}
2.  Node A: routing table lookup → Region 42 leader is Node C
3.  Node A → Node C (pipeline): forward write
4.  Node C: request TSO timestamp from PD leader → ts=1234567890:1
    (or use pre-allocated pool if PD is slow)
5.  Node C: propose to Raft group (Region 42)
    - Raft log entry: {ts: 1234567890:1, op: SET, key: "alice", val: ...}
6.  Majority of Region 42 peers confirm
7.  Apply to local engine (in-memory map)
8.  Reply to Node A → reply to client
```

## Read Path

**Linearizable reads (strong consistency):**

```
1. Client → Node A: GET user:alice
2. Node A → Node C (Region 42 leader)
3. Node C: wait for Raft applied index ≥ read ts
4. Read from local engine
5. Return result
```

**Follower reads (eventual consistency, lower latency):**

```
1. Client → Node A: GET user:alice (with read flag = stale)
2. Node A: routing table → any replica of Region 42
3. Read from nearest replica
4. Return result (may be slightly stale)
```

## Phased Implementation

### Phase 1: Region Abstraction + Raft Group

**Goal:** A single region replicated via Raft. No PD, no routing table.
When `--cluster-mode=false`, existing WAL behavior is unchanged.

**Decisions:** See [ADR-008](adr/008-phase1-implementation-decisions.md)
for Raft-vs-WAL, region mapping, transport, and package structure.

**Deliverables:**

- `internal/cluster/region.go` — Region struct, key range, peers
- `internal/cluster/raft.go` — Raft group wrapper using etcd-io/raft
- `internal/cluster/config.go` — Cluster configuration types
- `internal/cluster/network/transport.go` — gnet-based Raft transport
- `internal/cluster/network/protocol.go` — Wire format for Raft messages
- `internal/cluster/network/server.go` — Inbound Raft connections
- `internal/cluster/network/client.go` — Outbound Raft connections
- Region leader serves reads and writes via Raft consensus
- Follower receives replicated log entries and applies to local engine
- `shard.Execute()` branches on cluster mode: Raft proposal vs. direct WAL

**Verification:** Write 10K keys through the leader, kill the leader,
verify a follower takes over and all data is intact.

### Phase 2: Embedded PD + TSO

**Goal:** Cluster brain with timestamp allocation.

**Deliverables:**

- `internal/cluster/pd.go` — embedded etcd cluster membership
- `internal/cluster/tso.go` — timestamp allocation, pre-allocation,
  batching
- `cmd/tellstone/main.go` — `--cluster-mode` flag, cluster bootstrap
- Node joins etcd cluster on startup
- PD leader allocates timestamps for writes
- Pre-allocation pools on each data node

**Verification:** 3-node cluster, PD leader dies, writes continue for
pre-allocation duration, then block, then resume on PD recovery.

### Phase 3: Routing Table + Forwarding

**Goal:** Read/write anywhere. Any node can serve any request.

**Deliverables:**

- `internal/cluster/routing.go` — routing table, binary search lookup
- PD pushes routing updates to all nodes via gRPC stream
- Node A forwards writes to Region 42 leader on Node C
- Node A forwards reads to Region 42 leader (or any replica for
  follower reads)

**Verification:** Connect client to Node A, write keys that live on
Node C, read from Node B — all operations succeed.

### Phase 4: Region Splitting

**Goal:** Auto-scaling within a cluster. Regions split at 64MB.

**Deliverables:**

- PD monitors region sizes (via periodic reports from region leaders)
- PD instructs leader to split when threshold exceeded
- Split creates new region, updates routing table, notifies all nodes
- 64MB value chunking for large values

**Verification:** Write >64MB of data to a single key range, verify
automatic split, verify all data readable after split.

### Phase 5: Pipeline Streams

**Goal:** Mitigate SDN overhead. Multiplexed inter-node communication.

**Deliverables:**

- `internal/cluster/pipeline.go` — bidirectional gRPC stream, request
  batching, response dispatching
- All inter-node communication flows through pipelines
- Request ID-based response matching
- Connection health monitoring and automatic reconnection

**Verification:** 100K concurrent operations through pipelines, measure
throughput vs. per-request gRPC. Target: 5-10x improvement.

### Phase 6: Geo Routing

**Goal:** Worldwide region placement.

**Deliverables:**

- `internal/cluster/geo.go` — geo policy rules, zone-aware placement
- PD places regions according to geo policies
- Leader election prefers replicas in the same zone as the policy
- Cross-zone forwarding with latency tracking

**Verification:** 3 zones (us-east, eu-west, ap-south), regions pinned
to zones, verify writes from wrong zone are forwarded with correct
latency.

### Phase 7: Cross-Cluster Gateway

**Goal:** Multi-cluster federation. Worldwide scale.

**Deliverables:**

- `internal/cluster/gateway.go` — gateway node, cross-cluster pipelines
- Gateway nodes maintain pipelines to other clusters' gateways
- Cross-cluster reads via gateway forwarding
- Cross-cluster writes require TSO from the home cluster's PD

**Verification:** Two 3-node clusters, cross-cluster read/write,
verify consistency.

### Phase 8: PostgreSQL Wire Protocol

**Goal:** SQL interface. Clients connect with `psql`, ORMs, BI tools.

**Deliverables:**

- `internal/sql/pgwire.go` — PostgreSQL wire protocol implementation
- Authentication (MD5, SCRAM-SHA-256)
- Simple query protocol (SQL string → result set)
- SQL-to-KV translation: SELECT/INSERT/UPDATE/DELETE mapped to
  engine.Get/Set/Delete
- Error handling, notice responses, ready-for-query message

**Verification:** `psql -h localhost -p 5432` connects, executes
`SELECT 1`, `INSERT`, `DELETE`. Application with SQLAlchemy connects
and runs basic queries.

### Phase 9: Schema Layer

**Goal:** DDL support. Tables, columns, types, indexes.

**Deliverables:**

- `internal/sql/schema.go` — schema manager, DDL executor
- `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
- Column types: INT, BIGINT, FLOAT, VARCHAR, BYTES, TIMESTAMP, JSONB, BOOL
- Schema stored in meta region, cached on all nodes
- Schema changes go through Raft for consistency

**Verification:** `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))`,
insert rows, query by primary key, alter column, drop table.

### Phase 10: SQL Parser + Optimizer

**Goal:** Parse and plan SQL queries.

**Deliverables:**

- SQL parser via `pganalyze/pg_query_go` (PostgreSQL's real parser)
- `internal/sql/optimizer.go` — cost-based query optimizer
- Statistics collector (row counts, value histograms, index selectivity)
- Plan types: point lookup, range scan, index scan, full scan
- Cost model: network hops + scan bytes + filter selectivity

**Verification:** `SELECT * FROM users WHERE id = 42` plans as point
lookup. `SELECT * FROM users WHERE name LIKE 'A%'` plans as range scan.
Explain output shows costs.

### Phase 11: Query Executor

**Goal:** Execute query plans against the storage engine.

**Deliverables:**

- `internal/sql/executor.go` — Volcano-style iterator executor
- Scan executor (table scan, index scan)
- Join executor (hash join, merge join, nested loop)
- Aggregation executor (COUNT, SUM, MIN, MAX, GROUP BY)
- Sort executor, LIMIT/OFFSET
- Filter pushdown to storage engine

**Verification:** JOIN query across two tables returns correct results.
GROUP BY with aggregation produces correct counts. ORDER BY with LIMIT
returns top-N.

### Phase 12: Transactions

**Goal:** ACID transactions with snapshot isolation.

**Deliverables:**

- BEGIN / COMMIT / ROLLBACK
- MVCC: version keys by timestamp, read at snapshot
- Write intents: buffered writes visible only to the transaction
- Conflict detection: write-write conflicts abort one txn
- 2PC for cross-region transactions
- Transaction coordinator manages prepare/commit across regions

**Verification:** Two concurrent transactions, one commits, one aborts
on conflict. Snapshot isolation: read at T1 doesn't see writes at T2
if T2 started after T1.

### Phase 13: Secondary Indexes

**Goal:** Index-accelerated queries.

**Deliverables:**

- `CREATE INDEX` on any column expression
- Index maintenance on write (triggered by schema layer)
- Index-only scans (covering indexes)
- Composite indexes, partial indexes

**Verification:** Query with WHERE on indexed column uses index scan
(not full scan). Write performance with indexes within 20% of
write without indexes.

### Phase 14: Extended Query Protocol

**Goal:** Full PostgreSQL wire protocol. ORM-compatible.

**Deliverables:**

- Prepared statements (Parse/Bind/Execute)
- Parameter binding ($1, $2, ...)
- Portal suspend / cursor-based streaming
- COPY protocol (bulk data loading)
- Row description messages (column types, OIDs)

**Verification:** SQLAlchemy, ActiveRecord, pgx all connect and run
complex queries. COPY loads 1M rows in <5 seconds.

### Phase 15: Advanced SQL

**Goal:** Full SQL feature parity for common workloads.

**Deliverables:**

- CTEs (WITH queries)
- Subqueries (correlated, scalar, EXISTS)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- JSONB operators (->, ->>, @>, ?)
- FULL OUTER JOIN, CROSS JOIN, LEFT/RIGHT JOIN
- UNION, INTERSECT, EXCEPT
- CASE/WHEN, COALESCE, NULLIF

**Verification:** TPC-H-inspired analytical queries run correctly.
Complex nested CTEs with window functions produce correct results.

## Redis Compatibility Deprecation Plan

Tellstone started as a Redis-compatible in-memory store. As the product
evolves into a PostgreSQL-compatible NewSQL database, Redis compatibility
(RESP protocol) is deprecated and removed.

See [ADR-007](adr/007-drop-redis-compatibility.md) for the full
rationale.

### Timeline

| Version | Status | What happens |
|---------|--------|-------------|
| v2.0.0 | RESP deprecated | RESP listener logs deprecation warning at startup. All RESP commands still work. Binary protocol still works. |
| v2.x.x | Migration window | Documentation guides migration from RESP to PG wire. Client libraries, example code, and migration tooling provided. |
| v3.0.0 | RESP removed | RESP listener removed. Binary protocol removed. PG wire protocol is the only interface. |

### What changes for users

| Before (v1.x) | After (v3.0) |
|---|---|
| `redis-cli SET key value` | `psql -c "INSERT INTO tellstone (key, value) VALUES ('key', 'value')"` |
| `redis-cli GET key` | `psql -c "SELECT value FROM tellstone WHERE key = 'key'"` |
| Redis client library (any language) | PostgreSQL client library (any language) |
| Port 6379 (RESP) | Port 5432 (PG wire) |
| No schema | `CREATE TABLE` with column types |
| No joins | Full JOIN support |
| No transactions | BEGIN/COMMIT/ROLLBACK |

### Migration tooling

A `tellstone migrate` CLI tool will be provided:

```sh
# Migrate data from a Redis instance to Tellstone
tellstone migrate redis-to-tellstone \
  --redis-addr localhost:6379 \
  --tellstone-addr localhost:5432 \
  --batch-size 1000

# Generates schema from Redis key patterns
tellstone migrate detect-schema \
  --redis-addr localhost:6379 \
  --output schema.sql
```

### Backward compatibility guarantee

- **v1.x → v2.0:** No breaking changes. RESP still works. Deprecation
  warning only.
- **v2.0 → v2.x:** Migration window. RESP still works. Tooling provided.
- **v2.x → v3.0:** Breaking change. RESP removed. Users must migrate
  before upgrading.

The storage engine (Raft, MVCC, regions) is unaffected by protocol
changes. Data is the same — only the interface changes.

## Configuration

### New Flags

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--cluster-mode` | `TSD_CLUSTER_MODE` | `false` | Enable cluster mode |
| `--cluster-nodes` | `TSD_CLUSTER_NODES` | *(none)* | Comma-separated list of node addresses for initial cluster bootstrap |
| `--node-id` | `TSD_NODE_ID` | *(auto)* | Unique node identifier (auto-generated if not set) |
| `--pd-addr` | `TSD_PD_ADDR` | *(none)* | External PD address (optional — overrides embedded PD) |
| `--region-size` | `TSD_REGION_SIZE` | `64MiB` | Target region size before split |
| `--snapshot-interval` | `TSD_SNAPSHOT_INTERVAL` | `0` | Snapshot interval (existing, unchanged) |
| `--snapshot-bytes` | `TSD_SNAPSHOT_BYTES` | `64MiB` | Snapshot size trigger (existing, unchanged) |

### Example: 3-Node Cluster

```sh
# Node 1
./bin/tellstone \
  --cluster-mode \
  --cluster-nodes "node1:9988,node2:9988,node3:9988" \
  --node-id "node1" \
  --enable-persistence \
  --enable-encryption \
  --encryption-key-file /etc/tellstone/key

# Node 2
./bin/tellstone \
  --cluster-mode \
  --cluster-nodes "node1:9988,node2:9988,node3:9988" \
  --node-id "node2" \
  --enable-persistence \
  --enable-encryption \
  --encryption-key-file /etc/tellstone/key

# Node 3
./bin/tellstone \
  --cluster-mode \
  --cluster-nodes "node1:9988,node2:9988,node3:9988" \
  --node-id "node3" \
  --enable-persistence \
  --enable-encryption \
  --encryption-key-file /etc/tellstone/key
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Clock skew breaks TSO ordering | Write conflicts | NTP enforcement, maximum drift rejection |
| etcd cluster split-brain | Data inconsistency | etcd's own Raft handles this; 3-node cluster tolerates 1 failure |
| Region hot-spot | Load imbalance | PD auto-splits hot regions, moves to less loaded nodes |
| Pipeline head-of-line blocking | Latency spike | Per-region channels within pipeline, independent flow control |
| Cross-cluster latency | Write latency | Geo-routing keeps writes local when possible; async replication for eventual consistency |

## ADRs

- [ADR-001: Embedded Placement Driver](adr/001-embedded-placement-driver.md)
- [ADR-002: Raft Per Region](adr/002-raft-per-region.md)
- [ADR-003: TSO Graceful Degradation](adr/003-tso-graceful-degradation.md)
- [ADR-004: Region-Based Data Model](adr/004-region-based-data-model.md)
- [ADR-005: Pipelined Networking](adr/005-pipelined-networking.md)
- [ADR-006: Geo-Based Routing](adr/006-geo-based-routing.md)
- [ADR-007: Drop Redis Compatibility](adr/007-drop-redis-compatibility.md)
- [ADR-008: Phase 1 Implementation Decisions](adr/008-phase1-implementation-decisions.md)
