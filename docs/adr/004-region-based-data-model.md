# ADR-004: Region-Based Data Model

Status: Accepted
Date: 2026-08-20

## Context

Tellstone currently shards data by key hash across fixed shards
(default: GOMAXPROCS). Each shard has its own WAL and engine. This
model works for single-node but doesn't support:

- Dynamic resharding (splitting hot key ranges)
- Range-based operations (scan, geo-routing)
- Replication across nodes (each shard is local)

The new model needs to partition the keyspace into dynamically
sized, replicated, movable units.

## Decision

**Region-based partitioning.** The keyspace is divided into contiguous
key ranges called regions. Each region is managed by a Raft group and
can be split, merged, or moved between nodes independently.

### Region Structure

```go
type Region struct {
    ID       uint64          // globally unique, PD-allocated
    StartKey []byte          // inclusive
    EndKey   []byte          // exclusive
    Peers    []Peer          // Raft members
    Leader   uint64          // node ID of current leader
    Epoch    uint64          // incremented on split/move/conf change
}
```

### Key Range Encoding

Keys are compared lexicographically as byte slices. The routing table
is a sorted slice of regions by `StartKey`, enabling O(log n) binary
search lookup.

```
Region 1: ["",           "user:fff")     → Node 1 (leader)
Region 2: ["user:fff",   "user:zzz")     → Node 3 (leader)
Region 3: ["user:zzz",   "")             → Node 5 (leader)
```

The empty byte slice `""` represents negative infinity (start of
keyspace) and `0xFF...` represents positive infinity (end of keyspace).

### Routing Table

Every node caches a routing table:

```go
type RoutingTable struct {
    mu      sync.RWMutex
    regions []regionRoute  // sorted by StartKey
}

type regionRoute struct {
    StartKey []byte
    EndKey   []byte
    Leader   string       // "node3:9988"
    Epoch    uint64
}

func (rt *RoutingTable) Find(key []byte) *regionRoute {
    // Binary search: find region where StartKey <= key < EndKey
}
```

Updates arrive from the PD via gRPC stream. The `Epoch` field prevents
stale routing entries from being used after a split or move.

### 64MB Value Chunking

Large values are split into 64MB chunks to keep Raft log entries small:

```
SET bigkey <100MB value>

Internal storage:
  key = "bigkey\x00chunk:0"  value = bytes 0-67108863
  key = "bigkey\x00chunk:1"  value = bytes 67108864-104857599
```

- Chunking is transparent to clients (GET reassembles, DEL removes all)
- Chunks are stored as regular key-value pairs in the engine
- A 64MB value becomes 2 Raft log entries, each under the size limit
- The `\x00chunk:N` suffix is internal — clients never see it

### Region Splitting

When a region exceeds the size threshold (default 64MB):

```
Before: Region 42 ["user:aaa" — "user:zzz"]  (80MB)
After:  Region 42 ["user:aaa" — "user:fff"]  (40MB)
        Region 43 ["user:fff" — "user:zzz"]  (40MB)
```

Split process:
1. PD detects region size exceeds threshold (via periodic size reports)
2. PD allocates a new region ID and determines the split key (midpoint)
3. PD instructs the region leader to execute the split
4. Leader creates a new Raft group for the upper half
5. Leader updates routing table (new epoch)
6. PD pushes updated routing table to all nodes

### Interaction with Existing Code

Current single-node persistence:

```
shard_NNN.db     → WAL (append-only)
shard_NNN.snap   → Snapshot
shard_NNN.nonce  → Encryption nonce counter
```

In cluster mode, each region's Raft group uses the same persistence
mechanism but at a finer granularity:

```
region_NNN.wal   → Raft log (for crash recovery of Raft state machine)
region_NNN.snap  → Snapshot of applied state machine
```

The existing `internal/persistence` package is adapted, not replaced.
The WAL format changes to store Raft log entries instead of raw
key-value records.

## Consequences

### Positive

- **Dynamic rescaling.** Hot regions split automatically. Cold regions
  can be merged. No manual resharding.
- **Geo-routing.** Key ranges can be pinned to geographic zones. The
  routing table makes this natural.
- **Independent replication.** Each region has its own Raft group.
  Replication factor, leader placement, and peer count are
  per-region.
- **Efficient range operations.** A scan over "user:*" touches only
  the regions that contain that range, not the entire keyspace.

### Negative

- **Routing table consistency.** All nodes must have an up-to-date
  routing table. Stale entries cause forwarded requests to hit the
  wrong node. Mitigated by epoch-based staleness detection.
- **Split complexity.** Splitting a region while it's serving traffic
  requires careful coordination. The Raft group must be stable during
  the split.
- **Cross-region operations.** Future transactions that span regions
  need distributed coordination.

## Alternatives Considered

### Fixed sharding (current model)

Simple, but doesn't support dynamic resharding or geo-routing. Shards
are fixed at startup and can't be split or moved.

### Consistent hashing (like Dynamo/Cassandra)

Hash rings distribute keys evenly but don't support range-based
operations. A scan over "user:*" would need to query every shard.

## References

- [TiKV Region management](https://docs.pingcap.com/tikv/stable/tikv-architecture#region)
- [Spanner data model](https://research.google/pubs/pub39966/)
- [CockroachDB ranges](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer.html)
