# ADR-008: Phase 1 Implementation Decisions

Status: Accepted
Date: 2026-08-20

## Context

Phase 1 of Tellstone's multi-cluster architecture introduces Raft
consensus per region. Several implementation decisions needed to be
made before coding could begin.

## Decisions

### 1. Raft Log vs. Local WAL

**Decision:** Raft log replaces the local WAL in cluster mode. When
`--cluster-mode=false`, the existing WAL persists unchanged.

```
Cluster mode (ON):    Write → Raft proposal → consensus → apply to engine
                      Recovery: replay Raft log
                      
Cluster mode (OFF):   Write → local WAL → apply to engine  (existing behavior)
                      Recovery: replay local WAL
```

**Rationale:** The Raft log provides consensus and replication. A
separate local WAL would be redundant — every committed Raft entry
is already replicated to a majority. The local WAL is only needed
for the non-cluster case where there is no Raft group.

**Impact:** The persistence layer (`internal/persistence`) is
reused for non-cluster mode. In cluster mode, a new Raft log
storage layer replaces it. The `shard.Execute()` method branches
based on cluster mode.

### 2. Region ↔ Shard Mapping

**Decision:** 1:1 mapping — one region equals one shard equals one
Raft group. This holds until Phase 5.

**Rationale:** The existing shard model is already 1:1 with engines
and WAL files. Mapping regions 1:1 to shards means minimal changes
to the existing code. Each shard gets a Raft group, each Raft group
owns one engine.

**Phase 5+ consideration:** Regions may span multiple shards or
shards may be rebalanced within regions. The 1:1 constraint is
intentionally temporary.

### 3. Raft Transport

**Decision:** Custom batching and multiplexing layer built on gnet
(raw TCP), NOT gRPC.

**Rationale:** gRPC adds framing overhead and dependency weight.
gnet is already a dependency (used by the binary and RESP listeners)
and provides high-performance event-driven I/O. A custom transport
on gnet allows:
- Multiplexing all Raft regions over a single TCP connection per
  node pair (one connection carries messages for all regions)
- Batching multiple Raft messages into a single TCP frame
- Full control over framing, backpressure, and flow control
- No protobuf/gRPC dependency for the hot path

**Implementation:** `internal/cluster/network` handles the wire
protocol, connection management, and message batching.

### 4. Package Structure

**Decision:** Use sub-packages under `internal/cluster/`.

```
internal/cluster/
├── region.go          # Region struct, key range, lifecycle
├── raft.go            # Raft group manager (propose, apply, commit)
├── config.go          # Cluster configuration types
├── network/
│   ├── transport.go   # Raft message transport (gnet-based)
│   ├── protocol.go    # Wire format for Raft messages
│   ├── server.go      # Inbound Raft connections
│   └── client.go      # Outbound Raft connections
├── region_test.go
├── raft_test.go
└── network/
    ├── transport_test.go
    └── protocol_test.go
```

**Rationale:** The network layer is a distinct concern with its own
wire format, connection lifecycle, and testing needs. Separating it
into `internal/cluster/network` keeps the cluster package focused on
Raft logic and region management.

## Status

These decisions are accepted and will be implemented in Phase 1.
Revisit in Phase 5 when the 1:1 mapping constraint is lifted.
