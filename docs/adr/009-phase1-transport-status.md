# ADR-009: Phase 1 Transport — Implementation Status

Status: Accepted
Date: 2026-08-21

## What Was Built

### Custom Binary Codec (`internal/cluster/network/codec.go`)

Zero-protobuf binary codec for Raft messages. Encodes/decodes `raftpb.Message`
without touching the protobuf runtime. The hot path avoids reflection,
descriptor lookups, and `protoiface` allocation.

**Wire format:** Each message is a length-prefixed frame. The batch encoder
coalesces multiple messages into a single frame:

```
[total_len:4][count:2][msg1_len:4][msg1_bytes:...][msg2_len:4][msg2_bytes:...]...
```

**Measured:**
- 10 Raft messages → 1 TCP frame (75 bytes) = 1 packet on the wire
- 50 messages: 98% write reduction vs. per-message sends
- Zero protobuf runtime calls in `internal/cluster/network/`

### Batching Transport (`internal/cluster/network/transport.go`)

TCP transport with per-peer batching sender. Each peer gets a dedicated
`batchConn` that accumulates messages and flushes on either:
- 100μs timer expiry (configurable via `FlushInterval`)
- Batch reaching capacity (`MaxBatchSize`, default 64)

The transport uses `net.Listen`/`net.Conn` (plain Go stdlib TCP).
gnet was evaluated but cannot coexist with the binary data server's
gnet instance in the same process — two `gnet.Run` calls bind to
the same port and deadlock.

**Tests:** 4 transport tests pass (`TestTransportListenAndStop`,
`TestTransportSendReceive`, `TestTransportRegisterPeer`,
`TestTransportConnectionCount`).

### Error Propagation (`internal/cluster/node.go`, `proposals.go`, `server/cluster.go`)

FSM apply errors now propagate back through the proposal pipeline:
- `complete(id)` → `complete(id, err)` with `chan error`
- `shardDispatcher.Dispatch()` checks `resp.Execute()` and returns errors
- `processReady` passes FSM error to proposals

### Key Bug Fixes (`internal/cluster/manual_test.go`)

Two bugs caused `GET` to return `NOT_FOUND` on all nodes (including leader):

1. `binaryGet()`/`binaryDel()` allocated correct payload size but never
   `copy()`-ed the key bytes — server received null bytes
2. Concurrent goroutines sharing TCP connections corrupted the byte stream

Both fixed: `copy()` added, operations serialized per connection.

## Current State

| Component | Status |
|-----------|--------|
| Custom binary codec | ✅ Done — 4 tests pass, zero protobuf runtime |
| Batching transport | ✅ Done — 4 tests pass, 98% write reduction |
| Error propagation | ✅ Done — FSM errors surface to clients |
| SET/GET/DEL end-to-end | ✅ Done — TestManual passes across 3-node cluster |
| Leader election | ✅ Done — 3-node Raft cluster elects leader |
| Full cluster test suite | ✅ Done — all tests pass with race detector |

## What's NOT Done (deferred to future phases)

| Item | Reason |
|------|--------|
| gnet transport migration | Blocked by coexistence with binary data server's gnet instance. Two `gnet.Run` calls per process conflict. Needs architectural decision on single gnet event loop or process separation. |
| Multiplexing all regions over one connection | Currently one connection per peer. Multiplexing requires region-aware framing in the wire protocol. |
| Connection pooling / backpressure | Not yet implemented. Current batching handles throughput; backpressure needed for production load. |
| Performance benchmarks under load | Batching proof shows 98% reduction in sends. Real workload benchmarks needed. |

## Verification

```bash
# All cluster tests with race detector
go test -race -count=1 ./internal/cluster/... -timeout=120s

# Manual end-to-end test (3-node cluster, SET/GET/DEL)
go test -v -race -count=1 -run=TestManual ./internal/cluster/ -timeout=60s

# Codec proof (batching effectiveness)
go test -v -run=TestCodecSingleFrameProvesBatching ./internal/cluster/network/
go test -v -run=TestCodecBatchSizeProvesPacketReduction ./internal/cluster/network/
```

## References

- ADR-008: Phase 1 Implementation Decisions
- ADR-005: Pipelined Networking (100μs batching target)
- ARCHITECTURE.md: Package reference and request flow
