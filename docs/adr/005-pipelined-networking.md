# ADR-005: Pipelined Cross-Node Communication

Status: Accepted
Date: 2026-08-20

## Context

In cluster mode, nodes communicate constantly: forwarding reads/writes
to region leaders, replicating Raft log entries, pushing routing table
updates, and allocating timestamps. Each operation traditionally
requires its own gRPC call (or TCP connection), which has overhead:

- TCP connection setup: ~1-5ms per connection
- TLS handshake: ~5-20ms per connection
- gRPC framing overhead: ~100 bytes per message
- SDN switch limits: max concurrent connections, max packets/sec

At 100K operations/sec across 10 nodes, per-operation connections would
overwhelm both the application and the network.

## Decision

**Pipelined gRPC streams.** Each pair of nodes maintains a single
bidirectional gRPC stream. All inter-node communication multiplexes
over this stream. Requests are tagged with IDs; responses are matched
back to callers.

### Pipeline Architecture

```go
type Pipeline struct {
    nodeID   uint64
    conn     *grpc.ClientConn
    stream   pb.Tellstone_StreamClient
    sendCh   chan *pb.Request
    recvCh   chan *pb.Response
    pending  sync.Map  // request ID → chan *pb.Response
    done     chan struct{}
}

func (p *Pipeline) Send(req *pb.Request) (*pb.Response, error) {
    id := atomic.AddUint64(&p.nextID, 1)
    respCh := make(chan *pb.Response, 1)
    p.pending.Store(id, respCh)
    p.sendCh <- &pb.Request{Id: id, ...}

    select {
    case resp := <-respCh:
        return resp, nil
    case <-time.After(30 * time.Second):
        p.pending.Delete(id)
        return nil, ErrPipelineTimeout
    }
}
```

### Message Batching

Instead of sending one request at a time, the pipeline batches
outgoing messages:

```go
func (p *Pipeline) sender() {
    ticker := time.NewTicker(100 * time.Microsecond)
    batch := make([]*pb.Request, 0, 64)
    for {
        select {
        case req := <-p.sendCh:
            batch = append(batch, req)
            if len(batch) >= 64 {
                p.stream.Send(&pb.Batch{Requests: batch})
                batch = batch[:0]
            }
        case <-ticker.C:
            if len(batch) > 0 {
                p.stream.Send(&pb.Batch{Requests: batch})
                batch = batch[:0]
            }
        }
    }
}
```

Batching window: 100μs. This groups bursty writes into single TCP
frames without adding meaningful latency.

### Stream Per Node Pair

Each pair of nodes maintains exactly one stream:

```
Node A ←→ Node B: single bidirectional stream
Node A ←→ Node C: single bidirectional stream
Node B ←→ Node C: single bidirectional stream
```

For N nodes, each node maintains N-1 streams. With 10 nodes: 9 streams
per node, 45 total in the cluster.

### Failure Handling

- **Stream breaks:** Automatic reconnection with exponential backoff
  (100ms, 200ms, 400ms, ... up to 5s). Pending requests on the broken
  stream are retried on the new stream.
- **Node dies:** All streams to that node are closed. Pending requests
  time out. Routing table is updated to remove the dead node's regions.
- **Half-open connection:** Keepalive pings every 5s detect dead peers
  within 10-15s.

## Consequences

### Positive

- **Amortized connection overhead.** One TLS handshake per node pair,
  not per request. Saves ~5-20ms per operation.
- **SDN-friendly.** Fewer concurrent connections, fewer packets (batched).
  Stays within switch limits.
- **Throughput.** Batching 64 requests into one TCP frame reduces
  syscall overhead by ~64x. Target: 5-10x throughput improvement over
  per-request gRPC.
- **Backpressure.** The send channel applies natural backpressure.
  If the receiver is slow, the sender blocks, preventing memory
  exhaustion.

### Negative

- **Head-of-line blocking.** A slow request on one region blocks other
  requests in the same batch. Mitigated by:
  - Short batch window (100μs)
  - Separate pipelines for different request types (Raft replication
    vs. client forwarding vs. PD communication)
- **Complexity.** Request ID matching, reconnection, and retry logic
  add code. Well-contained in `internal/cluster/pipeline.go`.
- **Debugging.** Multiplexed streams are harder to debug than
  per-request connections. Mitigated by request ID logging and
  Prometheus metrics.

## Protocol

### gRPC Service Definition

```protobuf
service TellstoneStream {
    rpc Stream(stream Batch) returns (stream Batch);
}

message Batch {
    repeated Request requests = 1;
}

message Request {
    uint64 id = 1;
    oneof op {
        WriteRequest write = 2;
        ReadRequest read = 3;
        RaftMessage raft = 4;
        RoutingUpdate routing = 5;
        TSORequest tso = 6;
    }
}

message BatchResponse {
    repeated Response responses = 1;
}

message Response {
    uint64 id = 1;
    oneof result {
        WriteResult write = 2;
        ReadResult read = 3;
        RaftMessage raft = 4;
        RoutingUpdate routing = 5;
        TSOResult tso = 6;
    }
}
```

## Alternatives Considered

### Per-request gRPC

Simple, but each request requires a full gRPC frame (24+ bytes of
headers). At 100K req/sec, that's 2.4MB/sec of pure overhead. Plus
connection setup cost.

### HTTP/2 multiplexing (manual)

Similar to gRPC but without the protobuf framing. More control, but
reinvents gRPC's batching and flow control. Not worth the effort.

### QUIC

Modern protocol with built-in multiplexing. But no mature Go QUIC
library with gRPC compatibility. Premature optimization.

## References

- [gRPC bidirectional streaming](https://grpc.io/docs/languages/go/basics/#bidirectional-streaming-rpc)
- [TiKV coprocessor batching](https://docs.pingcap.com/tikv/stable/tikv-architecture#coprocessor)
