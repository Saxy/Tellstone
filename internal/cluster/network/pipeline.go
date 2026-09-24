/*
Package network
Tellstone Cloud-Native In-Memory Database
File: pipeline.go
Description: Phase 5 pipeline layer. Adds request/response semantics to the
existing per-peer TCP transport: application-level operations (writes forwarded
to a region leader, keepalive pings) are multiplexed over the same batched
peer connections as raft consensus traffic. Requests are tagged with a unique
ID; responses are dispatched back to the waiting caller via a pending map.

Design (ADR-005, without gRPC):
  - One transport per peer pair; pipeline messages share the connection with
    raft frames (different frame group ID, same TCP stream, same 100us flush).
  - Inbound dispatch runs on a small worker pool so a slow forwarded-write
    proposal (a blocked raft apply) does not stall raft message delivery or
    other pipeline operations on the same connection.
  - Keepalive pings detect dead or half-open peers and drop the peer
    connection so the next send redials (exponential backoff lives in the
    transport's dial path).

Note on the forwarding response framing: a response carries either data or an
error string, distinguished by a flags byte (see encodePipeResp). This keeps
success and failure on one code path and leaves room for data-returning ops
(e.g. future forwarded reads).

Authors:

	Maximilian Hagen
*/
package network

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

// OpKind identifies a pipeline operation carried in a pipeMsg.
type OpKind byte

const (
	// OpPing is a keepalive probe. The peer auto-replies with OpPong.
	OpPing OpKind = 1
	// OpPong is the reply to OpPing.
	OpPong OpKind = 2
	// OpForwardWrite forwards a single FSM operation payload to be proposed on
	// the region leader. The reply is OpForwardResp.
	OpForwardWrite OpKind = 3
	// OpForwardChunks forwards a chunk chain (a large value split into several
	// raft entries) to the region leader. The reply is OpForwardResp.
	OpForwardChunks OpKind = 4
	// OpForwardResp is the reply to OpForwardWrite / OpForwardChunks. The
	// framed payload is empty on success or the apply error text on failure.
	OpForwardResp OpKind = 5
	// OpXClusterWrite forwards a federation write (single op payload or a
	// chunk chain) to the gateway of the key's home cluster. The reply is
	// OpXClusterResp, mirroring OpForwardResp semantics.
	OpXClusterWrite OpKind = 6
	// OpXClusterRead forwards a linearizable read to the gateway of the key's
	// home cluster. The reply OpXClusterResp carries a framed "present + value"
	// body so a missing key differs from an empty value (Phase 7, ADR-011).
	OpXClusterRead OpKind = 7
	// OpXClusterResp is the reply to OpXClusterWrite / OpXClusterRead. On
	// error the framed payload holds the error text; on success it holds the
	// read result body or is empty for a write.
	OpXClusterResp OpKind = 8
)

// responseKind returns the reply kind a peer uses for a given request kind.
func responseKind(req OpKind) OpKind {
	switch req {
	case OpPing:
		return OpPong
	case OpXClusterWrite, OpXClusterRead:
		return OpXClusterResp
	default:
		return OpForwardResp
	}
}

// PipeHandler applies an inbound forwarded operation on the leader. payload is
// the request body; a non-nil error becomes the framed error response.
// Handlers run on the pipeline worker pool and may block (raft propose).
type PipeHandler func(op OpKind, payload []byte) ([]byte, error)

// pipeMsg is one message on the wire, structurally independent of raftpb. The
// pipeline is bidirectional: requests carry kind/op and responses carry
// kind=resp with the same reqID for correlation.
type pipeMsg struct {
	kind    OpKind
	reqID   uint64
	from    uint64 // node ID of the sender (used to route responses)
	gid     uint64 // region/raft group the op targets (0 for pings)
	payload []byte
}

// pipeResult is delivered to a blocked Call when its response arrives.
type pipeResult struct {
	payload []byte
	err     error
}

const (
	// pipeDispatchCap bounds in-flight inbound pipeline messages per process.
	// Reaching it rejects further messages with errPipelineOverloaded instead of
	// blocking, so a saturated dispatch never stalls the transport read loop
	// (which also carries raft message delivery on the same connection).
	pipeDispatchCap = 1024
	// keepaliveInterval is how often each registered peer is probed. It bounds
	// dead-peer detection latency: a dead peer is dropped within two intervals.
	keepaliveInterval = 5 * time.Second
	// keepaliveTimeout is how long a ping may take before the peer is
	// considered suspect. Generous so a saturated (but alive) peer is not
	// dropped on the first miss.
	keepaliveTimeout = 5 * time.Second
	// keepaliveFailuresBeforeDrop is how many consecutive ping misses trigger
	// dropping the peer connection (a single miss can be a scheduling blip).
	keepaliveFailuresBeforeDrop = 2
)

// PipeStats holds atomic counters for pipeline observability. All fields are
// lock-free; no allocation occurs on the hot path.
type PipeStats struct {
	Requests   atomic.Int64 // outbound request operations (Call)
	Responses  atomic.Int64 // inbound requests answered => responses sent
	RecvMsgs   atomic.Int64 // pipeline messages decoded from peers
	SentMsgs   atomic.Int64 // pipeline messages queued to peers
	Timeouts   atomic.Int64 // calls that timed out before a response
	Reconnects atomic.Int64 // peer connections dropped by keepalive
}

// Pipeline is the app-level request/response layer over a Transport. There is
// exactly one Pipeline per Transport (one per process; raft groups share the
// transport and the pipeline, distinguished by group ID).
type Pipeline struct {
	transport *Transport
	nodeID    uint64
	logger    log.Logger

	nextID  atomic.Uint64
	pending sync.Map // reqID -> chan pipeResult

	mu       sync.RWMutex
	handlers map[uint64]PipeHandler // region gid -> forward handler

	dispatchCh chan pipeMsg
	stopCh     chan struct{}
	started    atomic.Bool
	stopped    atomic.Bool
	wg         sync.WaitGroup

	stats PipeStats
}

// NewPipeline creates a pipeline bound to a transport. The transport assigns
// each pipeline its owner node ID; all raft groups on the shared transport use
// the same node ID.
func NewPipeline(t *Transport, nodeID uint64, logger log.Logger) *Pipeline {
	return &Pipeline{
		transport:  t,
		nodeID:     nodeID,
		logger:     logger,
		handlers:   make(map[uint64]PipeHandler),
		dispatchCh: make(chan pipeMsg, pipeDispatchCap),
		stopCh:     make(chan struct{}),
	}
}

// Stats returns the pipeline's live counters.
func (p *Pipeline) Stats() *PipeStats { return &p.stats }

// Start launches the dispatch workers and the keepalive loop. Idempotent.
func (p *Pipeline) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	workers := runtime.NumCPU()
	if workers < 4 {
		workers = 4
	}
	if workers > 16 {
		workers = 16
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	p.wg.Add(1)
	go p.keepaliveLoop()
	if p.logger.Enabled(log.LevelDebug) {
		p.logger.Log(log.LevelDebug, "cluster pipeline: started",
			log.Uint("workers", uint32(workers)),
			log.Uint64("node_id", p.nodeID),
		)
	}
}

// Stop shuts the pipeline down: it unblocks every pending call with
// errPipelineStopped, then waits for workers and the keepalive loop to exit.
func (p *Pipeline) Stop() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}
	close(p.stopCh)
	// Fail all pending calls so their callers do not hang until their own
	// context deadline fires.
	p.pending.Range(func(key, value any) bool {
		select {
		case value.(chan pipeResult) <- pipeResult{err: errPipelineStopped}:
		default:
		}
		return true
	})
	p.wg.Wait()
}

// RegisterHandler binds a forward-operation handler to a raft group. Inbound
// OpForwardWrite / OpForwardChunks targeting that group are delivered here.
// Registering for a group with nil handler is a no-op.
func (p *Pipeline) RegisterHandler(gid uint64, h PipeHandler) {
	if h == nil {
		return
	}
	p.mu.Lock()
	p.handlers[gid] = h
	p.mu.Unlock()
}

// handlerFor returns the forward handler for a group, or nil.
func (p *Pipeline) handlerFor(gid uint64) PipeHandler {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.handlers[gid]
}

// Call sends a request to peerID and blocks until the response arrives, the
// context is cancelled, or the pipeline stops. peerID is a raft node ID; gid
// is the region/raft group the request targets (0 for protocol-level ops like
// ping). The returned payload is the response data (empty for forward ops on
// success).
func (p *Pipeline) Call(ctx context.Context, peerID uint64, gid uint64, op OpKind, payload []byte) ([]byte, error) {
	if p.stopped.Load() {
		return nil, errPipelineStopped
	}
	reqID := p.nextID.Add(1)
	ch := make(chan pipeResult, 1)
	p.pending.Store(reqID, ch)
	pm := pipeMsg{kind: op, reqID: reqID, from: p.nodeID, gid: gid, payload: payload}
	if err := p.transport.sendPipe(peerID, pm); err != nil {
		p.pending.Delete(reqID)
		return nil, err
	}
	p.stats.SentMsgs.Add(1)
	p.stats.Requests.Add(1)
	select {
	case r := <-ch:
		return r.payload, r.err
	case <-ctx.Done():
		p.pending.Delete(reqID)
		p.stats.Timeouts.Add(1)
		return nil, ctx.Err()
	case <-p.stopCh:
		p.pending.Delete(reqID)
		return nil, errPipelineStopped
	}
}

// enqueue hands an inbound pipeline message to the worker pool without blocking
// the transport read loop: a saturated dispatch queue yields
// errPipelineOverloaded (the message is rejected by the caller) and a shutdown
// yields errPipelineStopped. Backpressure through the TCP stream was abandoned
// because it also stalled raft message delivery on the same read loop.
func (p *Pipeline) enqueue(pm pipeMsg) error {
	select {
	case p.dispatchCh <- pm:
		return nil
	case <-p.stopCh:
		return errPipelineStopped
	default:
		return errPipelineOverloaded
	}
}

// worker drains inbound pipeline messages and dispatches them. A blocked
// forward handler only stalls this worker, never the transport read loop or
// raft message delivery.
func (p *Pipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stopCh:
			return
		case pm := <-p.dispatchCh:
			p.deliver(pm)
		}
	}
}

// deliver routes one inbound pipeline message. Requests are answered by the
// group handler (or auto-answered for pings); responses complete the matching
// pending Call by request ID.
func (p *Pipeline) deliver(pm pipeMsg) {
	switch pm.kind {
	case OpPing:
		// Protocol-level: reply immediately without a handler.
		p.stats.Responses.Add(1)
		p.respond(pm, OpPong, encodePipeResp(nil, nil))
	case OpForwardWrite, OpForwardChunks:
		respPayload, aerr := p.handleForward(pm)
		p.stats.Responses.Add(1)
		p.respond(pm, OpForwardResp, encodePipeResp(respPayload, aerr))
	case OpXClusterWrite, OpXClusterRead:
		p.deliverXCluster(pm)
	case OpPong, OpForwardResp, OpXClusterResp:
		p.completeInline(pm)
	default:
		if p.logger.Enabled(log.LevelWarn) {
			p.logger.Log(log.LevelWarn, "cluster pipeline: dropping unknown inbound op",
				log.Uint("op", uint32(pm.kind)),
				log.Uint64("req_id", pm.reqID),
				log.Uint64("from", pm.from),
			)
		}
	}
}

// deliverXCluster answers an inbound federation op through the gateway
// handler (registered under the gateway group ID, e.g. the transport's owner
// node ID via lookups in Gateway). It reuses handleForward so the reply
// encoding and failure semantics are identical to forwarded writes.
func (p *Pipeline) deliverXCluster(pm pipeMsg) {
	respPayload, aerr := p.handleForward(pm)
	p.stats.Responses.Add(1)
	p.respond(pm, OpXClusterResp, encodePipeResp(respPayload, aerr))
}

// handleForward invokes the region handler for a forwarded write request. A
// missing handler is an error response, not a silently dropped request.
func (p *Pipeline) handleForward(pm pipeMsg) ([]byte, error) {
	h := p.handlerFor(pm.gid)
	if h == nil {
		return nil, fmt.Errorf("cluster pipeline: no forward handler for region %d", pm.gid)
	}
	return h(pm.kind, pm.payload)
}

// complete delivers a response to the pending Call registered under reqID. A
// call that already timed out (or was cancelled) has no pending entry.
func (p *Pipeline) complete(reqID uint64, res pipeResult) {
	if v, ok := p.pending.LoadAndDelete(reqID); ok {
		select {
		case v.(chan pipeResult) <- res:
		default:
		}
	}
}

// completeInline finishes an inbound response without going through the
// dispatch queue. decodePipeResp and complete are non-blocking, so it is safe
// on the transport read loop and is used as the saturation fallback.
func (p *Pipeline) completeInline(pm pipeMsg) {
	payload, rerr := decodePipeResp(pm.payload)
	p.complete(pm.reqID, pipeResult{payload: payload, err: rerr})
}

// respond queues a response frame to the peer that sent the request. sendPipe
// is a bounded, non-blocking queue (a saturated pipeCh yields
// errSendQueueFull), so response delivery never stalls the read loop or a
// dispatch worker. Delivery failures are surfaced in the debug log; the peer's
// pending Call then fails on its own context timeout and the follower retries.
func (p *Pipeline) respond(pm pipeMsg, kind OpKind, respPayload []byte) {
	if err := p.transport.sendPipe(pm.from, pipeMsg{
		kind:    kind,
		reqID:   pm.reqID,
		from:    p.nodeID,
		gid:     pm.gid,
		payload: respPayload,
	}); err != nil && p.logger.Enabled(log.LevelDebug) {
		p.logger.Log(log.LevelDebug, "cluster pipeline: response delivery failed",
			log.Uint64("req_id", pm.reqID),
			log.Uint64("peer_id", pm.from),
			log.String("error", err.Error()),
		)
	}
}

// reject answers a request it was not possible to enqueue, carrying the
// overload error so the follower's Call fails fast and retries immediately
// instead of waiting for a context timeout.
func (p *Pipeline) reject(pm pipeMsg, err error) {
	p.respond(pm, responseKind(pm.kind), encodePipeResp(nil, err))
}

// onOverload handles a message the saturated dispatch queue could not accept.
// Requests (ping, forwarded ops) get an explicit overload error so the follower
// fails fast and retries; responses are completed inline so the pending Call
// gets its result instead of waiting for a context expiry — never fabricating a
// response for a response.
func (p *Pipeline) onOverload(pm pipeMsg) {
	switch pm.kind {
	case OpPong, OpForwardResp, OpXClusterResp:
		p.completeInline(pm)
	default:
		p.reject(pm, errPipelineOverloaded)
	}
}

// keepaliveLoop probes every registered peer periodically and drops the peer
// connection after two consecutive misses — the transport transparently
// redials (with backoff) on the next send, so the drop itself is the health
// remedy. A dropped connection also surfaces peer liveness in the reconnect
// counter for observability.
func (p *Pipeline) keepaliveLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	failures := make(map[uint64]int)
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			for _, id := range p.transport.PeerIDs() {
				if id == p.nodeID {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), keepaliveTimeout)
				_, err := p.Call(ctx, id, 0, OpPing, nil)
				cancel()
				if err != nil {
					// Queue-full and overload errors mean the peer is alive but
					// busy — either our own pipe queue is saturated, or the peer
					// explicitly rejected the ping with an overload response.
					// Neither is a liveness miss, so neither counts toward
					// dropping the connection.
					if errors.Is(err, errSendQueueFull) || errors.Is(err, errPipelineOverloaded) {
						continue
					}
					failures[id]++
					if failures[id] >= keepaliveFailuresBeforeDrop {
						if p.logger.Enabled(log.LevelWarn) {
							p.logger.Log(log.LevelWarn, "cluster pipeline: peer unreachable, dropping connection",
								log.Uint64("peer_id", id),
								log.String("error", err.Error()),
							)
						}
						p.transport.DropPeer(id)
						delete(failures, id)
						p.stats.Reconnects.Add(1)
					}
					continue
				}
				delete(failures, id)
			}
		}
	}
}

// errPipelineStopped is returned by Call when the pipeline has been stopped.
var errPipelineStopped = errors.New("cluster network: pipeline stopped")

// errPipelineOverloaded is returned by enqueue when the inbound dispatch queue
// is saturated. The caller replies to the sending peer with this error so the
// follower can retry the op instead of stalling the transport read loop.
var errPipelineOverloaded = errors.New("cluster network: pipeline overloaded")
