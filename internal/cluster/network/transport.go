/*
Package network
Tellstone Cloud-Native In-Memory Database
File: transport.go
Description: TCP transport for Raft consensus messages between cluster nodes.
Uses a custom binary codec (codec.go) — zero protobuf on the hot path.
Outbound connections use a per-peer batching sender: messages are accumulated
in a buffer and flushed on a 100us timer or when the buffer fills, coalescing
multiple Raft messages into a single TCP frame = a single packet on the wire.
This is critical for SDN environments with per-packet overhead.

Wire format (from codec.go):

	Frame:  [4B big-endian payload_length][8B big-endian group_id][payload]
	Payload:[1B msg_count][msg_1]...[msg_N]

The group ID (a Phase 4 region ID) lets one peer connection multiplex consensus
traffic for multiple Raft groups: frames are demuxed by group ID to the region's
handler on the receiving side. A flush may emit one frame per non-empty group
present. Group ID 0 is the legacy single-group form.

Authors:

	Maximilian Hagen
*/
package network

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

// readBufPool reuses payload buffers for the read loop. Buffers are returned
// after each frame is decoded, avoiding a make([]byte, length) allocation per
// inbound frame. The pool only caches buffers up to readBufPoolMax bytes; larger
// buffers are allocated fresh and not returned (avoids hoarding giant slices).
var readBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 32*1024) // start at 32 KiB
		return &buf
	},
}

const readBufPoolMax = 4 * 1024 * 1024 // don't pool buffers larger than 4 MiB

const (
	// maxFrameSize is the largest raft message batch we accept (16 MiB).
	// allows large snapshot-sized batches while capping the per-frame allocation.
	maxFrameSize uint32 = 16 << 20
	// headerSize is the 4-byte big-endian length prefix.
	headerSize = 4
	// batchFlushInterval is how often the sender goroutine flushes pending
	// messages. 100us groups bursty writes without adding meaningful latency.
	batchFlushInterval = 100 * time.Microsecond
	// batchMaxBytes flushes the buffer when it exceeds this size, even if
	// the timer hasn't fired. 64 KiB keeps us well under typical MTU.
	batchMaxBytes = 64 * 1024
)

// MessageHandler is called for every valid raft message received from a peer.
// The handler runs on the transport's read-loop goroutine and must not block.
type MessageHandler func(msg *pb.Message)

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

// Transport is a TCP transport for Raft consensus messages between cluster
// nodes. It uses a custom binary codec and a per-peer batching sender to
// coalesce messages into single TCP frames, reducing packet count for SDN.
type Transport struct {
	addr    string
	nodeID  uint64
	handler MessageHandler
	logger  log.Logger
	stopCh  chan struct{}
	wg      sync.WaitGroup
	stopped atomic.Bool

	// groups maps a Raft group (region) ID to its inbound message handler.
	// The constructor handler is registered under group 0 (the legacy
	// single-group ID); additional groups are bound via RegisterGroup. A
	// frame whose group ID has no handler is dropped — it belongs to a region
	// this process does not host.
	groups sync.Map

	// listener is stored so Stop() can close it to unblock the accept loop.
	listener net.Listener

	// outbound: peer ID -> *batchConn
	conns sync.Map
	// peer address registry: "addr:<id>" -> "host:port"
	addrs sync.Map

	// pipe is the process-level app-layer request/response channel (Phase 5).
	// Lazily created by Pipeline(); docked on the transport so all raft groups
	// over a shared transport multiplex their forwarded traffic over the same
	// per-peer connections. Guarded by pipeMu.
	pipeMu sync.RWMutex
	pipe   *Pipeline

	// Counters track wire-level activity for observability. Lock-free
	// atomics so they add zero contention to the hot path.
	stats TransportStats
}

// TransportStats holds atomic counters for transport activity.
type TransportStats struct {
	MessagesSent atomic.Int64 // messages queued via Send/SendBatch
	FramesSent   atomic.Int64 // TCP frames written (one per batch flush)
	BytesSent    atomic.Int64 // bytes written to peer connections
	MessagesRecv atomic.Int64 // messages decoded from inbound frames
	FramesRecv   atomic.Int64 // inbound frames decoded
	PipeSent     atomic.Int64 // pipeline messages queued to peers
	PipeRecv     atomic.Int64 // pipeline messages decoded from peers
}

// Stats returns the transport's wire-level counters. The returned pointer
// refers to the live stats struct — callers must not copy it.
func (t *Transport) Stats() *TransportStats { return &t.stats }

// ClusterMetrics interface satisfaction — these methods satisfy
// metrics.ClusterMetrics structurally without importing the metrics package.
// Each method delegates to an atomic Load on the transport's stats, adding
// zero contention to the hot path.
func (t *Transport) ClusterMessagesSent() int64 { return t.stats.MessagesSent.Load() }
func (t *Transport) ClusterFramesSent() int64   { return t.stats.FramesSent.Load() }
func (t *Transport) ClusterBytesSent() int64    { return t.stats.BytesSent.Load() }
func (t *Transport) ClusterMessagesRecv() int64 { return t.stats.MessagesRecv.Load() }
func (t *Transport) ClusterFramesRecv() int64   { return t.stats.FramesRecv.Load() }

// NewTransport creates a new Raft TCP transport bound to the given address.
// The handler is registered as the group-0 (legacy single-group) handler.
func NewTransport(addr string, nodeID uint64, handler MessageHandler, logger log.Logger) *Transport {
	t := &Transport{
		addr:    addr,
		nodeID:  nodeID,
		handler: handler,
		logger:  logger,
		stopCh:  make(chan struct{}),
	}
	if handler != nil {
		t.groups.Store(uint64(0), handler)
	}
	return t
}

// RegisterGroup binds a message handler to a Raft group (region) ID. Inbound
// frames tagged with gid are delivered to this handler, allowing one transport
// to carry consensus traffic for multiple regions over the same peer
// connections. Registering group 0 replaces the constructor handler.
func (t *Transport) RegisterGroup(gid uint64, handler MessageHandler) {
	if handler != nil {
		t.groups.Store(gid, handler)
	}
}

// handlerFor returns the handler bound to a group ID, or nil when the group is
// not hosted. Group 0 always resolves to the constructor handler when present.
func (t *Transport) handlerFor(gid uint64) MessageHandler {
	if v, ok := t.groups.Load(gid); ok {
		return v.(MessageHandler)
	}
	return nil
}

// Listen starts the TCP listener for inbound connections. Non-blocking:
// accept-loop runs in a background goroutine.
func (t *Transport) Listen() error {
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	t.addr = ln.Addr().String()
	t.listener = ln
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		defer ln.Close()
		t.acceptLoop(ln)
	}()
	if t.logger.Enabled(log.LevelInfo) {
		t.logger.Log(log.LevelInfo, "cluster transport: listening",
			log.String("addr", t.addr),
			log.Uint64("node_id", t.nodeID),
		)
	}
	return nil
}

// Stop shuts down the transport: stops all batch senders, closes the listener,
// and waits for goroutines to exit.
func (t *Transport) Stop() {
	if t.stopped.Swap(true) {
		return
	}
	close(t.stopCh)
	if p := t.pipelined(); p != nil {
		p.Stop()
	}
	if t.listener != nil {
		t.listener.Close()
	}
	t.conns.Range(func(key, value any) bool {
		if bc, ok := value.(*batchConn); ok {
			bc.close()
		}
		return true
	})
	t.wg.Wait()
}

// Addr returns the transport's actual listen address.
func (t *Transport) Addr() string {
	return t.addr
}

// RegisterPeer records the address of a peer node so Send can dial it.
func (t *Transport) RegisterPeer(id uint64, addr string) {
	t.addrs.Store("addr:"+itoa(id), addr)
}

// ConnectionCount returns the number of active peer connections.
func (t *Transport) ConnectionCount() int {
	count := 0
	t.conns.Range(func(key, value any) bool {
		if _, ok := value.(*batchConn); ok {
			count++
		}
		return true
	})
	return count
}

// Pipeline returns the process-level pipeline, creating it on first use. All
// raft groups sharing this transport share the returned pipeline. The caller
// provides its node ID and logger once; subsequent calls reuse the existing
// pipeline.
func (t *Transport) Pipeline(nodeID uint64, logger log.Logger) *Pipeline {
	if p := t.pipelined(); p != nil {
		return p
	}
	t.pipeMu.Lock()
	defer t.pipeMu.Unlock()
	if t.pipe == nil {
		t.pipe = NewPipeline(t, nodeID, logger)
		t.pipe.Start()
	}
	return t.pipe
}

// pipelined returns the existing pipeline or nil when none was created yet.
func (t *Transport) pipelined() *Pipeline {
	t.pipeMu.RLock()
	defer t.pipeMu.RUnlock()
	return t.pipe
}

// PipelineStats exposes pipeline counters for metrics collection. It returns
// nil when no pipeline was ever created (non-cluster transports).
func (t *Transport) PipelineStats() *PipeStats {
	if p := t.pipelined(); p != nil {
		return p.Stats()
	}
	return nil
}

// PeerIDs returns the node IDs of all registered peers with a non-empty
// address. Used by the pipeline keepalive loop to probe liveness.
func (t *Transport) PeerIDs() []uint64 {
	var ids []uint64
	t.addrs.Range(func(key, value any) bool {
		id, ok := key.(string)
		if !ok {
			return true
		}
		const prefix = "addr:"
		if len(id) > len(prefix) && id[:len(prefix)] == prefix {
			if addr, ok := value.(string); ok && addr != "" {
				if n, err := strconv.ParseUint(id[len(prefix):], 10, 64); err == nil {
					ids = append(ids, n)
				}
			}
		}
		return true
	})
	return ids
}

// DropPeer closes and forgets the outbound connection to a peer. On the next
// send the transport redials a fresh connection. Used by the keepalive loop
// when a peer is deemed unreachable: dropping the stale connection (and its
// half-open state) is the health remedy, and the reconnect is lazy.
func (t *Transport) DropPeer(id uint64) {
	// LoadAndDelete atomically takes the peer's current connection, whatever
	// its identity, so a concurrent dial cannot hand the closed connection out
	// afterwards. onDead is left intact: it is identity-conditional and will
	// no-op for any future connection, so there is nothing to clear.
	if v, ok := t.conns.LoadAndDelete(id); ok {
		if bc, ok := v.(*batchConn); ok {
			bc.close()
		}
	}
}

// sendPipe queues a pipeline message for delivery to a peer. The message is
// batched with any other traffic destined for that peer by the peer's batch
// sender and written as a pipeline-tagged TCP frame on the next flush.
func (t *Transport) sendPipe(peerID uint64, pm pipeMsg) error {
	if t.stopped.Load() {
		return errTransportStopped
	}
	bc, err := t.getOrCreateConn(peerID)
	if err != nil {
		return err
	}
	t.stats.PipeSent.Add(1)
	return bc.sendPipe(pm)
}

// ClusterPipelineMetrics interface satisfaction — optional extension of
// metrics.ClusterMetrics. These methods delegate to the pipeline's live
// counters, returning 0 when no pipeline exists (plain raft transports).
func (t *Transport) ClusterPipeRequests() int64 {
	if s := t.PipelineStats(); s != nil {
		return s.Requests.Load()
	}
	return 0
}

func (t *Transport) ClusterPipeResponses() int64 {
	if s := t.PipelineStats(); s != nil {
		return s.Responses.Load()
	}
	return 0
}

func (t *Transport) ClusterPipeTimeouts() int64 {
	if s := t.PipelineStats(); s != nil {
		return s.Timeouts.Load()
	}
	return 0
}

func (t *Transport) ClusterPipeReconnects() int64 {
	if s := t.PipelineStats(); s != nil {
		return s.Reconnects.Load()
	}
	return 0
}

// ---------------------------------------------------------------------------
// Inbound accept loop
// ---------------------------------------------------------------------------

func (t *Transport) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				if t.logger.Enabled(log.LevelWarn) {
					t.logger.Log(log.LevelWarn, "cluster transport: accept failed",
						log.String("error", err.Error()))
				}
				continue
			}
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.readLoop(conn)
		}()
	}
}

// readLoop reads length-prefixed frames from a peer connection, decodes the
// batch, and delivers each message to the handler.
//
// Safety guardrails:
//   - Payload buffers are pooled (sync.Pool) to avoid per-frame allocation.
//   - maxFrameSize caps the header-derived length to 16 MiB.
//   - On any decode error the connection is killed immediately — a corrupted
//     frame means framing sync is broken and continuing would deliver garbage
//     to the handler or allocate based on adversarial lengths.
func (t *Transport) readLoop(conn net.Conn) {
	defer conn.Close()
	// Per-connection shutdown watcher. ctx is cancelled when this readLoop
	// exits (normal termination or read error), so the goroutine below does
	// not leak holding the connection until the next transport-wide Stop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-t.stopCh:
			// Transport shutdown: close the connection to unblock io.ReadFull.
			conn.Close()
		case <-ctx.Done():
		}
	}()
	hdr := make([]byte, headerSize)
	var gidBuf [frameGroupIDSize]byte
	for {
		// Read the 4-byte big-endian length prefix.
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(hdr)
		if length == 0 || length > maxFrameSize {
			// Malformed or oversized frame — kill the connection rather than
			// allocating based on an adversarial length.
			return
		}
		// Read the 8-byte group (region) ID that follows the length prefix.
		if _, err := io.ReadFull(conn, gidBuf[:]); err != nil {
			return
		}
		gid := binary.BigEndian.Uint64(gidBuf[:])
		// Reuse a pooled buffer when possible. The pool New func creates
		// 32 KiB buffers; we only use a pooled buffer if its capacity is
		// large enough for the declared frame length. Buffers larger than
		// readBufPoolMax are never returned to the pool.
		var payload []byte
		var bufPtr *[]byte
		if int(length) <= readBufPoolMax {
			bufPtr = readBufPool.Get().(*[]byte)
			if cap(*bufPtr) >= int(length) {
				payload = (*bufPtr)[:length]
			} else {
				// Pooled buffer too small — allocate fresh, don't return to pool.
				readBufPool.Put(bufPtr)
				bufPtr = nil
				payload = make([]byte, length)
			}
		} else {
			payload = make([]byte, length)
		}
		if _, err := io.ReadFull(conn, payload); err != nil {
			if bufPtr != nil {
				readBufPool.Put(bufPtr)
			}
			return
		}
		// Pipeline frames share the length-prefixed header but carry a
		// different payload encoding, distinguished by the reserved group ID.
		// Decode them with the pipeline codec and dispatch to the app layer.
		// The frame must be decoded BEFORE zeroing the pooled buffer, unlike
		// the raft path which decodes then wipes — DecodePipeBatch copies each
		// message's payload out of the buffer, so wiping after is safe.
		if gid == pipelineGroupID {
			perr := t.handlePipeFrame(payload)
			if bufPtr != nil {
				for i := range payload {
					payload[i] = 0
				}
				readBufPool.Put(bufPtr)
			}
			if perr != nil {
				// Framing sync may still be intact (length-prefixed), but the
				// pipeline codec rejected the payload. Kill the connection to
				// be consistent with the raft decode-error guardrail.
				if t.logger.Enabled(log.LevelWarn) {
					t.logger.Log(log.LevelWarn, "cluster transport: pipeline decode failed, killing connection",
						log.String("remote", conn.RemoteAddr().String()),
						log.String("error", perr.Error()))
				}
				return
			}
			continue
		}
		msgs, decErr := DecodeBatch(payload)
		if bufPtr != nil {
			// Zero the buffer before returning to pool to avoid retaining
			// references to decoded message data.
			for i := range payload {
				payload[i] = 0
			}
			readBufPool.Put(bufPtr)
		}
		if decErr != nil {
			// Framing sync is broken — kill the connection. Continuing
			// after a decode error means the next frame will be read from
			// an unknown offset, delivering garbage to the handler.
			if t.logger.Enabled(log.LevelWarn) {
				t.logger.Log(log.LevelWarn, "cluster transport: decode batch failed, killing connection",
					log.String("remote", conn.RemoteAddr().String()),
					log.String("error", decErr.Error()))
			}
			return
		}
		t.stats.FramesRecv.Add(1)
		t.stats.MessagesRecv.Add(int64(len(msgs)))
		handler := t.handlerFor(gid)
		if handler == nil {
			// Group not hosted on this process (e.g. a region that has not
			// been created here yet). Drop the batch — a future frame from
			// a host will be taken over after the region is instantiated.
			continue
		}
		for _, msg := range msgs {
			handler(msg)
		}
	}
}

// handlePipeFrame decodes a pipeline-tagged frame's payload and hands each
// message to the process pipeline for dispatch. Frames for transports without
// a pipeline are dropped (this transport is not part of a cluster). A decode
// error is returned so the caller kills the connection — framing sync is
// broken and continuing would deliver garbage at unknown offsets.
//
// The decode allocates fresh payload copies, so enqueueing onto the worker
// pool is safe even though the caller returns the read buffer to the pool
// afterwards.
func (t *Transport) handlePipeFrame(payload []byte) error {
	p := t.pipelined()
	if p == nil {
		return nil
	}
	msgs, err := DecodePipeBatch(payload)
	if err != nil {
		return err
	}
	t.stats.FramesRecv.Add(1)
	t.stats.PipeRecv.Add(int64(len(msgs)))
	for _, pm := range msgs {
		p.stats.RecvMsgs.Add(1)
		if err := p.enqueue(pm); err != nil {
			// Never stall the read loop: a saturated or stopped pipeline drops
			// the message here. Requests still get an explicit error reply so
			// the follower's Call fails fast and can retry; responses are
			// dropped and time out on the caller's own context.
			if errors.Is(err, errPipelineOverloaded) {
				p.reject(pm, err)
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Send - single message
// ---------------------------------------------------------------------------

// Send queues a single raft message for the legacy single-group (group 0).
func (t *Transport) Send(msg *pb.Message) error {
	return t.SendTo(0, msg)
}

// SendTo queues a single raft message in the given group (region) for delivery
// to the peer identified by msg.To.
func (t *Transport) SendTo(gid uint64, msg *pb.Message) error {
	if t.stopped.Load() {
		return errTransportStopped
	}
	to := msg.GetTo()
	if to == 0 {
		return errNoDestination
	}
	bc, err := t.getOrCreateConn(to)
	if err != nil {
		return err
	}
	t.stats.MessagesSent.Add(1)
	return bc.send(gid, []*pb.Message{msg})
}

// SendBatch queues a batch of raft messages for the same peer (group 0). All
// messages are written as a single TCP frame on the next flush.
func (t *Transport) SendBatch(msgs []*pb.Message) error {
	return t.SendBatchTo(0, msgs)
}

// SendBatchTo queues a batch of raft messages for the same peer. All messages
// share the given group (region) ID and are written as a single group-tagged
// TCP frame on the next flush.
func (t *Transport) SendBatchTo(gid uint64, msgs []*pb.Message) error {
	if t.stopped.Load() {
		return errTransportStopped
	}
	if len(msgs) == 0 {
		return nil
	}
	to := msgs[0].GetTo()
	if to == 0 {
		return errNoDestination
	}
	bc, err := t.getOrCreateConn(to)
	if err != nil {
		return err
	}
	t.stats.MessagesSent.Add(int64(len(msgs)))
	return bc.send(gid, msgs)
}

// ---------------------------------------------------------------------------
// Outbound connection management
// ---------------------------------------------------------------------------

func (t *Transport) getOrCreateConn(peerID uint64) (*batchConn, error) {
	if v, ok := t.conns.Load(peerID); ok {
		return v.(*batchConn), nil
	}
	return t.dial(peerID)
}

func (t *Transport) dial(peerID uint64) (*batchConn, error) {
	if v, ok := t.conns.Load(peerID); ok {
		return v.(*batchConn), nil
	}
	addr := t.lookupPeerAddr(peerID)
	if addr == "" {
		return nil, errUnknownPeer
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		// Wrap so callers can errors.Is(errDialFailed) while the log still
		// carries the underlying cause (refused, timeout, unreachable…).
		return nil, fmt.Errorf("%w: %v", errDialFailed, err)
	}
	bc := newBatchConn(conn, t.logger, t.stopCh, &t.stats)
	// Initialize onDead BEFORE the connection is published to conns, and make
	// the removal identity-conditional: a stale connection whose flush fails
	// must never delete a newer connection that later replaced it under the
	// same peer ID.
	bc.onDead = func() { t.dropConn(peerID, bc) }
	actual, _ := t.conns.LoadOrStore(peerID, bc)
	if actual != bc {
		conn.Close()
		return actual.(*batchConn), nil
	}
	t.wg.Add(1)
	go bc.run(t.wg.Done)
	return bc, nil
}

// dropConn removes bc from the peer connection map only if it is still the
// current connection for peerID. Identity is checked so a dead connection's
// delayed flush-failure callback cannot evict a freshly dialed replacement.
func (t *Transport) dropConn(peerID uint64, bc *batchConn) {
	if v, ok := t.conns.Load(peerID); ok {
		if cur, ok := v.(*batchConn); ok && cur == bc {
			t.conns.Delete(peerID)
		}
	}
}

func (t *Transport) lookupPeerAddr(id uint64) string {
	v, ok := t.addrs.Load("addr:" + itoa(id))
	if !ok {
		return ""
	}
	return v.(string)
}

// ---------------------------------------------------------------------------
// batchConn - per-peer batching sender
// ---------------------------------------------------------------------------

// groupBatch carries a set of messages for one Raft group (region) through the
// peer's send channel. Every message in the batch shares the same destination
// (SendBatchTo derives it from msgs[0]) and the same group ID.
type groupBatch struct {
	gid  uint64
	msgs []*pb.Message
}

type batchConn struct {
	conn   net.Conn
	logger log.Logger
	stopCh <-chan struct{}
	stats  *TransportStats
	// onDead removes this connection's entry from the owning Transport's
	// conn map so a later getOrCreateConn dials a fresh connection instead
	// of reusing the dead one. Nil in tests that build a batchConn directly.
	onDead func()

	sendCh chan groupBatch
	// pipeCh carries pipeline messages to the same peer. They share the
	// connection with raft traffic and are emitted as a pipelineGroupID-tagged
	// frame by the same run loop flush, so pipeline and raft messages never
	// interleave in one frame payload.
	pipeCh chan pipeMsg
	closed atomic.Bool
}

func newBatchConn(conn net.Conn, logger log.Logger, stopCh <-chan struct{}, stats *TransportStats) *batchConn {
	return &batchConn{
		conn:   conn,
		logger: logger,
		stopCh: stopCh,
		stats:  stats,
		sendCh: make(chan groupBatch, 256),
		pipeCh: make(chan pipeMsg, 256),
	}
}

// run is the sender goroutine. It accumulates messages from sendCh and flushes
// them as batched TCP frames on a timer, when the buffer fills, or when the
// message count reaches the wire format's one-byte limit. A frame carries a
// single group ID, so one connection multiplexes regions by emitting one frame
// per non-empty group on each flush — groups on the same peer never corrupt
// each other's demux. It exits after the first failed flush — the connection is
// dead at that point and retrying writes against it would drop every later
// batch too.
func (bc *batchConn) run(done func()) {
	defer done()
	defer bc.conn.Close()

	ticker := time.NewTicker(batchFlushInterval)
	defer ticker.Stop()

	// Per-group payload buffers and message counts, so each frame carries
	// exactly one group ID.
	bufByGroup := make(map[uint64][]byte)
	countByGroup := make(map[uint64]int)
	msgCount := 0
	byteCount := 0

	// Pipe batch accumulates pipeline messages destined for this peer. They
	// are flushed as a single pipelineGroupID-tagged frame alongside the raft
	// groups.
	var pipePayload []byte
	pipeCount := 0

	// flush writes the pending batch. Returns false when the write failed;
	// the caller must stop using the connection.
	flush := func() bool {
		if msgCount == 0 && pipeCount == 0 {
			return true
		}
		frames := 0
		var out []byte
		for gid, payload := range bufByGroup {
			if len(payload) == 0 {
				continue
			}
			frames++
			out = encodeGroupFrame(out, payload, countByGroup[gid], gid)
		}
		if pipeCount > 0 {
			frames++
			out = encodePipeFrame(out, pipePayload, pipeCount)
		}
		n, err := bc.conn.Write(out)
		if err != nil {
			if bc.logger.Enabled(log.LevelWarn) {
				bc.logger.Log(log.LevelWarn, "cluster transport: batch write failed",
					log.String("remote", bc.conn.RemoteAddr().String()),
					log.String("error", err.Error()))
			}
			bc.closed.Store(true)
			if bc.onDead != nil {
				bc.onDead()
			}
			return false
		}
		bc.stats.FramesSent.Add(int64(frames))
		bc.stats.BytesSent.Add(int64(n))
		for gid := range bufByGroup {
			bufByGroup[gid] = bufByGroup[gid][:0]
			countByGroup[gid] = 0
		}
		pipePayload = pipePayload[:0]
		pipeCount = 0
		msgCount = 0
		byteCount = 0
		return true
	}

	for {
		select {
		case <-bc.stopCh:
			flush()
			return
		case pm := <-bc.pipeCh:
			before := len(pipePayload)
			pipePayload = encodePipeMsg(pipePayload, &pm)
			pipeCount++
			byteCount += len(pipePayload) - before
			if byteCount >= batchMaxBytes || pipeCount >= maxBatchCount {
				if !flush() {
					return
				}
			}
		case gb := <-bc.sendCh:
			for _, m := range gb.msgs {
				payload := bufByGroup[gb.gid]
				before := len(payload)
				bm := fieldBitmask(m)
				payload = append(payload, byte(m.GetType()), byte(bm>>8), byte(bm))
				payload = encodeMsgFields(payload, m, bm)
				bufByGroup[gb.gid] = payload
				countByGroup[gb.gid]++
				byteCount += len(payload) - before
			}
			msgCount += len(gb.msgs)
			// Flush before msgCount exceeds maxBatchCount (255): the frame
			// stores the count in a single byte and would silently wrap.
			if byteCount >= batchMaxBytes || msgCount >= maxBatchCount {
				if !flush() {
					return
				}
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		}
	}
}

func (bc *batchConn) send(gid uint64, msgs []*pb.Message) error {
	if gid == pipelineGroupID {
		return errReservedGroupID
	}
	if bc.closed.Load() {
		return errConnectionClosed
	}
	select {
	case bc.sendCh <- groupBatch{gid: gid, msgs: msgs}:
		return nil
	default:
		if bc.logger.Enabled(log.LevelWarn) {
			bc.logger.Log(log.LevelWarn, "cluster transport: send channel full, dropping messages",
				log.Int("dropped", len(msgs)))
		}
		return errSendQueueFull
	}
}

// sendPipe queues a pipeline message on the same peer connection. It shares
// the batching sender with raft traffic and is written under a dedicated
// pipelineGroupID frame, so this is non-blocking under load like send; a full
// pipeCh drops the message (the pump path is best-effort) rather than stalling
// the caller.
func (bc *batchConn) sendPipe(pm pipeMsg) error {
	if bc.closed.Load() {
		return errConnectionClosed
	}
	select {
	case bc.pipeCh <- pm:
		return nil
	default:
		return errSendQueueFull
	}
}

func (bc *batchConn) close() {
	bc.closed.Store(true)
	bc.conn.Close()
}

// ---------------------------------------------------------------------------
// Frame encoding helpers (used by the batch sender)
// ---------------------------------------------------------------------------

// encodeGroupFrame appends a complete length-prefixed, group-tagged frame to
// out: `[4B payload_length][8B group_id][1B msg_count][msg payload]`. The
// length covers only the payload (count byte + messages); the group ID sits
// between the length prefix and the payload so the reader can demux by group
// before decoding the batch.
func encodeGroupFrame(out, payload []byte, msgCount int, gid uint64) []byte {
	totalPayload := 1 + len(payload) // 1 byte for msg_count
	out = append(out,
		byte(totalPayload>>24),
		byte(totalPayload>>16),
		byte(totalPayload>>8),
		byte(totalPayload),
	)
	var gb [frameGroupIDSize]byte
	binary.BigEndian.PutUint64(gb[:], gid)
	out = append(out, gb[:]...)
	out = append(out, byte(msgCount))
	out = append(out, payload...)
	return out
}

// encodeMsgFields appends the fields of a message to buf given the bitmask
// is already written. Hot-path encoder used by the batch sender.
func encodeMsgFields(buf []byte, m *pb.Message, bm uint16) []byte {
	if bm&fieldTo != 0 {
		buf = appendVarint(buf, m.GetTo())
	}
	if bm&fieldFrom != 0 {
		buf = appendVarint(buf, m.GetFrom())
	}
	if bm&fieldTerm != 0 {
		buf = appendVarint(buf, m.GetTerm())
	}
	if bm&fieldLogTerm != 0 {
		buf = appendVarint(buf, m.GetLogTerm())
	}
	if bm&fieldIndex != 0 {
		buf = appendVarint(buf, m.GetIndex())
	}
	if bm&fieldCommit != 0 {
		buf = appendVarint(buf, m.GetCommit())
	}
	if bm&fieldReject != 0 {
		if m.GetReject() {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	if bm&fieldRejectHint != 0 {
		buf = appendVarint(buf, m.GetRejectHint())
	}
	if bm&fieldContext != 0 {
		c := m.GetContext()
		buf = appendVarint(buf, uint64(len(c)))
		buf = append(buf, c...)
	}
	if bm&fieldEntries != 0 {
		entries := m.GetEntries()
		buf = appendVarint(buf, uint64(len(entries)))
		for _, e := range entries {
			buf = append(buf, byte(e.GetType()))
			buf = appendVarint(buf, e.GetTerm())
			buf = appendVarint(buf, e.GetIndex())
			d := e.GetData()
			buf = appendVarint(buf, uint64(len(d)))
			buf = append(buf, d...)
		}
	}
	if bm&fieldSnapshot != 0 {
		buf = encodeSnapshotAppend(buf, m.GetSnapshot())
	}
	if bm&fieldVote != 0 {
		buf = appendVarint(buf, m.GetVote())
	}
	if bm&fieldResponses != 0 {
		responses := m.GetResponses()
		buf = appendVarint(buf, uint64(len(responses)))
		for _, r := range responses {
			rm := fieldBitmask(r)
			buf = append(buf, byte(r.GetType()))
			buf = append(buf, byte(rm>>8), byte(rm))
			buf = encodeMsgFields(buf, r, rm)
		}
	}
	return buf
}

func encodeSnapshotAppend(buf []byte, s *pb.Snapshot) []byte {
	if s == nil {
		return append(buf, 0)
	}
	buf = append(buf, 1)
	meta := s.GetMetadata()
	if meta != nil {
		buf = appendVarint(buf, meta.GetIndex())
		buf = appendVarint(buf, meta.GetTerm())
		buf = encodeConfStateAppend(buf, meta.GetConfState())
	} else {
		buf = appendVarint(buf, 0)
		buf = appendVarint(buf, 0)
		buf = encodeConfStateAppend(buf, nil)
	}
	d := s.GetData()
	buf = appendVarint(buf, uint64(len(d)))
	buf = append(buf, d...)
	return buf
}

func encodeConfStateAppend(buf []byte, cs *pb.ConfState) []byte {
	// Nil ConfState encodes identically to an empty ConfState and the field
	// set is written directly with no leading presence byte — decodeConfState
	// always reads voters/learners/votersOutgoing/learnersNext/autoleave in
	// that order. The old presence byte shifted every subsequent field,
	// corrupting snapshots encoded by this batching path.
	if cs == nil {
		cs = &pb.ConfState{}
	}
	buf = appendVarint(buf, uint64(len(cs.GetVoters())))
	for _, v := range cs.GetVoters() {
		buf = appendVarint(buf, v)
	}
	buf = appendVarint(buf, uint64(len(cs.GetLearners())))
	for _, l := range cs.GetLearners() {
		buf = appendVarint(buf, l)
	}
	buf = appendVarint(buf, uint64(len(cs.GetVotersOutgoing())))
	for _, v := range cs.GetVotersOutgoing() {
		buf = appendVarint(buf, v)
	}
	buf = appendVarint(buf, uint64(len(cs.GetLearnersNext())))
	for _, l := range cs.GetLearnersNext() {
		buf = appendVarint(buf, l)
	}
	if cs.GetAutoLeave() {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

// appendVarint appends a varint-encoded uint64 to buf.
func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Distinct sentinel errors for every transport failure condition, so
// errors.Is can tell them apart. Aliasing several names to the same io error
// made e.g. errUnknownPeer and errSendQueueFull indistinguishable.
var (
	errTransportStopped = errors.New("cluster network: transport stopped")
	errNoDestination    = errors.New("cluster network: message has no destination peer")
	errUnknownPeer      = errors.New("cluster network: unknown peer")
	errDialFailed       = errors.New("cluster network: dial failed")
	errConnectionClosed = errors.New("cluster network: connection closed")
	errSendQueueFull    = errors.New("cluster network: send queue full")
	errReservedGroupID  = errors.New("cluster network: group ID is reserved for the pipeline")
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func itoa(id uint64) string {
	var buf [20]byte
	i := len(buf)
	for id >= 10 {
		i--
		buf[i] = byte('0' + id%10)
		id /= 10
	}
	i--
	buf[i] = byte('0' + id)
	return string(buf[i:])
}
