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

    Frame:  [4B big-endian payload_length][payload]
    Payload:[1B msg_count][msg_1]...[msg_N]

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

	// listener is stored so Stop() can close it to unblock the accept loop.
	listener net.Listener

	// outbound: peer ID -> *batchConn
	conns sync.Map
	// peer address registry: "addr:<id>" -> "host:port"
	addrs sync.Map

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
func NewTransport(addr string, nodeID uint64, handler MessageHandler, logger log.Logger) *Transport {
	return &Transport{
		addr:    addr,
		nodeID:  nodeID,
		handler: handler,
		logger:  logger,
		stopCh:  make(chan struct{}),
	}
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
		for _, msg := range msgs {
			t.handler(msg)
		}
	}
}

// ---------------------------------------------------------------------------
// Send - single message (backward compat with node.go)
// ---------------------------------------------------------------------------

// Send queues a single raft message for delivery to the peer.
func (t *Transport) Send(msg *pb.Message) error {
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
	return bc.send([]*pb.Message{msg})
}

// SendBatch queues a batch of raft messages for the same peer. All messages
// are written as a single TCP frame on the next flush.
func (t *Transport) SendBatch(msgs []*pb.Message) error {
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
	return bc.send(msgs)
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
	actual, _ := t.conns.LoadOrStore(peerID, bc)
	if actual != bc {
		conn.Close()
		return actual.(*batchConn), nil
	}
	// A failed flush must drop this entry from conns so the next send dials
	// a fresh connection instead of queueing into the dead one forever.
	bc.onDead = func() { t.conns.Delete(peerID) }
	t.wg.Add(1)
	go bc.run(t.wg.Done)
	return bc, nil
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

type batchConn struct {
	conn   net.Conn
	logger log.Logger
	stopCh <-chan struct{}
	stats  *TransportStats
	// onDead removes this connection's entry from the owning Transport's
	// conn map so a later getOrCreateConn dials a fresh connection instead
	// of reusing the dead one. Nil in tests that build a batchConn directly.
	onDead func()

	sendCh chan []*pb.Message
	closed atomic.Bool
}

func newBatchConn(conn net.Conn, logger log.Logger, stopCh <-chan struct{}, stats *TransportStats) *batchConn {
	return &batchConn{
		conn:   conn,
		logger: logger,
		stopCh: stopCh,
		stats:  stats,
		sendCh: make(chan []*pb.Message, 256),
	}
}

// run is the sender goroutine. It accumulates messages from sendCh and flushes
// them as batched TCP frames on a timer, when the buffer fills, or when the
// message count reaches the wire format's one-byte limit. It exits after the
// first failed flush — the connection is dead at that point and retrying
// writes against it would drop every later batch too.
func (bc *batchConn) run(done func()) {
	defer done()
	defer bc.conn.Close()

	ticker := time.NewTicker(batchFlushInterval)
	defer ticker.Stop()

	var buf []byte
	msgCount := 0

	// flush writes the pending batch. Returns false when the write failed;
	// the caller must stop using the connection.
	flush := func() bool {
		if msgCount == 0 {
			return true
		}
		frame := encodeFrame(buf, msgCount)
		n, err := bc.conn.Write(frame)
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
		bc.stats.FramesSent.Add(1)
		bc.stats.BytesSent.Add(int64(n))
		buf = buf[:0]
		msgCount = 0
		return true
	}

	for {
		select {
		case <-bc.stopCh:
			flush()
			return
		case batch := <-bc.sendCh:
			for _, m := range batch {
				bm := fieldBitmask(m)
				var mid [3]byte
				mid[0] = byte(m.GetType())
				mid[1] = byte(bm >> 8)
				mid[2] = byte(bm)
				buf = append(buf, mid[:]...)
				buf = encodeMsgFields(buf, m, bm)
				msgCount++
			}
			// Flush before msgCount exceeds maxBatchCount (255): the frame
			// stores the count in a single byte and would silently wrap.
			if len(buf) >= batchMaxBytes || msgCount >= maxBatchCount {
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

func (bc *batchConn) send(msgs []*pb.Message) error {
	if bc.closed.Load() {
		return errConnectionClosed
	}
	select {
	case bc.sendCh <- msgs:
		return nil
	default:
		if bc.logger.Enabled(log.LevelWarn) {
			bc.logger.Log(log.LevelWarn, "cluster transport: send channel full, dropping messages",
				log.Int("dropped", len(msgs)))
		}
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

// encodeFrame wraps a payload buffer into a length-prefixed frame.
func encodeFrame(payload []byte, msgCount int) []byte {
	totalPayload := 1 + len(payload) // 1 byte for msg_count
	frame := make([]byte, 4+totalPayload)
	frame[0] = byte(totalPayload >> 24)
	frame[1] = byte(totalPayload >> 16)
	frame[2] = byte(totalPayload >> 8)
	frame[3] = byte(totalPayload)
	frame[4] = byte(msgCount)
	copy(frame[5:], payload)
	return frame
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
