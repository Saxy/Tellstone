/*
Package network
Tellstone Secure Event-Driven Networking Package
File: server.go
Description: Implements an ultra‑high‑performance, zero‑allocation TCP server using an edge‑triggered epoll event‑loop (gnet). Handles incoming messages, dispatches them to storage, and writes responses. Supports optional TLS 1.3 transport encryption via the internal TLS library.

Authors:

	Maximilian Hagen
*/
package network

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/shard"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
	"github.com/panjf2000/gnet/v2"
)

const defaultAddr = "127.0.0.1:9988"
const defaultMaxMsgSize = 16 * 1024 * 1024

// connState holds per-connection state. When TLS is enabled, tlsConn wraps the
// raw gnet connection with TLS 1.3 encryption via the internal TLS library. readBuf is a
// reusable scratch buffer for TLS Read calls to avoid per-traffic allocations.
type connState struct {
	shardID uint64
	tlsConn *tlslib.Conn
	readBuf []byte
}

type Server struct {
	gnet.BuiltinEventEngine
	addr       string
	handler    func(msg *Message) ([]byte, MessageType, error)
	logger     log.Logger
	maxMsgSize uint64
	tlsConfig  *tlslib.Config

	// eng and ready let Shutdown reach the running gnet engine: OnBoot fires once the event
	// loop is accepting connections and hands us the Engine handle we need to stop it; ready
	// is closed at that point so a concurrent Shutdown call can block until it's safe to stop.
	eng   gnet.Engine
	ready chan struct{}

	connectedClients uint64
	totalConnections uint64
	bytesRead        uint64
	bytesWritten     uint64
	protocolErrors   uint64
	handlerErrors    uint64

	shards   []*shard.Shard
	nextConn uint64
}

// NewServer initializes an edge-triggered networking server engine instance.
// It applies defensive configuration defaults before spawning infrastructure.
// shards is optional — if nil, per-shard metrics are not tracked.
// tlsCfg is optional — if nil, plaintext TCP is used.
func NewServer(addr string, maxMsgSize uint64, shards []*shard.Shard, handler func(msg *Message) ([]byte, MessageType, error), logger log.Logger, tlsCfg *tlslib.Config) *Server {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	if addr == "" {
		if logger.Enabled(log.LevelDebug) {
			logger.Log(log.LevelDebug, "addr is nil using defaultAddr instead", log.String("listen to addr", defaultAddr))
		}
		addr = defaultAddr
	}
	if maxMsgSize == 0 {
		maxMsgSize = defaultMaxMsgSize
	}
	s := &Server{
		addr:       addr,
		handler:    handler,
		logger:     logger,
		maxMsgSize: maxMsgSize,
		tlsConfig:  tlsCfg,
		ready:      make(chan struct{}),
		shards:     shards,
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "tcp server created", log.Int("max_msg_size", int(maxMsgSize)))
	}
	return s
}

// ListenAndServe starts the multi-reactor epoll event loop.
func (s *Server) ListenAndServe() error {
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "network: event-driven engine initializing", log.String("address", s.addr))
	}
	return gnet.Run(s, "tcp://"+s.addr, gnet.WithMulticore(true), gnet.WithLogger(log.NewGnetAdapter(s.logger)))
}

// Shutdown gracefully stops the event loop, waiting for in-flight connections to drain or
// ctx to expire. It blocks until ListenAndServe has reached OnBoot, so it is safe to call
// concurrently with ListenAndServe from another goroutine (e.g. a signal handler).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.eng.Stop(ctx)
}

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	close(s.ready)
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, 1)
	atomic.AddUint64(&s.totalConnections, 1)
	var sid uint64
	if len(s.shards) > 0 {
		sid = atomic.AddUint64(&s.nextConn, 1) - 1
		sid = sid % uint64(len(s.shards))
		s.shards[sid].IncConnectedClients()
		s.shards[sid].IncTotalConnections()
	}
	st := &connState{shardID: sid}
	if s.tlsConfig != nil {
		adapter := tlslib.NewGnetConnAdapter(c)
		st.tlsConn = tlslib.Server(adapter, s.tlsConfig)
		st.readBuf = make([]byte, 0, 4096)
	}
	c.SetContext(st)
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, ^uint64(0))
	if st, ok := c.Context().(*connState); ok && int(st.shardID) < len(s.shards) {
		s.shards[st.shardID].DecConnectedClients()
	}
	return gnet.None
}

// OnTraffic handles incoming bytes on the socket asynchronously and lock-free.
// When TLS is enabled, encrypted bytes are decrypted via the internal TLS library before
// protocol parsing. The handshake is driven automatically by the first Read/Write
// calls on the TLS connection.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	st, _ := c.Context().(*connState)
	if st == nil {
		st = &connState{}
		c.SetContext(st)
	}
	if st.tlsConn != nil {
		return s.onTrafficTLS(c, st)
	}
	return s.onTrafficPlaintext(c, st)
}

// onTrafficTLS reads decrypted application data from the TLS connection, parses
// our binary protocol frames, dispatches them to the handler, and writes
// encrypted responses.
func (s *Server) onTrafficTLS(c gnet.Conn, st *connState) gnet.Action {
	for {
		n, err := st.tlsConn.Read(st.readBuf[len(st.readBuf):cap(st.readBuf)])
		if n > 0 {
			st.readBuf = st.readBuf[:len(st.readBuf)+n]
			if action := s.handleDecryptedFrames(st); action != gnet.None {
				return action
			}
		}
		if err != nil {
			if errors.Is(err, tlslib.ErrNotEnough) {
				return gnet.None
			}
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "tls read failed",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
	}
}

// handleDecryptedFrames parses zero or more Tellstone binary protocol frames
// from plaintext data and dispatches each to the handler. Responses are written
// through the TLS connection for automatic encryption. It returns gnet.Close
// on decode, handler, or TLS write errors so the caller propagates the close.
func (s *Server) handleDecryptedFrames(st *connState) gnet.Action {
	var msg Message
	offset := 0
	for offset < len(st.readBuf) {
		msg = Message{}
		payloadLen, err := Decode(st.readBuf[offset:], s.maxMsgSize, &msg)
		if err != nil {
			if errors.Is(err, errShortRead) {
				break
			}
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "protocol decoding failed catastrophically",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		totalPacketLen := 5 + payloadLen
		atomic.AddUint64(&s.bytesRead, uint64(totalPacketLen))
		if len(s.shards) > 0 && int(st.shardID) < len(s.shards) {
			s.shards[st.shardID].AddBytesRead(uint64(totalPacketLen))
		}
		if s.handler != nil {
			var (
				respType    MessageType
				respPayload []byte
			)
			respPayload, respType, err = s.handler(&msg)
			if err != nil {
				atomic.AddUint64(&s.handlerErrors, 1)
				if s.logger.Enabled(log.LevelWarn) {
					s.logger.Log(log.LevelWarn, "application handler returned execution error",
						log.String("error", err.Error()),
					)
				}
				return gnet.Close
			}
			if respPayload != nil {
				if err = Write(st.tlsConn, respType, respPayload); err != nil {
					if s.logger.Enabled(log.LevelError) {
						s.logger.Log(log.LevelError, "failed to write tls response frame",
							log.String("error", err.Error()),
						)
					}
					return gnet.Close
				}
				n := uint64(5 + len(respPayload))
				atomic.AddUint64(&s.bytesWritten, n)
				if len(s.shards) > 0 && int(st.shardID) < len(s.shards) {
					s.shards[st.shardID].AddBytesWritten(n)
				}
			}
		}
		offset += totalPacketLen
	}
	if offset > 0 {
		remaining := copy(st.readBuf, st.readBuf[offset:])
		st.readBuf = st.readBuf[:remaining]
	}
	return gnet.None
}

// onTrafficPlaintext handles the original zero-copy plaintext path. Raw bytes
// are peeked directly from the gnet ring buffer, parsed, and responses are
// written back through gnet without any intermediate copies.
func (s *Server) onTrafficPlaintext(c gnet.Conn, st *connState) gnet.Action {
	var msg Message
	for {
		buf, err := c.Peek(-1)
		if err != nil {
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "peek failed to return n bytes",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		msg = Message{}
		payloadLen, err := Decode(buf, s.maxMsgSize, &msg)
		if err != nil {
			if errors.Is(err, errShortRead) {
				break
			}
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "protocol decoding failed catastrophically",
					log.String("remote_addr", c.RemoteAddr().String()),
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		totalPacketLen := 5 + payloadLen
		atomic.AddUint64(&s.bytesRead, uint64(totalPacketLen))
		if len(s.shards) > 0 {
			if int(st.shardID) < len(s.shards) {
				s.shards[st.shardID].AddBytesRead(uint64(totalPacketLen))
			}
		}
		if s.handler != nil {
			var (
				respType    MessageType
				respPayload []byte
			)
			respPayload, respType, err = s.handler(&msg)
			if err != nil {
				atomic.AddUint64(&s.handlerErrors, 1)
				if s.logger.Enabled(log.LevelWarn) {
					s.logger.Log(log.LevelWarn, "application handler returned execution error",
						log.String("error", err.Error()),
					)
				}
				return gnet.Close
			}
			if respPayload != nil {
				if err = Write(c, respType, respPayload); err != nil {
					if s.logger.Enabled(log.LevelError) {
						s.logger.Log(log.LevelError, "failed to write network response frame",
							log.String("error", err.Error()),
						)
					}
					return gnet.Close
				}
				n := uint64(5 + len(respPayload))
				atomic.AddUint64(&s.bytesWritten, n)
				if len(s.shards) > 0 {
					if int(st.shardID) < len(s.shards) {
						s.shards[st.shardID].AddBytesWritten(n)
					}
				}
			}
		}
		_, err = c.Discard(totalPacketLen)
		if err != nil {
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "discarding packages not possible",
					log.Int("total packet length", totalPacketLen),
					log.String("error", err.Error()),
				)
			}
		}
	}
	return gnet.None
}

func (s *Server) ConnectedClients() uint64 { return atomic.LoadUint64(&s.connectedClients) }
func (s *Server) TotalConnections() uint64 { return atomic.LoadUint64(&s.totalConnections) }
func (s *Server) BytesRead() uint64        { return atomic.LoadUint64(&s.bytesRead) }
func (s *Server) BytesWritten() uint64     { return atomic.LoadUint64(&s.bytesWritten) }
func (s *Server) ProtocolErrors() uint64   { return atomic.LoadUint64(&s.protocolErrors) }
func (s *Server) HandlerErrors() uint64    { return atomic.LoadUint64(&s.handlerErrors) }
