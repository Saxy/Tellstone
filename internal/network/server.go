/*
Package network
Tellstone Secure Event-Driven Networking Package
File: server.go
Description: Implements an ultra‑high‑performance, zero‑allocation TCP server using an edge‑triggered epoll event‑loop (gnet). Handles incoming messages, dispatches them to storage, and writes responses.

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
	"github.com/panjf2000/gnet/v2"
)

const defaultAddr = "127.0.0.1:9988"
const defaultMaxMsgSize = 16 * 1024 * 1024

type Server struct {
	gnet.BuiltinEventEngine
	addr       string
	handler    func(msg *Message) ([]byte, MessageType, error)
	logger     log.Logger
	maxMsgSize uint64

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
func NewServer(addr string, maxMsgSize uint64, shards []*shard.Shard, handler func(msg *Message) ([]byte, MessageType, error), logger log.Logger) *Server {
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
	if len(s.shards) > 0 {
		sid := atomic.AddUint64(&s.nextConn, 1) - 1
		sid = sid % uint64(len(s.shards))
		c.SetContext(sid)
		s.shards[sid].IncConnectedClients()
		s.shards[sid].IncTotalConnections()
	}
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, ^uint64(0))
	if len(s.shards) > 0 {
		if sid, ok := c.Context().(uint64); ok && int(sid) < len(s.shards) {
			s.shards[sid].DecConnectedClients()
		}
	}
	return gnet.None
}

// OnTraffic handles incoming bytes on the socket asynchronously and lock-free.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
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
			if sid, ok := c.Context().(uint64); ok && int(sid) < len(s.shards) {
				s.shards[sid].AddBytesRead(uint64(totalPacketLen))
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
					if sid, ok := c.Context().(uint64); ok && int(sid) < len(s.shards) {
						s.shards[sid].AddBytesWritten(n)
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
