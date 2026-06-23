/*
Package network
Tellstone Secure Event-Driven Networking Package
File: server.go
Description: Implements an ultra-high performance, zero-allocation TCP server

	using an edge-triggered epoll event-loop (gnet).

Authors:

	Maximilian Hagen
*/
package network

import (
	"errors"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/panjf2000/gnet/v2"
)

type Server struct {
	gnet.BuiltinEventEngine
	addr    string
	handler func(msg *Message) ([]byte, MessageType, error)
	logger  log.Logger
}

// NewServer initializes an edge-triggered networking server engine instance.
// It applies defensive configuration defaults before spawning infrastructure.
func NewServer(addr string, handler func(msg *Message) ([]byte, MessageType, error), logger log.Logger) *Server {
	if addr == "" {
		addr = "127.0.0.1:9988"
	}
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	s := &Server{
		addr:    addr,
		handler: handler,
		logger:  logger,
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "tcp server created")
	}
	return s
}

// ListenAndServe starts the multi-reactor epoll event loop.
func (s *Server) ListenAndServe() error {
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "network: event-driven engine initializing", log.String("address", s.addr))
	}
	return gnet.Run(s, "tcp://"+s.addr, gnet.WithMulticore(true))
}

// OnTraffic handles incoming bytes on the socket asynchronously and lock-free.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		buf, _ := c.Peek(-1)
		var msg Message
		payloadLen, err := Decode(buf, &msg)
		if err != nil {
			if errors.Is(err, errShortRead) {
				break
			}
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "protocol decoding failed catastrophically",
					log.String("error", err.Error()),
					log.String("remote_addr", c.RemoteAddr().String()),
				)
			}
			return gnet.Close
		}
		totalPacketLen := 5 + payloadLen
		if s.handler != nil {
			var (
				respType    MessageType
				respPayload []byte
			)
			respPayload, respType, err = s.handler(&msg)
			if err != nil {
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
			}
		}
		_, _ = c.Discard(totalPacketLen)
	}
	return gnet.None
}
