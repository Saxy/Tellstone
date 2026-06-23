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
	"log"

	"github.com/panjf2000/gnet/v2"
)

type Server struct {
	gnet.BuiltinEventEngine
	addr    string
	handler func(msg *Message) ([]byte, MessageType, error)
}

// NewServer initializes an edge-triggered networking server engine instance.
// It applies defensive configuration defaults before spawning infrastructure.
func NewServer(addr string, handler func(msg *Message) ([]byte, MessageType, error)) *Server {
	if addr == "" {
		addr = "127.0.0.1:9988"
	}

	return &Server{
		addr:    addr,
		handler: handler,
	}
}

// ListenAndServe starts the multi-reactor epoll event loop.
func (s *Server) ListenAndServe() error {
	log.Printf("network: event-driven engine initializing on %s", s.addr)
	return gnet.Run(s, "tcp://"+s.addr, gnet.WithMulticore(true))
}

// OnTraffic handles incoming bytes on the socket asynchronously and lock-free.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		buf, _ := c.Peek(-1)
		var msg Message
		payloadLen, err := Decode(buf, &msg)
		if err != nil {
			break
		}
		totalPacketLen := 5 + payloadLen
		if s.handler != nil {
			var (
				respType    MessageType
				respPayload []byte
			)
			respPayload, respType, err = s.handler(&msg)
			if err != nil {
				return gnet.Close
			}
			if respPayload != nil {
				if err = Write(c, respType, respPayload); err != nil {
					return gnet.Close
				}
			}
		}
		_, _ = c.Discard(totalPacketLen)
	}
	return gnet.None
}
