/*
Package network
Tellstone Secure TCP Networking Package
File: client.go
Description: Implements a high-performance, synchronous, zero-allocation TCP client

	engineered for internal microservices and high-throughput connections.

Authors:

	Maximilian Hagen
*/
package network

import (
	"net"
	"time"
)

// Client represents a high-performance synchronous connection to a Tellstone server.
type Client struct {
	conn net.Conn
}

// Dial connects to a Tellstone server pool via the specified TCP address.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}
	return &Client{conn: conn}, nil
}

// Close gracefully closes the underlying network connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Call executes a synchronous Request-Response cycle completely allocation-free.
// The caller provides a scratchpad 'buf' where the incoming server payload will be written.
// It returns the populated Message structure pointing to 'buf', and any network/protocol error.
func (c *Client) Call(msgType MessageType, reqPayload []byte, buf []byte, out *Message) error {
	// 1. Transmit request via optimized writev scatter-gather pipeline
	if err := Write(c.conn, msgType, reqPayload); err != nil {
		return err
	}
	if err := Read(c.conn, buf, out); err != nil {
		return err
	}
	return nil
}
