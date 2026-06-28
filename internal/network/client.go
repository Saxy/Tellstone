/*
Package network
Tellstone Cloud-Native In-Memory Database
File: client.go
Description: Implements a high-performance, synchronous, zero-allocation TCP client using pre‑allocated buffers for request/response handling.

Authors:

	Maximilian Hagen
*/
package network

import (
	"encoding/binary"
	"errors"
	"net"
	"time"
)

var (
	// ErrResponseBufferTooSmall is returned if the scratchpad buffer cannot hold the incoming server frame.
	ErrResponseBufferTooSmall = errors.New("client: provided scratchpad buffer is too small for the server response")

	// ErrRequestTooLarge is returned if the generated request exceeds the local stack buffer boundaries.
	ErrRequestTooLarge = errors.New("client: key or value size exceeds local client packaging limitations")
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

// Set stores a binary key-value pair with a millisecond-based TTL inside the remote engine.
func (c *Client) Set(key, value []byte, ttlMs int64, scratchBuf []byte) ([]byte, error) {
	payloadLen := 1 + 2 + 8 + len(key) + len(value)

	// Use a fixed stack allocation for the outward payload serialization to keep it zero-allocation
	var reqBuf [2048]byte
	if payloadLen > len(reqBuf) {
		return nil, ErrRequestTooLarge
	}

	reqBuf[0] = byte(OpSet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len(key)))
	binary.BigEndian.PutUint64(reqBuf[3:11], uint64(ttlMs))

	copy(reqBuf[11:11+len(key)], key)
	copy(reqBuf[11+len(key):payloadLen], value)

	var resp Message
	// scratchBuf is now exclusively used to catch the incoming wire data safely
	if err := c.Call(MsgRequest, reqBuf[:payloadLen], scratchBuf, &resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Get retrieves a binary value from the remote engine using its key identifier.
func (c *Client) Get(key []byte, scratchBuf []byte) ([]byte, error) {
	payloadLen := 1 + 2 + 8 + len(key)

	var reqBuf [512]byte
	if payloadLen > len(reqBuf) {
		return nil, ErrRequestTooLarge
	}

	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len(key)))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)

	copy(reqBuf[11:11+len(key)], key)

	var resp Message
	if err := c.Call(MsgRequest, reqBuf[:payloadLen], scratchBuf, &resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Delete removes a key-value entity permanently from the remote cluster space.
func (c *Client) Delete(key []byte, scratchBuf []byte) ([]byte, error) {
	payloadLen := 1 + 2 + 8 + len(key)

	var reqBuf [512]byte
	if payloadLen > len(reqBuf) {
		return nil, ErrRequestTooLarge
	}

	reqBuf[0] = byte(OpDelete)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len(key)))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)

	copy(reqBuf[11:11+len(key)], key)

	var resp Message
	if err := c.Call(MsgRequest, reqBuf[:payloadLen], scratchBuf, &resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Call executes a synchronous Request-Response cycle completely allocation-free.
func (c *Client) Call(msgType MessageType, reqPayload []byte, buf []byte, out *Message) error {
	if err := Write(c.conn, msgType, reqPayload); err != nil {
		return err
	}
	if err := Read(c.conn, buf, out); err != nil {
		return err
	}
	return nil
}
