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
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

var (
	// ErrResponseBufferTooSmall is returned if the scratchpad buffer cannot hold the incoming server frame.
	ErrResponseBufferTooSmall = errors.New("client: provided scratchpad buffer is too small for the server response")

	// ErrRequestTooLarge is returned if the generated request exceeds the local stack buffer boundaries.
	ErrRequestTooLarge = errors.New("client: key or value size exceeds local client packaging limitations")

	// ErrAuthCredentialsTooLong is returned when an AUTH credential exceeds the
	// uint16 length prefix of the binary auth frame.
	ErrAuthCredentialsTooLong = errors.New("client: auth credential exceeds 65535 byte protocol limit")
)

// Client represents a high-performance synchronous connection to a Tellstone server.
type Client struct {
	conn   net.Conn
	logger log.Logger
}

// Dial connects to a Tellstone server pool via the specified TCP address.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	return DialWithLogger(addr, timeout, nil)
}

// DialWithLogger connects like Dial and reports connection lifecycle events to logger.
func DialWithLogger(addr string, timeout time.Duration, logger log.Logger) (*Client, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "client: connected", log.String("addr", addr))
	}
	return &Client{conn: conn, logger: logger}, nil
}

// DialTLS connects to a Tellstone server with TLS 1.3 encryption.
// certPath/keyPath are the client certificate and key for mTLS (pass empty for one-way TLS).
// caPath is the CA certificate to verify the server (pass empty to skip verification).
func DialTLS(addr string, certPath, keyPath, caPath string, timeout time.Duration) (*Client, error) {
	return DialTLSWithLogger(addr, certPath, keyPath, caPath, timeout, nil)
}

// DialTLSWithLogger connects like DialTLS and reports connection lifecycle events to logger.
func DialTLSWithLogger(addr string, certPath, keyPath, caPath string, timeout time.Duration, logger log.Logger) (*Client, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}

	if caPath != "" {
		caPEM, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls: parse CA certificate")
		}
		tlsCfg.RootCAs = pool
	} else {
		tlsCfg.InsecureSkipVerify = true
	}

	if (certPath == "") != (keyPath == "") {
		return nil, fmt.Errorf("tls: both cert and key are required when using client certificates")
	}

	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("tls: load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
	if err != nil {
		return nil, err
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "client: connected", log.String("addr", addr))
	}
	return &Client{conn: conn, logger: logger}, nil
}

// Close gracefully closes the underlying network connection.
func (c *Client) Close() error {
	err := c.conn.Close()
	if c.logger != nil {
		if err != nil {
			if c.logger.Enabled(log.LevelWarn) {
				c.logger.Log(log.LevelWarn, "client: close failed", log.String("error", err.Error()))
			}
		} else if c.logger.Enabled(log.LevelInfo) {
			c.logger.Log(log.LevelInfo, "client: connection closed")
		}
	}
	return err
}

// errReply converts a response into an error when it arrives in a MsgError
// frame — the binary protocol's dedicated error type. Data values ride in
// MsgResponse frames and pass through unchanged, including a stored value that
// itself begins with "ERR ": the frame type, not the payload, distinguishes an
// error from data.
func errReply(resp Message) error {
	if resp.Type == MsgError {
		return fmt.Errorf("server: %s", resp.Value)
	}
	return nil
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
	if err := errReply(resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// callKeyOp issues a keyed request (GET or DEL) and returns the response
// value. scratchBuf must be large enough to hold the server response.
func (c *Client) callKeyOp(op OpCode, key []byte, scratchBuf []byte) ([]byte, error) {
	payloadLen := 1 + 2 + 8 + len(key)

	var reqBuf [512]byte
	if payloadLen > len(reqBuf) {
		return nil, ErrRequestTooLarge
	}

	reqBuf[0] = byte(op)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len(key)))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)

	copy(reqBuf[11:11+len(key)], key)

	var resp Message
	if err := c.Call(MsgRequest, reqBuf[:payloadLen], scratchBuf, &resp); err != nil {
		return nil, err
	}
	if err := errReply(resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Get retrieves a binary value from the remote engine using its key identifier.
func (c *Client) Get(key []byte, scratchBuf []byte) ([]byte, error) {
	return c.callKeyOp(OpGet, key, scratchBuf)
}

// Delete removes a key-value entity permanently from the remote cluster space.
func (c *Client) Delete(key []byte, scratchBuf []byte) ([]byte, error) {
	return c.callKeyOp(OpDelete, key, scratchBuf)
}

// Auth authenticates the client with a password (single-password mode, username empty).
// scratchBuf must be large enough to hold the server response. Returns nil on success.
func (c *Client) Auth(password string, scratchBuf []byte) error {
	// The auth frame's length prefixes are uint16 fields; a longer credential
	// would wrap them on the wire, so reject it up front instead of sending a
	// corrupt frame.
	if len(password) > math.MaxUint16 {
		return ErrAuthCredentialsTooLong
	}
	payloadLen := 2 + 0 + 2 + len(password)
	// Short passwords serialize into the stack buffer, keeping connection setup
	// allocation-free. OIDC bearer tokens (id_tokens) routinely exceed 512 bytes,
	// so fall back to a one-time heap buffer when the payload overflows the stack.
	var stackBuf [512]byte
	var payload []byte
	if payloadLen > len(stackBuf) {
		payload = make([]byte, payloadLen)
	} else {
		payload = stackBuf[:payloadLen]
	}
	binary.BigEndian.PutUint16(payload[0:2], 0) // usernameLen = 0 (single-password mode)
	binary.BigEndian.PutUint16(payload[2:4], uint16(len(password)))
	copy(payload[4:payloadLen], password)

	var resp Message
	if err := c.Call(MsgAuth, payload, scratchBuf, &resp); err != nil {
		return err
	}
	if resp.Type != MsgAuthOk {
		return fmt.Errorf("auth failed: %s", resp.Value)
	}
	return nil
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
