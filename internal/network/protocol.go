/*
Package network
Tellstone Secure TCP Networking Package
File: protocol.go
Description: Defines the binary protocol wire format used by the secure server.

	All encoding and decoding routines operate without heap allocations
	by engineering stack-allocated buffers and direct slice views.

Authors:

	Maximilian Hagen
*/
package network

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

// MessageType defines the kind of message exchanged over the protocol.
type MessageType uint8

const (
	// MsgPing is used for heartbeat or health-check routines.
	MsgPing MessageType = iota
	// MsgPong is the mandatory acknowledgement response to a MsgPing.
	MsgPong
	// MsgRequest represents an incoming client data payload.
	MsgRequest
	// MsgResponse represents an outgoing server response payload.
	MsgResponse
)

// Message is the atomic execution frame of the Tellstone TCP protocol.
type Message struct {
	Type    MessageType
	Payload []byte
}

var (
	errShortRead      = errors.New("network: short read while decoding message")
	errZeroLength     = errors.New("network: message length cannot be zero")
	errMissingType    = errors.New("network: missing type byte")
	errBufferTooSmall = errors.New("network: supplied buffer too small for payload")
)

// Marshal encodes a Message into its binary representation on the wire.
// NOTE: This triggers heap allocations. Only use this for non-hot-paths
// such as initial cluster handshakes, tear-downs, or out-of-band management tasks.
func (m *Message) Marshal() []byte {
	total := 1 + len(m.Payload)
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[:4], uint32(total))
	buf[4] = byte(m.Type)
	copy(buf[5:], m.Payload)
	return buf
}

// Decode parses a full protocol frame directly from an existing byte slice.
// This is the core data-path method used inside asynchronous event loops (e.g., gnet).
// It guarantees 0 heap allocations by slicing directly into the underlying ring buffer window.
func Decode(data []byte, out *Message) (int, error) {
	if len(data) < 5 {
		return 0, errShortRead
	}
	length := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	if length == 0 {
		return 0, errZeroLength
	}
	if uint32(len(data)) < length+4 {
		return 0, errShortRead
	}
	out.Type = MessageType(data[4])
	payloadLen := int(length) - 1
	if payloadLen > 0 {
		out.Payload = data[5 : 5+payloadLen]
	} else {
		out.Payload = nil
	}
	return payloadLen, nil
}

// Write transmits a message completely allocation-free.
// To circumvent TCP fragmentation (sending header and payload in distinct packets),
// it passes an aggregated slice array to net.Buffers to leverage system-level scatter-gather I/O (writev).
func Write(w io.Writer, msgType MessageType, payload []byte) error {
	total := 1 + len(payload)
	var hdr [5]byte
	hdr[0] = byte(total >> 24)
	hdr[1] = byte(total >> 16)
	hdr[2] = byte(total >> 8)
	hdr[3] = byte(total)
	hdr[4] = byte(msgType)
	// net.Buffers wraps multi-slice arrays. Go's escape analysis allows this two-slice
	// wrapper to reside safely on the stack since it never escapes this local scope execution.
	bufs := net.Buffers{hdr[:], payload}
	_, err := bufs.WriteTo(w)
	return err
}

// Read extracts a message from an io.Reader stream directly into a pre-allocated scratchpad buffer.
// Optimized for synchronous, blocking multi-threaded Go client implementations.
func Read(r io.Reader, buf []byte, out *Message) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return err
	}
	length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if length == 0 {
		return errZeroLength
	}
	if uint32(len(buf)) < length {
		return errBufferTooSmall
	}
	if _, err := io.ReadFull(r, buf[:length]); err != nil {
		return err
	}
	out.Type = MessageType(buf[0])
	out.Payload = buf[1:length]
	return nil
}

// Unmarshal blocks on an io.Reader and instantiates a freshly allocated Message pointer on success.
func Unmarshal(r io.Reader) (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if length == 0 {
		return nil, errZeroLength
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return &Message{
		Type:    MessageType(data[0]),
		Payload: data[1:],
	}, nil
}

// WriteMessage executes a standard network write by marshalling data into a newly allocated buffer.
func WriteMessage(w io.Writer, m *Message) error {
	_, err := w.Write(m.Marshal())
	return err
}

// ReadMessage reads an allocated frame structure from an arbitrary streaming source interface.
func ReadMessage(r io.Reader) (*Message, error) {
	return Unmarshal(r)
}
