/*
Package network
Tellstone Secure TCP Networking Package
File: protocol.go
Description: Defines the binary protocol wire format used by the secure server. Provides Message struct, MessageType constants, and zero‑allocation encode/decode helpers.

Authors:

	Maximilian Hagen
*/
package network

import (
	"encoding/binary"
	"errors"
	"io"
)

// MessageType defines the kind of message exchanged over the protocol.
type MessageType uint8

const (
	MsgPing MessageType = iota
	MsgPong
	MsgRequest
	MsgResponse
)

// OpCode defines the backend database operation.
type OpCode uint8

func (o OpCode) String() string {
	switch o {
	case OpGet:
		return "GET"
	case OpSet:
		return "SET"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

const (
	OpGet OpCode = iota + 1
	OpSet
	OpDelete
)

// Message is the atomic execution frame of the Tellstone TCP protocol.
type Message struct {
	Type    MessageType
	Op      OpCode
	TTL     int64
	Key     []byte
	Value   []byte
	Payload []byte
}

var (
	errShortRead      = errors.New("network: short read while decoding message")
	errTooLong        = errors.New("network: msg size exceeded limit")
	errZeroLength     = errors.New("network: message length cannot be zero")
	errMissingType    = errors.New("network: missing type byte")
	errBufferTooSmall = errors.New("network: supplied buffer too small for payload")
	errMalformedFrame = errors.New("network: malformed frame structure")
)

var (
	ResponseOK             = []byte("OK")
	ResponseNotFound       = []byte("ERR NOT_FOUND")
	ResponseEmptyKey       = []byte("ERR EMPTY_KEY")
	ResponseStorageFailure = []byte("ERR STORAGE_FAILURE")
	ResponseInvalidOpCode  = []byte("ERR INVALID_OPCODE")
)

// Marshal encodes the Message into its binary wire format.
// It allocates a new slice – suitable for one‑off operations such as handshakes.
func (m *Message) Marshal() []byte {
	var payload []byte
	if m.Type == MsgRequest {
		keyLen := len(m.Key)
		totalPayloadLen := 1 + 2 + 8 + keyLen + len(m.Value)
		payload = make([]byte, totalPayloadLen)

		payload[0] = byte(m.Op)
		binary.BigEndian.PutUint16(payload[1:3], uint16(keyLen))
		binary.BigEndian.PutUint64(payload[3:11], uint64(m.TTL))
		copy(payload[11:11+keyLen], m.Key)
		copy(payload[11+keyLen:], m.Value)
	} else {
		payload = m.Payload
	}

	total := 1 + len(payload)
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[:4], uint32(total))
	buf[4] = byte(m.Type)
	copy(buf[5:], payload)
	return buf
}

// Unmarshal blocks on an io.Reader and instantiates a freshly allocated Message pointer on success.
func Unmarshal(r io.Reader) (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 0 {
		return nil, errZeroLength
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	msg := &Message{Type: MessageType(data[0])}
	rawPayload := data[1:]
	msg.Payload = rawPayload

	if msg.Type == MsgRequest {
		if len(rawPayload) < 11 {
			return nil, errMalformedFrame
		}
		msg.Op = OpCode(rawPayload[0])
		keyLen := int(binary.BigEndian.Uint16(rawPayload[1:3]))
		msg.TTL = int64(binary.BigEndian.Uint64(rawPayload[3:11]))
		msg.Key = rawPayload[11 : 11+keyLen]
		msg.Value = rawPayload[11+keyLen:]
	} else {
		msg.Value = rawPayload
	}
	return msg, nil
}

// Decode parses a full protocol frame from an existing byte slice.
// It returns the payload length and populates the supplied Message struct.
// Guarantees 0 heap allocations by slicing directly into the network ring buffer.
func Decode(data []byte, maxMsgSize uint64, out *Message) (int, error) {
	if len(data) < 5 {
		return 0, errShortRead
	}
	length := binary.BigEndian.Uint32(data[:4])
	if length == 0 {
		return 0, errZeroLength
	}
	if uint64(length) > maxMsgSize {
		return 0, errTooLong
	}
	if uint32(len(data)) < length+4 {
		return 0, errShortRead
	}
	out.Type = MessageType(data[4])
	payloadLen := int(length) - 1
	if payloadLen > 0 {
		rawPayload := data[5 : 5+payloadLen]
		out.Payload = rawPayload
		if out.Type == MsgRequest {
			if len(rawPayload) < 11 {
				return 0, errMalformedFrame
			}
			out.Op = OpCode(rawPayload[0])
			keyLen := int(binary.BigEndian.Uint16(rawPayload[1:3]))
			out.TTL = int64(binary.BigEndian.Uint64(rawPayload[3:11]))

			if len(rawPayload) < 11+keyLen {
				return 0, errMalformedFrame
			}
			out.Key = rawPayload[11 : 11+keyLen]
			out.Value = rawPayload[11+keyLen:]
		} else {
			out.Value = rawPayload
		}
	}
	return payloadLen, nil
}

// writeFastPathMax is the payload size below which Write coalesces the 5-byte header and
// the payload into a single buffer and one Write call. Small responses (OK, NOT_FOUND, and
// typical cache values) dominate, so this avoids a second syscall on a raw net.Conn and a
// second outbound-buffer operation under gnet.
const writeFastPathMax = 512

// Write transmits a message completely allocation-free.
func Write(w io.Writer, msgType MessageType, payload []byte) error {
	total := 1 + len(payload)
	if len(payload) <= writeFastPathMax {
		var frame [5 + writeFastPathMax]byte
		binary.BigEndian.PutUint32(frame[:4], uint32(total))
		frame[4] = byte(msgType)
		n := copy(frame[5:], payload)
		_, err := w.Write(frame[:5+n])
		return err
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(total))
	hdr[4] = byte(msgType)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// Read extracts a message from an io.Reader stream directly into a pre-allocated scratchpad buffer.
func Read(r io.Reader, buf []byte, out *Message) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
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
	rawPayload := buf[1:length]
	out.Payload = rawPayload

	if out.Type == MsgRequest {
		if len(rawPayload) < 11 {
			return errMalformedFrame
		}
		out.Op = OpCode(rawPayload[0])
		keyLen := int(binary.BigEndian.Uint16(rawPayload[1:3]))
		out.TTL = int64(binary.BigEndian.Uint64(rawPayload[3:11]))

		if len(rawPayload) < 11+keyLen {
			return errMalformedFrame
		}
		out.Key = rawPayload[11 : 11+keyLen]
		out.Value = rawPayload[11+keyLen:]
	} else {
		out.Value = rawPayload
	}
	return nil
}

func ReadMessage(r io.Reader) (*Message, error) {
	return Unmarshal(r)
}
