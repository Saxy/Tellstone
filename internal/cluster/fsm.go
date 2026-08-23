/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: fsm.go
Description: Raft finite state machine that bridges consensus log entries to
local shard storage. Write operations (SET, DEL) proposed on the leader are
encoded into compact binary log entries, replicated via Raft, and applied to
every node's local storage on commit. The codec is a simple binary format
optimised for the hot path: no protobuf, no JSON, no allocations in the
encode/decode hot path.

Log entry wire format:
  [1B op][8B TTL ms][2B keyLen][keyLen key][remaining value]

  op = 0x01 → SET   (value present, length = total - 11 - keyLen)
  op = 0x02 → DEL   (no value, length = 0)

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	// OpSet is the log entry opcode for SET operations.
	OpSet byte = 0x01
	// OpDel is the log entry opcode for DEL operations.
	OpDel byte = 0x02
	// opHeaderSize is the fixed header: 1B op + 8B TTL + 2B keyLen = 11 bytes.
	opHeaderSize = 11
)

// Dispatcher routes a decoded operation to the correct local shard engine.
// Implementations must use the same FNV-1a routing as the normal request
// path so that replicated data lands in the same shard on every node.
type Dispatcher interface {
	Dispatch(key string, op byte, value []byte, ttl time.Duration) error
}

// FSM applies committed Raft log entries to local storage via a Dispatcher.
type FSM struct {
	dispatcher Dispatcher
	logger     log.Logger
}

// NewFSM creates a new FSM that routes committed entries through the given
// Dispatcher.
func NewFSM(dispatcher Dispatcher, logger log.Logger) *FSM {
	return &FSM{
		dispatcher: dispatcher,
		logger:     logger,
	}
}

// EncodeSet creates a Raft log entry payload for a SET operation.
func EncodeSet(key string, value []byte, ttl time.Duration) []byte {
	var ttlMs int64
	if ttl > 0 {
		ttlMs = ttl.Milliseconds()
	}
	keyBytes := []byte(key)
	buf := make([]byte, opHeaderSize+len(keyBytes)+len(value))
	buf[0] = OpSet
	binary.BigEndian.PutUint64(buf[1:9], uint64(ttlMs))
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(keyBytes)))
	copy(buf[11:11+len(keyBytes)], keyBytes)
	copy(buf[11+len(keyBytes):], value)
	return buf
}

// EncodeDel creates a Raft log entry payload for a DEL operation.
func EncodeDel(key string) []byte {
	keyBytes := []byte(key)
	buf := make([]byte, opHeaderSize+len(keyBytes))
	buf[0] = OpDel
	binary.BigEndian.PutUint64(buf[1:9], 0)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(keyBytes)))
	copy(buf[11:11+len(keyBytes)], keyBytes)
	return buf
}

// DecodeLogEntry parses a log entry payload into its components.
func DecodeLogEntry(data []byte) (op byte, key string, value []byte, ttl time.Duration, err error) {
	if len(data) < opHeaderSize {
		return 0, "", nil, 0, errors.New("cluster fsm: entry too short")
	}
	op = data[0]
	ttlMs := binary.BigEndian.Uint64(data[1:9])
	ttl = time.Duration(ttlMs) * time.Millisecond
	keyLen := binary.BigEndian.Uint16(data[9:11])
	if int(keyLen) > len(data)-opHeaderSize {
		return 0, "", nil, 0, fmt.Errorf("cluster fsm: key length %d exceeds data length %d", keyLen, len(data))
	}
	key = string(data[opHeaderSize : opHeaderSize+int(keyLen)])
	value = data[opHeaderSize+int(keyLen):]
	return op, key, value, ttl, nil
}

// Apply applies a single committed Raft log entry to local storage via the
// Dispatcher. It decodes the binary payload and dispatches.
func (f *FSM) Apply(entry *pb.Entry) error {
	if entry.GetData() == nil {
		return nil
	}
	op, key, value, ttl, err := DecodeLogEntry(entry.GetData())
	if err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "cluster fsm: decode error",
				log.String("error", err.Error()),
				log.Uint64("index", entry.GetIndex()),
				log.Int("data_len", len(entry.GetData())),
			)
		}
		return err
	}
	if f.logger.Enabled(log.LevelDebug) {
		f.logger.Log(log.LevelDebug, "cluster fsm: dispatching decoded entry",
			log.Uint64("index", entry.GetIndex()),
			log.Uint("op", uint32(op)),
			log.String("key", key),
			log.Int("value_len", len(value)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	if err := f.dispatcher.Dispatch(key, op, value, ttl); err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "cluster fsm: dispatch failed",
				log.String("error", err.Error()),
				log.Uint64("index", entry.GetIndex()),
				log.String("key", key),
				log.Uint("op", uint32(op)),
			)
		}
		return err
	}
	if f.logger.Enabled(log.LevelDebug) {
		f.logger.Log(log.LevelDebug, "cluster fsm: dispatch succeeded",
			log.Uint64("index", entry.GetIndex()),
			log.String("key", key),
			log.Uint("op", uint32(op)),
		)
	}
	return nil
}
