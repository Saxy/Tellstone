/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: chunk.go
Description: Value chunking for large SET operations (Phase 6). A value larger
than ChunkMax cannot travel as a single transport-efficient frame, so it is
split into an ordered chain of OpChunkSet log entries. The FSM reassembles the
chain via a chunkAssembler and dispatches the full value exactly once, when the
final chunk arrives.

Wire format for one chunk entry:

	[1B op=0x03 CHUNK_SET][8B TTL ms][2B keyLen][key][8B writeSeq][2B totalChunks][2B chunkIdx][chunk bytes]

writeSeq guards against interleaved or retried chains: the assembler keys
partial state by (key, writeSeq). A chain whose writeSeq differs from the
buffered one supersedes it (log order is total, so the newest write wins).

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// ChunkMax is the maximum number of value bytes carried by a single
	// OpChunkSet log entry. Values larger than this are split into a chain
	// of entries. The transport frame allows 16 MiB and raft MaxSizePerMsg
	// is 1 MiB, so a 1 MiB chunk always fits a single raft message.
	ChunkMax = 1 << 20

	// chunkFixedHeader is the fixed part before the key bytes:
	// 1B op + 8B TTL + 2B keyLen + 8B writeSeq + 2B total + 2B idx = 23.
	chunkFixedHeader = opHeaderSize + chunkSeqSize + 2 + 2
	chunkSeqSize     = 8
)

// OpChunkSet is the FSM opcode for a chunk of a chunked SET value.
const OpChunkSet byte = 0x03

// ErrChunkMalformed is returned when a chunk entry cannot be decoded.
var ErrChunkMalformed = errors.New("cluster fsm: malformed chunk entry")

// EncodeChunkSet encodes one chunk of a chunked SET value into a log entry
// payload. total is the number of chunks in the chain (>= 1); idx is this
// chunk's zero-based position. writeSeq identifies the whole write chain.
func EncodeChunkSet(key string, ttl time.Duration, writeSeq uint64, total, idx int, chunk []byte) ([]byte, error) {
	keyBytes := []byte(key)
	if len(keyBytes) > maxKeyLen {
		return nil, ErrKeyTooLarge
	}
	if total < 1 || idx < 0 || idx >= total {
		return nil, fmt.Errorf("cluster fsm: chunk index %d out of range [0,%d)", idx, total)
	}
	var ttlMs int64
	if ttl > 0 {
		ttlMs = ttl.Milliseconds()
	}
	buf := make([]byte, chunkFixedHeader+len(keyBytes)+len(chunk))
	buf[0] = OpChunkSet
	binary.BigEndian.PutUint64(buf[1:9], uint64(ttlMs))
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(keyBytes)))
	off := opHeaderSize
	copy(buf[off:off+len(keyBytes)], keyBytes)
	off += len(keyBytes)
	binary.BigEndian.PutUint64(buf[off:off+chunkSeqSize], writeSeq)
	off += chunkSeqSize
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(total))
	off += 2
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(idx))
	off += 2
	copy(buf[off:], chunk)
	return buf, nil
}

// chunkEntry is the decoded view of one OpChunkSet log entry.
type chunkEntry struct {
	key   string
	ttl   time.Duration
	seq   uint64
	total int
	idx   int
	chunk []byte
}

// DecodeChunkEntry parses an OpChunkSet payload.
func DecodeChunkEntry(data []byte) (*chunkEntry, error) {
	if len(data) < chunkFixedHeader || data[0] != OpChunkSet {
		return nil, ErrChunkMalformed
	}
	ttlMs := binary.BigEndian.Uint64(data[1:9])
	keyLen := int(binary.BigEndian.Uint16(data[9:11]))
	off := opHeaderSize
	if off+keyLen >= len(data) {
		return nil, ErrChunkMalformed
	}
	key := string(data[off : off+keyLen])
	off += keyLen
	if len(data) < off+chunkSeqSize+4 {
		return nil, ErrChunkMalformed
	}
	seq := binary.BigEndian.Uint64(data[off : off+chunkSeqSize])
	off += chunkSeqSize
	total := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	idx := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	return &chunkEntry{
		key:   key,
		ttl:   time.Duration(ttlMs) * time.Millisecond,
		seq:   seq,
		total: total,
		idx:   idx,
		chunk: data[off:],
	}, nil
}

// chunkAssembler reassembles chunked SET values as their entries stream in from
// the log. It is safe for concurrent use (the readyLoop applies sequentially,
// but a shared engine dispatcher may be used by multiple groups).
type chunkAssembler struct {
	mu      sync.Mutex
	pending map[string]*chunkState // keyed by key: total ordering keeps one chain per key
}

type chunkState struct {
	key   string
	seq   uint64
	total int
	ttl   time.Duration
	parts [][]byte
	got   map[int]bool
}

// newChunkAssembler creates an empty assembler.
func newChunkAssembler() *chunkAssembler {
	return &chunkAssembler{pending: make(map[string]*chunkState)}
}

// add feeds one chunk into the assembler. When the chain is complete it
// returns the reassembled value, the chain's TTL and true; the partial state
// is removed. A chunk whose writeSeq differs from the buffered chain resets
// the buffer (the newest chain wins).
func (a *chunkAssembler) add(ce *chunkEntry) (key string, value []byte, ttl time.Duration, complete bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := ce.key
	st, ok := a.pending[id]
	if !ok || st.seq != ce.seq {
		st = &chunkState{
			key:   ce.key,
			seq:   ce.seq,
			total: ce.total,
			ttl:   ce.ttl,
			parts: make([][]byte, ce.total),
			got:   make(map[int]bool),
		}
		a.pending[id] = st
	}
	if ce.idx >= 0 && ce.idx < st.total && !st.got[ce.idx] {
		st.got[ce.idx] = true
		st.parts[ce.idx] = ce.chunk
	}
	if len(st.got) != st.total {
		return st.key, nil, 0, false
	}
	size := 0
	for _, p := range st.parts {
		size += len(p)
	}
	value = make([]byte, 0, size)
	for _, p := range st.parts {
		value = append(value, p...)
	}
	delete(a.pending, id)
	return st.key, value, st.ttl, true
}
