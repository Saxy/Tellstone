/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: chunk_test.go
Description: Tests for value chunking: encode/decode round-trips, the
chunkAssembler reassembly, write-sequence supersede behavior, and FSM-level
dispatch of a chunked value.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bytes"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

// testChunkDispatcher records dispatched (key, value, ttl) pairs for FSM tests.
type testChunkDispatcher struct {
	key   string
	value []byte
	ttl   time.Duration
	op    byte
	calls int
}

func (d *testChunkDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	d.key = key
	d.op = op
	d.value = value
	d.ttl = ttl
	d.calls++
	return nil
}

func TestEncodeDecodeChunkEntry(t *testing.T) {
	chunk := []byte("hello-chunk")
	data, err := EncodeChunkSet("ck", 1500*time.Millisecond, 42, 3, 1, chunk)
	if err != nil {
		t.Fatalf("EncodeChunkSet: %v", err)
	}
	if data[0] != OpChunkSet {
		t.Fatalf("op = %x, want 0x03", data[0])
	}
	ce, err := DecodeChunkEntry(data)
	if err != nil {
		t.Fatalf("DecodeChunkEntry: %v", err)
	}
	if ce.key != "ck" || ce.seq != 42 || ce.total != 3 || ce.idx != 1 {
		t.Fatalf("decoded fields wrong: %+v", ce)
	}
	if !bytes.Equal(ce.chunk, chunk) {
		t.Fatalf("chunk = %q, want %q", ce.chunk, chunk)
	}
	if ce.ttl != 1500*time.Millisecond {
		t.Fatalf("ttl = %v, want 1.5s", ce.ttl)
	}
}

func TestEncodeChunkRejectsBadIndex(t *testing.T) {
	if _, err := EncodeChunkSet("k", 0, 1, 3, 3, []byte("x")); err == nil {
		t.Fatal("expected error for idx == total")
	}
	if _, err := EncodeChunkSet("k", 0, 1, 0, 0, []byte("x")); err == nil {
		t.Fatal("expected error for total == 0")
	}
}

func TestDecodeChunkRejectsMalformed(t *testing.T) {
	if _, err := DecodeChunkEntry([]byte{0x01, 0x02}); err == nil {
		t.Fatal("expected error for wrong opcode + short payload")
	}
	if _, err := DecodeChunkEntry([]byte{0x03, 0x00}); err == nil {
		t.Fatal("expected error for short header")
	}
	// Reject a payload whose keyLen overruns the buffer.
	buf := make([]byte, chunkFixedHeader)
	buf[0] = OpChunkSet
	buf[9] = 0xFF
	buf[10] = 0xFF
	if _, err := DecodeChunkEntry(buf); err == nil {
		t.Fatal("expected error for keyLen overrun")
	}
}

func TestChunkAssemblerReassemblesChain(t *testing.T) {
	a := newChunkAssembler()
	value := bytes.Repeat([]byte("xyz"), ChunkMax/3+123) // > 1 MiB
	chunks := splitValueForTest(value, ChunkMax)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	for idx, ch := range chunks {
		key, val, _, complete := a.add(&chunkEntry{
			key:   "big",
			seq:   7,
			total: len(chunks),
			idx:   idx,
			chunk: ch,
		})
		if idx < len(chunks)-1 && complete {
			t.Fatal("assembler completed before the final chunk")
		}
		if idx == len(chunks)-1 && !complete {
			t.Fatal("assembler did not complete on the final chunk")
		}
		if idx == len(chunks)-1 {
			if key != "big" || !bytes.Equal(val, value) {
				t.Fatalf("reassembled value mismatch: len=%d want %d", len(val), len(value))
			}
		}
	}
}

func TestChunkAssemblerSupersedesStaleChain(t *testing.T) {
	a := newChunkAssembler()
	// Feed chunk 0 of write sequence 1, then a full retry with sequence 2.
	a.add(&chunkEntry{key: "k", seq: 1, total: 2, idx: 0, chunk: []byte("old0")})
	// New chain for the same key with a different write seq must discard the
	// stale partial state and reassemble cleanly.
	key, val, _, complete := a.add(&chunkEntry{key: "k", seq: 2, total: 2, idx: 0, chunk: []byte("new0")})
	if complete {
		t.Fatal("chain should not complete with one chunk")
	}
	key, val, _, complete = a.add(&chunkEntry{key: "k", seq: 2, total: 2, idx: 1, chunk: []byte("new1")})
	if !complete || key != "k" || string(val) != "new0new1" {
		t.Fatalf("supersede reassembly failed: key=%q value=%q complete=%v", key, val, complete)
	}
}

func TestFSMAppliesChunkedSet(t *testing.T) {
	d := &testChunkDispatcher{}
	fsm := NewFSM(d, log.NewNoOpLogger())
	value := bytes.Repeat([]byte("ab"), ChunkMax/2+7)
	chunks := splitValueForTest(value, ChunkMax)
	for idx, ch := range chunks {
		data, err := EncodeChunkSet("ckey", time.Minute, 99, len(chunks), idx, ch)
		if err != nil {
			t.Fatalf("EncodeChunkSet: %v", err)
		}
		index := uint64(idx + 1)
		if err := fsm.Apply(&pb.Entry{Index: &index, Data: data}); err != nil {
			t.Fatalf("Apply(chunk %d): %v", idx, err)
		}
	}
	if d.calls != 1 {
		t.Fatalf("dispatch calls = %d, want 1", d.calls)
	}
	if d.key != "ckey" || d.op != OpSet || !bytes.Equal(d.value, value) {
		t.Fatal("dispatched value mismatch")
	}
	if d.ttl != time.Minute {
		t.Fatalf("ttl = %v, want 1m", d.ttl)
	}
}

// splitValueForTest is the chunk boundary helper exercised in tests above.
func splitValueForTest(value []byte, max int) [][]byte {
	var chunks [][]byte
	for len(value) > max {
		chunks = append(chunks, value[:max])
		value = value[max:]
	}
	if len(value) > 0 {
		chunks = append(chunks, value)
	}
	if len(chunks) == 0 {
		chunks = [][]byte{{}}
	}
	return chunks
}
