package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

type mockDispatcher struct {
	lastOp    byte
	lastKey   string
	lastValue []byte
	lastTTL   time.Duration
	callCount int
}

func (d *mockDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	d.lastOp = op
	d.lastKey = key
	d.lastValue = append([]byte(nil), value...)
	d.lastTTL = ttl
	d.callCount++
	return nil
}

func TestApplySet(t *testing.T) {
	md := &mockDispatcher{}
	fsm := NewFSM(md, log.NewNoOpLogger())

	data := EncodeSet("hello", []byte("world"), 5*time.Second)
	entry := &pb.Entry{Data: data}
	if err := fsm.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if md.lastOp != OpSet {
		t.Fatalf("op: got %d, want %d", md.lastOp, OpSet)
	}
	if md.lastKey != "hello" {
		t.Fatalf("key: got %q, want %q", md.lastKey, "hello")
	}
	if string(md.lastValue) != "world" {
		t.Fatalf("value: got %q, want %q", md.lastValue, "world")
	}
	if md.lastTTL != 5*time.Second {
		t.Fatalf("ttl: got %v, want 5s", md.lastTTL)
	}
}

func TestApplyDel(t *testing.T) {
	md := &mockDispatcher{}
	fsm := NewFSM(md, log.NewNoOpLogger())

	data := EncodeDel("foo")
	entry := &pb.Entry{Data: data}
	if err := fsm.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if md.lastOp != OpDel {
		t.Fatalf("op: got %d, want %d", md.lastOp, OpDel)
	}
	if md.lastKey != "foo" {
		t.Fatalf("key: got %q, want %q", md.lastKey, "foo")
	}
}

func TestApplyNilData(t *testing.T) {
	md := &mockDispatcher{}
	fsm := NewFSM(md, log.NewNoOpLogger())

	entry := &pb.Entry{Data: nil}
	if err := fsm.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if md.callCount != 0 {
		t.Fatalf("expected no dispatch calls for nil data, got %d", md.callCount)
	}
}

type errDispatcher struct{}

func (d *errDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	return errors.New("dispatch failed")
}

func TestApplyDispatchError(t *testing.T) {
	ed := &errDispatcher{}
	fsm := NewFSM(ed, log.NewNoOpLogger())

	data := EncodeSet("k", []byte("v"), 0)
	entry := &pb.Entry{Data: data}
	if err := fsm.Apply(entry); err == nil {
		t.Fatal("expected error from dispatcher")
	}
}

func TestEncodeDecodeSet(t *testing.T) {
	key := "hello"
	value := []byte("world")
	ttl := 5 * time.Second

	encoded := EncodeSet(key, value, ttl)
	op, gotKey, gotValue, gotTTL, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry: %v", err)
	}
	if op != OpSet {
		t.Fatalf("op: got %d, want %d", op, OpSet)
	}
	if gotKey != key {
		t.Fatalf("key: got %q, want %q", gotKey, key)
	}
	if string(gotValue) != string(value) {
		t.Fatalf("value: got %q, want %q", gotValue, value)
	}
	if gotTTL != ttl {
		t.Fatalf("ttl: got %v, want %v", gotTTL, ttl)
	}
}

func TestEncodeDecodeDel(t *testing.T) {
	key := "foo"
	encoded := EncodeDel(key)
	op, gotKey, gotValue, gotTTL, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry: %v", err)
	}
	if op != OpDel {
		t.Fatalf("op: got %d, want %d", op, OpDel)
	}
	if gotKey != key {
		t.Fatalf("key: got %q, want %q", gotKey, key)
	}
	if len(gotValue) != 0 {
		t.Fatalf("DEL should have empty value, got %d bytes", len(gotValue))
	}
	if gotTTL != 0 {
		t.Fatalf("ttl: got %v, want 0", gotTTL)
	}
}

func TestEncodeDecodeSetZeroTTL(t *testing.T) {
	encoded := EncodeSet("k", []byte("v"), 0)
	op, _, _, gotTTL, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry: %v", err)
	}
	if op != OpSet {
		t.Fatalf("op: got %d, want %d", op, OpSet)
	}
	if gotTTL != 0 {
		t.Fatalf("ttl: got %v, want 0", gotTTL)
	}
}

func TestEncodeDecodeLargeValue(t *testing.T) {
	value := make([]byte, 64*1024)
	for i := range value {
		value[i] = byte(i % 256)
	}
	encoded := EncodeSet("big", value, time.Minute)
	_, _, gotValue, _, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry: %v", err)
	}
	if len(gotValue) != len(value) {
		t.Fatalf("value length: got %d, want %d", len(gotValue), len(value))
	}
}

func TestDecodeLogEntryTooShort(t *testing.T) {
	_, _, _, _, err := DecodeLogEntry([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected error for short data")
	}
}

func TestDecodeLogEntryKeyTooLong(t *testing.T) {
	buf := make([]byte, opHeaderSize+5)
	buf[0] = OpDel
	// Set keyLen to 100, but only 5 bytes of data follow.
	buf[9] = 0
	buf[10] = 100
	_, _, _, _, err := DecodeLogEntry(buf)
	if err == nil {
		t.Fatal("expected error for oversized key length")
	}
}
