/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: fsm_test.go
Description: Tests for the raft finite state machine and its binary log
entry codec: SET/DEL encode-decode round trips and apply dispatch.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"errors"
	"strings"
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

	data, err := EncodeSet("hello", []byte("world"), 5*time.Second)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
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

	data, err := EncodeDel("foo")
	if err != nil {
		t.Fatalf("EncodeDel: %v", err)
	}
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

	data, err := EncodeSet("k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	entry := &pb.Entry{Data: data}
	if err := fsm.Apply(entry); err == nil {
		t.Fatal("expected error from dispatcher")
	}
}

func TestEncodeDecodeSet(t *testing.T) {
	key := "hello"
	value := []byte("world")
	ttl := 5 * time.Second

	encoded, err := EncodeSet(key, value, ttl)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
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
	encoded, err := EncodeDel(key)
	if err != nil {
		t.Fatalf("EncodeDel: %v", err)
	}
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
	encoded, err := EncodeSet("k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
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
	encoded, err := EncodeSet("big", value, time.Minute)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
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

func TestEncodeKeyExceedsWireFormat(t *testing.T) {
	// A 65536-byte key cannot fit the uint16 length field. The encoders must
	// reject it — truncating the length would silently corrupt the entry for
	// every replica decoding it.
	tooBig := strings.Repeat("x", maxKeyLen+1)
	if _, err := EncodeSet(tooBig, []byte("v"), 0); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("EncodeSet(65536-byte key): got %v, want ErrKeyTooLarge", err)
	}
	if _, err := EncodeDel(tooBig); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("EncodeDel(65536-byte key): got %v, want ErrKeyTooLarge", err)
	}
}

func TestEncodeKeyAtWireFormatBoundary(t *testing.T) {
	// Exactly maxKeyLen (65535) bytes must still encode and decode intact.
	maxKey := strings.Repeat("k", maxKeyLen)
	data, err := EncodeSet(maxKey, []byte("val"), time.Second)
	if err != nil {
		t.Fatalf("EncodeSet(65535-byte key): %v", err)
	}
	op, gotKey, gotValue, gotTTL, err := DecodeLogEntry(data)
	if err != nil {
		t.Fatalf("DecodeLogEntry: %v", err)
	}
	if op != OpSet || gotKey != maxKey || string(gotValue) != "val" || gotTTL != time.Second {
		t.Fatalf("round trip mismatch: op=%d keyLen=%d value=%q ttl=%v",
			op, len(gotKey), gotValue, gotTTL)
	}

	delData, err := EncodeDel(maxKey)
	if err != nil {
		t.Fatalf("EncodeDel(65535-byte key): %v", err)
	}
	op, gotKey, gotValue, _, err = DecodeLogEntry(delData)
	if err != nil {
		t.Fatalf("DecodeLogEntry(del): %v", err)
	}
	if op != OpDel || gotKey != maxKey || len(gotValue) != 0 {
		t.Fatalf("del round trip mismatch: op=%d keyLen=%d valueLen=%d",
			op, len(gotKey), len(gotValue))
	}
}
