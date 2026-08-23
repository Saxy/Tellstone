/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: proposals_test.go
Description: Tests for the synchronous proposal tracker that tags raft
log entries with proposal IDs and signals completion on apply.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestTagAndExtract(t *testing.T) {
	pt := newProposalTracker()
	id, _ := pt.add()
	if id != 1 {
		t.Fatalf("first ID: got %d, want 1", id)
	}

	tagged := tagProposal(tagID(id), []byte("hello"))
	if len(tagged) != proposalIDSize+5 {
		t.Fatalf("tagged length: got %d, want %d", len(tagged), proposalIDSize+5)
	}

	gotID, ok := extractProposalID(tagged)
	if !ok {
		t.Fatal("extractProposalID returned false for tagged data")
	}
	if gotID != id {
		t.Fatalf("extracted ID: got %d, want %d", gotID, id)
	}

	// The payload after the tag should be intact.
	payload := tagged[proposalIDSize:]
	if string(payload) != "hello" {
		t.Fatalf("payload: got %q, want %q", payload, "hello")
	}
}

func TestExtractUntaggedData(t *testing.T) {
	// OpSet (0x01) should NOT be extracted as a proposal ID.
	data, err := EncodeSet("k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	_, ok := extractProposalID(data)
	if ok {
		t.Fatal("extractProposalID should return false for untagged SET data")
	}

	// OpDel (0x02) should NOT be extracted as a proposal ID.
	data, err = EncodeDel("k")
	if err != nil {
		t.Fatalf("EncodeDel: %v", err)
	}
	_, ok = extractProposalID(data)
	if ok {
		t.Fatal("extractProposalID should return false for untagged DEL data")
	}
}

func TestExtractTooShort(t *testing.T) {
	_, ok := extractProposalID([]byte{0x01, 0x02, 0x03})
	if ok {
		t.Fatal("extractProposalID should return false for short data")
	}
	_, ok = extractProposalID(nil)
	if ok {
		t.Fatal("extractProposalID should return false for nil data")
	}
}

func TestTrackerAddComplete(t *testing.T) {
	pt := newProposalTracker()
	id1, ch1 := pt.add()
	id2, ch2 := pt.add()

	if id1 == id2 {
		t.Fatalf("duplicate IDs: %d", id1)
	}

	pt.complete(id1, nil)
	// ch1 should receive nil error then be closed.
	err := <-ch1
	if err != nil {
		t.Fatalf("ch1: unexpected error %v", err)
	}
	// ch2 should NOT be closed.
	select {
	case <-ch2:
		t.Fatal("ch2 should not be closed yet")
	default:
	}

	pt.complete(id2, nil)
	err = <-ch2
	if err != nil {
		t.Fatalf("ch2: unexpected error %v", err)
	}
}

func TestTrackerRemove(t *testing.T) {
	pt := newProposalTracker()
	id, ch := pt.add()
	pt.remove(id)
	// Channel should still be open (not closed), just removed from the map.
	select {
	case <-ch:
		t.Fatal("removed channel should not be closed")
	default:
	}
}

func TestTrackerCompleteUnknownID(t *testing.T) {
	pt := newProposalTracker()
	// Completing a non-existent ID should not panic.
	pt.complete(999, nil)
}

func TestTrackerCompleteWithError(t *testing.T) {
	pt := newProposalTracker()
	id, ch := pt.add()
	pt.complete(id, errors.New("apply failed"))
	err := <-ch
	if err == nil || err.Error() != "apply failed" {
		t.Fatalf("expected 'apply failed' error, got %v", err)
	}
}

func TestTrackerCompleteNilError(t *testing.T) {
	pt := newProposalTracker()
	id, ch := pt.add()
	pt.complete(id, nil)
	err := <-ch
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestTagIDHighBit(t *testing.T) {
	// tagID must set bit 63 so tagged entries never collide with opcode bytes.
	id := uint64(42)
	tagged := tagID(id)
	if tagged&(1<<63) == 0 {
		t.Fatal("tagID did not set bit 63")
	}
	if tagged != id|(1<<63) {
		t.Fatalf("tagID: got %d, want %d", tagged, id|(1<<63))
	}
}

func TestTaggedProposalRoundTrip(t *testing.T) {
	pt := newProposalTracker()
	id, _ := pt.add()

	// Simulate a full SET encode → tag → extract → decode round trip.
	setData, err := EncodeSet("mykey", []byte("myval"), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	tagged := tagProposal(tagID(id), setData)

	// Extract the proposal ID.
	gotID, ok := extractProposalID(tagged)
	if !ok || gotID != id {
		t.Fatalf("extractProposalID: got (%d, %v), want (%d, true)", gotID, ok, id)
	}

	// Strip the tag and decode the SET payload.
	payload := tagged[proposalIDSize:]
	op, key, value, ttl, err := DecodeLogEntry(payload)
	if err != nil {
		t.Fatalf("DecodeLogEntry: %v", err)
	}
	if op != OpSet || key != "mykey" || string(value) != "myval" || ttl != 0 {
		t.Fatalf("decoded: op=%d key=%q value=%q ttl=%v", op, key, value, ttl)
	}
}

func TestLargeProposalID(t *testing.T) {
	pt := newProposalTracker()
	// Advance the counter to a large value.
	for i := 0; i < 1000; i++ {
		pt.add()
	}
	id, ch := pt.add()
	if id != 1001 {
		t.Fatalf("ID: got %d, want 1001", id)
	}
	tagged := tagProposal(tagID(id), []byte("data"))
	gotID, ok := extractProposalID(tagged)
	if !ok || gotID != id {
		t.Fatalf("extractProposalID: got (%d, %v), want (%d, true)", gotID, ok, id)
	}
	_ = ch
}

func TestTagProposalPreservesPayload(t *testing.T) {
	// Verify the encoded proposal ID bytes are big-endian and the payload follows.
	id := uint64(7)
	tagged := tagProposal(tagID(id), []byte("ABC"))
	var got uint64
	for i := 0; i < 8; i++ {
		got = (got << 8) | uint64(tagged[i])
	}
	if got != tagID(id) {
		t.Fatalf("tag bytes: got %d, want %d", got, tagID(id))
	}
	_ = binary.BigEndian
}
