/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: storage_test.go
Description: Tests for the in-memory raft.Storage implementation: log
append/compact/snapshot behavior and index-term boundary lookups.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func makeEntry(index, term uint64) *pb.Entry {
	return &pb.Entry{
		Index: &index,
		Term:  &term,
		Data:  []byte("test"),
	}
}

func TestStorageInitialState(t *testing.T) {
	s := NewStorage()
	hs, cs, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	// HardState is nil on first start (no state persisted yet).
	if hs == nil {
		t.Log("HardState nil on first start (expected)")
	}
	if cs == nil {
		t.Fatal("expected non-nil ConfState")
	}
}

func TestStorageAppendAndGet(t *testing.T) {
	s := NewStorage()
	e1 := makeEntry(1, 1)
	e2 := makeEntry(2, 1)
	s.Append([]*pb.Entry{e1, e2})

	last, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != 2 {
		t.Fatalf("LastIndex: got %d, want 2", last)
	}

	first, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if first != 1 {
		t.Fatalf("FirstIndex: got %d, want 1", first)
	}

	term, err := s.Term(1)
	if err != nil {
		t.Fatalf("Term(1): %v", err)
	}
	if term != 1 {
		t.Fatalf("Term(1): got %d, want 1", term)
	}

	entries, err := s.Entries(1, 3, 1024)
	if err != nil {
		t.Fatalf("Entries(1,3): %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Entries: got %d, want 2", len(entries))
	}
}

func TestStorageCompact(t *testing.T) {
	s := NewStorage()
	e1 := makeEntry(1, 1)
	e2 := makeEntry(2, 1)
	e3 := makeEntry(3, 1)
	s.Append([]*pb.Entry{e1, e2, e3})
	s.Compact(2)

	first, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if first != 3 {
		t.Fatalf("FirstIndex after compact: got %d, want 3", first)
	}

	// Accessing a compacted entry should return ErrCompacted.
	entries, err := s.Entries(1, 3, 1024)
	if err == nil {
		t.Fatalf("expected error for compacted entries, got %d entries", len(entries))
	}
}

func TestStorageSetSnapshot(t *testing.T) {
	s := NewStorage()
	e1 := makeEntry(1, 1)
	e2 := makeEntry(2, 1)
	e3 := makeEntry(3, 1)
	s.Append([]*pb.Entry{e1, e2, e3})

	idx := uint64(2)
	snap := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index: &idx,
		},
	}
	s.SetSnapshot(snap)

	first, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if first != 3 {
		t.Fatalf("FirstIndex after snapshot: got %d, want 3", first)
	}
}

func TestStorageSetHardState(t *testing.T) {
	s := NewStorage()
	term := uint64(5)
	vote := uint64(42)
	commit := uint64(10)
	hs := &pb.HardState{
		Term:   &term,
		Vote:   &vote,
		Commit: &commit,
	}
	s.SetHardState(hs)

	got, _, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if got.GetTerm() != 5 {
		t.Fatalf("HardState.Term: got %d, want 5", got.GetTerm())
	}
	if got.GetVote() != 42 {
		t.Fatalf("HardState.Vote: got %d, want 42", got.GetVote())
	}
	if got.GetCommit() != 10 {
		t.Fatalf("HardState.Commit: got %d, want 10", got.GetCommit())
	}
}
