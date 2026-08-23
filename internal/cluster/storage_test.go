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

// TestStorageTermBoundaryAfterCompaction verifies that the term of the entry
// just below the retained window — FirstIndex()-1, which raft queries while
// restoring across a truncated log — stays queryable after compaction,
// whether entries remain or the log was emptied.
func TestStorageTermBoundaryAfterCompaction(t *testing.T) {
	t.Run("partial compaction keeps boundary term", func(t *testing.T) {
		s := NewStorage()
		s.Append([]*pb.Entry{
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 2), makeEntry(4, 2),
		})
		// Drop entries 1-3; the window now starts at 4.
		s.Compact(3)

		first, err := s.FirstIndex()
		if err != nil {
			t.Fatalf("FirstIndex: %v", err)
		}
		if first != 4 {
			t.Fatalf("FirstIndex: got %d, want 4", first)
		}
		term, err := s.Term(first - 1)
		if err != nil {
			t.Fatalf("Term(%d): %v", first-1, err)
		}
		if term != 2 {
			t.Fatalf("boundary Term: got %d, want 2", term)
		}
		// Retained entries still resolve normally.
		if got, err := s.Term(4); err != nil || got != 2 {
			t.Fatalf("Term(4): got (%d, %v), want (2, nil)", got, err)
		}
		if _, err := s.Term(1); err == nil {
			t.Fatal("expected ErrCompacted for index below the window")
		}
	})

	t.Run("full compaction preserves final entry as anchor", func(t *testing.T) {
		s := NewStorage()
		s.Append([]*pb.Entry{makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 2)})
		s.Compact(3)

		first, err := s.FirstIndex()
		if err != nil {
			t.Fatalf("FirstIndex: %v", err)
		}
		if first != 4 {
			t.Fatalf("FirstIndex: got %d, want 4", first)
		}
		last, err := s.LastIndex()
		if err != nil {
			t.Fatalf("LastIndex: %v", err)
		}
		if last != 3 {
			t.Fatalf("LastIndex: got %d, want 3 (anchor)", last)
		}
		term, err := s.Term(last)
		if err != nil {
			t.Fatalf("Term(anchor): %v", err)
		}
		if term != 2 {
			t.Fatalf("anchor Term: got %d, want 2", term)
		}
	})
}

// TestStorageSetSnapshotAnchorConsistency verifies that installing a snapshot
// advances the anchor to the snapshot index/term in every truncation shape —
// partial slice, full consumption, and a snapshot beyond the stored tail —
// and that stale snapshots leave the anchor untouched.
func TestStorageSetSnapshotAnchorConsistency(t *testing.T) {
	newSnap := func(index, term uint64) *pb.Snapshot {
		return &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term}}
	}

	t.Run("partial truncation advances anchor", func(t *testing.T) {
		s := NewStorage()
		s.Append([]*pb.Entry{
			makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 2), makeEntry(4, 2),
		})
		s.SetSnapshot(newSnap(3, 2))

		if got, _ := s.FirstIndex(); got != 4 {
			t.Fatalf("FirstIndex: got %d, want 4", got)
		}
		if term, err := s.Term(3); err != nil || term != 2 {
			t.Fatalf("Term(snapshot boundary): got (%d, %v), want (2, nil)", term, err)
		}
		if got, _ := s.LastIndex(); got != 4 {
			t.Fatalf("LastIndex: got %d, want 4", got)
		}
	})

	t.Run("truncation consuming all entries", func(t *testing.T) {
		s := NewStorage()
		s.Append([]*pb.Entry{makeEntry(1, 1), makeEntry(2, 2)})
		s.SetSnapshot(newSnap(2, 2))

		if got, _ := s.FirstIndex(); got != 3 {
			t.Fatalf("FirstIndex: got %d, want 3", got)
		}
		if got, _ := s.LastIndex(); got != 2 {
			t.Fatalf("LastIndex: got %d, want 2", got)
		}
		if term, err := s.Term(2); err != nil || term != 2 {
			t.Fatalf("Term(boundary): got (%d, %v), want (2, nil)", term, err)
		}
	})

	t.Run("snapshot beyond stored tail resets to snapshot boundary", func(t *testing.T) {
		s := NewStorage()
		s.Append([]*pb.Entry{makeEntry(1, 1), makeEntry(2, 1)})
		// A follower restored from a leader snapshot ahead of its local tail.
		s.SetSnapshot(newSnap(9, 3))

		if got, _ := s.FirstIndex(); got != 10 {
			t.Fatalf("FirstIndex: got %d, want 10", got)
		}
		if got, _ := s.LastIndex(); got != 9 {
			t.Fatalf("LastIndex: got %d, want 9", got)
		}
		if term, err := s.Term(9); err != nil || term != 3 {
			t.Fatalf("Term(9): got (%d, %v), want (3, nil)", term, err)
		}
	})

	t.Run("stale snapshot leaves anchor untouched", func(t *testing.T) {
		s := NewStorage()
		s.Append([]*pb.Entry{makeEntry(1, 1), makeEntry(2, 2)})
		s.SetSnapshot(newSnap(5, 3))
		s.SetSnapshot(newSnap(2, 1)) // behind the installed boundary

		if got, _ := s.LastIndex(); got != 5 {
			t.Fatalf("LastIndex: got %d, want 5", got)
		}
		if term, err := s.Term(5); err != nil || term != 3 {
			t.Fatalf("Term(5): got (%d, %v), want (3, nil)", term, err)
		}
		if _, err := s.Term(6); err == nil {
			t.Fatal("expected ErrUnavailable past the anchor")
		}
	})
}
