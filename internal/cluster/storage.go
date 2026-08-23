/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: storage.go
Description: In-memory Raft log and state storage implementing raft.Storage.
Holds hard state (term, vote, commit), log entries, conf state, and the latest
snapshot in memory. Concurrent-safe via RWMutex. Designed for the shared-nothing
architecture: each cluster node owns exactly one Storage instance.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"sync"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Storage implements raft.Storage for in-memory Raft log persistence. All
// methods are safe for concurrent use. Log entries are stored as pointers
// because protobuf v2 message types contain a sync.Mutex and must not be
// copied.
type Storage struct {
	mu        sync.RWMutex
	hardState *pb.HardState
	confState *pb.ConfState
	entries   []*pb.Entry // 1-based; index 0 is unused
	snapshot  *pb.Snapshot
}

// NewStorage creates an empty Raft storage instance.
func NewStorage() *Storage {
	return &Storage{
		confState: &pb.ConfState{},
	}
}

// InitialState returns the saved HardState and ConfState. HardState may be
// nil on first start (no state persisted yet). ConfState must not be nil.
func (s *Storage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, s.confState, nil
}

// SetHardState atomically replaces the stored HardState.
func (s *Storage) SetHardState(st *pb.HardState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = st
}

// SetConfState atomically replaces the stored ConfState.
func (s *Storage) SetConfState(cs *pb.ConfState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confState = cs
}

// firstIndex returns the first log entry index without locking (caller holds lock).
func (s *Storage) firstIndex() uint64 {
	if len(s.entries) > 0 {
		return s.entries[0].GetIndex()
	}
	return 1
}

// lastIndex returns the last log entry index without locking (caller holds lock).
func (s *Storage) lastIndex() uint64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].GetIndex()
	}
	if s.snapshot != nil {
		return s.snapshot.GetMetadata().GetIndex()
	}
	return 0
}

// Entries returns log entries in the range [lo, hi). maxSize limits the total
// byte size; at least one entry is always returned if the range is valid.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if lo > hi {
		return nil, raft.ErrCompacted
	}
	if len(s.entries) == 0 {
		return nil, raft.ErrUnavailable
	}
	first := s.firstIndex()
	if lo < first {
		return nil, raft.ErrCompacted
	}
	last := s.lastIndex()
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}
	loIdx := lo - first
	hiIdx := hi - first
	if hiIdx > uint64(len(s.entries)) {
		hiIdx = uint64(len(s.entries))
	}
	var result []*pb.Entry
	var size uint64
	for i := loIdx; i < hiIdx; i++ {
		e := s.entries[i]
		size += uint64(len(e.GetData()))
		if len(result) > 0 && size > maxSize {
			break
		}
		result = append(result, e)
	}
	return result, nil
}

// Term returns the term of the entry at index i. Index 0 is a sentinel used
// by raft during bootstrap (lastEntryID on empty log); we return 0, nil for
// this case to avoid ErrUnavailable panics.
func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Index 0 is a sentinel: empty log with no snapshot. Raft calls Term(0)
	// during bootstrap when LastIndex returns 0.
	if i == 0 && len(s.entries) == 0 && s.snapshot == nil {
		return 0, nil
	}
	if len(s.entries) == 0 {
		return 0, raft.ErrUnavailable
	}
	first := s.firstIndex()
	if i < first {
		return 0, raft.ErrCompacted
	}
	last := s.lastIndex()
	if i > last {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-first].GetTerm(), nil
}

// LastIndex returns the index of the last entry.
func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex(), nil
}

// FirstIndex returns the index of the first available log entry.
func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.entries) > 0 {
		return s.entries[0].GetIndex(), nil
	}
	if s.snapshot != nil {
		return s.snapshot.GetMetadata().GetIndex() + 1, nil
	}
	return 1, nil
}

// Snapshot returns the most recent snapshot.
func (s *Storage) Snapshot() (*pb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshot == nil {
		return &pb.Snapshot{Metadata: &pb.SnapshotMetadata{}}, nil
	}
	return s.snapshot, nil
}

// Append appends new entries to the log. The caller retains ownership of the
// slice elements; the storage stores them directly. Duplicates at the tail
// are silently replaced (raft may re-send entries).
func (s *Storage) Append(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	firstNew := entries[0].GetIndex()
	if len(s.entries) > 0 {
		last := s.entries[len(s.entries)-1].GetIndex()
		if firstNew <= last {
			cutPoint := firstNew - s.entries[0].GetIndex()
			if cutPoint > uint64(len(s.entries)) {
				cutPoint = uint64(len(s.entries))
			}
			s.entries = s.entries[:cutPoint]
		}
	}
	s.entries = append(s.entries, entries...)
}

// SetSnapshot replaces the current snapshot and compacts the log to the
// snapshot's last index.
func (s *Storage) SetSnapshot(snap *pb.Snapshot) {
	if snap == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snap
	idx := snap.GetMetadata().GetIndex()
	if len(s.entries) > 0 {
		first := s.entries[0].GetIndex()
		if idx >= first {
			s.entries = s.entries[idx-first+1:]
		}
	}
}

// Compact discards all log entries up to and including index.
func (s *Storage) Compact(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return
	}
	first := s.entries[0].GetIndex()
	if index < first {
		return
	}
	s.entries = s.entries[index-first+1:]
}
