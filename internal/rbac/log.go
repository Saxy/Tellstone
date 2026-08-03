/*
Package rbac
Tellstone Role-Based Access Control
File: log.go
Description: The ACL auth-failure log behind ACL LOG. A mutex-protected circular buffer of
recent rejected AUTH attempts (timestamp, username, remote address, reason) kept on the Store,
so it survives policy hot-swaps and is shared by both protocol layers. Recording happens only
on the failed-AUTH path — never on the hot path.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"sync/atomic"
	"time"
)

// DefaultAuthLogCap is the capacity of the ACL LOG circular buffer: 100 recent
// rejected AUTH attempts, oldest evicted once full.
const DefaultAuthLogCap = 100

// AuthLogEntry is one rejected AUTH attempt. Timestamp is wall-clock time at
// record time; Username may be empty when it was not parseable from the frame.
type AuthLogEntry struct {
	Timestamp  time.Time
	Username   string
	RemoteAddr string
	Reason     string
}

// LogAuthFailure records one rejected AUTH attempt in the circular buffer and
// bumps the store-wide auth-failure counter (the ACL LOG view of IncAuthFailure).
// The buffer is lazily allocated at DefaultAuthLogCap and evicts the oldest
// entry once full, newest last. Callers are the protocol AUTH paths only.
func (s *Store) LogAuthFailure(username, remoteAddr, reason string) {
	atomic.AddUint64(&s.authFailures, 1)
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if s.log == nil {
		s.log = make([]AuthLogEntry, DefaultAuthLogCap)
	}
	s.log[s.logHead] = AuthLogEntry{
		Timestamp:  time.Now(),
		Username:   username,
		RemoteAddr: remoteAddr,
		Reason:     reason,
	}
	s.logHead = (s.logHead + 1) % len(s.log)
	if s.logLen < len(s.log) {
		s.logLen++
	}
}

// AuthLog returns the buffered auth-failure entries in chronological order,
// oldest first. It returns a copy, so the caller may hold the result after the
// buffer advances. Empty when nothing has failed. ACL LOG is the only consumer;
// the allocation is fine off the hot path.
func (s *Store) AuthLog() []AuthLogEntry {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if s.logLen == 0 {
		return nil
	}
	start := s.logHead - s.logLen
	if start < 0 {
		start += len(s.log)
	}
	out := make([]AuthLogEntry, s.logLen)
	for i := 0; i < s.logLen; i++ {
		out[i] = s.log[(start+i)%len(s.log)]
	}
	return out
}
