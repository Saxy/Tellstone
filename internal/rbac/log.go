/*
Package rbac
Tellstone Role-Based Access Control
File: log.go
Description: The security-event log behind ACL LOG. A mutex-protected circular buffer of
recent rejected AUTH attempts and denied commands (timestamp, username, remote address,
reason) kept on the Store, so it survives policy hot-swaps and is shared by both protocol
layers. Recording happens only on the failed-AUTH and NOPERM paths — never on the hot path.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"sync/atomic"
	"time"
)

// DefaultAuthLogCap is the capacity of the ACL LOG circular buffer: 100 recent
// security events, oldest evicted once full. Auth failures and command denials
// share the capacity, matching Redis, where one bounded log holds both.
const DefaultAuthLogCap = 100

// AuthLogEntry is one recorded security event — a rejected AUTH attempt or a
// denied command. Timestamp is wall-clock time at record time; Username may be
// empty when it was not parseable from the frame. Reason carries the failure
// cause verbatim for AUTH rejections and a formatted "NOPERM command=<cmd>
// key=<key>" string for denials, so both kinds share one wire encoding.
type AuthLogEntry struct {
	Timestamp  time.Time
	Username   string
	RemoteAddr string
	Reason     string
}

// appendLocked stores one entry in the circular buffer, lazily allocating it at
// DefaultAuthLogCap and evicting the oldest once full, newest last. Callers must
// hold logMu — logMu is not reentrant and every caller already holds it.
func (s *Store) appendLocked(entry AuthLogEntry) {
	if s.log == nil {
		s.log = make([]AuthLogEntry, DefaultAuthLogCap)
	}
	s.log[s.logHead] = entry
	s.logHead = (s.logHead + 1) % len(s.log)
	if s.logLen < len(s.log) {
		s.logLen++
	}
}

// LogAuthFailure records one rejected AUTH attempt in the circular buffer and
// bumps the store-wide auth-failure counter (the ACL LOG view of IncAuthFailure).
// Callers are the protocol AUTH paths only.
func (s *Store) LogAuthFailure(username, remoteAddr, reason string) {
	atomic.AddUint64(&s.authFailures, 1)
	s.logMu.Lock()
	defer s.logMu.Unlock()
	s.appendLocked(AuthLogEntry{
		Timestamp:  time.Now(),
		Username:   username,
		RemoteAddr: remoteAddr,
		Reason:     reason,
	})
}

// LogDenied records one command rejected by the policy (NOPERM) in the same
// circular buffer as LogAuthFailure, so ACL LOG covers authorization denials and
// not just authentication failures. cmd and key are folded into Reason rather
// than carried as their own fields: the ACL LOG wire encodings on both protocols
// are fixed at four length-prefixed fields per entry and carry no version tag,
// so widening the entry would desynchronize existing clients mid-reply.
//
// The denied-command counter is not bumped here — IncDenied already runs at
// these call sites.
func (s *Store) LogDenied(username, remoteAddr, cmd, key string) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	s.appendLocked(AuthLogEntry{
		Timestamp:  time.Now(),
		Username:   username,
		RemoteAddr: remoteAddr,
		Reason:     "NOPERM command=" + cmd + " key=" + key,
	})
}

// SeedAuthLog pre-fills the buffer with entries recovered from a persisted
// audit log, restoring ACL LOG's history across a restart. entries must be in
// chronological order, oldest first, and are subject to the usual capacity —
// seeding more than DefaultAuthLogCap keeps the newest.
//
// The failure counters are deliberately left alone: they count what this process
// has seen, and a restart resetting them is the behavior rate() and increase()
// already assume. Called once at startup, before any listener runs.
func (s *Store) SeedAuthLog(entries []AuthLogEntry) {
	if len(entries) == 0 {
		return
	}
	s.logMu.Lock()
	defer s.logMu.Unlock()
	for _, entry := range entries {
		s.appendLocked(entry)
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
