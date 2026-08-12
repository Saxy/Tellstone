package rbac

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

// itoa is a tiny alias so the eviction test's expected usernames read clearly.
func itoa(i int) string { return strconv.Itoa(i) }

// TestACLLogOrder verifies the auth-failure buffer returns entries in
// chronological order and that overflowing it evicts the oldest entries.
func TestACLLogOrder(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	for i := 0; i < 5; i++ {
		s.LogAuthFailure("alice", "1.2.3.4:5", "invalid password")
	}
	entries := s.AuthLog()
	if len(entries) != 5 {
		t.Fatalf("AuthLog len = %d, want 5", len(entries))
	}
	for i, e := range entries {
		if e.Username != "alice" || e.Reason != "invalid password" || e.RemoteAddr != "1.2.3.4:5" {
			t.Fatalf("entry %d = %+v", i, e)
		}
		if e.Timestamp.IsZero() {
			t.Fatalf("entry %d has zero timestamp", i)
		}
	}
	if got := s.AuthFailures(); got != 5 {
		t.Fatalf("AuthFailures = %d, want 5", got)
	}
}

// TestACLLogEviction overflows the default capacity and verifies the oldest
// entries are dropped while order is preserved and capacity stays bounded.
func TestACLLogEviction(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	total := DefaultAuthLogCap + 7
	for i := 0; i < total; i++ {
		s.LogAuthFailure("u"+itoa(i), "addr", "reason")
	}
	entries := s.AuthLog()
	if len(entries) != DefaultAuthLogCap {
		t.Fatalf("AuthLog len = %d, want %d", len(entries), DefaultAuthLogCap)
	}
	// The first 7 writes are evicted; the oldest survivor is write #7, and
	// the newest is the last write (total-1), in that order.
	if entries[0].Username != "u7" {
		t.Fatalf("oldest survivor = %q, want u7", entries[0].Username)
	}
	if entries[len(entries)-1].Username != "u"+itoa(total-1) {
		t.Fatalf("newest entry = %q, want u%d", entries[len(entries)-1].Username, total-1)
	}
	if got := s.AuthFailures(); got != uint64(total) {
		t.Fatalf("AuthFailures = %d, want %d", got, total)
	}
}

// TestACLLogEmpty verifies an untouched store reports no entries.
func TestACLLogEmpty(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	if entries := s.AuthLog(); entries != nil {
		t.Fatalf("AuthLog = %v, want nil", entries)
	}
}

// TestACLLogConcurrent exercises the mutex-protected buffer from many
// goroutines; run with -race to prove no data race on append/read.
func TestACLLogConcurrent(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.LogAuthFailure("alice", "1.2.3.4:5", "invalid password")
				_ = s.AuthLog()
			}
		}()
	}
	wg.Wait()
	entries := s.AuthLog()
	if len(entries) != DefaultAuthLogCap {
		t.Fatalf("AuthLog len = %d, want %d", len(entries), DefaultAuthLogCap)
	}
	for i, e := range entries {
		if strings.TrimSpace(e.Username) == "" || e.Reason == "" {
			t.Fatalf("entry %d has empty fields: %+v", i, e)
		}
	}
}

// TestACLLogDenied verifies a denial records the command and key folded into
// Reason, the format both ACL LOG wire encodings render verbatim.
func TestACLLogDenied(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	s.LogDenied("bob", "10.0.0.5:5512", "SET", "forbidden:key")
	entries := s.AuthLog()
	if len(entries) != 1 {
		t.Fatalf("AuthLog len = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.Username != "bob" || e.RemoteAddr != "10.0.0.5:5512" {
		t.Fatalf("entry = %+v", e)
	}
	if want := "NOPERM command=SET key=forbidden:key"; e.Reason != want {
		t.Fatalf("Reason = %q, want %q", e.Reason, want)
	}
	if e.Timestamp.IsZero() {
		t.Fatal("entry has zero timestamp")
	}
}

// TestACLLogDeniedKeyless covers keyless commands (ROLE/ACL have no key scope),
// which pass an empty key through to the Reason string.
func TestACLLogDeniedKeyless(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	s.LogDenied("bob", "10.0.0.5:5512", "ACL", "")
	entries := s.AuthLog()
	if len(entries) != 1 {
		t.Fatalf("AuthLog len = %d, want 1", len(entries))
	}
	if want := "NOPERM command=ACL key="; entries[0].Reason != want {
		t.Fatalf("Reason = %q, want %q", entries[0].Reason, want)
	}
}

// TestACLLogMixedAuthAndDenied verifies both event kinds share one buffer and
// stay in chronological order relative to each other.
func TestACLLogMixedAuthAndDenied(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	s.LogAuthFailure("alice", "1.2.3.4:5", "invalid password")
	s.LogDenied("bob", "1.2.3.4:6", "GET", "k1")
	s.LogAuthFailure("carol", "1.2.3.4:7", "unknown user")
	entries := s.AuthLog()
	if len(entries) != 3 {
		t.Fatalf("AuthLog len = %d, want 3", len(entries))
	}
	wantUsers := []string{"alice", "bob", "carol"}
	for i, want := range wantUsers {
		if entries[i].Username != want {
			t.Fatalf("entry %d username = %q, want %q", i, entries[i].Username, want)
		}
	}
	if entries[0].Reason != "invalid password" {
		t.Fatalf("entry 0 Reason = %q", entries[0].Reason)
	}
	if entries[1].Reason != "NOPERM command=GET key=k1" {
		t.Fatalf("entry 1 Reason = %q", entries[1].Reason)
	}
}

// TestACLLogDeniedDoesNotCountAuthFailure guards the counter split: denials are
// counted by IncDenied at the call sites, never by LogDenied.
func TestACLLogDeniedDoesNotCountAuthFailure(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	for i := 0; i < 3; i++ {
		s.LogDenied("bob", "addr", "SET", "k")
	}
	if got := s.AuthFailures(); got != 0 {
		t.Fatalf("AuthFailures = %d, want 0", got)
	}
	if got := s.DeniedCommands(); got != 0 {
		t.Fatalf("DeniedCommands = %d, want 0 (IncDenied owns the counter)", got)
	}
}

// TestACLLogDeniedEviction verifies denials share the buffer's bounded capacity
// with auth failures rather than getting their own.
func TestACLLogDeniedEviction(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	total := DefaultAuthLogCap + 7
	for i := 0; i < total; i++ {
		s.LogDenied("u"+itoa(i), "addr", "GET", "k")
	}
	entries := s.AuthLog()
	if len(entries) != DefaultAuthLogCap {
		t.Fatalf("AuthLog len = %d, want %d", len(entries), DefaultAuthLogCap)
	}
	if entries[0].Username != "u7" {
		t.Fatalf("oldest survivor = %q, want u7", entries[0].Username)
	}
	if entries[len(entries)-1].Username != "u"+itoa(total-1) {
		t.Fatalf("newest entry = %q, want u%d", entries[len(entries)-1].Username, total-1)
	}
}

// TestSeedAuthLog verifies replayed history is restored verbatim, keeping the
// timestamps the records carried rather than the time of the restore.
func TestSeedAuthLog(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	past := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	s.SeedAuthLog([]AuthLogEntry{
		{Timestamp: past, Username: "alice", RemoteAddr: "10.0.0.1:1", Reason: "invalid password"},
		{Timestamp: past.Add(time.Minute), Username: "bob", RemoteAddr: "10.0.0.2:2", Reason: "NOPERM command=set key=k"},
	})
	entries := s.AuthLog()
	if len(entries) != 2 {
		t.Fatalf("AuthLog len = %d, want 2", len(entries))
	}
	if !entries[0].Timestamp.Equal(past) {
		t.Fatalf("entry 0 timestamp = %v, want %v", entries[0].Timestamp, past)
	}
	if entries[0].Username != "alice" || entries[1].Username != "bob" {
		t.Fatalf("entries = %+v, want alice then bob", entries)
	}
	if entries[1].Reason != "NOPERM command=set key=k" {
		t.Fatalf("entry 1 Reason = %q", entries[1].Reason)
	}
}

// TestSeedAuthLogEmpty verifies seeding nothing leaves the buffer untouched, the
// case where no audit history existed to replay.
func TestSeedAuthLogEmpty(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	s.SeedAuthLog(nil)
	if entries := s.AuthLog(); entries != nil {
		t.Fatalf("AuthLog = %v, want nil", entries)
	}
}

// TestSeedAuthLogDoesNotCount verifies restored history does not inflate the
// counters, which report what this process has seen.
func TestSeedAuthLogDoesNotCount(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	s.SeedAuthLog([]AuthLogEntry{
		{Timestamp: time.Now(), Username: "alice", Reason: "invalid password"},
	})
	if got := s.AuthFailures(); got != 0 {
		t.Fatalf("AuthFailures = %d, want 0", got)
	}
}

// TestSeedAuthLogEviction verifies seeded entries obey the buffer's capacity
// instead of getting an exemption from it.
func TestSeedAuthLogEviction(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	total := DefaultAuthLogCap + 7
	seed := make([]AuthLogEntry, 0, total)
	for i := 0; i < total; i++ {
		seed = append(seed, AuthLogEntry{Timestamp: time.Now(), Username: "u" + itoa(i), Reason: "invalid password"})
	}
	s.SeedAuthLog(seed)
	entries := s.AuthLog()
	if len(entries) != DefaultAuthLogCap {
		t.Fatalf("AuthLog len = %d, want %d", len(entries), DefaultAuthLogCap)
	}
	if entries[0].Username != "u7" {
		t.Fatalf("oldest survivor = %q, want u7", entries[0].Username)
	}
}

// TestSeedAuthLogThenLive verifies live events append after restored history
// rather than replacing it.
func TestSeedAuthLogThenLive(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	s.SeedAuthLog([]AuthLogEntry{
		{Timestamp: time.Now().Add(-time.Hour), Username: "restored", Reason: "invalid password"},
	})
	s.LogAuthFailure("live", "10.0.0.9:9", "unknown user")
	entries := s.AuthLog()
	if len(entries) != 2 {
		t.Fatalf("AuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Username != "restored" || entries[1].Username != "live" {
		t.Fatalf("entries = %+v, want restored then live", entries)
	}
}

// TestACLLogDeniedConcurrent exercises interleaved denial and auth-failure
// writes; run with -race to prove appendLocked stays serialized.
func TestACLLogDeniedConcurrent(t *testing.T) {
	s := NewStore(&PolicyStore{}, log.NewNoOpLogger())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if g%2 == 0 {
					s.LogDenied("bob", "1.2.3.4:5", "SET", "k")
				} else {
					s.LogAuthFailure("alice", "1.2.3.4:5", "invalid password")
				}
				_ = s.AuthLog()
			}
		}(g)
	}
	wg.Wait()
	entries := s.AuthLog()
	if len(entries) != DefaultAuthLogCap {
		t.Fatalf("AuthLog len = %d, want %d", len(entries), DefaultAuthLogCap)
	}
	for i, e := range entries {
		if strings.TrimSpace(e.Username) == "" || e.Reason == "" {
			t.Fatalf("entry %d has empty fields: %+v", i, e)
		}
	}
}
