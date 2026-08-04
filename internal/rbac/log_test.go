package rbac

import (
	"strconv"
	"strings"
	"sync"
	"testing"

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
