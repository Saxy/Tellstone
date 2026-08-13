/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: audit_test.go
Description: Verifies EventType definitions, eventSet filtering, ParseEventTypes
semantics, and the LogEngine record/close lifecycle.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

func TestEventSetNilReceiver(t *testing.T) {
	var s *eventSet
	if s.has(EventAuthSuccess) {
		t.Fatal("nil eventSet.Has should return false")
	}
}

func TestParseEventTypesEmpty(t *testing.T) {
	s := ParseEventTypes("")
	if !s.has(EventAuthSuccess) || !s.has(EventAuthFailure) || !s.has(EventACLDeny) {
		t.Fatal("empty flag should yield default event set (auth_success, auth_failure, acl_deny)")
	}
	if s.has(EventConnect) || s.has(EventDisconnect) || s.has(EventCommand) {
		t.Fatal("empty flag should not include connect, disconnect, or command")
	}
}

func TestParseEventTypesAuthShorthand(t *testing.T) {
	s := ParseEventTypes("auth")
	if !s.has(EventAuthSuccess) || !s.has(EventAuthFailure) {
		t.Fatal("'auth' shorthand should enable both auth_success and auth_failure")
	}
	if s.has(EventACLDeny) {
		t.Fatal("'auth' shorthand should not enable acl_deny")
	}
}

func TestParseEventTypesACLShorthand(t *testing.T) {
	s := ParseEventTypes("acl")
	if !s.has(EventACLDeny) {
		t.Fatal("'acl' shorthand should enable acl_deny")
	}
	if s.has(EventAuthSuccess) {
		t.Fatal("'acl' shorthand should not enable auth_success")
	}
}

func TestParseEventTypesAll(t *testing.T) {
	s := ParseEventTypes("all")
	for _, et := range allEvents {
		if !s.has(et) {
			t.Fatalf("'all' should enable every event type, missing %s", et)
		}
	}
}

func TestParseEventTypesExactTokens(t *testing.T) {
	s := ParseEventTypes("auth_success,auth_failure,acl_deny,connect,disconnect,command")
	for _, et := range allEvents {
		if !s.has(et) {
			t.Fatalf("explicit token list should enable %s", et)
		}
	}
}

func TestParseEventTypesUnknownIgnored(t *testing.T) {
	s := ParseEventTypes("auth_success,bogus,acl_deny")
	if !s.has(EventAuthSuccess) || !s.has(EventACLDeny) {
		t.Fatal("known tokens should still be enabled when unknowns are present")
	}
	if s.has(EventAuthFailure) {
		t.Fatal("auth_failure should not be enabled when not explicitly listed")
	}
}

func TestParseEventTypesWhitespace(t *testing.T) {
	s := ParseEventTypes(" auth_success , acl_deny ")
	if !s.has(EventAuthSuccess) || !s.has(EventACLDeny) {
		t.Fatal("whitespace around tokens should be trimmed")
	}
}

func TestParseEventTypesWhitespaceOnlyAppliesDefaults(t *testing.T) {
	for _, raw := range []string{"   ", "\t\n", " \t "} {
		s := ParseEventTypes(raw)
		if s.count == 0 {
			t.Fatalf("whitespace-only filter %q must apply defaults, got an empty set", raw)
		}
		for _, d := range defaultEventTypes {
			if !s.has(d) {
				t.Fatalf("whitespace-only filter %q must enable default event %q", raw, d)
			}
		}
	}
}

func TestEventTypesAreDistinct(t *testing.T) {
	seen := make(map[EventType]bool, len(allEvents))
	for _, et := range allEvents {
		if seen[et] {
			t.Fatalf("duplicate event type constant: %s", et)
		}
		seen[et] = true
	}
}

func TestDefaultEventTypesExist(t *testing.T) {
	for _, et := range defaultEventTypes {
		found := false
		for _, a := range allEvents {
			if a == et {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("DefaultEventTypes contains %s which is not in allEvents", et)
		}
	}
}

// --- Engine tests ---

// newTestEngine wires a disabled-crypto engine and a no-op logger, mirroring
// how the server constructs an audit engine without encryption enabled.
func newTestEngine(t *testing.T, enabled bool, filter *eventSet, dir string) *LogEngine {
	t.Helper()
	e, err := NewLogEngine(enabled, filter, dir, log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// readAuditFiles returns the contents of every audit file in dir. It locates
// them the way replay does, so a rename of the suffix moves this helper with the
// code instead of failing it; the name itself is pinned by
// TestFileNameContainsTimestampHashAndMarker.
func readAuditFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, auditFileGlob))
	if err != nil {
		t.Fatal("Glob:", err)
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		var data []byte
		data, err = os.ReadFile(m)
		if err != nil {
			t.Fatal("ReadFile:", err)
		}
		out = append(out, string(data))
	}
	return out
}

func TestDisabledEngineNoop(t *testing.T) {
	filter := ParseEventTypes("all")
	e := newTestEngine(t, false, filter, "stdout")
	e.Record(EventAuthSuccess, "should not appear")
	if err := e.Close(); err != nil {
		t.Fatal("disabled engine Close should return nil")
	}
}

func TestRecordWritesJSON(t *testing.T) {
	dir := t.TempDir()
	filter := ParseEventTypes("auth_success")
	e := newTestEngine(t, true, filter, dir)

	e.Record(EventAuthSuccess, "user logged in",
		log.String("user", "alice"),
		log.String("remote_addr", "10.0.0.1:54321"),
		log.String("protocol", "resp"),
	)

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	files := readAuditFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected one audit file, got %d", len(files))
	}

	line := strings.TrimSpace(files[0])
	if line == "" {
		t.Fatal("expected JSON output, got empty")
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
	}
	if m["level"] != "AUDIT" {
		t.Fatalf("level = %v, want AUDIT", m["level"])
	}
	if m["event"] != "auth_success" {
		t.Fatalf("event = %v, want auth_success", m["event"])
	}
	if m["user"] != "alice" {
		t.Fatalf("user = %v, want alice", m["user"])
	}
	if m["remote_addr"] != "10.0.0.1:54321" {
		t.Fatalf("remote_addr = %v", m["remote_addr"])
	}
	if m["time"] == nil || m["time"] == "" {
		t.Fatal("time field missing or empty")
	}
}

func TestRecordFiltersOutDisabledEvents(t *testing.T) {
	dir := t.TempDir()
	filter := ParseEventTypes("auth_success")
	e := newTestEngine(t, true, filter, dir)

	e.Record(EventACLDeny, "should be filtered")
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	files := readAuditFiles(t, dir)
	if len(files) != 1 || len(files[0]) != 0 {
		t.Fatalf("filtered event produced output: %v", files)
	}
}

func TestRecordMultipleEvents(t *testing.T) {
	dir := t.TempDir()
	filter := ParseEventTypes("auth_success,acl_deny")
	e := newTestEngine(t, true, filter, dir)

	e.Record(EventAuthSuccess, "login", log.String("user", "alice"))
	e.Record(EventACLDeny, "denied", log.String("user", "bob"))
	e.Record(EventConnect, "should be filtered")

	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	files := readAuditFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected one audit file, got %d", len(files))
	}
	lines := strings.Split(strings.TrimSpace(files[0]), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %s", len(lines), files[0])
	}
}

func TestCloseWithCloser(t *testing.T) {
	filter := ParseEventTypes("all")
	e := newTestEngine(t, true, filter, t.TempDir())
	if err := e.Close(); err != nil {
		t.Fatalf("Close on a file-backed engine should not error: %v", err)
	}
}

// failingWriter rejects every write with a per-write error, so a test can tell
// which attempt produced the failure.
type failingWriter struct {
	writes int
}

var errAuditWrite = errors.New("audit: write failed")

func (f *failingWriter) Write(p []byte) (int, error) {
	f.writes++
	return 0, fmt.Errorf("%w: attempt %d", errAuditWrite, f.writes)
}

func TestRecordRetainsFirstWriteError(t *testing.T) {
	fw := &failingWriter{}
	e := &LogEngine{
		enabled: true,
		filter:  ParseEventTypes("all"),
		writer:  fw,
		enc:     json.NewEncoder(fw),
	}

	e.Record(EventAuthSuccess, "first")
	e.Record(EventAuthSuccess, "second")
	e.Record(EventACLDeny, "third")

	err := e.Close()
	if err == nil {
		t.Fatal("Close must report the retained write error")
	}
	if !errors.Is(err, errAuditWrite) {
		t.Fatalf("Close error must wrap the write failure, got %v", err)
	}
	// Attempt 1 failed first; the later attempts must not replace it, so the
	// reported error is the original one, not the last write's.
	if !strings.Contains(err.Error(), "attempt 1") {
		t.Fatalf("Close must report the first write failure, got %v", err)
	}
	if fw.writes != 1 {
		t.Fatalf("expected one write attempt before the sink was marked broken, got %d", fw.writes)
	}
}
