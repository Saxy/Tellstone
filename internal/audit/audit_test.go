/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: audit_test.go
Description: Verifies EventType definitions, eventSet filtering, parseEventTypes
semantics, and the LogEngine record/close lifecycle.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"bytes"
	"encoding/json"
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
	s := parseEventTypes("")
	if !s.has(EventAuthSuccess) || !s.has(EventAuthFailure) || !s.has(EventACLDeny) {
		t.Fatal("empty flag should yield default event set (auth_success, auth_failure, acl_deny)")
	}
	if s.has(EventConnect) || s.has(EventDisconnect) || s.has(EventCommand) {
		t.Fatal("empty flag should not include connect, disconnect, or command")
	}
}

func TestParseEventTypesAuthShorthand(t *testing.T) {
	s := parseEventTypes("auth")
	if !s.has(EventAuthSuccess) || !s.has(EventAuthFailure) {
		t.Fatal("'auth' shorthand should enable both auth_success and auth_failure")
	}
	if s.has(EventACLDeny) {
		t.Fatal("'auth' shorthand should not enable acl_deny")
	}
}

func TestParseEventTypesACLShorthand(t *testing.T) {
	s := parseEventTypes("acl")
	if !s.has(EventACLDeny) {
		t.Fatal("'acl' shorthand should enable acl_deny")
	}
	if s.has(EventAuthSuccess) {
		t.Fatal("'acl' shorthand should not enable auth_success")
	}
}

func TestParseEventTypesAll(t *testing.T) {
	s := parseEventTypes("all")
	for _, et := range allEvents {
		if !s.has(et) {
			t.Fatalf("'all' should enable every event type, missing %s", et)
		}
	}
}

func TestParseEventTypesExactTokens(t *testing.T) {
	s := parseEventTypes("auth_success,auth_failure,acl_deny,connect,disconnect,command")
	for _, et := range allEvents {
		if !s.has(et) {
			t.Fatalf("explicit token list should enable %s", et)
		}
	}
}

func TestParseEventTypesUnknownIgnored(t *testing.T) {
	s := parseEventTypes("auth_success,bogus,acl_deny")
	if !s.has(EventAuthSuccess) || !s.has(EventACLDeny) {
		t.Fatal("known tokens should still be enabled when unknowns are present")
	}
	if s.has(EventAuthFailure) {
		t.Fatal("auth_failure should not be enabled when not explicitly listed")
	}
}

func TestParseEventTypesWhitespace(t *testing.T) {
	s := parseEventTypes(" auth_success , acl_deny ")
	if !s.has(EventAuthSuccess) || !s.has(EventACLDeny) {
		t.Fatal("whitespace around tokens should be trimmed")
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

func TestDisabledEngineNoop(t *testing.T) {
	var buf bytes.Buffer
	filter := parseEventTypes("all")
	e := NewLogEngine(false, filter, &buf)
	e.Record(EventAuthSuccess, "should not appear")
	if buf.Len() != 0 {
		t.Fatalf("disabled engine produced output: %s", buf.String())
	}
	if err := e.Close(); err != nil {
		t.Fatal("disabled engine Close should return nil")
	}
}

func TestRecordWritesJSON(t *testing.T) {
	var buf bytes.Buffer
	filter := parseEventTypes("auth_success")
	e := NewLogEngine(true, filter, &buf)

	e.Record(EventAuthSuccess, "user logged in",
		log.String("user", "alice"),
		log.String("remote_addr", "10.0.0.1:54321"),
		log.String("protocol", "resp"),
	)

	line := strings.TrimSpace(buf.String())
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
	var buf bytes.Buffer
	filter := parseEventTypes("auth_success")
	e := NewLogEngine(true, filter, &buf)

	e.Record(EventACLDeny, "should be filtered")
	if buf.Len() != 0 {
		t.Fatalf("filtered event produced output: %s", buf.String())
	}
}

func TestRecordMultipleEvents(t *testing.T) {
	var buf bytes.Buffer
	filter := parseEventTypes("auth_success,acl_deny")
	e := NewLogEngine(true, filter, &buf)

	e.Record(EventAuthSuccess, "login", log.String("user", "alice"))
	e.Record(EventACLDeny, "denied", log.String("user", "bob"))
	e.Record(EventConnect, "should be filtered")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %s", len(lines), buf.String())
	}
}

func TestCloseWithCloser(t *testing.T) {
	filter := parseEventTypes("all")
	var buf bytes.Buffer
	e := NewLogEngine(true, filter, &buf)
	if err := e.Close(); err != nil {
		t.Fatalf("Close on bytes.Buffer should not error: %v", err)
	}
}
