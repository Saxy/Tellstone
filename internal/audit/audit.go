/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: audit.go
Description: Audit event type definitions and the per-event filter used by the audit
engine. Every security-relevant operation maps to exactly one EventType. The filter set
is parsed once from the --audit-events flag at startup and consulted per Record() call;
when the engine is nil (audit disabled), no filtering or allocation occurs.

Authors:

	Maximilian Hagen
*/
package audit

import "strings"

// EventType identifies a class of security-relevant operations that the audit
// engine may record. Each constant maps 1:1 to a string emitted in the JSON
// output and accepted by the --audit-events flag.
type EventType string

const (
	// EventConnect fires when a new TCP connection is accepted by either
	// the binary or RESP listener. Fields: remote_addr, protocol, shard_id.
	EventConnect EventType = "connect"

	// EventDisconnect fires when a connection is closed. Fields: remote_addr,
	// duration, bytes_read, bytes_written.
	EventDisconnect EventType = "disconnect"

	// EventAuthSuccess fires on a successful AUTH handshake. Fields: user,
	// remote_addr, protocol.
	EventAuthSuccess EventType = "auth_success"

	// EventAuthFailure fires on a failed AUTH attempt — wrong password,
	// unknown user, or missing credentials. Fields: user, remote_addr,
	// reason.
	EventAuthFailure EventType = "auth_failure"

	// EventACLDeny fires when RBAC rejects a command (NOPERM). Fields: user,
	// command, key, remote_addr.
	EventACLDeny EventType = "acl_deny"

	// EventCommand fires for every dispatched data and admin command
	// (GET, SET, DEL, PING, ROLE, ACL, FLUSH, …). Fields: command, key,
	// user, remote_addr, and for GET: result (hit/miss). Enabling this
	// event type adds per-command overhead on the dispatch path.
	EventCommand EventType = "command"
)

// allEvents is the complete set of recognized event types. Used by
// ParseEventTypes to validate user input and by the default set builder.
var allEvents = [6]EventType{
	EventConnect,
	EventDisconnect,
	EventAuthSuccess,
	EventAuthFailure,
	EventACLDeny,
	EventCommand,
}

// defaultEventTypes is the event filter applied when --audit-events is not
// set. It includes the security-relevant events that compliance frameworks
// require (auth + acl) without the high-volume connect/disconnect and
// command events that operators opt into explicitly.
var defaultEventTypes = []EventType{EventAuthSuccess, EventAuthFailure, EventACLDeny}

// eventSet is a compact lookup structure for the per-event filter. A nil
// *eventSet means "log nothing" (audit disabled). An empty non-nil set
// means "log everything" (future extension; currently unused).
type eventSet struct {
	events [len(allEvents)]bool
	count  int
}

// has reports whether eventType is in the set. The nil receiver is a
// deliberate no-op: callers on the hot path check e == nil before calling,
// so this method is only reached when audit is enabled.
func (s *eventSet) has(eventType EventType) bool {
	if s == nil {
		return false
	}
	for i, t := range allEvents {
		if t == eventType {
			return s.events[i]
		}
	}
	return false
}

// ParseEventTypes builds an eventSet from a comma-separated flag value
// (e.g. "auth_success,auth_failure,acl_deny" or "auth,acl,command").
// Unrecognized tokens are silently ignored, so the flag stays forward-compatible.
// An empty string yields a set containing only defaultEventTypes.
func ParseEventTypes(raw string) *eventSet {
	s := &eventSet{}
	// Trim before the emptiness check so a whitespace-only value (e.g.
	// --audit-events "  ") applies the defaults instead of silently yielding
	// a filter that disables every event.
	raw = strings.TrimSpace(raw)
	tokens := strings.Split(raw, ",")
	if len(tokens) == 1 && tokens[0] == "" {
		// Empty flag value — apply defaults.
		for _, t := range defaultEventTypes {
			s.enable(t)
		}
		return s
	}
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		switch token {
		case "auth":
			s.enable(EventAuthSuccess)
			s.enable(EventAuthFailure)
		case "acl":
			s.enable(EventACLDeny)
		case "connect":
			s.enable(EventConnect)
		case "disconnect":
			s.enable(EventDisconnect)
		case "command":
			s.enable(EventCommand)
		case "all":
			for _, t := range allEvents {
				s.enable(t)
			}
		default:
			for _, t := range allEvents {
				if token == string(t) {
					s.enable(t)
					break
				}
			}
		}
	}
	return s
}

func (s *eventSet) enable(t EventType) {
	for i, et := range allEvents {
		if et == t && !s.events[i] {
			s.events[i] = true
			s.count++
			return
		}
	}
}
