/*
Package rbac
Tellstone Role-Based Access Control
File: metrics.go
Description: Authorization counters backing the /metrics endpoint: per-role executed commands plus
store-wide auth failures and denied attempts. The request path only bumps atomics — no locks, no
allocations — and the map-building scrape accessors are the only allocation points.

Authors:

	Maximilian Hagen
*/
package rbac

import "sync/atomic"

// IncCommands records one data command executed by a session pinned to this
// role. Roles are otherwise immutable once built, so a lone atomic bump needs
// no lock. This is the hot-path counter behind tellstone_rbac_commands_total.
func (r *Role) IncCommands() { atomic.AddUint64(&r.commands, 1) }

// Commands returns the role's executed-command count. Scrape-time only.
func (r *Role) Commands() uint64 { return atomic.LoadUint64(&r.commands) }

// CountCommand records one executed command against the session's pinned role.
// No-op for deny-all sessions (no role pinned). Zero allocations.
func (s *SessionContext) CountCommand() {
	if s.role != nil {
		s.role.IncCommands()
	}
}

// IncAuthFailure records a rejected AUTH attempt. The protocol layers call it
// only when RBAC is enabled, so the counter is always meaningful.
func (s *Store) IncAuthFailure() { atomic.AddUint64(&s.authFailures, 1) }

// AuthFailures returns the store-wide failed-AUTH counter. Scrape-time only.
func (s *Store) AuthFailures() uint64 { return atomic.LoadUint64(&s.authFailures) }

// IncDenied records one authorization-denied command attempt (NOPERM).
func (s *Store) IncDenied() { atomic.AddUint64(&s.deniedCommands, 1) }

// DeniedCommands returns the store-wide denial counter. Scrape-time only.
func (s *Store) DeniedCommands() uint64 { return atomic.LoadUint64(&s.deniedCommands) }

// RoleCommandCounts snapshots the executed-command counter of every role in the
// active policy, keyed by role name. Scrape-time only (the map allocation is
// fine here); the request path never touches it.
//
// The counters live on the immutable Role value a session pins at AUTH time, so
// a SIGHUP reload that swaps in freshly parsed Role values resets every
// per-role count: continuity across reloads is not supported because live
// sessions keep counting against their pinned (pre-reload) role, which is no
// longer read after the swap. Runtime ROLE CREATE does not reset anything —
// Clone shares the existing Role values, so their counters carry over.
func (s *Store) RoleCommandCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	if p := s.Load(); p != nil {
		for name, r := range p.Roles {
			counts[name] = r.Commands()
		}
	}
	return counts
}
