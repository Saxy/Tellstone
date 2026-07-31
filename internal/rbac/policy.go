/*
Package rbac
Tellstone Role-Based Access Control
File: policy.go
Description: PolicyStore, the immutable authorization snapshot, and Store, the atomic hot-swap holder.
Readers Load a snapshot non-blocking; writers build a complete replacement and swap it in one operation.

Authors:

	Maximilian Hagen
*/
package rbac

import "sync/atomic"

// PolicyStore is an immutable snapshot of the authorization state: role
// definitions, user→role assignments, and the default role for users without
// an explicit assignment. It is replaced wholesale via Store.Store, never
// mutated in place.
type PolicyStore struct {
	Roles   map[string]*Role  // role name → role
	Users   map[string]string // username → role name
	Default *Role             // fallback for users without an explicit role
}

// RoleFor resolves the effective role for a username: the user's explicit
// assignment, falling back to Default. Returns nil when no role applies —
// callers treat nil as deny-all (fail-closed).
func (p *PolicyStore) RoleFor(username string) *Role {
	if p == nil {
		return nil
	}
	if name, ok := p.Users[username]; ok {
		var r *Role
		if r, ok = p.Roles[name]; ok {
			return r
		}
	}
	return p.Default
}

// Store publishes the active PolicyStore behind an atomic pointer. Load never
// blocks and never allocates; Store swaps the whole snapshot in one operation,
// so readers always observe a complete policy.
type Store struct {
	active atomic.Pointer[PolicyStore]
}

// NewStore returns a Store seeded with policy.
func NewStore(policy *PolicyStore) *Store {
	s := new(Store)
	s.active.Store(policy)
	return s
}

// Load returns the current policy snapshot, or nil if none was ever stored.
func (s *Store) Load() *PolicyStore {
	return s.active.Load()
}

// Store atomically publishes policy for future Load calls.
func (s *Store) Store(policy *PolicyStore) {
	s.active.Store(policy)
}
