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

import (
	"sync"
	"sync/atomic"
)

// PolicyStore is an immutable snapshot of the authorization state: role
// definitions, user records (role + password hash), and the default role for
// users without an explicit assignment. It is replaced wholesale via
// Store.Store, never mutated in place.
type PolicyStore struct {
	Roles   map[string]*Role // role name → role
	Users   map[string]*User // username → user
	Default *Role            // fallback for users without an explicit role
}

// RoleFor resolves the effective role for a username: the user's explicit
// assignment, falling back to Default. Returns nil when no role applies —
// callers treat nil as deny-all (fail-closed).
func (p *PolicyStore) RoleFor(username string) *Role {
	if p == nil {
		return nil
	}
	if u, ok := p.Users[username]; ok {
		if r, ok := p.Roles[u.Role]; ok {
			return r
		}
	}
	return p.Default
}

// UserFor returns the user record for username, or nil when the user does not
// exist. The AUTH path uses it to fetch the password hash before verification.
func (p *PolicyStore) UserFor(username string) *User {
	if p == nil {
		return nil
	}
	return p.Users[username]
}

// NoPassDefault reports whether the built-in "default" user exists without a
// password. When it does, connections may start authenticated and inherit the
// default user's effective role (Redis semantics); otherwise they must AUTH.
func (p *PolicyStore) NoPassDefault() bool {
	if p == nil {
		return false
	}
	u, ok := p.Users["default"]
	return ok && len(u.PasswordHash) == 0
}

// Clone returns a shallow copy of p with fresh maps, safe to mutate before
// republishing via Store. Role and User values are shared — they are immutable
// once built. ROLE mutations are the only callers, so allocations here are fine.
func (p *PolicyStore) Clone() *PolicyStore {
	if p == nil {
		return nil
	}
	np := *p
	np.Roles = make(map[string]*Role, len(p.Roles))
	for name, r := range p.Roles {
		np.Roles[name] = r
	}
	np.Users = make(map[string]*User, len(p.Users))
	for name, u := range p.Users {
		np.Users[name] = u
	}
	return &np
}

// Store publishes the active PolicyStore behind an atomic pointer. Load never
// blocks and never allocates; Store swaps the whole snapshot in one operation,
// so readers always observe a complete policy. mu serializes the read-modify-
// write mutations (CreateRole, SetUser, DelUser, DeleteRole) so two concurrent
// ROLE commands cannot overwrite each other's changes; Load remains lock-free.
type Store struct {
	active atomic.Pointer[PolicyStore]
	mu     sync.Mutex
	// authFailures counts rejected AUTH attempts, deniedCommands counts
	// authorization-denied command attempts. Unlike the per-role command
	// counter, both are store-wide: a denial can happen without a pinned role
	// (fail-closed deny-all). The request path only bumps them atomically.
	authFailures   uint64
	deniedCommands uint64
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

// Reload publishes a complete replacement snapshot (e.g. from a SIGHUP policy
// file reload) serialized against concurrent ROLE mutations. Publishing under
// mu means a reload can never land between a mutation's clone and its
// republish, which would silently discard the other operation; it either lands
// before the mutation or after it. mu is acquired here, so the caller must not
// hold it and must not call the locking mutation helpers (CreateRole, SetUser,
// DelUser, DeleteRole) while it is held.
func (s *Store) Reload(policy *PolicyStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Store(policy)
}
