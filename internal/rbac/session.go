/*
Package rbac
Tellstone Role-Based Access Control
File: session.go
Description: SessionContext, the per-connection authorization state pinned at handshake.
IsAllowed is the zero-allocation hot-path check: one bit test plus whitelist prefix matching.

Authors:

	Maximilian Hagen
*/
package rbac

// SessionContext is the authorization state pinned to one connection at
// handshake time. It references the resolved Role — roles are immutable once
// built, and policy updates replace the whole store, so a pinned session is
// unaffected by hot-swaps. Fail-closed: a session with no role denies
// everything.
type SessionContext struct {
	role     *Role
	RoleName string
	Username string
}

// NewSessionContext pins the role to a connection. A nil role yields a deny-all
// session (fail-closed); the caller decides whether that means dropping the
// connection or letting it use only the default role.
func NewSessionContext(username string, role *Role) *SessionContext {
	sc := &SessionContext{Username: username, role: role}
	if role != nil {
		sc.RoleName = role.Name
	}
	return sc
}

// AllowsCommand reports whether the session may run cmd, ignoring key scope.
// Used for keyless commands (ROLE, PING, AUTH) where the namespace whitelist
// must not be consulted. Single bit test, no allocations.
func (s *SessionContext) AllowsCommand(cmd uint16) bool {
	if s.role == nil {
		return false
	}
	return s.role.Permissions.Has(cmd)
}

// IsAllowed reports whether the session may run cmd on a key. The command check
// is a single bit test; the namespace check iterates the role's prefix
// whitelist over the raw key bytes. No allocations, no locks.
func (s *SessionContext) IsAllowed(cmd uint16, key []byte) bool {
	if !s.AllowsCommand(cmd) {
		return false
	}
	return s.role.AllowsKey(key)
}
