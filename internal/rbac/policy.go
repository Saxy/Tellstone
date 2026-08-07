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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Saxy/Tellstone/internal/log"
)

// PolicyStore is an immutable snapshot of the authorization state: role
// definitions, user records (role + password hash), the default role for
// users without an explicit assignment, and the compiled OAuth claim-to-role
// rules. It is replaced wholesale via Store.Store, never mutated in place.
type PolicyStore struct {
	Roles   map[string]*Role // role name → role
	Users   map[string]*User // username → user
	Default *Role            // fallback for users without an explicit role
	// OAuthRules maps token claims to roles, applied in policy-file order.
	// Compiled at load time (unknown target roles fail the load) so the
	// resolver on the AUTH path only compares strings.
	OAuthRules []oauthRule
}

// oauthRule is one compiled claim-to-role mapping. A rule matches when the
// named claim is present and any of its values satisfies the match pattern.
// Exactly one of exact, prefix, or suffix describes the pattern; a bare "*"
// leaves all three empty, which matches any value.
type oauthRule struct {
	claim  string
	exact  string
	prefix string
	suffix string
	role   *Role
}

// parseMatch compiles a match pattern. Only exact values and leading or
// trailing "*" globs are allowed; a middle wildcard is ambiguous and rejected
// at load time, so a typo surfaces before it silently denies access.
func parseMatch(match string) (exact, prefix, suffix string, err error) {
	switch {
	case strings.Count(match, "*") == 0:
		return match, "", "", nil
	case strings.HasPrefix(match, "*"):
		return "", "", strings.TrimPrefix(match, "*"), nil
	case strings.HasSuffix(match, "*"):
		return "", strings.TrimSuffix(match, "*"), "", nil
	default:
		return "", "", "", fmt.Errorf("rbac: oauth rule match %q: only a leading or trailing * is allowed", match)
	}
}

// matches reports whether any of the claim's values satisfies the pattern.
// A bare "*" (all three fields empty) matches a claim with any value.
func (r *oauthRule) matches(values []string) bool {
	for _, v := range values {
		if r.exact != "" && v == r.exact {
			return true
		}
		if r.prefix != "" && strings.HasPrefix(v, r.prefix) {
			return true
		}
		if r.suffix != "" && strings.HasSuffix(v, r.suffix) {
			return true
		}
		if r.exact == "" && r.prefix == "" && r.suffix == "" {
			return true
		}
	}
	return false
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

// RoleForClaims resolves the effective role for a verified token's claims,
// applying oauth.rules in policy-file order. The first match wins; a claim set
// matching no rule returns nil, which callers treat as deny-all. Oauth.Claims
// is a map[string][]string, so the resolver is written against the plain map,
// and rbac stays independent of the oauth package.
func (p *PolicyStore) RoleForClaims(claims map[string][]string) *Role {
	if p == nil {
		return nil
	}
	for i := range p.OAuthRules {
		r := &p.OAuthRules[i]
		if values, ok := claims[r.claim]; ok && r.matches(values) {
			return r.role
		}
	}
	return nil
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
	// logger reports policy mutations (CreateRole, SetUser, DelUser, DeleteRole)
	// so operators see admin changes in the server log. Set via NewStore; nil
	// falls back to the no-op logger, so RBAC never depends on logging being on.
	logger log.Logger
	// authFailures counts rejected AUTH attempts, deniedCommands counts
	// authorization-denied command attempts. Unlike the per-role command
	// counter, both are store-wide: a denial can happen without a pinned role
	// (fail-closed deny-all). The request path only bumps them atomically.
	authFailures   uint64
	deniedCommands uint64
	// logMu serializes the ACL auth-failure log. The log is mutable state that
	// lives for the Store's lifetime and survives policy Reload swaps; entries
	// are recorded off the hot path at failed AUTH time.
	logMu   sync.Mutex
	log     []AuthLogEntry // circular buffer, oldest at (logHead-logLen) mod cap
	logHead int            // next write slot
	logLen  int            // number of valid entries
}

// NewStore returns a Store seeded with policy. A nil logger falls back to the
// no-op logger, so RBAC keeps working with logging disabled.
func NewStore(policy *PolicyStore, logger log.Logger) *Store {
	s := new(Store)
	s.active.Store(policy)
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	s.logger = logger
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

// ResolveOAuthToken verifies a bearer token through verify and maps its claims
// to a role via oauth.rules, returning the pinned session and the subject it
// was built from. (nil, "") means the token failed verification or matched no
// rule — the AUTH workers in the RESP and binary listeners treat that as a deny
// (fail-closed). verify is a closure rather than an oauth.Provider so this
// store stays independent of the oauth package, matching RoleForClaims's
// map-based signature.
func (s *Store) ResolveOAuthToken(verify func() (map[string][]string, error)) (*SessionContext, string) {
	if s == nil {
		return nil, ""
	}
	claims, err := verify()
	if err != nil {
		return nil, ""
	}
	p := s.Load()
	if p == nil {
		return nil, ""
	}
	role := p.RoleForClaims(claims)
	if role == nil {
		return nil, ""
	}
	subject := "default"
	if subs, ok := claims["sub"]; ok && len(subs) > 0 && subs[0] != "" {
		subject = subs[0]
	}
	return NewSessionContext(subject, role), subject
}
