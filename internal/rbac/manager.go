/*
Package rbac
Tellstone Role-Based Access Control
File: manager.go
Description: Store mutation helpers backing the ROLE command family. Each helper validates
against the active snapshot, clones it, mutates the copy, and republishes with one atomic
swap — the published policy is never modified in place, so readers always see a complete state.

Authors:

	Maximilian Hagen
*/
package rbac

import "fmt"

// CreateRole adds a role built from rule tokens (+GET, +@read, ~prefix).
// Fails when the role already exists or the rules are invalid. Existing
// sessions pin the role value they authenticated with, so a swapped-in role
// only affects future connections and fresh policy lookups.
func (s *Store) CreateRole(name string, rules []string) error {
	role, err := ParseRole(name, rules...)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Load()
	if p != nil {
		if _, ok := p.Roles[name]; ok {
			return fmt.Errorf("rbac: role %q already exists", name)
		}
		p = p.Clone()
	} else {
		p = &PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}
	}
	p.Roles[name] = role
	s.Store(p)
	return nil
}

// SetUser assigns username to roleName with the given bcrypt password hash
// (nil for nopass). Fails when the role does not exist, so a user can never
// reference a dangling role name.
func (s *Store) SetUser(username, roleName string, passHash []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Load()
	if p != nil {
		p = p.Clone()
	} else {
		p = &PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}
	}
	// Validate against the initialized snapshot so a fresh store rejects an
	// unknown role just like an existing policy does — a user must never
	// reference a dangling role name.
	if _, ok := p.Roles[roleName]; !ok {
		return fmt.Errorf("rbac: unknown role %q", roleName)
	}
	p.Users[username] = &User{Role: roleName, PasswordHash: passHash}
	s.Store(p)
	return nil
}

// DelUser removes username. Deleting the only "default" nopass user forces
// future connections to authenticate. It rejects deleting the last user whose
// effective role grants the ACL command bit: runtime ACL changes are never
// written back to the policy file, so losing the final administrator would
// lock every client out of ACL management until a restart or SIGHUP reload
// restores one. Pinned sessions keep working either way; this only closes the
// re-administration path.
func (s *Store) DelUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Load()
	if p == nil {
		return nil
	}
	if _, ok := p.Users[username]; ok {
		if r := p.RoleFor(username); r != nil && r.Permissions.Has(CmdACL) && !p.hasOtherAdmin(username) {
			return fmt.Errorf("rbac: cannot delete %q: last user with ACL management rights", username)
		}
	}
	p = p.Clone()
	delete(p.Users, username)
	s.Store(p)
	return nil
}

// hasOtherAdmin reports whether any user other than username has an effective
// role — explicit or the Default fallback — granting the ACL command bit.
func (p *PolicyStore) hasOtherAdmin(username string) bool {
	for name := range p.Users {
		if name == username {
			continue
		}
		if r := p.RoleFor(name); r != nil && r.Permissions.Has(CmdACL) {
			return true
		}
	}
	return false
}

// DeleteRole removes a role. Users referencing the removed role fall back to
// the default role on their next policy lookup (fail-safe); already-pinned
// sessions keep the role they authenticated with.
func (s *Store) DeleteRole(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Load()
	if p == nil {
		return fmt.Errorf("rbac: role %q does not exist", name)
	}
	if _, ok := p.Roles[name]; !ok {
		return fmt.Errorf("rbac: role %q does not exist", name)
	}
	p = p.Clone()
	// If the deleted role is also the default fallback, drop the pointer too:
	// otherwise RoleFor keeps resolving unassigned users to a role that no
	// longer exists in p.Roles, silently failing open.
	if p.Default == p.Roles[name] {
		p.Default = nil
	}
	delete(p.Roles, name)
	s.Store(p)
	return nil
}
