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
// future connections to authenticate.
func (s *Store) DelUser(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Load()
	if p == nil {
		return
	}
	p = p.Clone()
	delete(p.Users, username)
	s.Store(p)
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
