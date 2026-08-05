/*
Package rbac
Tellstone Role-Based Access Control Tests
File: manager_test.go
Description: Verifies the Store mutation helpers backing ROLE CREATE/SETUSER/DELUSER/DELETE:
validation against the active snapshot, atomic clone-and-swap semantics, and that mutations are
visible to later Load calls while never mutating the previously published snapshot.
*/
package rbac

import (
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

// recordingLogger captures every Log call so tests can assert on the exact set
// of events emitted (or not emitted) by the Store mutation helpers.
type recordingLogger struct {
	msgs []string
}

func (r *recordingLogger) Enabled(level log.Level) bool { return true }

func (r *recordingLogger) Log(level log.Level, msg string, fields ...log.Field) {
	r.msgs = append(r.msgs, msg)
}

func TestStoreCreateRole(t *testing.T) {
	store := NewStore(&PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}, log.NewNoOpLogger())
	if err := store.CreateRole("reader", []string{"+@read", "~*"}); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if err := store.CreateRole("reader", []string{"+@write"}); err == nil {
		t.Fatal("duplicate role must be rejected")
	}
	if err := store.CreateRole("bad", []string{"+BOGUS"}); err == nil {
		t.Fatal("invalid rules must be rejected")
	}
	if r := store.Load().Roles["reader"]; r == nil || !r.Permissions.Has(CmdGet) {
		t.Fatal("created role must be visible with its permissions")
	}
}

func TestStoreSetUserValidatesRole(t *testing.T) {
	store := NewStore(&PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}, log.NewNoOpLogger())
	_ = store.CreateRole("reader", []string{"+@read"})
	if err := store.SetUser("alice", "reader", []byte("hash")); err != nil {
		t.Fatalf("SetUser: %v", err)
	}
	if err := store.SetUser("bob", "ghost", nil); err == nil {
		t.Fatal("SetUser must reject an unknown role")
	}
	u := store.Load().UserFor("alice")
	if u == nil || u.Role != "reader" || string(u.PasswordHash) != "hash" {
		t.Fatalf("SetUser did not persist user record: %+v", u)
	}
}

func TestStoreDelUserAndDeleteRole(t *testing.T) {
	store := NewStore(&PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}, log.NewNoOpLogger())
	_ = store.CreateRole("r", []string{"+GET"})
	_ = store.SetUser("alice", "r", nil)

	if err := store.DeleteRole("ghost"); err == nil {
		t.Fatal("deleting a missing role must fail")
	}
	if err := store.DeleteRole("r"); err != nil {
		t.Fatalf("DeleteRole: %v", err)
	}
	if _, ok := store.Load().Roles["r"]; ok {
		t.Fatal("deleted role must be gone")
	}
	// alice's only role was deleted and the policy has no Default: her next
	// lookup must resolve to nil (deny-all), never to a stale role.
	if r := store.Load().RoleFor("alice"); r != nil {
		t.Fatalf("user referencing a deleted role must resolve to the nil default, got %q", r.Name)
	}

	if err := store.DelUser("alice"); err != nil {
		t.Fatalf("DelUser: %v", err)
	}
	if store.Load().UserFor("alice") != nil {
		t.Fatal("deleted user must be gone")
	}
}

func TestStoreDelUserAbsentUserLogsNothing(t *testing.T) {
	rec := &recordingLogger{}
	store := NewStore(&PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}, rec)
	snapshot := store.Load()

	if err := store.DelUser("ghost"); err != nil {
		t.Fatalf("DelUser on an absent user must succeed as a no-op: %v", err)
	}
	// No republish: Load must return the exact snapshot published before the call.
	if store.Load() != snapshot {
		t.Fatal("absent-user DelUser must not republish the policy")
	}
	for _, msg := range rec.msgs {
		if msg == "rbac: user deleted" {
			t.Fatal("absent-user DelUser must not emit a deletion event")
		}
	}

	// Contrast: deleting an existing user still republishes and logs, so the
	// early return above only short-circuits the genuinely absent case.
	rec2 := &recordingLogger{}
	role, err := ParseRole("r", "+GET")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	store2 := NewStore(&PolicyStore{
		Roles: map[string]*Role{"r": role},
		Users: map[string]*User{"alice": {Role: "r"}},
	}, rec2)
	if err := store2.DelUser("alice"); err != nil {
		t.Fatalf("DelUser on an existing user: %v", err)
	}
	if store2.Load().UserFor("alice") != nil {
		t.Fatal("existing user must be deleted")
	}
	seen := false
	for _, msg := range rec2.msgs {
		if msg == "rbac: user deleted" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("deleting an existing user must still emit the deletion event")
	}
}

func TestStoreDelUserLastAdminGuard(t *testing.T) {
	admin, err := ParseRole("admin", "+@admin", "~*")
	if err != nil {
		t.Fatalf("parse admin role: %v", err)
	}
	limited, err := ParseRole("limited", "+get", "~*")
	if err != nil {
		t.Fatalf("parse limited role: %v", err)
	}
	store := NewStore(&PolicyStore{
		Roles: map[string]*Role{"admin": admin, "limited": limited},
		Users: map[string]*User{
			"admin":   {Role: "admin"},
			"alice":   {Role: "limited"},
			"limited": {Role: "limited"},
		},
		Default: limited,
	}, log.NewNoOpLogger())

	// Deleting the last user whose effective role grants CmdACL is rejected.
	if err := store.DelUser("admin"); err == nil {
		t.Fatal("deleting the last admin user must be rejected")
	}
	if store.Load().UserFor("admin") == nil {
		t.Fatal("rejected deletion must not mutate the store")
	}

	// A non-admin user, even the last one, may always be deleted.
	if err := store.DelUser("alice"); err != nil {
		t.Fatalf("deleting a non-admin user: %v", err)
	}
	// With a second admin present, deleting one is allowed.
	if err := store.SetUser("nopass", "admin", nil); err != nil {
		t.Fatalf("SetUser: %v", err)
	}
	if err := store.DelUser("admin"); err != nil {
		t.Fatalf("deleting one of two admins: %v", err)
	}
	// Deleting an unknown user stays a no-op success.
	if err := store.DelUser("ghost"); err != nil {
		t.Fatalf("deleting an unknown user: %v", err)
	}
}

func TestStoreDeleteRoleClearsDefault(t *testing.T) {
	role, err := ParseRole("r", "+GET")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	store := NewStore(&PolicyStore{
		Roles:   map[string]*Role{"r": role},
		Users:   map[string]*User{"alice": {Role: "r"}},
		Default: role,
	}, log.NewNoOpLogger())
	if err := store.DeleteRole("r"); err != nil {
		t.Fatalf("DeleteRole: %v", err)
	}
	p := store.Load()
	if p.Default != nil {
		t.Fatalf("Default must be cleared when it refers to the deleted role, got %q", p.Default.Name)
	}
	if r := p.RoleFor("alice"); r != nil {
		t.Fatalf("user with only the deleted role must resolve to nil (deny-all), got %q", r.Name)
	}
}

func TestStoreMutationsDoNotMutatePublishedSnapshot(t *testing.T) {
	store := NewStore(&PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}, log.NewNoOpLogger())
	_ = store.CreateRole("r", []string{"+GET"})
	snapshot := store.Load()
	_ = store.CreateRole("s", []string{"+SET"})
	if _, ok := snapshot.Roles["s"]; ok {
		t.Fatal("the previously loaded snapshot must be unaffected by a later mutation")
	}
}

func TestPolicyStoreClone(t *testing.T) {
	role, err := ParseRole("r", "+GET")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	p := &PolicyStore{
		Roles:   map[string]*Role{"r": role},
		Users:   map[string]*User{"a": {Role: "r"}},
		Default: role,
	}
	cp := p.Clone()
	cp.Roles["x"] = &Role{Name: "x"}
	cp.Users["b"] = &User{Role: "x"}
	if _, ok := p.Roles["x"]; ok {
		t.Fatal("cloned maps must not alias the source")
	}
	if _, ok := p.Users["b"]; ok {
		t.Fatal("cloned user map must not alias the source")
	}
	if cp.Roles["r"] != role || cp.Default != role {
		t.Fatal("clone must share immutable role values")
	}
	if (*PolicyStore)(nil).Clone() != nil {
		t.Fatal("nil store must clone to nil")
	}
}

func TestPolicyStoreNoPassDefault(t *testing.T) {
	empty := &PolicyStore{Roles: map[string]*Role{}, Users: map[string]*User{}}
	if empty.NoPassDefault() {
		t.Fatal("no users must not be nopass-default")
	}
	withHash := &PolicyStore{Users: map[string]*User{"default": {Role: "x", PasswordHash: []byte("h")}}}
	if withHash.NoPassDefault() {
		t.Fatal("a default user with a password must not be nopass")
	}
	noPass := &PolicyStore{Users: map[string]*User{"default": {Role: "x"}}}
	if !noPass.NoPassDefault() {
		t.Fatal("a default user without a password must be nopass")
	}
	if (*PolicyStore)(nil).NoPassDefault() {
		t.Fatal("nil store must not be nopass-default")
	}
}
