/*
Package rbac
Tellstone Role-Based Access Control Tests
File: session_test.go
Description: Verifies the SessionContext hot path: command + namespace checks, fail-closed behavior, and that a pinned session is unaffected by policy hot-swaps.
*/
package rbac

import "testing"

func TestSessionContextIsAllowed(t *testing.T) {
	role, err := ParseRole("readonly", "+@read")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	sc := NewSessionContext("alice", role)
	if !sc.IsAllowed(CmdGet, []byte("k")) {
		t.Error("GET should be allowed")
	}
	if sc.IsAllowed(CmdSet, []byte("k")) {
		t.Error("SET must be denied for a read-only session")
	}
	if sc.RoleName != "readonly" || sc.Username != "alice" {
		t.Fatalf("session identity not pinned: role=%q user=%q", sc.RoleName, sc.Username)
	}
}

func TestSessionContextNamespaceDeny(t *testing.T) {
	role, err := ParseRole("cache-manager", "+@write", "~cache:")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	sc := NewSessionContext("bob", role)
	if !sc.IsAllowed(CmdSet, []byte("cache:1")) {
		t.Error("key inside the whitelist must be allowed")
	}
	if sc.IsAllowed(CmdSet, []byte("users:1")) {
		t.Error("key outside the whitelist must be denied even with the command granted")
	}
}

func TestSessionContextFailClosedWithoutRole(t *testing.T) {
	sc := NewSessionContext("mallory", nil)
	if sc.IsAllowed(CmdGet, []byte("k")) {
		t.Fatal("a session with no role must deny every command")
	}
}

func TestSessionPinnedAcrossPolicySwap(t *testing.T) {
	oldRole, err := ParseRole("old", "+@read")
	if err != nil {
		t.Fatalf("ParseRole old: %v", err)
	}
	newRole, err := ParseRole("new", "+@all")
	if err != nil {
		t.Fatalf("ParseRole new: %v", err)
	}
	store := NewStore(&PolicyStore{
		Roles: map[string]*Role{"old": oldRole, "new": newRole},
		Users: map[string]*User{"alice": {Role: "old"}},
	})

	session := NewSessionContext("alice", store.Load().RoleFor("alice"))
	store.Store(&PolicyStore{
		Roles: map[string]*Role{"new": newRole},
		Users: map[string]*User{"alice": {Role: "new"}},
	})

	if !session.IsAllowed(CmdGet, []byte("k")) {
		t.Error("pinned session must keep its old read permission after the swap")
	}
	if session.IsAllowed(CmdShutdown, []byte("k")) {
		t.Error("pinned session must not gain the new role's permissions")
	}
	fresh := store.Load().RoleFor("alice")
	if fresh == nil || fresh.Name != "new" {
		t.Fatal("new lookups must resolve to the swapped-in policy")
	}
}
