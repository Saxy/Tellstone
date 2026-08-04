/*
Package rbac
Tellstone Role-Based Access Control Tests
File: policy_test.go
Description: Verifies user→role resolution (explicit, default, fail-closed) and atomic policy store swap semantics.
*/
package rbac

import (
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

func TestPolicyStoreRoleFor(t *testing.T) {
	readonly, err := ParseRole("readonly", "+@read")
	if err != nil {
		t.Fatalf("ParseRole readonly: %v", err)
	}
	defaultRole, err := ParseRole("default", "+PING")
	if err != nil {
		t.Fatalf("ParseRole default: %v", err)
	}
	p := &PolicyStore{
		Roles:   map[string]*Role{"readonly": readonly, "default": defaultRole},
		Users:   map[string]*User{"alice": {Role: "readonly"}},
		Default: defaultRole,
	}
	if got := p.RoleFor("alice"); got != readonly {
		t.Error("explicit user assignment must win")
	}
	if got := p.RoleFor("unknown"); got != defaultRole {
		t.Error("unassigned user must fall back to the default role")
	}
	if got := (*PolicyStore)(nil).RoleFor("alice"); got != nil {
		t.Error("nil store must resolve to no role (fail-closed)")
	}
}

func TestStoreAtomicSwap(t *testing.T) {
	v1, err := ParseRole("v1", "+GET")
	if err != nil {
		t.Fatalf("ParseRole v1: %v", err)
	}
	v2, err := ParseRole("v2", "+SET")
	if err != nil {
		t.Fatalf("ParseRole v2: %v", err)
	}
	store := NewStore(&PolicyStore{Roles: map[string]*Role{"v1": v1}}, log.NewNoOpLogger())
	if got := store.Load().Roles["v1"]; got != v1 {
		t.Fatal("initial policy must be visible")
	}
	store.Store(&PolicyStore{Roles: map[string]*Role{"v2": v2}})
	got := store.Load()
	if _, ok := got.Roles["v1"]; ok {
		t.Error("old role must be gone after the swap")
	}
	if got.Roles["v2"] != v2 {
		t.Error("new policy must be visible after the swap")
	}
}
