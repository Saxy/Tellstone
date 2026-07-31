/*
Package rbac
Tellstone Role-Based Access Control Tests
File: parser_test.go
Description: Verifies ROLE CREATE rule parsing: category expansion, deny-overrides-allow, wildcard namespaces, and error handling for unknown or malformed rules.
*/
package rbac

import "testing"

func TestParseRoleCategoryExpansion(t *testing.T) {
	r, err := ParseRole("readonly", "+@read", "-SET", "~users:*")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	if !r.Permissions.Has(CmdGet) || !r.Permissions.Has(CmdInfo) {
		t.Error("read category commands should be granted")
	}
	if r.Permissions.Has(CmdSet) {
		t.Error("SET must not be granted by the read category")
	}
	if !r.AllowsKey([]byte("users:1")) {
		t.Error("key matching ~users:* must be allowed")
	}
	if r.AllowsKey([]byte("cache:1")) {
		t.Error("key outside the whitelist must be denied")
	}
}

func TestParseRoleDenyOverridesAllowRegardlessOfOrder(t *testing.T) {
	a, err := ParseRole("a", "+@read", "-INFO")
	if err != nil {
		t.Fatalf("ParseRole a: %v", err)
	}
	b, err := ParseRole("b", "-INFO", "+@read")
	if err != nil {
		t.Fatalf("ParseRole b: %v", err)
	}
	for name, r := range map[string]*Role{"a": a, "b": b} {
		if !r.Permissions.Has(CmdGet) {
			t.Errorf("%s: GET should be granted", name)
		}
		if r.Permissions.Has(CmdInfo) {
			t.Errorf("%s: explicit deny must override allow regardless of order", name)
		}
	}
}

func TestParseRoleWildcardNamespaces(t *testing.T) {
	r, err := ParseRole("allkeys", "+GET", "~*")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	if len(r.Namespaces) != 0 {
		t.Fatalf("~* must produce an empty whitelist, got %d rules", len(r.Namespaces))
	}
	if !r.AllowsKey([]byte("anything:123")) {
		t.Fatal("~* must allow every key")
	}
}

func TestParseRoleCompositeCategoryIncludesAuth(t *testing.T) {
	r, err := ParseRole("rw", "+@readwrite")
	if err != nil {
		t.Fatalf("ParseRole: %v", err)
	}
	if !r.Permissions.Has(CmdAuth) {
		t.Fatal("readwrite category must include AUTH so the role can log in")
	}
}

func TestParseRoleRejectsBareTilde(t *testing.T) {
	if _, err := ParseRole("x", "~cache:", "~"); err == nil {
		t.Fatal("bare ~ rule must be rejected, got no error")
	}
}

func TestParseRoleErrors(t *testing.T) {
	cases := []struct {
		name  string
		rules []string
	}{
		{"no rules", nil},
		{"unknown command", []string{"+BOGUS"}},
		{"unknown category", []string{"+@bogus"}},
		{"malformed rule", []string{"nonsense"}},
	}
	for _, tc := range cases {
		if _, err := ParseRole("x", tc.rules...); err == nil {
			t.Errorf("%s: expected an error, got none", tc.name)
		}
	}
}
