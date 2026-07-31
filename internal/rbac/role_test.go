/*
Package rbac
Tellstone Role-Based Access Control Tests
File: role_test.go
Description: Verifies role namespace whitelist semantics: empty rules allow all keys, any rule means default-deny, and prefixes match raw key bytes.
*/
package rbac

import "testing"

func TestRoleAllowsKeyNoNamespaces(t *testing.T) {
	r := &Role{Name: "allkeys"}
	if !r.AllowsKey([]byte("anything:123")) {
		t.Fatal("role without namespace rules must allow every key")
	}
}

func TestRoleAllowsKeyWhitelist(t *testing.T) {
	r := &Role{
		Name:       "cache-manager",
		Namespaces: [][]byte{[]byte("cache:"), []byte("session:")},
	}
	for _, key := range []string{"cache:abc", "session:xyz"} {
		if !r.AllowsKey([]byte(key)) {
			t.Errorf("key %q must be allowed by whitelist", key)
		}
	}
	for _, key := range []string{"users:123", "config:timeout", ""} {
		if r.AllowsKey([]byte(key)) {
			t.Errorf("key %q must be denied by default-deny whitelist", key)
		}
	}
}
