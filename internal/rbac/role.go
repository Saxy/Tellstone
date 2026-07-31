/*
Package rbac
Tellstone Role-Based Access Control
File: role.go
Description: Defines the Role type: a named bundle of command permissions and key-prefix whitelist rules.
Roles are flat in v1 — no inheritance, every permission is explicit.

Authors:

	Maximilian Hagen
*/
package rbac

import "bytes"

// Role is a named permission set. Permissions is a Bitset of granted commands
// (zero-alloc to check). Namespaces are a key-prefix whitelist: an empty list
// means every key is allowed, any list means only matching prefixes are
// allowed (default-deny). An explicit "~*" rule is equivalent to an empty
// list, so no wildcard flag is needed.
type Role struct {
	Name        string
	Permissions Bitset
	Namespaces  [][]byte
}

// AllowsKey reports whether a key matches the role's namespace whitelist.
// Default-deny: if any namespace rule exists, only matching prefixes pass.
func (r *Role) AllowsKey(key []byte) bool {
	for _, prefix := range r.Namespaces {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return len(r.Namespaces) == 0
}
