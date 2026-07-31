/*
Package rbac
Tellstone Role-Based Access Control
File: parser.go
Description: Parses ROLE CREATE rule tokens (+cmd, +@category, -cmd, -@category, ~prefix) into a Role.
Categories expand here at build time, so the authorization hot path never re-expands them.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"fmt"
	"strings"
)

// ParseRole builds a Role from ROLE CREATE rule tokens. Rules are read
// left-to-right, but deny rules always win over grants regardless of position,
// matching Redis ACL precedence. At least one rule is required.
//
//	+<cmd>        grant a command
//	+@<category>  grant a command category
//	-<cmd>        deny a command
//	-@<category>  deny a command category
//	~<prefix>     allow keys with the given prefix (whitelist)
//	~*            allow all keys (equivalent to no prefix rules)
func ParseRole(name string, rules ...string) (*Role, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("rbac: role %q requires at least one rule", name)
	}
	r := &Role{Name: name}
	perms := NewBitset(nil)
	var denies []uint16
	for _, rule := range rules {
		switch {
		case strings.HasPrefix(rule, "+@"):
			ids, err := categoryIDs(rule[2:])
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				perms.Set(id)
			}
		case strings.HasPrefix(rule, "-@"):
			ids, err := categoryIDs(rule[2:])
			if err != nil {
				return nil, err
			}
			denies = append(denies, ids...)
		case strings.HasPrefix(rule, "+"):
			id, ok := LookupCommand(rule[1:])
			if !ok {
				return nil, fmt.Errorf("rbac: unknown command %q in role %q", rule[1:], name)
			}
			perms.Set(id)
		case strings.HasPrefix(rule, "-"):
			id, ok := LookupCommand(rule[1:])
			if !ok {
				return nil, fmt.Errorf("rbac: unknown command %q in role %q", rule[1:], name)
			}
			denies = append(denies, id)
		case strings.HasPrefix(rule, "~"):
			// Matching is literal prefix matching (HasPrefix), but the wire
			// convention is Redis-style "~users:*". Strip the trailing glob
			// star so "~users:*" stores the prefix "users." "~*" maps to an
			// empty whitelist (all keys), so nothing is added. A bare "~"
			// would store an empty prefix that matches every key, silently
			// defeating the default-deny whitelist — reject it.
			if rule != "~*" {
				prefix := strings.TrimSuffix(rule[1:], "*")
				if prefix == "" {
					return nil, fmt.Errorf("rbac: malformed rule %q in role %q", rule, name)
				}
				r.Namespaces = append(r.Namespaces, []byte(prefix))
			}
		default:
			return nil, fmt.Errorf("rbac: malformed rule %q in role %q", rule, name)
		}
	}
	for _, id := range denies {
		perms.Clear(id)
	}
	r.Permissions = perms
	return r, nil
}

// categoryIDs expands a category name into its command IDs.
func categoryIDs(name string) ([]uint16, error) {
	ids := Category(strings.ToLower(name))
	if ids == nil {
		return nil, fmt.Errorf("rbac: unknown category %q", name)
	}
	return ids, nil
}
