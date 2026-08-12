/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: acl.go
Description: ACL command family (SETUSER, DELUSER, LIST, LOG) — the Redis-flavored management
alias over the RBAC policy store. Users bind to a role plus password options exactly like
ROLE SETUSER; LIST renders each user with its role's command and namespace permissions but
never a password hash; LOG returns the recent auth-failure buffer. Reply building is RESP2
only; the binary protocol carries its own wire encoding.

Authors:

	Maximilian Hagen
*/
package resp

import (
	"sort"
	"time"

	"github.com/Saxy/Tellstone/internal/rbac"
)

// acl dispatches ACL subcommands. Connections need the ACL command permission
// (granted by the "admin" category) to reach this handler; the check happens
// in dispatch.
func (s *Server) acl(st *connState, args [][]byte, out []byte) []byte {
	if s.policy == nil {
		return AppendError(out, "ERR RBAC is not enabled")
	}
	if len(args) < 2 {
		return AppendError(out, "ERR wrong number of arguments for 'acl' command")
	}
	switch {
	case EqualFold(args[1], "SETUSER"):
		return s.aclSetUser(args, out)
	case EqualFold(args[1], "DELUSER"):
		return s.aclDelUser(args, out)
	case EqualFold(args[1], "LIST"):
		return s.aclList(args, out)
	case EqualFold(args[1], "LOG"):
		return s.aclLog(args, out)
	default:
		return AppendError(out, "ERR unknown acl subcommand '"+string(args[1])+"'")
	}
}

// aclSetUser implements ACL SETUSER <username> <role> [>password] [nopass].
// Tellstone's RBAC model binds a user to a role, so the permission rules live
// on the role rather than inline on the user as in Redis; the password options
// and the fail-closed unknown-role rejection mirror ROLE SETUSER.
func (s *Server) aclSetUser(args [][]byte, out []byte) []byte {
	if len(args) < 4 {
		return AppendError(out, "ERR wrong number of arguments for 'acl|setuser' command")
	}
	if len(args) == 4 {
		return AppendError(out, "ERR acl|setuser requires a '>password' or 'nopass' option")
	}
	return s.setUser(args, out)
}

// aclDelUser implements ACL DELUSER <username>. Deleting the only "default"
// nopass user forces future connections to authenticate.
func (s *Server) aclDelUser(args [][]byte, out []byte) []byte {
	if len(args) != 3 {
		return AppendError(out, "ERR wrong number of arguments for 'acl|deluser' command")
	}
	if err := s.policy.DelUser(string(args[2])); err != nil {
		return AppendError(out, "ERR "+err.Error())
	}
	return append(out, respOK...)
}

// aclList implements ACL LIST: one entry per user, sorted by name, with the
// username, bound role (null when the default role applies), whether a password
// is set, and the role's granted commands and namespace whitelist. Password
// hashes are never rendered (mirrors ROLE LIST).
func (s *Server) aclList(args [][]byte, out []byte) []byte {
	if len(args) != 2 {
		return AppendError(out, "ERR wrong number of arguments for 'acl|list' command")
	}
	p := s.policy.Load()
	if p == nil {
		return AppendError(out, "ERR policy not loaded")
	}
	names := make([]string, 0, len(p.Users))
	for name := range p.Users {
		names = append(names, name)
	}
	sort.Strings(names)
	out = AppendArray(out, len(names))
	for _, name := range names {
		u := p.Users[name]
		// Effective permissions come from the resolved role — the explicit
		// assignment or the Default role for unassigned users and for users
		// whose role was deleted — so LIST never shows empty grants for a user
		// the default role covers. The role identity field stays u.Role (null
		// for the default-role case) so the assignment is visible.
		r := p.RoleFor(name)
		hasPass := 0
		if len(u.PasswordHash) > 0 {
			hasPass = 1
		}
		out = AppendArray(out, 5)
		out = AppendBulk(out, []byte(name))
		if u.Role == "" {
			out = AppendNullBulk(out)
		} else {
			out = AppendBulk(out, []byte(u.Role))
		}
		out = AppendInt(out, int64(hasPass))
		out = appendACLCommands(out, r)
		out = appendACLNamespaces(out, r)
	}
	return out
}

// aclLog implements ACL LOG: the recent security-event buffer in chronological
// order, each entry a [timestamp, username, remote address, reason] tuple.
// Rejected AUTH attempts carry the failure cause in reason; commands denied by
// the policy carry "NOPERM command=<cmd> key=<key>". Unlike Redis's keyed field
// map the response is a flat array of tuples; the content (who was rejected,
// when, from where, why) is the same.
func (s *Server) aclLog(args [][]byte, out []byte) []byte {
	if len(args) != 2 {
		return AppendError(out, "ERR wrong number of arguments for 'acl|log' command")
	}
	entries := s.policy.AuthLog()
	out = AppendArray(out, len(entries))
	for _, e := range entries {
		out = AppendArray(out, 4)
		out = AppendBulk(out, []byte(e.Timestamp.Format(time.RFC3339)))
		out = AppendBulk(out, []byte(e.Username))
		out = AppendBulk(out, []byte(e.RemoteAddr))
		out = AppendBulk(out, []byte(e.Reason))
	}
	return out
}

// appendACLCommands renders a user's role's granted commands as a RESP array.
// A nil role (no explicit assignment) grants nothing.
func appendACLCommands(out []byte, r *rbac.Role) []byte {
	if r == nil {
		return AppendArray(out, 0)
	}
	names := r.GrantedCommands()
	out = AppendArray(out, len(names))
	for _, name := range names {
		out = AppendBulk(out, []byte(name))
	}
	return out
}

// appendACLNamespaces renders a user's role's namespace whitelist as a RESP
// array. An empty array means every key is allowed.
func appendACLNamespaces(out []byte, r *rbac.Role) []byte {
	if r == nil {
		return AppendArray(out, 0)
	}
	out = AppendArray(out, len(r.Namespaces))
	for _, ns := range r.Namespaces {
		out = AppendBulk(out, ns)
	}
	return out
}
