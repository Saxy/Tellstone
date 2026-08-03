/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: role.go
Description: ROLE command family (CREATE, SETUSER, DELUSER, DELETE, LIST, GETUSER). Mutations
delegate to the rbac.Store helpers, which clone the active snapshot and republish it with one
atomic swap, so readers always observe a complete policy. Reply building is RESP2 only; the
binary protocol carries its own wire encoding.

Authors:

	Maximilian Hagen
*/
package resp

import (
	"sort"

	"github.com/Saxy/Tellstone/internal/rbac"
)

// role dispatches ROLE subcommands. Connections need the ROLE command
// permission (granted by the "admin" category) to reach this handler; the
// check happens in dispatch.
func (s *Server) role(st *connState, args [][]byte, out []byte) []byte {
	if s.policy == nil {
		return AppendError(out, "ERR RBAC is not enabled")
	}
	if len(args) < 2 {
		return AppendError(out, "ERR wrong number of arguments for 'role' command")
	}
	switch {
	case EqualFold(args[1], "CREATE"):
		return s.roleCreate(args, out)
	case EqualFold(args[1], "SETUSER"):
		return s.roleSetUser(args, out)
	case EqualFold(args[1], "DELUSER"):
		return s.roleDelUser(args, out)
	case EqualFold(args[1], "DELETE"):
		return s.roleDelete(args, out)
	case EqualFold(args[1], "LIST"):
		return s.roleList(args, out)
	case EqualFold(args[1], "GETUSER"):
		return s.roleGetUser(args, out)
	default:
		return AppendError(out, "ERR unknown role subcommand '"+string(args[1])+"'")
	}
}

// roleCreate implements ROLE CREATE <name> <rule>... Rules use the
// Redis-style tokens +cmd / +@category / -cmd / -@category / ~prefix. Fails
// when the role already exists — updating a role is DELETE then CREATE.
func (s *Server) roleCreate(args [][]byte, out []byte) []byte {
	if len(args) < 4 {
		return AppendError(out, "ERR wrong number of arguments for 'role|create' command")
	}
	name := string(args[2])
	rules := make([]string, 0, len(args)-3)
	for _, r := range args[3:] {
		rules = append(rules, string(r))
	}
	if err := s.policy.CreateRole(name, rules); err != nil {
		return AppendError(out, "ERR "+err.Error())
	}
	return append(out, respOK...)
}

// roleSetUser implements ROLE SETUSER <username> <role> [>password] [nopass].
// At least one password option is required: an omitted option would silently
// create a nopass user, so the operator must write nopass explicitly. The last
// password option wins; nopass clears the hash (passwordless user).
func (s *Server) roleSetUser(args [][]byte, out []byte) []byte {
	if len(args) < 4 {
		return AppendError(out, "ERR wrong number of arguments for 'role|setuser' command")
	}
	if len(args) == 4 {
		return AppendError(out, "ERR role|setuser requires a '>password' or 'nopass' option")
	}
	return s.setUser(args, out)
}

// setUser applies the SETUSER tail shared by ROLE SETUSER and ACL SETUSER:
// parse the password options and bind the user to the named role, replying OK
// on success or an error otherwise. The arg-count checks stay in the callers so
// each family reports its own command name in the error message.
func (s *Server) setUser(args [][]byte, out []byte) []byte {
	passHash, err := rbac.PasswordFromOpts(args[4:])
	if err != nil {
		return AppendError(out, "ERR "+err.Error())
	}
	if err := s.policy.SetUser(string(args[2]), string(args[3]), passHash); err != nil {
		return AppendError(out, "ERR "+err.Error())
	}
	return append(out, respOK...)
}

// roleDelUser implements ROLE DELUSER <username>.
func (s *Server) roleDelUser(args [][]byte, out []byte) []byte {
	if len(args) != 3 {
		return AppendError(out, "ERR wrong number of arguments for 'role|deluser' command")
	}
	if err := s.policy.DelUser(string(args[2])); err != nil {
		return AppendError(out, "ERR "+err.Error())
	}
	return append(out, respOK...)
}

// roleDelete implements ROLE DELETE <role>. Users referencing the deleted role
// fall back to the default role on their next policy lookup (fail-safe).
func (s *Server) roleDelete(args [][]byte, out []byte) []byte {
	if len(args) != 3 {
		return AppendError(out, "ERR wrong number of arguments for 'role|delete' command")
	}
	if err := s.policy.DeleteRole(string(args[2])); err != nil {
		return AppendError(out, "ERR "+err.Error())
	}
	return append(out, respOK...)
}

// roleList implements ROLE LIST, returning for each role its name, granted
// commands, and namespace whitelist. Names and commands are sorted for
// deterministic output. Password hashes are never rendered.
func (s *Server) roleList(args [][]byte, out []byte) []byte {
	if len(args) != 2 {
		return AppendError(out, "ERR wrong number of arguments for 'role|list' command")
	}
	p := s.policy.Load()
	if p == nil {
		return AppendError(out, "ERR policy not loaded")
	}
	names := make([]string, 0, len(p.Roles))
	for name := range p.Roles {
		names = append(names, name)
	}
	sort.Strings(names)
	out = AppendArray(out, len(names))
	for _, name := range names {
		r := p.Roles[name]
		out = AppendBulk(out, []byte(name))
		out = appendRoleCommands(out, r)
		out = appendRoleNamespaces(out, r)
	}
	return out
}

// roleGetUser implements ROLE GETUSER <username>, returning the username, its
// assigned role (or null when the default role applies), and whether a
// password is set. The hash itself is never exposed.
func (s *Server) roleGetUser(args [][]byte, out []byte) []byte {
	if len(args) != 3 {
		return AppendError(out, "ERR wrong number of arguments for 'role|getuser' command")
	}
	username := string(args[2])
	p := s.policy.Load()
	if p == nil {
		return AppendError(out, "ERR policy not loaded")
	}
	u := p.UserFor(username)
	if u == nil {
		return AppendError(out, "ERR user '"+username+"' does not exist")
	}
	hasPass := 0
	if len(u.PasswordHash) > 0 {
		hasPass = 1
	}
	out = AppendArray(out, 3)
	out = AppendBulk(out, []byte(username))
	if u.Role == "" {
		out = AppendNullBulk(out)
	} else {
		out = AppendBulk(out, []byte(u.Role))
	}
	return AppendInt(out, int64(hasPass))
}

// appendRoleCommands renders the role's granted commands as a RESP array.
func appendRoleCommands(out []byte, r *rbac.Role) []byte {
	names := r.GrantedCommands()
	out = AppendArray(out, len(names))
	for _, name := range names {
		out = AppendBulk(out, []byte(name))
	}
	return out
}

// appendRoleNamespaces renders the role's namespace whitelist as a RESP array.
// An empty array means every key is allowed.
func appendRoleNamespaces(out []byte, r *rbac.Role) []byte {
	out = AppendArray(out, len(r.Namespaces))
	for _, ns := range r.Namespaces {
		out = AppendBulk(out, ns)
	}
	return out
}
