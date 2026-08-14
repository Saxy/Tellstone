/*
Package rbac
Tellstone Role-Based Access Control
File: cmd.go
Description: Defines the command ID catalog and bulk permission categories.
Categories expand into a Bitset at policy load time so role creation is the only allocation point.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"sort"
	"strings"
)

// Command IDs map to bit positions in a Bitset (word = id/64, offset = id%64).
// IDs are stable wire values: never renumber, never reuse, only append.
const (
	CmdGet uint16 = iota + 1
	CmdSet
	CmdDel
	CmdPing
	CmdCommand
	CmdAuth
	CmdRole
	CmdACL
	CmdInfo
	CmdFlush
	CmdShutdown
	CmdConfig
	CmdDebug
	CmdMonitor
	CmdUser
	CmdGrant
	CmdRevoke
)

// AllCommands lists every registered command in ascending ID order. The "all"
// category is derived from it, so a newly added command is covered automatically.
var AllCommands = []uint16{
	CmdGet, CmdSet, CmdDel, CmdPing, CmdCommand, CmdAuth, CmdRole, CmdACL,
	CmdInfo, CmdFlush, CmdShutdown, CmdConfig, CmdDebug, CmdMonitor,
	CmdUser, CmdGrant, CmdRevoke,
}

// commandNames maps command names to their IDs for the rule parser. RESP
// command names are matched case-insensitively; stored uppercase.
var commandNames = map[string]uint16{
	"GET": CmdGet, "SET": CmdSet, "DEL": CmdDel, "PING": CmdPing,
	"COMMAND": CmdCommand, "AUTH": CmdAuth, "ROLE": CmdRole, "ACL": CmdACL,
	"INFO": CmdInfo, "FLUSH": CmdFlush, "SHUTDOWN": CmdShutdown,
	"CONFIG": CmdConfig, "DEBUG": CmdDebug, "MONITOR": CmdMonitor,
	"USER": CmdUser, "GRANT": CmdGrant, "REVOKE": CmdRevoke,
}

// LookupCommand resolves a command name to its ID. Names are case-insensitive.
func LookupCommand(name string) (uint16, bool) {
	id, ok := commandNames[strings.ToUpper(name)]
	return id, ok
}

// CommandName returns the canonical uppercase name for a command ID, or ""
// when id is not registered. Used for ROLE LIST output, not on the hot path.
func CommandName(id uint16) string {
	for name, cmd := range commandNames {
		if cmd == id {
			return name
		}
	}
	return ""
}

// categories are bulk grants expanded into a Bitset at role creation time.
// They split the command surface into three planes:
//
//   - login: handshake commands (AUTH, PING, COMMAND). The dispatcher
//     special-cases these on unauthenticated connections, so they never deadlock
//     a deny-all role; the category governs the same commands after authentication.
//   - read/write: minimal data-plane grants, kept small so composites own the
//     connection commands.
//   - maintenance/admin: ops vs identity management, kept separate so an
//     operator who may FLUSH cannot manage users.
//
// readwrite and operator are composite grants for the common user shapes.
var categories = map[string][]uint16{
	"login":       {CmdAuth, CmdPing, CmdCommand},
	"read":        {CmdGet, CmdInfo},
	"write":       {CmdSet, CmdDel},
	"readwrite":   {CmdGet, CmdSet, CmdDel, CmdPing, CmdCommand, CmdInfo, CmdAuth},
	"operator":    {CmdGet, CmdSet, CmdDel, CmdPing, CmdCommand, CmdInfo, CmdAuth, CmdFlush},
	"maintenance": {CmdFlush, CmdShutdown, CmdConfig, CmdDebug, CmdMonitor},
	"admin":       {CmdAuth, CmdRole, CmdACL, CmdUser, CmdGrant, CmdRevoke},
	"all":         append([]uint16(nil), AllCommands...),
	"none":        {},
}

// Category returns the commands granted by the named category, or nil if the
// category is unknown. The returned slice must not be mutated.
func Category(name string) []uint16 {
	return categories[name]
}

// CategoriesForCommand returns the names of the built-in categories that grant
// the command, sorted alphabetically for deterministic output, or nil when id
// is not registered. The implicit "all" grant is reported; the empty "none"
// grant never matches. Used by the RESP COMMAND family so introspection stays
// in step with the authorization model instead of duplicating it.
func CategoriesForCommand(id uint16) []string {
	var names []string
	for name, ids := range categories {
		if name == "none" {
			continue
		}
		for _, cid := range ids {
			if cid == id {
				names = append(names, name)
				break
			}
		}
	}
	sort.Strings(names)
	return names
}
