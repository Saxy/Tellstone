/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: command.go
Description: COMMAND command family (COUNT, DOCS, INFO, LIST, HELP). Introspection
surface for the RESP server: redis-cli and cluster-aware clients probe it during the
handshake to learn the server's capabilities — which commands exist, their arity, and
where the key arguments sit. The table is static because Tellstone's command set is
fixed at build time; subcommands nest under their parent (ROLE, ACL, COMMAND) exactly
like Valkey. Reply building is RESP2 only; the binary protocol carries its own wire
encoding and does not expose this surface.

Authors:

	Maximilian Hagen
*/
package resp

import (
	"sort"
	"strings"

	"github.com/Saxy/Tellstone/internal/rbac"
)

// commandSpec is the static metadata of one command, rendered by COMMAND and
// COMMAND INFO. Arity includes the command name itself and is negative when the
// argument count is a minimum; firstKey/lastKey/step follow the Valkey key
// positions convention (0 = keyless, lastKey -1 = until the end of arguments).
type commandSpec struct {
	name       string   // lowercase wire name
	arity      int      // negative = minimum argument count
	flags      []string // simple-string flags (readonly, write, admin, ...)
	firstKey   int      // position of the first key argument, 0 = keyless
	lastKey    int      // position of the last key, -1 = until the end
	step       int      // increment between successive key arguments
	summary    string   // COMMAND DOCS summary
	since      string   // COMMAND DOCS introduction version
	group      string   // COMMAND DOCS functional group
	complexity string   // COMMAND DOCS time complexity
	subs       []commandSpec
}

// aclCategories maps each command's lowercase wire name to the RBAC categories
// that grant it. It is derived once from the rbac catalog so COMMAND INFO and
// COMMAND LIST FILTERBY ACLCAT report the real authorization model instead of a
// parallel, hand-maintained list that would drift from it. QUIT and STARTTLS
// are not registered commands (dispatch special-cases them and never gates
// them), so they report no categories.
var aclCategories = func() map[string][]string {
	m := make(map[string][]string, len(respCommands))
	for i := range respCommands {
		if id, ok := rbac.LookupCommand(respCommands[i].name); ok {
			m[respCommands[i].name] = rbac.CategoriesForCommand(id)
		}
	}
	return m
}()

// respCommands is the fixed command table of the RESP server, sorted
// alphabetically to match the order Valkey emits. COMMAND COUNT reports the
// number of top-level entries (COMMAND LIST's scope); subcommands nest inside
// their parent's reply and are counted individually by the CLIENT-side tools.
var respCommands = []commandSpec{
	{
		name: "acl", arity: -2,
		flags:   []string{"admin", "noscript", "loading", "stale"},
		summary: "Manage the access control list", since: "1.1.0",
		group: "server", complexity: "O(1)",
		subs: []commandSpec{
			{name: "setuser", arity: -4, summary: "Modify or create a user", since: "1.1.0", group: "server", complexity: "O(1)"},
			{name: "deluser", arity: 3, summary: "Delete a user", since: "1.1.0", group: "server", complexity: "O(1)"},
			{name: "list", arity: 2, summary: "List users and their rules", since: "1.1.0", group: "server", complexity: "O(N)"},
			{name: "log", arity: -2, summary: "Show recent ACL denials", since: "1.1.0", group: "server", complexity: "O(N)"},
		},
	},
	{
		name: "auth", arity: -2,
		flags:   []string{"noscript", "loading", "stale", "fast"},
		summary: "Authenticate to the server", since: "1.0.0",
		group: "connection", complexity: "O(1)",
	},
	{
		name: "command", arity: -1,
		flags:   []string{},
		summary: "Get details about server commands", since: "2.8.13",
		group: "server", complexity: "O(N)",
		subs: []commandSpec{
			{name: "count", arity: 2, summary: "Return the total number of commands in the server", since: "1.2.0", group: "server", complexity: "O(1)"},
			{name: "docs", arity: -2, summary: "Return documentary information about commands", since: "1.2.0", group: "server", complexity: "O(N)"},
			{name: "info", arity: -2, summary: "Return details about multiple commands", since: "1.2.0", group: "server", complexity: "O(N)"},
			{name: "list", arity: -2, summary: "Return a list of the server's command names", since: "1.2.0", group: "server", complexity: "O(N)"},
			{name: "help", arity: 2, summary: "Print this help", since: "1.2.0", group: "server", complexity: "O(1)"},
		},
	},
	{
		name: "del", arity: -2,
		flags:    []string{"write"},
		firstKey: 1, lastKey: -1, step: 1,
		summary: "Delete one or more keys", since: "1.0.0",
		group: "generic", complexity: "O(N)",
	},
	{
		name: "get", arity: 2,
		flags:    []string{"readonly", "fast"},
		firstKey: 1, lastKey: 1, step: 1,
		summary: "Get the value of a key", since: "1.0.0",
		group: "string", complexity: "O(1)",
	},
	{
		name: "ping", arity: -1,
		flags:   []string{"fast"},
		summary: "Ping the server", since: "1.0.0",
		group: "connection", complexity: "O(1)",
	},
	{
		name: "quit", arity: 1,
		flags:   []string{"noscript", "loading", "stale"},
		summary: "Close the connection", since: "1.0.0",
		group: "connection", complexity: "O(1)",
	},
	{
		name: "role", arity: -2,
		flags:   []string{"admin", "noscript", "loading", "stale"},
		summary: "Manage the access control list roles", since: "1.1.0",
		group: "server", complexity: "O(1)",
		subs: []commandSpec{
			{name: "create", arity: -4, summary: "Create a role", since: "1.1.0", group: "server", complexity: "O(1)"},
			{name: "setuser", arity: -4, summary: "Assign a role and password to a user", since: "1.1.0", group: "server", complexity: "O(1)"},
			{name: "deluser", arity: 3, summary: "Delete a user", since: "1.1.0", group: "server", complexity: "O(1)"},
			{name: "delete", arity: 3, summary: "Delete a role", since: "1.1.0", group: "server", complexity: "O(1)"},
			{name: "list", arity: 2, summary: "List roles and their users", since: "1.1.0", group: "server", complexity: "O(N)"},
			{name: "getuser", arity: 3, summary: "Show a user's role", since: "1.1.0", group: "server", complexity: "O(1)"},
		},
	},
	{
		name: "set", arity: -3,
		flags:    []string{"write", "denyoom"},
		firstKey: 1, lastKey: 1, step: 1,
		summary: "Set the string value of a key, ignoring its type. The key is created if it doesn't exist.", since: "1.0.0",
		group: "string", complexity: "O(1)",
	},
	{
		name: "starttls", arity: 1,
		flags:   []string{"noscript", "loading", "stale"},
		summary: "Upgrade the connection to TLS", since: "1.1.0",
		group: "connection", complexity: "O(1)",
	},
}

// command dispatches COMMAND subcommands. Connections need the COMMAND command
// permission (granted by the "login" category) to reach this handler; the
// check happens in dispatch.
func (s *Server) command(st *connState, args [][]byte, out []byte) []byte {
	if len(args) == 1 {
		// Bare COMMAND is an alias of COMMAND INFO with no names: the details
		// of every command, which clients cache for key-position routing.
		return s.commandInfoAll(out)
	}
	switch {
	case EqualFold(args[1], "COUNT"):
		return s.commandCount(args, out)
	case EqualFold(args[1], "DOCS"):
		return s.commandDocs(args, out)
	case EqualFold(args[1], "INFO"):
		return s.commandInfo(args, out)
	case EqualFold(args[1], "LIST"):
		return s.commandList(args, out)
	case EqualFold(args[1], "HELP"):
		return s.commandHelp(args, out)
	default:
		return AppendError(out, "ERR unknown subcommand '"+string(args[1])+"'. Try COMMAND HELP.")
	}
}

// commandCount implements COMMAND COUNT: the number of top-level commands. It
// deliberately matches the number of entries COMMAND (bare) returns.
func (s *Server) commandCount(args [][]byte, out []byte) []byte {
	if len(args) != 2 {
		return AppendError(out, "ERR wrong number of arguments for 'command|count' command")
	}
	return AppendInt(out, int64(len(respCommands)))
}

// commandInfo implements COMMAND INFO [name ...]: one detail array per name,
// null for unknown names. No names means every command.
func (s *Server) commandInfo(args [][]byte, out []byte) []byte {
	if len(args) == 2 {
		return s.commandInfoAll(out)
	}
	out = AppendArray(out, len(args)-2)
	for _, name := range args[2:] {
		if c, parent := findCommand(name); c != nil {
			out = appendCommandInfo(out, parent, c)
		} else {
			out = AppendNullBulk(out)
		}
	}
	return out
}

// commandInfoAll renders every command, mirroring the no-arg COMMAND reply.
func (s *Server) commandInfoAll(out []byte) []byte {
	out = AppendArray(out, len(respCommands))
	for i := range respCommands {
		out = appendCommandInfo(out, "", &respCommands[i])
	}
	return out
}

// commandDocs implements COMMAND DOCS [name ...]: a RESP2 map (flattened array)
// of command name to documentation. Subcommands appear under their parent|sub
// name. Unknown names are omitted, matching Valkey.
func (s *Server) commandDocs(args [][]byte, out []byte) []byte {
	if len(args) == 2 {
		var n int
		for i := range respCommands {
			n += 1 + len(respCommands[i].subs)
		}
		out = AppendArray(out, 2*n)
		for i := range respCommands {
			out = appendCommandDoc(out, "", &respCommands[i])
			for j := range respCommands[i].subs {
				out = appendCommandDoc(out, respCommands[i].name, &respCommands[i].subs[j])
			}
		}
		return out
	}
	var n int
	for _, name := range args[2:] {
		if c, parent := findCommand(name); c != nil {
			n += 2
			if parent == "" {
				n += 2 * len(c.subs)
			}
		}
	}
	out = AppendArray(out, n)
	for _, name := range args[2:] {
		if c, parent := findCommand(name); c != nil {
			out = appendCommandDoc(out, parent, c)
			if parent == "" {
				out = appendCommandDocSubs(out, c)
			}
		}
	}
	return out
}

// commandList implements COMMAND LIST [FILTERBY MODULE m|ACLCAT c|PATTERN p]:
// the sorted command names, optionally filtered. Module filtering always yields
// an empty list because Tellstone loads no modules.
func (s *Server) commandList(args [][]byte, out []byte) []byte {
	names := make([]string, 0, len(respCommands))
	switch {
	case len(args) == 2:
		for i := range respCommands {
			names = append(names, respCommands[i].name)
		}
	case EqualFold(args[2], "FILTERBY"):
		if len(args) != 5 {
			return AppendError(out, "ERR wrong number of arguments for 'command|list' command")
		}
		switch {
		case EqualFold(args[3], "MODULE"):
			// Tellstone has no modules; no name matches.
		case EqualFold(args[3], "ACLCAT"):
			// The category may arrive with or without the '@' prefix
			// (redis-cli emits "@read"); normalize to the bare rbac name.
			cat := string(args[4])
			cat = strings.TrimPrefix(cat, "@")
			cat = strings.ToLower(cat)
			for i := range respCommands {
				if containsFold(aclCategories[respCommands[i].name], cat) {
					names = append(names, respCommands[i].name)
				}
			}
		case EqualFold(args[3], "PATTERN"):
			pat := strings.ToLower(string(args[4]))
			for i := range respCommands {
				if globMatch(pat, respCommands[i].name) {
					names = append(names, respCommands[i].name)
				}
			}
		default:
			return AppendError(out, "ERR unknown command list filter type '"+string(args[3])+"'")
		}
	default:
		return AppendError(out, "ERR syntax error")
	}
	sort.Strings(names)
	out = AppendArray(out, len(names))
	for _, n := range names {
		out = AppendBulk(out, []byte(n))
	}
	return out
}

// commandHelp implements COMMAND HELP: the subcommand reference.
func (s *Server) commandHelp(args [][]byte, out []byte) []byte {
	if len(args) != 2 {
		return AppendError(out, "ERR wrong number of arguments for 'command|help' command")
	}
	lines := []string{
		"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"COUNT",
		"    Return the total number of commands in the server.",
		"DOCS [<command-name> ...]",
		"    Return documentary information about commands.",
		"INFO [<command-name> ...]",
		"    Return details about multiple commands.",
		"LIST [FILTERBY MODULE <module-name> | ACLCAT <category> | PATTERN <pattern>]",
		"    Return a list of the server's command names.",
		"HELP",
		"    Print this help.",
	}
	out = AppendArray(out, len(lines))
	for _, l := range lines {
		out = AppendBulk(out, []byte(l))
	}
	return out
}

// findCommand resolves a possibly case-insensitive command name, or a parent|sub
// name such as "role|create", against the table. parent is empty for a top-level
// match and the parent's lowercase name for a subcommand match, which the
// renderers use to prefix the reply name.
func findCommand(name []byte) (*commandSpec, string) {
	for i := range respCommands {
		if strings.EqualFold(string(name), respCommands[i].name) {
			return &respCommands[i], ""
		}
	}
	if idx := indexByte(name, '|'); idx >= 0 {
		for i := range respCommands {
			if !strings.EqualFold(string(name[:idx]), respCommands[i].name) {
				continue
			}
			for j := range respCommands[i].subs {
				if strings.EqualFold(string(name[idx+1:]), respCommands[i].subs[j].name) {
					return &respCommands[i].subs[j], respCommands[i].name
				}
			}
		}
	}
	return nil, ""
}

// appendCommandInfo renders the 10-element detail array Valkey emits since 7.0:
// name, arity, flags, first key, last key, step, ACL categories, tips, key
// specifications and subcommands. parent prefixes the name (e.g. "role|create").
func appendCommandInfo(out []byte, parent string, c *commandSpec) []byte {
	name := c.name
	if parent != "" {
		name = parent + "|" + c.name
	}
	out = AppendArray(out, 10)
	out = AppendBulk(out, []byte(name))
	out = AppendInt(out, int64(c.arity))
	out = appendSimpleStringArray(out, c.flags)
	out = AppendInt(out, int64(c.firstKey))
	out = AppendInt(out, int64(c.lastKey))
	out = AppendInt(out, int64(c.step))
	out = appendACLCats(out, aclCategories[name])
	out = AppendArray(out, 0) // tips
	out = appendKeySpecs(out, c)
	out = appendSubcommands(out, name, c)
	return out
}

// appendCommandDoc renders one map entry of COMMAND DOCS: the command name and
// its summary/since/group/complexity. The map is a flattened array in RESP2.
func appendCommandDoc(out []byte, parent string, c *commandSpec) []byte {
	name := c.name
	if parent != "" {
		name = parent + "|" + c.name
	}
	out = AppendBulk(out, []byte(name))
	out = AppendArray(out, 8)
	out = AppendBulk(out, []byte("summary"))
	out = AppendBulk(out, []byte(c.summary))
	out = AppendBulk(out, []byte("since"))
	out = AppendBulk(out, []byte(c.since))
	out = AppendBulk(out, []byte("group"))
	out = AppendBulk(out, []byte(c.group))
	out = AppendBulk(out, []byte("complexity"))
	out = AppendBulk(out, []byte(c.complexity))
	return out
}

// appendCommandDocSubs renders the subcommand docs of a top-level command,
// mirroring Valkey's inclusion of "command|sub" entries under a parent.
func appendCommandDocSubs(out []byte, c *commandSpec) []byte {
	for j := range c.subs {
		out = appendCommandDoc(out, c.name, &c.subs[j])
	}
	return out
}

// appendSubcommands renders the element-10 subcommand array of a detail entry.
func appendSubcommands(out []byte, parent string, c *commandSpec) []byte {
	out = AppendArray(out, len(c.subs))
	for j := range c.subs {
		out = appendCommandInfo(out, parent, &c.subs[j])
	}
	return out
}

// appendKeySpecs renders the element-9 key specifications. Keyed commands get a
// single specification locating their key arguments; keyless commands an empty
// array. begin_search pinpoints the first key and find_keys a range of positions
// relative to it (lastkey 0 = only the begin key, -1 = until the end).
func appendKeySpecs(out []byte, c *commandSpec) []byte {
	if c.firstKey == 0 {
		return AppendArray(out, 0)
	}
	flags := []string{"RO", "access"}
	for _, f := range c.flags {
		if f == "write" {
			flags = []string{"RW", "access", "update"}
			break
		}
	}
	lastkey := c.lastKey - c.firstKey
	if c.lastKey == -1 {
		lastkey = -1
	}
	out = AppendArray(out, 1)
	out = AppendArray(out, 6)
	out = AppendBulk(out, []byte("flags"))
	out = appendSimpleStringArray(out, flags)
	out = AppendBulk(out, []byte("begin_search"))
	out = AppendArray(out, 4)
	out = AppendBulk(out, []byte("type"))
	out = AppendBulk(out, []byte("index"))
	out = AppendBulk(out, []byte("spec"))
	out = AppendArray(out, 2)
	out = AppendBulk(out, []byte("index"))
	out = AppendInt(out, int64(c.firstKey))
	out = AppendBulk(out, []byte("find_keys"))
	out = AppendArray(out, 4)
	out = AppendBulk(out, []byte("type"))
	out = AppendBulk(out, []byte("range"))
	out = AppendBulk(out, []byte("spec"))
	out = AppendArray(out, 6)
	out = AppendBulk(out, []byte("lastkey"))
	out = AppendInt(out, int64(lastkey))
	out = AppendBulk(out, []byte("keystep"))
	out = AppendInt(out, int64(c.step))
	out = AppendBulk(out, []byte("limit"))
	out = AppendInt(out, 0)
	return out
}

// appendSimpleStringArray renders a list of simple strings (flags).
func appendSimpleStringArray(out []byte, s []string) []byte {
	out = AppendArray(out, len(s))
	for _, v := range s {
		out = AppendSimpleString(out, v)
	}
	return out
}

// appendACLCats renders a command's ACL categories as Valkey-style simple
// strings prefixed with '@' (e.g. +@read). The names come from the rbac
// catalog, bare; the prefix is presentation only.
func appendACLCats(out []byte, cats []string) []byte {
	out = AppendArray(out, len(cats))
	for _, v := range cats {
		out = AppendSimpleString(out, "@"+v)
	}
	return out
}

// containsFold reports whether s contains val case-insensitively.
func containsFold(s []string, val string) bool {
	for _, v := range s {
		if strings.EqualFold(v, val) {
			return true
		}
	}
	return false
}

// indexByte returns the byte index of c in b, or -1.
func indexByte(b []byte, c byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// globMatch reports whether s matches the glob pattern pat. Supported
// wildcards are '*' (any run) and '?' (any single byte); the '[' character
// class is not part of the pattern language because command names are simple
// lower-case tokens, and no Tellstone command needs it.
func globMatch(pat, s string) bool {
	for len(pat) > 0 && pat[0] == '*' {
		pat = pat[1:]
		for len(pat) > 0 && pat[0] == '*' {
			pat = pat[1:]
		}
		if len(pat) == 0 {
			return true
		}
		for i := 0; i <= len(s); i++ {
			if globMatch(pat, s[i:]) {
				return true
			}
		}
		return false
	}
	if len(pat) == 0 {
		return len(s) == 0
	}
	if pat[0] == '?' {
		if len(s) == 0 {
			return false
		}
		return globMatch(pat[1:], s[1:])
	}
	if len(s) == 0 || s[0] != pat[0] {
		return false
	}
	return globMatch(pat[1:], s[1:])
}
