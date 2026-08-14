/*
Package command
Tellstone Shared Command Layer
File: command.go
Description: The data-command core shared by both frontends (binary and RESP). GET,
SET and DEL live here exactly once: argument validation, optional EX/PX TTL parsing,
the RBAC gate, the per-role command count, the audit hooks and the storage access.
Each protocol only supplies Reply and its own calling conventions;

Authors:

	Maximilian Hagen
*/
package command

import (
	"strings"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// Store is the storage seam every handler writes to
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, ttl time.Duration) error
	Delete(key string) bool
}

// Reply is the transport-specific wire encoder.
// A handler calls exactly one method per request
type Reply interface {
	OK()
	Bulk(b []byte)
	Null()
	Int(n int64)
	Denied(cmd string)
	ErrorMsg(s string)
	StorageErr(err error)
}

// Ctx is built for one request. Pool the Ctx per connection
type Ctx struct {
	Store    Store
	Args     [][]byte
	Reply    Reply
	TTL      time.Duration
	Policy   *rbac.Store
	Session  *rbac.SessionContext
	Remote   string
	Protocol string
	Audit    *audit.LogEngine
	Logger   log.Logger
}

// Command describes one data command: the minimum argument count
//
//	which arguments are keys for the gate, and what to execute.
type Command struct {
	Name    string
	RbacID  uint16
	Arity   int
	Keys    func([][]byte) [][]byte
	Handler func(*Ctx)
}

// commands is a dict.
var commands = [...]Command{
	{Name: "GET", RbacID: rbac.CmdGet, Arity: 2, Keys: keyArg, Handler: get},
	{Name: "SET", RbacID: rbac.CmdSet, Arity: 3, Keys: keyArg, Handler: set},
	{Name: "DEL", RbacID: rbac.CmdDel, Arity: 2, Keys: keyArgs, Handler: del},
}

// Lookup returns the command which matches the name or nil if not found
func Lookup(name []byte) *Command {
	for i := range commands {
		if equalFoldASCII(name, commands[i].Name) {
			return &commands[i]
		}
	}
	return nil
}

// Execute runs one data command end to end.
// The reply is written through a ReplyMethod
func Execute(c *Ctx) bool {
	if len(c.Args) == 0 {
		return false
	}
	cmd := Lookup(c.Args[0])
	if cmd == nil {
		return false
	}
	if len(c.Args) < cmd.Arity {
		c.Reply.ErrorMsg("ERR wrong number of arguments for '" + strings.ToLower(cmd.Name) + "' command")
		return true
	}
	if c.Policy != nil {
		if key := c.denyKey(cmd); key != nil {
			c.deny(cmd, key)
			return true
		}
		if c.Session != nil {
			c.Session.CountCommand()
		}
	}
	c.auditCommand(cmd)
	cmd.Handler(c)
	return true
}

// denyKey returns the first key the session may not run cmd on
// When a policy is configured, but the connection has no session!
// the check fails to close
func (c *Ctx) denyKey(cmd *Command) []byte {
	if c.Session == nil {
		return c.firstKey(cmd)
	}
	for _, k := range cmd.Keys(c.Args) {
		if !c.Session.IsAllowed(cmd.RbacID, k) {
			return k
		}
	}
	return nil
}

func (c *Ctx) firstKey(cmd *Command) []byte {
	keys := cmd.Keys(c.Args)
	if len(keys) == 0 {
		return nil
	}
	return keys[0]
}

// deny records one RBAC denial (NOPERM) in the policy's denial counter and
// ACL LOG buffer, in the audit trail, and in the warn log, then encodes the
// wire reply. key is the specific offending key so DEL reports the key that
// actually failed.
func (c *Ctx) deny(cmd *Command, key []byte) {
	c.Policy.IncDenied()
	user := ""
	if c.Session != nil {
		user = c.Session.Username
	}
	keyStr := alias(key)
	c.Policy.LogDenied(user, c.Remote, strings.ToLower(cmd.Name), keyStr)
	c.Audit.Record(audit.EventACLDeny, "command denied by rbac policy",
		log.String("user", user),
		log.String("command", strings.ToLower(cmd.Name)),
		log.String("key", keyStr),
		log.String("remote_addr", c.Remote),
		log.String("protocol", c.Protocol),
	)
	if c.Logger.Enabled(log.LevelWarn) {
		fields := []log.Field{
			log.String("remote_addr", c.Remote),
			log.String("command", strings.ToLower(cmd.Name)),
		}
		if c.Session != nil {
			fields = append(fields, log.String("username", c.Session.Username))
		}
		c.Logger.Log(log.LevelWarn, "command denied by rbac policy", fields...)
	}
	c.Reply.Denied(strings.ToLower(cmd.Name))
}

// auditCommand records one permitted command. The command token aliases the
// read buffer, so it must be consumed synchronously — audit.Record encodes it
// before returning.
func (c *Ctx) auditCommand(cmd *Command) {
	if c.Audit == nil {
		return
	}
	user := "default"
	if c.Session != nil {
		user = c.Session.Username
	}
	key := ""
	if len(c.Args) >= 2 {
		key = alias(c.Args[1])
	}
	c.Audit.Record(audit.EventCommand, "command dispatched",
		log.String("command", strings.ToLower(cmd.Name)),
		log.String("key", key),
		log.String("user", user),
		log.String("remote_addr", c.Remote),
		log.String("protocol", c.Protocol),
	)
}

// alias converts b to a string without copying, mirroring the zero-copy
func alias(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// equalFoldASCII compares a against the ASCII literal b case-insensitively,
// mirroring the RESP EqualFold so command matching is Redis-compatible.
func equalFoldASCII(a []byte, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i]|0x20 != b[i]|0x20 {
			return false
		}
	}
	return true
}

// keyArg selects the single key of GET and SET for the gate.
func keyArg(args [][]byte) [][]byte { return args[1:2] }

// keyArgs selects every key of DEL so each one is gated.
func keyArgs(args [][]byte) [][]byte { return args[1:] }
