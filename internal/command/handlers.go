/*
Package command
Tellstone Shared Command Layer
File: handlers.go
Description: The GET, SET and DEL handlers, plus the EX/PX TTL parser moved out of
the RESP frontend. These are the single source of truth for data-command semantics;
the binary and RESP frontends call Execute and encode the reply through Reply.

Authors:

	Maximilian Hagen
*/
package command

import (
	"strconv"
	"time"
	"unsafe"
)

// get implements GET key.
func get(c *Ctx) {
	if len(c.Args) != 2 {
		c.Reply.ErrorMsg("ERR wrong number of arguments for 'get' command")
		return
	}
	val, ok := c.Store.Get(alias(c.Args[1]))
	if !ok {
		c.Reply.Null()
		return
	}
	c.Reply.Bulk(val)
}

// set implements SET key value [EX seconds|PX milliseconds]. The frontend's
// TTL hint (binary frames carry the expiry directly) applies only when the
// command has no EX/PX option.
func set(c *Ctx) {
	if len(c.Args) != 3 && len(c.Args) != 5 {
		c.Reply.ErrorMsg("ERR wrong number of arguments for 'set' command")
		return
	}
	ttl, ok := parseSetTTL(c.Args)
	if !ok {
		c.Reply.ErrorMsg("ERR syntax error")
		return
	}
	if ttl == 0 {
		ttl = c.TTL
	}
	if err := c.Store.Set(alias(c.Args[1]), c.Args[2], ttl); err != nil {
		c.Reply.StorageErr(err)
		return
	}
	c.Reply.OK()
}

// del implements DEL key [key ...], counting the keys that were present.
// Delete reports existence directly, so a DEL costs one map lookup per key
// instead of Get-then-Delete. The reply count is a wire detail, not part of
// the semantics; the binary frontend encodes it as an unconditional OK.
func del(c *Ctx) {
	if len(c.Args) < 2 {
		c.Reply.ErrorMsg("ERR wrong number of arguments for 'del' command")
		return
	}
	var n int64
	for _, k := range c.Args[1:] {
		if c.Store.Delete(alias(k)) {
			n++
		}
	}
	c.Reply.Int(n)
}

// parseSetTTL parses the optional EX/PX option of SET. A bare key/value pair
// carries no expiry; anything other than EX or PX (including a negative
// number) is a syntax error.
func parseSetTTL(args [][]byte) (time.Duration, bool) {
	if len(args) == 3 {
		return 0, true
	}
	v, err := strconv.Atoi(unsafe.String(unsafe.SliceData(args[4]), len(args[4])))
	if err != nil || v <= 0 {
		return 0, false
	}
	switch {
	case equalFoldASCII(args[3], "EX"):
		return time.Duration(v) * time.Second, true
	case equalFoldASCII(args[3], "PX"):
		return time.Duration(v) * time.Millisecond, true
	default:
		return 0, false
	}
}
