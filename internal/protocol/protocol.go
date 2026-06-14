/*
Package protocol
Tellstone Cloud-Native In-Memory Database
Description: The Relational Translation Matrix. A high-performance,

	zero-allocation text-query scanner that decouples database
	wire frontends from the inner storage engine.

"Every protocol speaks a different language; bytes are the ultimate truth."

Authors:

	Maximilian Hagen
*/
package protocol

import "time"

// CommandType represents a raw, byte-bound enumerator signaling the
// transactional intent extracted from an incoming database query.
// It maps relational SQL keywords directly onto internal key-value operations.
type CommandType byte

const (
	// CmdUnknown indicates an unparseable syntax, missing keywords, or a non-KV query.
	// In production, this triggers a fallback database error response to the client.
	CmdUnknown CommandType = iota

	// CmdGet signals a value retrieval intent (e.g., matching a relational SELECT).
	// This routes the execution path directly to Engine.Get().
	CmdGet

	// CmdSet signals an upsert intent (e.g., matching a relational INSERT or UPDATE).
	// This routes the execution path directly to Engine.Set().
	CmdSet

	// CmdDelete signals an explicit key evacuation intent (e.g., matching a relational DELETE).
	// This routes the execution path directly to Engine.Delete().
	CmdDelete
)

// ParsedQuery is a completely flat, stack-allocated abstraction layer representing
// the finalized execution intent extracted by the relational byte-scanner.
//
// DESIGN GUARANTEE: To maintain the core requirement of strict zero-allocation performance
// during the hot parsing path, this struct does not contain pointers, strings, or dynamic
// heap-escaped objects. All fields are either inlined values or zero-copy sub-slices.
type ParsedQuery struct {
	// Type dictates the tactical routing path inside the database engine loop.
	Type CommandType

	// Key is a direct zero-copy sub-slice pointing precisely to the key window
	// within the incoming TCP connection network read buffer.
	// All surrounding quotes or whitespaces are already stripped inline.
	//
	// LIFETIME WARNING: This slice points directly to transient network memory.
	// It must either be consumed immediately during a read operation or copied
	// by the storage map engine during a write operation.
	Key []byte

	// Value represents the raw, unparsed binary database payload. It is populated
	// exclusively during a CmdSet transaction. Like the Key field, it is a zero-copy
	// sub-slice pointing directly to the active TCP connection buffer frame.
	Value []byte

	// TTL defines the optional lifetime configuration extracted from the relational query
	// parameters via an allocation-free inline integer parser.
	// A value of 0 indicates persistent storage without automatic background eviction.
	TTL time.Duration
}
