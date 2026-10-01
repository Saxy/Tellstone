/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: ddl_entry.go
Description: Wire format and replication entry point for schema mutations.

DDL does not travel as a plain SET. A schema definition is a statement about the
shape of every future read and write in a subtree, so three properties distinguish
it from a KV write and each one is a reason for a separate opcode:

  - It has no TTL. A KV entry that expires and disappears is unremarkable; a
    schema entry that expires deletes a table out from under its rows.
  - It carries a proposer-assigned timestamp. Raft replicas replay the same log
    independently, so anything a replica has to decide for itself (a clock
    read, a local counter) makes Apply a non-deterministic function of the log
    and the replicas diverge. The timestamp is therefore allocated once, on
    the proposing node, and committed inside the entry (ADR-013 guardrail 4).
  - Its replay is observable. Applying CREATE twice must not be a second
    create, and applying DROP twice must not fail.

DDL entry wire format:

	[1B op][8B TSO][2B keyLen][keyLen key][remaining body]

The 11-byte header is the same width as the KV header, so a decoder can tell
the two apart by the opcode alone and the entry stays one contiguous buffer
with no length-prefix-of-a-length-prefix.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrDDLMalformed reports a DDL entry that cannot be decoded: too short, or
// carrying a key length that does not fit the payload.
var ErrDDLMalformed = errors.New("cluster ddl: malformed entry")

// DDLEntry is a decoded schema mutation.
//
// TSO is the timestamp the proposer assigned, committed with the entry so that
// every replica records the same value without consulting a clock. Key is the
// catalog key naming the table. Body is the TSCH schema blob for a create and
// is empty for a drop, which is a tombstone rather than a zero-length write.
type DDLEntry struct {
	Op   byte
	TSO  uint64
	Key  string
	Body []byte
}

// EncodeDDL builds a DDL log entry payload. tso must already be allocated by
// the proposer; this function does not read a clock, which is what keeps Apply
// deterministic.
func EncodeDDL(op byte, tso uint64, key string, body []byte) ([]byte, error) {
	if !IsDDL(op) {
		return nil, fmt.Errorf("cluster ddl: opcode 0x%02x is not a DDL opcode", op)
	}
	keyBytes := []byte(key)
	if len(keyBytes) > maxKeyLen {
		return nil, ErrKeyTooLarge
	}
	buf := make([]byte, opHeaderSize+len(keyBytes)+len(body))
	buf[0] = op
	binary.BigEndian.PutUint64(buf[1:9], tso)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(keyBytes)))
	copy(buf[opHeaderSize:opHeaderSize+len(keyBytes)], keyBytes)
	copy(buf[opHeaderSize+len(keyBytes):], body)
	return buf, nil
}

// EncodeCreateTable builds an OpCreateTable entry carrying a schema blob.
func EncodeCreateTable(tso uint64, key string, schema []byte) ([]byte, error) {
	return EncodeDDL(OpCreateTable, tso, key, schema)
}

// EncodeDropTable builds an OpDropTable entry. The body is empty by
// construction: a drop carries no data to restore, so a replica that has
// already applied it reaches the same state as one applying it for the first
// time.
func EncodeDropTable(tso uint64, key string) ([]byte, error) {
	return EncodeDDL(OpDropTable, tso, key, nil)
}

// DecodeDDLEntry parses a DDL payload.
//
// The key-length bound is checked against the buffer before slicing, because a
// corrupt or truncated entry must be rejected rather than panicking on a
// half-decoded length. That is the difference between a replayed log and an
// attacker-supplied one, and it costs one comparison.
func DecodeDDLEntry(data []byte) (DDLEntry, error) {
	var e DDLEntry
	if len(data) < opHeaderSize {
		return e, fmt.Errorf("%w: entry is %d bytes, want at least %d", ErrDDLMalformed, len(data), opHeaderSize)
	}
	e.Op = data[0]
	if !IsDDL(e.Op) {
		return e, fmt.Errorf("%w: opcode 0x%02x is not a DDL opcode", ErrDDLMalformed, e.Op)
	}
	e.TSO = binary.BigEndian.Uint64(data[1:9])
	keyLen := int(binary.BigEndian.Uint16(data[9:11]))
	if keyLen > len(data)-opHeaderSize {
		return e, fmt.Errorf("%w: key length %d exceeds %d remaining bytes", ErrDDLMalformed, keyLen, len(data)-opHeaderSize)
	}
	e.Key = string(data[opHeaderSize : opHeaderSize+keyLen])
	e.Body = data[opHeaderSize+keyLen:]
	return e, nil
}

// DDLProposer builds DDL log entries, allocating the timestamp that the entry
// carries.
//
// The timestamp comes from the pool only here, on the request path of the node
// proposing the entry. Nothing in the apply path allocates one, so a replica
// that is behind on the log still applies the same value the proposer chose
// rather than stamping its own.
type DDLProposer struct {
	pool *TSOPool
}

// NewDDLProposer returns a proposer backed by pool. A nil pool is allowed: the
// entry is then stamped with TSO zero, which is a real timestamp value (the
// epoch) rather than a sentinel, so a decode of a stamped entry never has to
// distinguish "not stamped" from "stamped at the epoch".
func NewDDLProposer(pool *TSOPool) *DDLProposer { return &DDLProposer{pool: pool} }

// Alloc reserves the next timestamp. It blocks until one is available, the pool
// closes, or ctx is cancelled, matching the write-blocking semantic of the
// cluster's TSO design.
func (p *DDLProposer) Alloc(ctx context.Context) (uint64, error) {
	if p == nil || p.pool == nil {
		return 0, nil
	}
	return p.pool.Alloc(ctx)
}

// ProposeCreateTable allocates a timestamp and encodes the entry. It does not
// submit it: the caller decides which Raft group receives the entry, because
// the group is chosen by the catalog key's owning region.
func (p *DDLProposer) ProposeCreateTable(ctx context.Context, key string, schema []byte) ([]byte, error) {
	tso, err := p.Alloc(ctx)
	if err != nil {
		return nil, fmt.Errorf("cluster ddl: allocate tso: %w", err)
	}
	return EncodeCreateTable(tso, key, schema)
}

// ProposeDropTable allocates a timestamp and encodes the drop entry.
func (p *DDLProposer) ProposeDropTable(ctx context.Context, key string) ([]byte, error) {
	tso, err := p.Alloc(ctx)
	if err != nil {
		return nil, fmt.Errorf("cluster ddl: allocate tso: %w", err)
	}
	return EncodeDropTable(tso, key)
}

// RangeChecker answers "does any key exist under this prefix" for the whole
// cluster, not just for the local node's engines.
//
// It is a separate interface for the same reason CatalogDispatcher was: the
// emptiness guard is a proposal-side decision, and a caller that cannot answer
// it must refuse the drop rather than assume the table is empty.
//
// The method is named PrefixExists rather than ScanPrefix because the store's
// own ScanPrefix is a callback-based listing with a different contract, and a
// guard that silently reused it would inherit its best-effort stale-read
// fallback. A guard must fail loudly, not answer from a replica that may simply
// be behind.
type RangeChecker interface {
	// PrefixExists reports whether any key exists under prefix, consulting
	// every region leader so the answer covers the cluster. Returning an error
	// means "cannot determine" and is never treated as "empty".
	PrefixExists(prefix string) (found bool, err error)
}

// GuardedDropProposer builds an OpDropTable entry only when it can prove the
// table is empty.
//
// The guard runs here, on the proposing node, and never in FSM.Apply. That
// placement is the whole point:
//
//   - The proposer can reach every region's leader, so "empty" means empty
//     across the cluster.
//   - Apply cannot. A node's local engines hold the data of every region it
//     hosts, and different nodes host different regions, so two replicas of one
//     group can hold different numbers of the dropped table's rows. Guarding in
//     Apply would let one replica delete the catalog key while another refused,
//     and the state machines would diverge permanently.
//
// Once an entry is in the log, applying it is unconditional and identical on
// every replica.
type GuardedDropProposer struct {
	*DDLProposer
	checker RangeChecker
}

// NewGuardedDropProposer returns a drop proposer that verifies emptiness
// through checker before encoding an entry. A nil checker is allowed and makes
// every drop fail with ErrEmptinessUnverifiable, because a guard that cannot run
// must never be reported as one that passed.
func NewGuardedDropProposer(p *DDLProposer, checker RangeChecker) *GuardedDropProposer {
	return &GuardedDropProposer{DDLProposer: p, checker: checker}
}

// ProposeDropTable verifies the table is empty and then encodes the drop entry.
//
// A non-empty table and an unverifiable check are reported as different errors:
// a client that still has rows to delete should not be told the cluster could
// not answer the question, and vice versa. Neither is reported as success.
func (p *GuardedDropProposer) ProposeDropTable(ctx context.Context, key string) ([]byte, error) {
	prefix, err := RowPrefixFor(key)
	if err != nil {
		return nil, err
	}
	if p.checker == nil {
		return nil, fmt.Errorf("%w: no range checker configured for %s", ErrEmptinessUnverifiable, prefix)
	}
	found, err := p.checker.PrefixExists(prefix)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrEmptinessUnverifiable, prefix, err)
	}
	if found {
		return nil, fmt.Errorf("%w: %s", ErrTableNotEmpty, prefix)
	}
	return p.DDLProposer.ProposeDropTable(ctx, key)
}
