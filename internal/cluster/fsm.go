/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: fsm.go
Description: Raft finite state machine that bridges consensus log entries to
local shard storage. Write operations (SET, DEL) proposed on the leader are
encoded into compact binary log entries, replicated via Raft, and applied to
every node's local storage on commit. The codec is a simple binary format
optimised for the hot path: no protobuf, no JSON, no allocations in the
encode/decode hot path.

Log entry wire format:

	[1B op][8B TTL ms][2B keyLen][keyLen key][remaining value]

	op = 0x01 → SET   (value present, length = total - 11 - keyLen)
	op = 0x02 → DEL   (no value, length = 0)

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/keyspace"
	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	// OpSet is the log entry opcode for SET operations.
	OpSet byte = 0x01
	// OpDel is the log entry opcode for DEL operations.
	OpDel byte = 0x02
	// OpSetNX is the log entry opcode for a conditional SET that only creates
	// the key. It is the replication-safe form of an INSERT: the log orders
	// the check and the write, so two racing inserts cannot both apply.
	OpSetNX byte = 0x04
	// OpSetXX is the log entry opcode for a conditional SET that only
	// overwrites an already-present key, the replication-safe UPDATE.
	OpSetXX byte = 0x05
	// OpCreateTable is the log entry opcode for a replicated schema creation.
	// It is a distinct opcode rather than an OpSetNX of the catalog key
	// because a schema mutation is not a KV write: it carries a proposer-
	// assigned timestamp, it has no TTL, and its replay is observable.
	OpCreateTable byte = 0x06
	// OpDropTable is the log entry opcode for a replicated schema removal.
	OpDropTable byte = 0x07
	// opHeaderSize is the fixed header: 1B op + 8B TTL/TSO + 2B keyLen = 11 bytes.
	opHeaderSize = 11
)

// IsDDL reports whether op is a schema-mutating opcode. DDL entries are decoded
// by their own codec because the 8-byte field after the opcode carries a TSO
// rather than a TTL, and inheriting TTL semantics would let a schema definition
// expire and silently delete a table.
func IsDDL(op byte) bool { return op == OpCreateTable || op == OpDropTable }

// ErrConditionNotMet is what a Dispatcher reports for a conditional log entry
// (OpSetNX/OpSetXX) whose precondition is unsatisfied. It travels back to the
// proposer as the proposal's apply error, so a failed INSERT reads as a
// duplicate key rather than a successful write.
var ErrConditionNotMet = errors.New("cluster fsm: write condition not met")

// IsConditionNotMet reports whether err signals an unsatisfied write
// precondition. The Raft proposal path preserves the sentinel, but the
// pipelined follower-forward and cross-cluster gateway hops rebuild the error
// from its wire text, so the message is matched as a fallback.
func IsConditionNotMet(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrConditionNotMet) || strings.Contains(err.Error(), ErrConditionNotMet.Error())
}

// Dispatcher routes a decoded operation to the correct local shard engine.
// Implementations must use the same FNV-1a routing as the normal request
// path so that replicated data lands in the same shard on every node.
type Dispatcher interface {
	Dispatch(key string, op byte, value []byte, ttl time.Duration) error
}

// RegionResolver maps a key to its owning region ID via the routing table.
// Returns 0 if no region covers the key (e.g. routing table not yet loaded).
type RegionResolver func(key string) uint64

// FSM applies committed Raft log entries to local storage via a Dispatcher.
type FSM struct {
	dispatcher Dispatcher
	logger     log.Logger
	// mu protects tracker and resolver from concurrent access between
	// SetSizeTracker (called once during startup) and Apply (hot path).
	mu       sync.RWMutex
	tracker  *RegionSizeTracker
	resolver RegionResolver
	// chunks reassembles chunked SET values (OpChunkSet chains). nil until
	// the first chunk is seen, at which point it is created once.
	chunksMu sync.Mutex
	chunks   *chunkAssembler
}

// NewFSM creates a new FSM that routes committed entries through the given
// Dispatcher.
func NewFSM(dispatcher Dispatcher, logger log.Logger) *FSM {
	return &FSM{
		dispatcher: dispatcher,
		logger:     logger,
	}
}

// SetSizeTracker attaches a region size tracker and key-to-region resolver to
// the FSM. When set, every SET/DEL applied through the FSM will update the
// tracked byte count for the owning region. Both fields may be nil to disable
// tracking (e.g. single-region startup before the PD is ready).
func (f *FSM) SetSizeTracker(tracker *RegionSizeTracker, resolver RegionResolver) {
	f.mu.Lock()
	f.tracker = tracker
	f.resolver = resolver
	f.mu.Unlock()
}

// trackerSnapshot returns the current tracker and resolver under the read lock.
func (f *FSM) trackerSnapshot() (*RegionSizeTracker, RegionResolver) {
	f.mu.RLock()
	t, r := f.tracker, f.resolver
	f.mu.RUnlock()
	return t, r
}

// maxKeyLen is the largest key the wire format can carry: the key length is
// a uint16 field. Longer keys must be rejected by the encoders — truncating
// the length field would corrupt the entry for every replica decoding it.
const maxKeyLen = math.MaxUint16

// ErrKeyTooLarge is returned by EncodeSet and EncodeDel when the key exceeds
// maxKeyLen and cannot be represented in the log entry wire format.
var ErrKeyTooLarge = errors.New("cluster fsm: key exceeds wire format maximum of 65535 bytes")

// EncodeSet creates a Raft log entry payload for a SET operation.
func EncodeSet(key string, value []byte, ttl time.Duration) ([]byte, error) {
	return encodeEntry(OpSet, key, value, ttl)
}

// EncodeSetNX creates a Raft log entry payload for a conditional SET that only
// applies when the key is absent.
func EncodeSetNX(key string, value []byte, ttl time.Duration) ([]byte, error) {
	return encodeEntry(OpSetNX, key, value, ttl)
}

// EncodeSetXX creates a Raft log entry payload for a conditional SET that only
// applies when the key is present.
func EncodeSetXX(key string, value []byte, ttl time.Duration) ([]byte, error) {
	return encodeEntry(OpSetXX, key, value, ttl)
}

// encodeEntry lays out the 11-byte header followed by the key and the value.
func encodeEntry(op byte, key string, value []byte, ttl time.Duration) ([]byte, error) {
	keyBytes := []byte(key)
	if len(keyBytes) > maxKeyLen {
		return nil, ErrKeyTooLarge
	}
	var ttlMs int64
	if ttl > 0 {
		ttlMs = ttl.Milliseconds()
	}
	buf := make([]byte, opHeaderSize+len(keyBytes)+len(value))
	buf[0] = op
	binary.BigEndian.PutUint64(buf[1:9], uint64(ttlMs))
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(keyBytes)))
	copy(buf[11:11+len(keyBytes)], keyBytes)
	copy(buf[11+len(keyBytes):], value)
	return buf, nil
}

// EncodeDel creates a Raft log entry payload for a DEL operation.
func EncodeDel(key string) ([]byte, error) {
	return encodeEntry(OpDel, key, nil, 0)
}

// DecodeLogEntry parses a log entry payload into its components.
func DecodeLogEntry(data []byte) (op byte, key string, value []byte, ttl time.Duration, err error) {
	if len(data) < opHeaderSize {
		return 0, "", nil, 0, errors.New("cluster fsm: entry too short")
	}
	op = data[0]
	ttlMs := binary.BigEndian.Uint64(data[1:9])
	ttl = time.Duration(ttlMs) * time.Millisecond
	keyLen := binary.BigEndian.Uint16(data[9:11])
	if int(keyLen) > len(data)-opHeaderSize {
		return 0, "", nil, 0, fmt.Errorf("cluster fsm: key length %d exceeds data length %d", keyLen, len(data))
	}
	key = string(data[opHeaderSize : opHeaderSize+int(keyLen)])
	value = data[opHeaderSize+int(keyLen):]
	return op, key, value, ttl, nil
}

// Apply applies a single committed Raft log entry to local storage via the
// Dispatcher. It decodes the binary payload and dispatches. When a
// RegionSizeTracker is configured, the entry's byte impact is reflected in the
// owning region's tracked size.
func (f *FSM) Apply(entry *pb.Entry) error {
	if entry.GetData() == nil {
		return nil
	}
	// Chunked SET values are reassembled across multiple entries and
	// dispatched as one OpSet once the final chunk arrives.
	if len(entry.GetData()) > 0 && entry.GetData()[0] == OpChunkSet {
		return f.applyChunk(entry)
	}
	// DDL entries share the 11-byte header width with KV entries but carry a
	// TSO where a KV entry carries a TTL, so they are decoded separately rather
	// than being read as a SET with a nonsensical expiration.
	if len(entry.GetData()) > 0 && IsDDL(entry.GetData()[0]) {
		return f.applyDDL(entry)
	}
	op, key, value, ttl, err := DecodeLogEntry(entry.GetData())
	if err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "cluster fsm: decode error",
				log.String("error", err.Error()),
				log.Uint64("index", entry.GetIndex()),
				log.Int("data_len", len(entry.GetData())),
			)
		}
		return err
	}
	if f.logger.Enabled(log.LevelDebug) {
		f.logger.Log(log.LevelDebug, "cluster fsm: dispatching decoded entry",
			log.Uint64("index", entry.GetIndex()),
			log.Uint("op", uint32(op)),
			log.String("key", key),
			log.Int("value_len", len(value)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	if err := f.dispatcher.Dispatch(key, op, value, ttl); err != nil {
		// An unsatisfied precondition is a normal outcome, not a fault: it is
		// how a losing INSERT and a stale UPDATE are reported back to the
		// proposer. Logging it at error level would emit a line per duplicate
		// key, which is the normal case for a primary key under load.
		if !IsConditionNotMet(err) && f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "cluster fsm: dispatch failed",
				log.String("error", err.Error()),
				log.Uint64("index", entry.GetIndex()),
				log.String("key", key),
				log.Uint("op", uint32(op)),
			)
		}
		return err
	}
	// Track per-region byte delta for split decisions.
	if tracker, resolver := f.trackerSnapshot(); tracker != nil && resolver != nil {
		if regionID := resolver(key); regionID != 0 {
			switch op {
			case OpSet, OpSetNX, OpSetXX:
				tracker.TrackSet(regionID, key, value)
			case OpDel:
				tracker.TrackDel(regionID, key, value)
			}
		}
	}
	if f.logger.Enabled(log.LevelDebug) {
		f.logger.Log(log.LevelDebug, "cluster fsm: dispatch succeeded",
			log.Uint64("index", entry.GetIndex()),
			log.String("key", key),
			log.Uint("op", uint32(op)),
		)
	}
	return nil
}

// applyChunk feeds one OpChunkSet entry into the assembler and dispatches the
// reassembled value when the final chunk in the chain arrives.
func (f *FSM) applyChunk(entry *pb.Entry) error {
	ce, err := DecodeChunkEntry(entry.GetData())
	if err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "cluster fsm: chunk decode error",
				log.String("error", err.Error()),
				log.Uint64("index", entry.GetIndex()),
			)
		}
		return err
	}
	f.chunksMu.Lock()
	if f.chunks == nil {
		f.chunks = newChunkAssembler()
	}
	f.chunksMu.Unlock()

	key, value, ttl, complete := f.chunks.add(ce)
	if !complete {
		return nil
	}
	if f.logger.Enabled(log.LevelDebug) {
		f.logger.Log(log.LevelDebug, "cluster fsm: chunk chain complete",
			log.String("key", key),
			log.Int("value_len", len(value)),
		)
	}
	if err := f.dispatcher.Dispatch(key, OpSet, value, ttl); err != nil {
		return err
	}
	if tracker, resolver := f.trackerSnapshot(); tracker != nil && resolver != nil {
		if regionID := resolver(key); regionID != 0 {
			tracker.TrackSet(regionID, key, value)
		}
	}
	return nil
}

// ErrTableNotEmpty reports that a DROP TABLE was refused because keys still
// exist under the table's row prefix. It is a client error rather than a
// cluster fault, and is raised while *proposing* the entry, not while applying
// it, so the SQL layer can map it to 0A000.
var ErrTableNotEmpty = errors.New("cluster ddl: table is not empty")

// IsTableNotEmpty reports whether err is an emptiness-guard refusal, matching on
// text as a fallback because the error is rebuilt from its wire form on the
// follower-forward and gateway hops.
func IsTableNotEmpty(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrTableNotEmpty) || strings.Contains(err.Error(), ErrTableNotEmpty.Error())
}

// ErrEmptinessUnverifiable reports that a DROP TABLE could not be proven safe
// because this node cannot complete a cluster-wide range read. It is
// deliberately distinct from ErrTableNotEmpty: "I could not check" must not be
// reported to a client as "your table has rows", and neither may be reported as
// success.
var ErrEmptinessUnverifiable = errors.New("cluster ddl: cannot verify table is empty on this node")

// RowPrefixFor returns the key prefix under which a table's rows live. It is
// derived from the catalog key rather than accepted separately, so the guard
// cannot be pointed at a different range than the one the drop removes.
//
// The catalog key is <db>/~meta/tables/<db>/<table>, so the row prefix is the
// same key with the reserved catalog subtree removed. Deriving it by string
// surgery is what makes this worth doing carefully: a subtly wrong prefix
// would make the guard scan the wrong range, and a guard that scans too wide
// refuses every drop.
func RowPrefixFor(catalogKey string) (string, error) {
	marker := keyspace.Separator + keyspace.MetaPrefix + keyspace.Separator
	i := strings.Index(catalogKey, marker)
	if i < 0 {
		return "", fmt.Errorf("cluster ddl: %q is not a catalog key", catalogKey)
	}
	db := catalogKey[:i]
	// What remains after <db>/~meta/ is "tables/<db>/<table>".
	rest := catalogKey[i+len(marker):]
	const tablesSeg = "tables" + keyspace.Separator
	if !strings.HasPrefix(rest, tablesSeg) {
		return "", fmt.Errorf("cluster ddl: %q is not a catalog key", catalogKey)
	}
	// The table is named by <db>/<table> in the same namespace the rows use, so
	// the row prefix is <db>/<table>/. Reuse the layout's own segment rule
	// rather than trusting the split, and require the names to match the
	// database the key was filed under: a mismatch means the key is not a
	// catalog key this guard understands, and guessing which of the two to
	// trust is how a guard ends up scanning the wrong table.
	names := rest[len(tablesSeg):]
	rowDB, table, ok := strings.Cut(names, keyspace.Separator)
	if !ok {
		return "", fmt.Errorf("cluster ddl: %q names no table", catalogKey)
	}
	if table == "" {
		return "", fmt.Errorf("cluster ddl: %q names no table", catalogKey)
	}
	if rowDB != db {
		return "", fmt.Errorf("cluster ddl: %q files table under %q but names %q", catalogKey, db, rowDB)
	}
	if err := keyspace.ValidateDatabaseName(rowDB); err != nil {
		return "", fmt.Errorf("cluster ddl: %q: %w", catalogKey, err)
	}
	if err := keyspace.ValidateTableName(table); err != nil {
		return "", fmt.Errorf("cluster ddl: %q: %w", catalogKey, err)
	}
	return keyspace.TablePrefix(rowDB, table), nil
}

// applyDDL applies a schema mutation.
//
// Determinism is the whole design constraint here. Every branch below is a pure
// function of the committed entry plus the dispatcher's own state, which every
// replica of this group shares: two replicas that have applied the same log
// prefix are in the same state. In particular nothing here reads a clock or
// allocates a sequence number, and the emptiness guard is evaluated against the
// local store rather than any node-local intent.
func (f *FSM) applyDDL(entry *pb.Entry) error {
	e, err := DecodeDDLEntry(entry.GetData())
	if err != nil {
		if f.logger.Enabled(log.LevelError) {
			f.logger.Log(log.LevelError, "cluster ddl: decode error",
				log.String("error", err.Error()),
				log.Uint64("index", entry.GetIndex()))
		}
		return err
	}
	if f.logger.Enabled(log.LevelDebug) {
		f.logger.Log(log.LevelDebug, "cluster ddl: applying",
			log.Uint("op", uint32(e.Op)),
			log.String("key", e.Key),
			log.Uint64("tso", e.TSO),
			log.Int("body_len", len(e.Body)),
			log.Uint64("index", entry.GetIndex()))
	}

	switch e.Op {
	case OpCreateTable:
		// The schema blob is the body. It is written with the ordinary
		// conditional-create opcode so that a replayed entry cannot overwrite a
		// table another entry created first: the first creator wins, and a
		// duplicate create is reported to the proposer rather than silently
		// redefining a table that may already hold rows.
		if err := f.dispatcher.Dispatch(e.Key, OpSetNX, e.Body, 0); err != nil {
			return err
		}
		return nil

	case OpDropTable:
		// The emptiness guard is NOT here, and that is deliberate.
		//
		// Apply must be a pure function of the log. A guard that scanned local
		// state would break that: a node's engines hold the data of every
		// region it hosts, and different nodes host different regions, so two
		// replicas of this same group can hold different amounts of the
		// dropped table's rows. One replica would delete the catalog key while
		// another refused, and the state machines would diverge permanently.
		//
		// The guard therefore runs before the entry is proposed, on the node
		// that can see every region's leader, and an entry only enters the log
		// if the table was already proven empty. Once it is here, deletion is
		// unconditional and identical everywhere.
		if err := f.dispatcher.Dispatch(e.Key, OpDel, nil, 0); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("cluster ddl: unhandled opcode 0x%02x", e.Op)
}
