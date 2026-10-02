/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: store.go
Description: The data seam the PG frontend executes statements against, and the
package overview. A single implicit "tellstone" table -- key text primary key,
value bytea -- is mapped onto this interface, so standalone, cluster and
federated modes all inherit the command layer's proven read/write routing.
Conditional writes are part of the seam because INSERT and UPDATE need their
existence check and their write to be one atomic step at the store.
*/
package sql

import (
	"context"
	"time"
)

// Store is the data seam SQL statements execute against. command.Store already
// satisfies it, so the server passes the same store the binary frontend uses.
type Store interface {
	// GetErr reads a key, reporting whether it exists and any storage error.
	GetErr(key string) ([]byte, bool, error)
	// Set writes a key. ttl is a time-to-live in nanoseconds (0 = no TTL).
	Set(key string, value []byte, ttl time.Duration) error
	// SetIfAbsent writes a key only when it is currently absent and reports
	// whether the write was applied. The check and the write are one atomic
	// step at the store, which is what makes INSERT safe under concurrency.
	SetIfAbsent(key string, value []byte, ttl time.Duration) (bool, error)
	// SetIfPresent writes a key only when it already exists and reports whether
	// the write was applied, atomically with the existence check. It backs the
	// UPDATE affected-row count.
	SetIfPresent(key string, value []byte, ttl time.Duration) (bool, error)
	// Delete removes a key, reporting whether it existed and any storage error.
	Delete(key string) (bool, error)
	// ScanPrefix calls fn for every live key beginning with prefix, in key
	// order, reporting how many keys were delivered. Returning false from fn
	// stops the scan early. It is the range read that makes a row one
	// contiguous key range (ADR-013), and it is also how the schema catalog
	// lists tables and counts a table's rows.
	//
	// The key and value passed to fn are owned by the call: they stay valid for
	// the duration of the call, but a caller that retains them must copy.
	// Key order is the order the entries are stored in, so a row's columns
	// arrive in the order they were written.
	ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error)
}

// DDLStore is an optional capability a Store implements when it can replicate a
// schema mutation as a purpose-built log entry rather than an ordinary
// conditional write.
//
// It is separate from Store because a schema mutation is not a key/value write.
// It carries no TTL, it carries a timestamp the proposer allocated rather than
// one the applier invents, and a DROP has to be proven safe across every region
// before it is allowed into the log. A store that cannot do those things still
// satisfies Store, so standalone mode keeps working; it just keeps using
// SetIfAbsent/Delete, which is correct and merely less explicit.
//
// Any implementation must preserve the ordering guarantees the plain path has:
// the create is conditional so two concurrent CREATEs cannot both win, and the
// drop refuses a table that still has rows.
type DDLStore interface {
	// CreateTable replicates a schema definition. It reports whether the entry
	// was applied; false means the name was already taken, which is a normal
	// outcome rather than a fault.
	CreateTable(ctx context.Context, key string, schema []byte) (bool, error)
	// DropTable replicates the removal of a table's catalog entry. It refuses a
	// table that still has rows, and refuses a check it could not complete
	// rather than reporting an unverified table as empty.
	DropTable(ctx context.Context, key string) error
}
