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

import "time"

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
}
