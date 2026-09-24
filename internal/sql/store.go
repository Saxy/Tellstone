/*
Package sql implements the Phase 8 PostgreSQL wire frontend (ADR-012). It
speaks the frontend/backend protocol version 3.0 over a plain net.Listener and
maps a single implicit "tellstone" table -- key text primary key, value bytea
-- onto the command-layer Store seam, so standalone, cluster and federated
modes all inherit their proven read/write routing.
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
	// Delete removes a key, reporting whether it existed and any storage error.
	Delete(key string) (bool, error)
}
