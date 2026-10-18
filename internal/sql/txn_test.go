/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: txn_test.go
Description: End-to-end transaction tests (Phase 12): buffered writes, read-
your-writes, COMMIT/ROLLBACK, write-write conflict abort, and the failed-block
rule.
*/
package sql

import "testing"

// TestTxnWriteWriteConflictAborts drives two overlapping blocks onto the same
// row and checks the second commit is refused rather than silently overwriting
// the first.
func TestTxnWriteWriteConflictAborts(t *testing.T) {
	srv, store := newTestServer(t, srvOpts{})
	a := dialServer(t, srv.Addr())
	a.startupTrust("default")
	b := dialServer(t, srv.Addr())
	b.startupTrust("default")

	queryOK(t, a, `INSERT INTO tellstone (key, value) VALUES ('k', 'v0')`, "INSERT 0 1")

	queryOK(t, a, `BEGIN`, "BEGIN")
	queryOK(t, b, `BEGIN`, "BEGIN")

	queryOK(t, a, `UPDATE tellstone SET value = 'a' WHERE key = 'k'`, "UPDATE 1")
	queryOK(t, b, `UPDATE tellstone SET value = 'b' WHERE key = 'k'`, "UPDATE 1")

	// Neither write is visible outside its block yet.
	if v, ok, _ := store.GetErr("k"); !ok || string(v) != "v0" {
		t.Fatalf("committed value changed before COMMIT: %q ok=%v", v, ok)
	}

	// A commits first.
	frames := queryOK(t, a, `COMMIT`, "COMMIT")
	if code, _, ok := findError(frames); ok {
		t.Fatalf("first COMMIT failed: %s", code)
	}
	if v, ok, _ := store.GetErr("k"); !ok || string(v) != "a" {
		t.Fatalf("committed value = %q ok=%v, want a", v, ok)
	}

	// B started before A committed and wrote the same row, so B must abort.
	frames = b.query(`COMMIT`)
	code, _, ok := findError(frames)
	if !ok || code != errSerializationFailure {
		t.Fatalf("conflicting COMMIT: code=%q ok=%v, want %s", code, ok, errSerializationFailure)
	}
	if v, _, _ := store.GetErr("k"); string(v) != "a" {
		t.Fatalf("aborted transaction overwrote the committed value: %q", v)
	}
}

// TestTxnReadWriteConflictAborts checks the read-write arm: a block that read a
// row another transaction replaced may not commit a later decision.
func TestTxnReadWriteConflictAborts(t *testing.T) {
	srv, store := newTestServer(t, srvOpts{})
	a := dialServer(t, srv.Addr())
	a.startupTrust("default")
	b := dialServer(t, srv.Addr())
	b.startupTrust("default")

	queryOK(t, a, `INSERT INTO tellstone (key, value) VALUES ('k', 'v0')`, "INSERT 0 1")

	queryOK(t, a, `BEGIN`, "BEGIN")
	// A reads the current value of k.
	if tag := findTag(a.query(`SELECT * FROM tellstone WHERE key = 'k'`)); tag != "SELECT 1" {
		t.Fatalf("read tag = %q", tag)
	}
	// B replaces k and commits while A is still open.
	queryOK(t, b, `BEGIN`, "BEGIN")
	queryOK(t, b, `UPDATE tellstone SET value = 'v1' WHERE key = 'k'`, "UPDATE 1")
	queryOK(t, b, `COMMIT`, "COMMIT")

	// A writes a different row and tries to commit: its read of k is stale.
	queryOK(t, a, `INSERT INTO tellstone (key, value) VALUES ('k2', 'x')`, "INSERT 0 1")
	frames := a.query(`COMMIT`)
	code, _, ok := findError(frames)
	if !ok || code != errSerializationFailure {
		t.Fatalf("read-write conflict COMMIT: code=%q ok=%v, want %s", code, ok, errSerializationFailure)
	}
	if _, ok, _ := store.GetErr("k2"); ok {
		t.Fatalf("aborted block's write landed anyway")
	}
	if v, _, _ := store.GetErr("k"); string(v) != "v1" {
		t.Fatalf("concurrent writer's value lost: %q", v)
	}
}

// TestTxnFailedBlockRefusesStatements checks the PostgreSQL rule that an error
// inside a block poisons it until it ends, and that COMMIT of a poisoned block
// reports ROLLBACK.
func TestTxnFailedBlockRefusesStatements(t *testing.T) {
	srv, store := newTestServer(t, srvOpts{})
	sess := dialServer(t, srv.Addr())
	sess.startupTrust("default")

	queryOK(t, sess, `BEGIN`, "BEGIN")
	// A duplicate insert fails, poisoning the block.
	queryOK(t, sess, `INSERT INTO tellstone (key, value) VALUES ('dup', 'x')`, "INSERT 0 1")
	frames := sess.query(`INSERT INTO tellstone (key, value) VALUES ('dup', 'y')`)
	if code, _, ok := findError(frames); !ok || code != errDuplicateKey {
		t.Fatalf("duplicate insert: code=%q ok=%v, want %s", code, ok, errDuplicateKey)
	}

	// Every later statement is refused until the block ends.
	frames = sess.query(`SELECT * FROM tellstone WHERE key = 'dup'`)
	code, _, ok := findError(frames)
	if !ok || code != errInFailedSQLTransaction {
		t.Fatalf("statement after failure: code=%q ok=%v, want %s", code, ok, errInFailedSQLTransaction)
	}
	if f := findFrame(frames, msgReadyForQuery); f == nil || f.val[0] != 'E' {
		t.Fatalf("failed block must report tx status 'E'")
	}

	// COMMIT of a poisoned block rolls back and discards the buffered write.
	frames = sess.query(`COMMIT`)
	if tag := findTag(frames); tag != "ROLLBACK" {
		t.Fatalf("COMMIT of failed block tag = %q", tag)
	}
	if _, ok, _ := store.GetErr("dup"); ok {
		t.Fatalf("buffered write from a rolled-back block survived")
	}
}

// TestMVCCSnapshotIsolation checks that a catalog-table block reads column
// values as of its start timestamp: a write another block commits after this one
// began is invisible to it, while a fresh read sees it.
func TestMVCCSnapshotIsolation(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	a := dialServer(t, srv.Addr())
	a.startupTrust("default")
	b := dialServer(t, srv.Addr())
	b.startupTrust("default")

	queryOK(t, a, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")
	queryOK(t, a, `INSERT INTO users (id, name) VALUES (1, 'v0')`, "INSERT 0 1")

	// A opens a snapshot and reads the row.
	queryOK(t, a, `BEGIN`, "BEGIN")
	if got := selectRow(t, a, `SELECT name FROM users WHERE id = 1`); string(got[0]) != "v0" {
		t.Fatalf("snapshot read = %q, want v0", got[0])
	}

	// B replaces the row and commits after A's snapshot.
	queryOK(t, b, `BEGIN`, "BEGIN")
	queryOK(t, b, `UPDATE users SET name = 'v1' WHERE id = 1`, "UPDATE 1")
	queryOK(t, b, `COMMIT`, "COMMIT")
	queryOK(t, b, `INSERT INTO users (id, name) VALUES (2, 'new')`, "INSERT 0 1")

	// A still sees the value as of its start, and cannot see a row inserted
	// after it began -- through a point lookup or a scan.
	if got := selectRow(t, a, `SELECT name FROM users WHERE id = 1`); string(got[0]) != "v0" {
		t.Fatalf("A saw B's committed update: %q, want v0", got[0])
	}
	if tag := findTag(a.query(`SELECT * FROM users WHERE id = 2`)); tag != "SELECT 0" {
		t.Fatalf("A saw a row inserted after its snapshot: tag=%q", tag)
	}
	if tag := findTag(a.query(`SELECT * FROM users`)); tag != "SELECT 1" {
		t.Fatalf("snapshot scan = %q, want SELECT 1", tag)
	}
	queryOK(t, a, `ROLLBACK`, "ROLLBACK")

	// A fresh read sees the latest committed state, both versions intact.
	if got := selectRow(t, a, `SELECT name FROM users WHERE id = 1`); string(got[0]) != "v1" {
		t.Fatalf("latest read = %q, want v1", got[0])
	}
	if tag := findTag(a.query(`SELECT * FROM users`)); tag != "SELECT 2" {
		t.Fatalf("latest scan = %q, want SELECT 2", tag)
	}
}

// TestMVCCAutocommitWriteConflictsWithBlock checks the frame-timestamp conflict
// oracle: an autocommit UPDATE never records into the node-local commit map, so
// only the version it appends can tell a concurrent block that the key changed
// under it.
func TestMVCCAutocommitWriteConflictsWithBlock(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	a := dialServer(t, srv.Addr())
	a.startupTrust("default")
	b := dialServer(t, srv.Addr())
	b.startupTrust("default")

	queryOK(t, a, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")
	queryOK(t, a, `INSERT INTO users (id, name) VALUES (1, 'v0')`, "INSERT 0 1")

	// A reads the row inside a block.
	queryOK(t, a, `BEGIN`, "BEGIN")
	selectRow(t, a, `SELECT name FROM users WHERE id = 1`)

	// B changes the same row in autocommit, appending a version rather than
	// going through the transaction commit path.
	queryOK(t, b, `UPDATE users SET name = 'v1' WHERE id = 1`, "UPDATE 1")

	// A writes a different row and commits: its read is stale, so it aborts.
	queryOK(t, a, `INSERT INTO users (id, name) VALUES (2, 'x')`, "INSERT 0 1")
	frames := a.query(`COMMIT`)
	if code, _, ok := findError(frames); !ok || code != errSerializationFailure {
		t.Fatalf("COMMIT over an autocommit write: code=%q ok=%v, want %s", code, ok, errSerializationFailure)
	}
	if got := selectRow(t, b, `SELECT name FROM users WHERE id = 1`); string(got[0]) != "v1" {
		t.Fatalf("autocommit writer's value lost: %q", got[0])
	}
	if tag := findTag(b.query(`SELECT id FROM users WHERE id = 2`)); tag != "SELECT 0" {
		t.Fatalf("aborted block's write landed: tag=%q", tag)
	}
}

// TestMVCCReadYourWrites checks that a block sees its own buffered catalog write
// before commit, and that ROLLBACK discards it, leaving the prior version.
func TestMVCCReadYourWrites(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO users (id, name) VALUES (1, 'v0')`, "INSERT 0 1")

	queryOK(t, cl, `BEGIN`, "BEGIN")
	queryOK(t, cl, `UPDATE users SET name = 'v1' WHERE id = 1`, "UPDATE 1")
	if got := selectRow(t, cl, `SELECT name FROM users WHERE id = 1`); string(got[0]) != "v1" {
		t.Fatalf("read-your-writes = %q, want v1", got[0])
	}
	queryOK(t, cl, `ROLLBACK`, "ROLLBACK")
	if got := selectRow(t, cl, `SELECT name FROM users WHERE id = 1`); string(got[0]) != "v0" {
		t.Fatalf("value after rollback = %q, want v0", got[0])
	}
}

// TestTxnCommitVisibility checks that a committed block's writes are visible to
// a later session and that a rolled-back block's are not.
func TestTxnCommitVisibility(t *testing.T) {
	srv, store := newTestServer(t, srvOpts{})
	writer := dialServer(t, srv.Addr())
	writer.startupTrust("default")
	reader := dialServer(t, srv.Addr())
	reader.startupTrust("default")

	queryOK(t, writer, `BEGIN`, "BEGIN")
	queryOK(t, writer, `INSERT INTO tellstone (key, value) VALUES ('c', '1')`, "INSERT 0 1")
	queryOK(t, writer, `COMMIT`, "COMMIT")
	if v, ok, _ := store.GetErr("c"); !ok || string(v) != "1" {
		t.Fatalf("committed write not visible: %q ok=%v", v, ok)
	}
	if tag := findTag(reader.query(`SELECT * FROM tellstone WHERE key = 'c'`)); tag != "SELECT 1" {
		t.Fatalf("later session cannot see committed row: tag=%q", tag)
	}

	queryOK(t, writer, `BEGIN`, "BEGIN")
	queryOK(t, writer, `DELETE FROM tellstone WHERE key = 'c'`, "DELETE 1")
	queryOK(t, writer, `ROLLBACK`, "ROLLBACK")
	if _, ok, _ := store.GetErr("c"); !ok {
		t.Fatalf("rolled-back delete removed the row")
	}
}
