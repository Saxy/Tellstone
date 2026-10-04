package sql

import (
	"strings"
	"testing"
)

/*
Tests for DML against a table created through the wire protocol.

The regression this file exists for: a DML statement naming any table other than
the implicit one was answered with 42P01 "relation does not exist" from a
hardcoded name check in the translator, no matter what the catalog said. The
table existed. The error was a lie, and a client that trusted it would conclude
its CREATE TABLE had failed.

These tests therefore check the whole round trip over the socket rather than the
translator in isolation, since the bug lived in the seam between the two: a
statement that is accepted, described, executed and read back consistently.
*/

// queryOK runs a statement and fails unless it succeeded, returning its tag.
func queryOK(t *testing.T, cl *tclient, q, wantTag string) []frame {
	t.Helper()
	frames := cl.query(q)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("%s: unexpected error %s %s", q, code, msg)
	}
	if tag := findTag(frames); tag != wantTag {
		t.Fatalf("%s: tag = %q, want %q", q, tag, wantTag)
	}
	return frames
}

// queryErr runs a statement and fails unless it failed with wantCode.
func queryErr(t *testing.T, cl *tclient, q, wantCode string) string {
	t.Helper()
	frames := cl.query(q)
	code, msg, ok := findError(frames)
	if !ok {
		t.Fatalf("%s: expected %s, but the statement succeeded", q, wantCode)
	}
	if code != wantCode {
		t.Fatalf("%s: code = %s (%s), want %s", q, code, msg, wantCode)
	}
	return msg
}

// selectRow runs a single-row SELECT and returns its cells.
func selectRow(t *testing.T, cl *tclient, q string) [][]byte {
	t.Helper()
	frames := queryOK(t, cl, q, "SELECT 1")
	data := findFrame(frames, msgDataRow)
	if data == nil {
		t.Fatalf("%s: SELECT 1 returned no DataRow (types %v)", q, frameTypes(frames))
	}
	row, err := parseDataRow(data.val)
	if err != nil {
		t.Fatalf("%s: parseDataRow: %v", q, err)
	}
	return row
}

func TestCatalogTableDMLOverSimpleQuery(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text, age int)`, "CREATE TABLE")

	// INSERT, then read the row back through both projections.
	queryOK(t, cl, `INSERT INTO users (id, name, age) VALUES (1, 'ada', 36)`, "INSERT 0 1")

	all := selectRow(t, cl, `SELECT * FROM users WHERE id = 1`)
	if len(all) != 3 || string(all[0]) != "1" || string(all[1]) != "ada" || string(all[2]) != "36" {
		t.Fatalf("SELECT * = %q, want [1 ada 36]", all)
	}

	// A projection must return the columns in the order asked for, not in
	// schema or key order. Key order is lexical, so a schema of
	// (id, name, age) scans back age, id, name.
	proj := selectRow(t, cl, `SELECT name, age FROM users WHERE id = 1`)
	if len(proj) != 2 || string(proj[0]) != "ada" || string(proj[1]) != "36" {
		t.Fatalf("projection = %q, want [ada 36]", proj)
	}

	queryOK(t, cl, `UPDATE users SET age = 37 WHERE id = 1`, "UPDATE 1")
	after := selectRow(t, cl, `SELECT age FROM users WHERE id = 1`)
	if len(after) != 1 || string(after[0]) != "37" {
		t.Fatalf("after UPDATE age = %q, want [37]", after)
	}

	// An UPDATE must leave columns it did not name alone. Name is checked
	// because an implementation that rewrote the whole row would drop it.
	kept := selectRow(t, cl, `SELECT name, age FROM users WHERE id = 1`)
	if len(kept) != 2 || string(kept[0]) != "ada" || string(kept[1]) != "37" {
		t.Fatalf("after UPDATE name,age = %q, want [ada 37]", kept)
	}

	queryOK(t, cl, `DELETE FROM users WHERE id = 1`, "DELETE 1")
	// The row's key range is gone, so a re-read finds nothing. This is the
	// assertion that the delete swept every column, not just the key.
	frames := cl.query(`SELECT * FROM users WHERE id = 1`)
	if findFrame(frames, msgDataRow) != nil {
		t.Fatal("row still readable after DELETE")
	}
	if tag := findTag(frames); tag != "SELECT 0" {
		t.Fatalf("post-DELETE SELECT tag = %q", tag)
	}
}

// TestCatalogTableMissingIsRealNotGuessed is the direct regression test. A table
// that does not exist must be reported missing by something that looked, and a
// table that does exist must never be reported missing by a name check.
func TestCatalogTableMissingIsRealNotGuessed(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	// Before it is created, it is genuinely missing.
	queryErr(t, cl, `SELECT * FROM users WHERE id = 1`, errUndefinedTable)
	queryErr(t, cl, `INSERT INTO users (id, name) VALUES (1, 'x')`, errUndefinedTable)
	queryErr(t, cl, `UPDATE users SET name = 'x' WHERE id = 1`, errUndefinedTable)
	queryErr(t, cl, `DELETE FROM users WHERE id = 1`, errUndefinedTable)

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	// Now it exists, so the same statements must not claim it is missing.
	// The check that matters is that none of them returns 42P01.
	for _, q := range []string{
		`SELECT * FROM users WHERE id = 1`,
		`INSERT INTO users (id, name) VALUES (1, 'ada')`,
		`UPDATE users SET name = 'ada' WHERE id = 1`,
		`DELETE FROM users WHERE id = 1`,
	} {
		frames := cl.query(q)
		if code, msg, ok := findError(frames); ok {
			if code == errUndefinedTable {
				t.Fatalf("%s: table exists but was reported missing: %s", q, msg)
			}
		}
	}

	// A table that never existed is still reported, and the name in the message
	// is the one the client asked about rather than a placeholder.
	msg := queryErr(t, cl, `SELECT * FROM ghost WHERE id = 1`, errUndefinedTable)
	if !strings.Contains(msg, "ghost") {
		t.Fatalf("undefined-table message does not name the table: %q", msg)
	}
}

// TestCatalogTableRowIdentity covers the rules that decide which row a
// statement means, and that two spellings of one value are one row.
func TestCatalogTableRowIdentity(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE t (id bigint PRIMARY KEY, v text)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO t (id, v) VALUES (7, 'seven')`, "INSERT 0 1")

	// A typed key is canonicalized, so 007, 7 and +7 all name row 7 rather
	// than three rows. Without this a scan would merge rows the client
	// considers identical.
	for _, spelling := range []string{"7", "007", "+7"} {
		row := selectRow(t, cl, `SELECT v FROM t WHERE id = `+spelling)
		if len(row) != 1 || string(row[0]) != "seven" {
			t.Fatalf("id = %s read %q, want [seven]", spelling, row)
		}
	}
	// A duplicate on the canonical value is a real duplicate.
	queryErr(t, cl, `INSERT INTO t (id, v) VALUES (007, 'other')`, errDuplicateKey)
	// A value the column's type cannot hold is rejected where it is named.
	queryErr(t, cl, `SELECT v FROM t WHERE id = 'abc'`, errSyntax)
	queryErr(t, cl, `INSERT INTO t (id, v) VALUES (99999999999999999999, 'x')`, errSyntax)

	// A filter on a non-key column is planned -- as a full scan, which is what
	// it needs -- and then refused, because this phase can return only one row
	// (ADR-014 decision 3). The message says which plan was refused and which
	// phase is coming, so a client can tell an incomplete engine from a wrong
	// query.
	msg := queryErr(t, cl, `SELECT v FROM t WHERE v = 'seven'`, errFeatureNotSupported)
	for _, want := range []string{"FullScan", "Phase 11"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("non-key filter message should mention %q: %q", want, msg)
		}
	}
	queryErr(t, cl, `UPDATE t SET v = 'x' WHERE v = 'seven'`, errFeatureNotSupported)
	queryErr(t, cl, `DELETE FROM t WHERE v = 'seven'`, errFeatureNotSupported)

	// A range on an integer key plans as a range scan and is refused the same
	// way. Asserting the plan appears in the message is what pins the fact that
	// the query was planned at all -- a blanket refusal would pass the checks
	// above too.
	msg = queryErr(t, cl, `SELECT v FROM t WHERE id > 1 AND id < 100`, errFeatureNotSupported)
	if !strings.Contains(msg, "RangeScan") {
		t.Fatalf("integer range message should name the plan: %q", msg)
	}

	// A column the table does not have is a column error, not a missing table.
	queryErr(t, cl, `SELECT nosuch FROM t WHERE id = 7`, errUndefinedColumn)
	queryErr(t, cl, `INSERT INTO t (id, nosuch) VALUES (8, 'x')`, errUndefinedColumn)

	// Statements that address a row without a key are refused, not widened
	// into a full-table operation.
	// A statement with no filter is still refused, and still not widened into a
	// whole-table delete. The planner now classifies it as a full scan and
	// execution refuses it, so the code moved from 42601 to 0A000 -- and the
	// message naming the plan is stronger evidence than the old code was,
	// because "FullScan" shows the unfiltered statement was recognised as one
	// rather than quietly executed.
	msg = queryErr(t, cl, `DELETE FROM t`, errFeatureNotSupported)
	if !strings.Contains(msg, "FullScan") {
		t.Fatalf("unfiltered DELETE should be planned as a full scan: %q", msg)
	}
	// A range predicate now *plans* -- the optimizer recognises it, and an
	// integer key is range-scannable since row ids became order-preserving --
	// and is refused at execution instead, because this phase returns one row
	// (ADR-014 decision 3). The code moved from 42601 to 0A000 as a result: this
	// is no longer a statement the translator cannot express.
	msg = queryErr(t, cl, `SELECT v FROM t WHERE id > 1`, errFeatureNotSupported)
	if !strings.Contains(msg, "RangeScan") {
		t.Fatalf("open-ended range should plan as a range scan: %q", msg)
	}
}

func TestCatalogTableNullsAndDefaults(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE t (id bigint PRIMARY KEY, req text NOT NULL, opt text, def text DEFAULT 'fallback')`, "CREATE TABLE")

	// An omitted column takes its default; an explicit NULL on a nullable
	// column stays NULL, which on the wire is -1 length.
	queryOK(t, cl, `INSERT INTO t (id, req) VALUES (1, 'r')`, "INSERT 0 1")
	row := selectRow(t, cl, `SELECT req, opt, def FROM t WHERE id = 1`)
	if len(row) != 3 || string(row[0]) != "r" {
		t.Fatalf("req = %q", row[0])
	}
	if row[1] != nil {
		t.Fatalf("omitted nullable column = %q, want NULL", row[1])
	}
	if string(row[2]) != "fallback" {
		t.Fatalf("default = %q, want fallback", row[2])
	}

	// NULL is absence, not an empty value: an explicit NULL and an empty
	// string must stay distinguishable, or a client can never clear a column
	// back to NULL.
	queryOK(t, cl, `INSERT INTO t (id, req, opt) VALUES (2, 'r', '')`, "INSERT 0 1")
	empty := selectRow(t, cl, `SELECT opt FROM t WHERE id = 2`)
	if len(empty) != 1 || string(empty[0]) != "" {
		t.Fatalf("empty string = %q, want empty (not NULL)", empty[0])
	}
	// Setting a column back to NULL must clear it.
	queryOK(t, cl, `UPDATE t SET opt = NULL WHERE id = 2`, "UPDATE 1")
	cleared := selectRow(t, cl, `SELECT opt FROM t WHERE id = 2`)
	if len(cleared) != 1 || cleared[0] != nil {
		t.Fatalf("after NULL update = %q, want NULL", cleared[0])
	}

	// NOT NULL is enforced, including against a default fallback that is not
	// there to paper over a missing value.
	queryErr(t, cl, `INSERT INTO t (id, opt) VALUES (3, 'x')`, errNotNullViolation)
	queryErr(t, cl, `INSERT INTO t (id, req) VALUES (4, NULL)`, errNotNullViolation)
	queryErr(t, cl, `UPDATE t SET req = NULL WHERE id = 1`, errNotNullViolation)

	// The primary key cannot be NULL or empty.
	queryErr(t, cl, `INSERT INTO t (id, req) VALUES (NULL, 'x')`, errNotNullViolation)
}

// TestCatalogTableColumnTypes proves each declared type survives the round trip
// through the wire, the store encoding and back.
func TestCatalogTableColumnTypes(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE t (id text PRIMARY KEY, a bigint, b int, c float, d bool, e bytea, f jsonb, g timestamp)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO t (id, a, b, c, d, e, f, g) VALUES ('k', 9007199254740993, 42, 1.5, true, '\x0102ff', '{"a":1}', '2024-01-02 03:04:05')`, "INSERT 0 1")

	row := selectRow(t, cl, `SELECT a, b, c, d, e, f, g FROM t WHERE id = 'k'`)
	want := []string{"9007199254740993", "42", "1.5", "t", `\x0102ff`, `{"a":1}`, "2024-01-02 03:04:05+00"}
	if len(row) != len(want) {
		t.Fatalf("got %d columns, want %d", len(row), len(want))
	}
	for i, w := range want {
		if string(row[i]) != w {
			t.Errorf("col %d = %q, want %q", i, row[i], w)
		}
	}

	// The RowDescription must carry the schema's types, or a client decodes
	// these bytes against the wrong type OIDs.
	frames := cl.query(`SELECT a, b, c, d, e, f, g FROM t WHERE id = 'k'`)
	rd := findFrame(frames, msgRowDescription)
	if rd == nil {
		t.Fatalf("no RowDescription (types %v)", frameTypes(frames))
	}
	assertRowDescription(t, rd.val,
		[]string{"a", "b", "c", "d", "e", "f", "g"},
		[]int32{oidInt8, oidInt4, oidFloat8, oidBool, oidBytea, oidJSONB, oidTimestamptz})

	// A value that does not parse as its column's type is rejected at write
	// time. Storing it would defer the failure to the next read, which is how
	// corrupt values get into the store.
	queryErr(t, cl, `INSERT INTO t (id, a) VALUES ('x', 'notanumber')`, errSyntax)
}

// TestCatalogTableExtendedProtocol covers the same lifecycle over the extended
// protocol, where a statement is parsed, described and bound separately. The
// Describe step happens before execution, so it is where a projection that
// depends on the schema must already be resolvable.
func TestCatalogTableExtendedProtocol(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	// Describe a parameterized SELECT: the parameter and result types must come
	// from the schema.
	cl.send(msgParse, concat(cstring("sel"), cstring(`SELECT name FROM users WHERE id = $1`), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgDescribe, concat([]byte{'S'}, cstring("sel")))
	pd := cl.expectLine(msgParameterDesc)
	assertParameterDescription(t, pd.val, []int32{oidInt8})
	rd := cl.expectLine(msgRowDescription)
	assertRowDescription(t, rd.val, []string{"name"}, []int32{oidText})
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	// Bind and execute with the key as a parameter.
	// Bind: portal, statement, 0 parameter format codes, 1 parameter whose value
	// is the one byte "1", then 0 result format codes. Format codes live in the
	// list ahead of the values, not beside them.
	cl.send(msgBind, concat(cstring(""), cstring("sel"), two(0), two(1), four(1), []byte("1"), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	cl.expectLine(msgRowDescription)
	ec := cl.expectLine(msgCommandComplete)
	if tag, _, _ := consumeCString(ec.val); tag != "SELECT 0" {
		t.Fatalf("tag = %q, want SELECT 0", tag)
	}
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	// Parameterized INSERT, then read it back.
	cl.send(msgParse, concat(cstring("ins"), cstring(`INSERT INTO users (id, name) VALUES ($1, $2)`), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgBind, concat(cstring(""), cstring("ins"), two(0), two(2),
		four(1), []byte("1"), four(3), []byte("ada"), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	ec = cl.expectLine(msgCommandComplete)
	if tag, _, _ := consumeCString(ec.val); tag != "INSERT 0 1" {
		t.Fatalf("INSERT tag = %q", tag)
	}
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	row := selectRow(t, cl, `SELECT name FROM users WHERE id = 1`)
	if len(row) != 1 || string(row[0]) != "ada" {
		t.Fatalf("row = %q, want [ada]", row)
	}

	// A NULL parameter must be honored as NULL, not as an empty value.
	cl.send(msgParse, concat(cstring("nul"), cstring(`INSERT INTO users (id, name) VALUES ($1, $2)`), two(0)))
	cl.expectLine(msgParseComplete)
	// A NULL parameter is a length of -1 and nothing else: no data bytes follow.
	cl.send(msgBind, concat(cstring(""), cstring("nul"), two(0), two(2),
		four(1), []byte("2"), four(-1), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	cl.expectLine(msgCommandComplete)
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	nullRow := selectRow(t, cl, `SELECT name FROM users WHERE id = 2`)
	if len(nullRow) != 1 || nullRow[0] != nil {
		t.Fatalf("NULL parameter = %q, want NULL", nullRow[0])
	}
}

// TestCatalogTableDropRefusesNonEmptyTable proves DROP and DML agree: a table
// with rows is not silently removed out from under them, because that would
// leave orphaned keys no schema describes.
func TestCatalogTableDropRefusesNonEmptyTable(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE t (id text PRIMARY KEY)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO t (id) VALUES ('a')`, "INSERT 0 1")
	queryErr(t, cl, `DROP TABLE t`, errFeatureNotSupported)

	queryOK(t, cl, `DELETE FROM t WHERE id = 'a'`, "DELETE 1")
	queryOK(t, cl, `DROP TABLE t`, "DROP TABLE")
	// Once dropped, the table is missing again -- proven by the catalog, not
	// by a name.
	queryErr(t, cl, `SELECT * FROM t WHERE id = 'a'`, errUndefinedTable)
}
