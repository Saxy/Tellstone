/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: stageb_test.go
Description: ADR-016 Stage B -- ORDER BY, DISTINCT, LIMIT/OFFSET, output
aliases, and system-catalog relation names.

The transcript harness (transcript_test.go) is what decides which statements
this engine has to accept; this file pins the semantics of the ones it accepted,
so a later change to sorting, NULL ordering, folding or bounding fails here
rather than in a client. Each expectation is PostgreSQL's: numbers order by
value rather than by their wire text, NULLs go last ascending and first
descending, a sort key the projection does not return is read and not sent, and
a command tag counts the rows the client actually receives.
*/
package sql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// TestStageBSelectModifiers exercises the SELECT clauses Stage B adds, against a
// table whose rows are deliberately neither in key order nor in value order.
func TestStageBSelectModifiers(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE m (id bigint PRIMARY KEY, v text)`, "CREATE TABLE")
	for _, row := range []struct {
		id int
		v  string
	}{{1, "b"}, {2, "b"}, {3, "c"}, {4, "a"}, {9, "z"}, {10, ""}} {
		if row.v == "" {
			queryOK(t, cl, fmt.Sprintf(`INSERT INTO m (id, v) VALUES (%d, NULL)`, row.id), "INSERT 0 1")
			continue
		}
		queryOK(t, cl, fmt.Sprintf(`INSERT INTO m (id, v) VALUES (%d, '%s')`, row.id, row.v), "INSERT 0 1")
	}

	// Ordering compares the declared type, so 10 sorts after 9 even though the
	// wire text "10" sorts before "9".
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m ORDER BY id DESC`, 6),
		"10", "9", "4", "3", "2", "1")

	// A sort key the projection does not return is read from the row and never
	// sent: v is ordered by, and the result is still one column wide. ASC puts
	// NULL last, and equal keys keep their scan order.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m ORDER BY v, id`, 6),
		"4", "1", "2", "3", "9", "10")

	// DESC puts NULL first, and the ties among the two 'b' rows stay put.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m ORDER BY v DESC`, 6),
		"10", "9", "3", "1", "2", "4")

	// An explicit NULLS FIRST overrides the default for ascending order.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m ORDER BY v NULLS FIRST`, 6),
		"10", "4", "1", "2", "3", "9")

	// A qualified reference answers to the statement's alias.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m AS x ORDER BY x.id DESC LIMIT 2`, 2),
		"10", "9")

	// An ordinal is a position in the select list, and the aliased name below is
	// what the client is told the column is called.
	names, rows := queryNames(t, cl, `SELECT id AS i FROM m ORDER BY i DESC LIMIT 3`, 3)
	if len(names) != 1 || names[0] != "i" {
		t.Fatalf("RowDescription names = %q, want [i]", names)
	}
	wantIDs(t, rows, "10", "9", "4")

	// LIMIT and OFFSET bound the ordered result, and the tag counts what the
	// client received rather than what the scan produced.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m ORDER BY id LIMIT 3 OFFSET 2`, 3),
		"3", "4", "9")

	// A written zero bound is a bound: no rows, and a SELECT 0 tag.
	if rows := selectRows(t, cl, `SELECT id FROM m ORDER BY id LIMIT 0`, 0); len(rows) != 0 {
		t.Fatalf("LIMIT 0 returned %d rows, want 0", len(rows))
	}

	// DISTINCT folds equal cells together, keeping the first row's position in
	// the scan; the NULL cell folds into itself. Ordering afterwards puts NULL
	// last, as ASC does.
	wantIDs(t, selectRows(t, cl, `SELECT DISTINCT v FROM m`, 5), "b", "c", "a", "z", "NULL")
	wantIDs(t, selectRows(t, cl, `SELECT DISTINCT v FROM m ORDER BY v`, 5), "a", "b", "c", "z", "NULL")

	// The point-lookup path carries the same clauses as a scan does.
	names, rows = queryNames(t, cl, `SELECT v FROM m WHERE id = 4 ORDER BY v`, 1)
	if len(names) != 1 || names[0] != "v" {
		t.Fatalf("lookup RowDescription names = %q, want [v]", names)
	}
	if string(rows[0][0]) != "a" {
		t.Fatalf("lookup row = %q, want a", rows[0])
	}
	wantIDs(t, selectRows(t, cl, `SELECT id FROM m WHERE id = 9 ORDER BY id DESC`, 1), "9")
	// OFFSET past a single matched row empties the result rather than being
	// dropped as meaningless for a lookup.
	if rows := selectRows(t, cl, `SELECT id FROM m WHERE id = 9 OFFSET 1`, 0); len(rows) != 0 {
		t.Fatalf("point lookup with OFFSET returned %d rows, want 0", len(rows))
	}

	// DISTINCT may not order by a column it did not select, which is refused
	// with PostgreSQL's feature-not-supported rather than sorted wrongly.
	queryErr(t, cl, `SELECT DISTINCT v FROM m ORDER BY id`, errFeatureNotSupported)

	// An ORDER BY that is not a column or a position is refused at translation,
	// as is a modifier on a FROM-less SELECT -- both would otherwise be dropped.
	queryErr(t, cl, `SELECT id FROM m ORDER BY lower(v)`, errSyntax)
	queryErr(t, cl, `SELECT 1 ORDER BY 1`, errSyntax)
	queryErr(t, cl, `SELECT 1 LIMIT 1`, errSyntax)
}

// TestStageBJoinModifiers runs the same clauses down the join path, where the
// input row is the merged row of both tables and a sort key may belong to
// either side.
func TestStageBJoinModifiers(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE l (id bigint PRIMARY KEY, w text)`, "CREATE TABLE")
	queryOK(t, cl, `CREATE TABLE r (id bigint PRIMARY KEY, w text)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO l (id, w) VALUES (1, 'x')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO l (id, w) VALUES (2, 'y')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO r (id, w) VALUES (1, 'p')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO r (id, w) VALUES (2, 'q')`, "INSERT 0 1")

	// A key from the right side the projection does not return is read from the
	// merged row and hidden, exactly as on a single table.
	wantIDs(t, selectRows(t, cl, `SELECT l.id FROM l JOIN r ON l.id = r.id ORDER BY l.w DESC`, 2),
		"2", "1")
	// A key the projection does return is ordered in place, and both cells of
	// the projected row are sent.
	joined := selectRows(t, cl, `SELECT l.id, r.w FROM l JOIN r ON l.id = r.id ORDER BY r.w DESC`, 2)
	if len(joined[0]) != 2 || string(joined[0][0]) != "2" || string(joined[0][1]) != "q" ||
		string(joined[1][0]) != "1" || string(joined[1][1]) != "p" {
		t.Fatalf("join rows = %q, want [2 q] then [1 p]", joined)
	}
	// A qualified key and a bare one both resolve against the joined names.
	wantIDs(t, selectRows(t, cl, `SELECT l.id FROM l JOIN r ON l.id = r.id ORDER BY id LIMIT 1`, 1),
		"1")
	// A qualifier naming neither side is the missing FROM-clause entry.
	queryErr(t, cl, `SELECT l.id FROM l JOIN r ON l.id = r.id ORDER BY z.id`, errUndefinedTable)
}

// TestStageBSystemCatalogRelations pins the change Stage B makes to system
// catalog names: they are accepted and planned rather than refused as shapes.
// Stage C gives the relations their contents, so the statements now answer;
// what they answer is pinned in stagec_test.go.
func TestStageBSystemCatalogRelations(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryNoErr(t, cl, `SELECT relname FROM pg_catalog.pg_class ORDER BY relname`)
	queryNoErr(t, cl, `SELECT table_name FROM information_schema.tables LIMIT 10`)
	queryNoErr(t, cl, `SELECT n.nspname FROM pg_catalog.pg_namespace n ORDER BY n.nspname`)
	queryNoErr(t, cl, `SELECT c.relname, n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid ORDER BY 1`)
	// An unqualified system catalog name is a relation too, not a shape refusal.
	queryNoErr(t, cl, `SELECT relname FROM pg_class`)
}

// TestStageBAggregates exercises count, sum, min, max and avg with and without
// GROUP BY and HAVING, on the scan path and the point-lookup path, pinning the
// wire types and the empty-input row PostgreSQL returns.
func TestStageBAggregates(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE g (id bigint PRIMARY KEY, k text, n bigint)`, "CREATE TABLE")
	for _, row := range []struct {
		id int
		k  string
		n  string
	}{{1, "a", "10"}, {2, "a", "20"}, {3, "b", "30"}, {4, "b", "NULL"}, {5, "c", "50"}} {
		if row.n == "NULL" {
			queryOK(t, cl, fmt.Sprintf(`INSERT INTO g (id, k, n) VALUES (%d, '%s', NULL)`, row.id, row.k), "INSERT 0 1")
			continue
		}
		queryOK(t, cl, fmt.Sprintf(`INSERT INTO g (id, k, n) VALUES (%d, '%s', %s)`, row.id, row.k, row.n), "INSERT 0 1")
	}

	// An ungrouped aggregate folds the whole input into one row; count(*) is
	// bigint and is announced as such.
	frames := cl.query(`SELECT count(*), sum(n), min(n), max(n), avg(n) FROM g`)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("aggregates: unexpected error %s %s", code, msg)
	}
	if tag := findTag(frames); tag != "SELECT 1" {
		t.Fatalf("aggregates tag = %q, want SELECT 1", tag)
	}
	fd := findFrame(frames, msgRowDescription)
	oids := rowDescOIDs(t, fd.val)
	want := []int32{oidInt8, oidInt8, oidInt8, oidInt8, oidFloat8}
	for i := range want {
		if oids[i] != want[i] {
			t.Fatalf("aggregate oid[%d] = %d, want %d", i, oids[i], want[i])
		}
	}
	row := selectRow(t, cl, `SELECT count(*), sum(n), min(n), max(n), avg(n) FROM g`)
	if got := []string{string(row[0]), string(row[1]), string(row[2]), string(row[3]), string(row[4])}; fmt.Sprint(got) != "[5 110 10 50 27.5]" {
		t.Fatalf("aggregate row = %v, want [5 110 10 50 27.5]", got)
	}
	// The names the client is told are the function names.
	names, _ := queryNames(t, cl, `SELECT count(*) FROM g`, 1)
	if len(names) != 1 || names[0] != "count" {
		t.Fatalf("count RowDescription names = %q, want [count]", names)
	}
	// count(column) counts non-NULL cells; the others skip them too.
	wantIDs(t, selectRows(t, cl, `SELECT count(n) FROM g`, 1), "4")
	wantIDs(t, selectRows(t, cl, `SELECT count(k) FROM g`, 1), "5")

	// GROUP BY folds per key in first-appearance order, and a column the
	// projection returns must be one of the keys.
	grouped := selectRows(t, cl, `SELECT k, count(*), sum(n) FROM g GROUP BY k ORDER BY k`, 3)
	if fmt.Sprint(string(grouped[0][0]), string(grouped[0][1]), string(grouped[0][2])) != "a230" ||
		fmt.Sprint(string(grouped[1][0]), string(grouped[1][1]), string(grouped[1][2])) != "b230" ||
		fmt.Sprint(string(grouped[2][0]), string(grouped[2][1]), string(grouped[2][2])) != "c150" {
		t.Fatalf("grouped rows = %q", grouped)
	}
	// The grouping column may be one the projection does not return.
	aggOnly := selectRows(t, cl, `SELECT count(*) FROM g GROUP BY k ORDER BY count(*)`, 3)
	wantIDs(t, aggOnly, "1", "2", "2")
	// An ordinal groups by the select-list position.
	wantIDs(t, selectRows(t, cl, `SELECT k FROM g GROUP BY 1 ORDER BY 1`, 3), "a", "b", "c")
	// HAVING filters groups after the fold, and an aggregate that appears only
	// in HAVING is computed but not projected.
	having := selectRows(t, cl, `SELECT k FROM g GROUP BY k HAVING count(*) > 1 ORDER BY k`, 2)
	wantIDs(t, having, "a", "b")
	queryNames(t, cl, `SELECT k FROM g GROUP BY k HAVING count(*) > 1`, 2)
	// A qualified grouping key resolves through the alias.
	wantIDs(t, selectRows(t, cl, `SELECT k FROM g AS t GROUP BY t.k ORDER BY t.k`, 3), "a", "b", "c")
	// GROUP BY folds the one row a point lookup addresses.
	pk := selectRow(t, cl, `SELECT k, count(*) FROM g WHERE id = 1 GROUP BY k`)
	if string(pk[0]) != "a" || string(pk[1]) != "1" {
		t.Fatalf("point-lookup group row = %q, want [a 1]", pk)
	}
	// A missing row still yields the ungrouped aggregate's one row.
	empty := selectRow(t, cl, `SELECT count(*), sum(n) FROM g WHERE id = 99`)
	if string(empty[0]) != "0" || empty[1] != nil {
		t.Fatalf("empty-input aggregate = %q, want [0 NULL]", empty)
	}
	// Grouping an empty input yields no rows at all.
	if rows := selectRows(t, cl, `SELECT k, count(*) FROM g WHERE id = 99 GROUP BY k`, 0); len(rows) != 0 {
		t.Fatalf("grouped empty input returned %d rows, want 0", len(rows))
	}

	// Refusals: a projected column that is neither grouped nor aggregated,
	// an expression in GROUP BY, and an aggregate over a non-numeric column
	// that cannot add.
	queryErr(t, cl, `SELECT k, id, count(*) FROM g GROUP BY k`, errGroupColumnRequired)
	queryErr(t, cl, `SELECT id, count(*) FROM g`, errGroupColumnRequired)
	queryErr(t, cl, `SELECT count(*) FROM g GROUP BY lower(k)`, errSyntax)
	queryErr(t, cl, `SELECT sum(k) FROM g`, errFeatureNotSupported)
	// count(DISTINCT ...) and casts inside an aggregate are refused.
	queryErr(t, cl, `SELECT count(DISTINCT k) FROM g`, errSyntax)
	queryErr(t, cl, `SELECT count(n::text) FROM g`, errSyntax)
	// The implicit table takes neither.
	queryErr(t, cl, `SELECT count(*) FROM tellstone WHERE key = 'a'`, errSyntax)
}

// TestStageBJoinAggregates folds a join's matched rows, where a grouping key
// or an aggregate argument may name either side of the join.
func TestStageBJoinAggregates(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE ja (id bigint PRIMARY KEY, k text)`, "CREATE TABLE")
	queryOK(t, cl, `CREATE TABLE jb (id bigint PRIMARY KEY, n bigint)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO ja (id, k) VALUES (1, 'a')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO ja (id, k) VALUES (2, 'a')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO ja (id, k) VALUES (3, 'b')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO jb (id, n) VALUES (1, 10)`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO jb (id, n) VALUES (2, 20)`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO jb (id, n) VALUES (3, 40)`, "INSERT 0 1")

	// An ungrouped aggregate over the join is one row; the aggregate's
	// argument comes from the right side and is never projected.
	row := selectRow(t, cl, `SELECT count(*), sum(n) FROM ja JOIN jb ON ja.id = jb.id`)
	if string(row[0]) != "3" || string(row[1]) != "70" {
		t.Fatalf("join aggregate row = %q, want [3 70]", row)
	}
	// GROUP BY a column from the left side, with an aggregate over the right.
	rows := selectRows(t, cl, `SELECT k, count(*), sum(n) FROM ja JOIN jb ON ja.id = jb.id GROUP BY k ORDER BY k`, 2)
	if string(rows[0][0]) != "a" || string(rows[0][1]) != "2" || string(rows[0][2]) != "30" ||
		string(rows[1][0]) != "b" || string(rows[1][1]) != "1" || string(rows[1][2]) != "40" {
		t.Fatalf("join grouped rows = %q", rows)
	}
	// A qualified grouping key and a HAVING over the fold both resolve against
	// the merged row.
	rows = selectRows(t, cl, `SELECT k FROM ja JOIN jb ON ja.id = jb.id GROUP BY ja.k HAVING count(*) >= 1 ORDER BY ja.k`, 2)
	if string(rows[0][0]) != "a" || string(rows[1][0]) != "b" {
		t.Fatalf("join having rows = %q", rows)
	}
	// A grouping column neither side declares is refused, as is a bare name
	// that both sides declare.
	queryErr(t, cl, `SELECT k, count(*) FROM ja JOIN jb ON ja.id = jb.id GROUP BY z`, errUndefinedColumn)
	queryOK(t, cl, `CREATE TABLE jc (id bigint PRIMARY KEY, n bigint)`, "CREATE TABLE")
	queryErr(t, cl, `SELECT id, count(*) FROM ja JOIN jc ON ja.id = jc.id GROUP BY id`, errAmbiguousColumn)
}

// TestStageBWhereOperators exercises the residual filter shapes DataGrip and
// JDBC send that are not a key equality: inequality, IN / NOT IN, and LIKE in
// both directions and every pattern shape. NULL semantics are part of what is
// pinned: a NULL column matches neither direction of an inequality, and NOT IN
// over a list containing NULL never matches at all.
func TestStageBWhereOperators(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE w (id bigint PRIMARY KEY, k text)`, "CREATE TABLE")
	for _, row := range []struct {
		id int
		k  string
	}{{1, "alpha"}, {2, "beta"}, {3, "NULL"}, {4, "pg_temp_x"}, {5, "a%b"}, {6, "gamma"}, {7, "pg_toast_q"}} {
		if row.k == "NULL" {
			queryOK(t, cl, fmt.Sprintf(`INSERT INTO w (id, k) VALUES (%d, NULL)`, row.id), "INSERT 0 1")
			continue
		}
		queryOK(t, cl, fmt.Sprintf(`INSERT INTO w (id, k) VALUES (%d, '%s')`, row.id, row.k), "INSERT 0 1")
	}

	// Inequality: the NULL cell is UNKNOWN and drops out, in both spellings and
	// from either operand side.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k <> 'alpha' ORDER BY id`, 5), "2", "4", "5", "6", "7")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k != 'alpha' ORDER BY id`, 5), "2", "4", "5", "6", "7")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE 'alpha' <> k ORDER BY id`, 5), "2", "4", "5", "6", "7")
	// Composed with a range, and over the primary key, which answers as a scan
	// rather than a point lookup: an inequality narrows no key.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k <> 'alpha' AND id > 3 ORDER BY id`, 4), "4", "5", "6", "7")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE id <> 1 ORDER BY id`, 6), "2", "3", "4", "5", "6", "7")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE id = 4 AND k <> 'zzz'`, 1), "4")

	// IN is the OR it means; NOT IN is the AND of inequalities, and both drop
	// the NULL column. NOT IN over a list containing NULL is never true, so it
	// answers zero rows rather than the rows that merely failed to match.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k IN ('beta', 'gamma') ORDER BY id`, 2), "2", "6")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k IN ('beta') ORDER BY id`, 1), "2")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k NOT IN ('alpha', 'beta') ORDER BY id`, 4), "4", "5", "6", "7")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k NOT IN ('alpha', NULL)`, 0))
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k IN (NULL)`, 0))

	// LIKE: a trailing wildcard answers as a prefix, a mid-pattern wildcard and
	// an underscore answer through the general matcher, and a backslash escapes
	// the byte after it, which is how a literal underscore is written.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k LIKE 'a%' ORDER BY id`, 2), "1", "5")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k LIKE 'a%b' ORDER BY id`, 1), "5")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k LIKE 'a_b' ORDER BY id`, 1), "5")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k LIKE '%t%' ORDER BY id`, 3), "2", "4", "7")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k LIKE 'pg\_temp%' ORDER BY id`, 1), "4")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k LIKE 'pg\_toast%' ORDER BY id`, 1), "7")
	// NOT LIKE excludes the prefix the pattern names and still drops the NULL
	// cell; reading it as LIKE would return exactly the complement's rows.
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k NOT LIKE 'pg%' ORDER BY id`, 4), "1", "2", "5", "6")
	wantIDs(t, selectRows(t, cl, `SELECT id FROM w WHERE k NOT LIKE '%t%' ORDER BY id`, 3), "1", "5", "6")

	// An inequality takes a bound parameter, and the parameter is reported in
	// ParameterDescription with its column's type -- which is what tells the
	// client to send it as text.
	cl.send(msgParse, concat(cstring("ne"), cstring(`SELECT id FROM w WHERE k <> $1 ORDER BY id`), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgDescribe, concat([]byte{'S'}, cstring("ne")))
	pd := cl.expectLine(msgParameterDesc)
	assertParameterDescription(t, pd.val, []int32{oidText})
	rd := cl.expectLine(msgRowDescription)
	assertRowDescription(t, rd.val, []string{"id"}, []int32{oidInt8})
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)
	cl.send(msgBind, concat(cstring(""), cstring("ne"), two(0), two(1), four(5), []byte("alpha"), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	all := cl.recvUntil(msgCommandComplete)
	if tag := findTag(all); tag != "SELECT 5" {
		t.Fatalf("bound <> tag = %q, want SELECT 5", tag)
	}
	var got []string
	for _, fr := range all {
		if fr.typ == msgDataRow {
			cells, err := parseDataRow(fr.val)
			if err != nil {
				t.Fatalf("parse data row: %v", err)
			}
			got = append(got, string(cells[0]))
		}
	}
	if fmt.Sprint(got) != "[2 4 5 6 7]" {
		t.Fatalf("bound <> rows = %v, want [2 4 5 6 7]", got)
	}
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	// Refusals: a pattern-matching operator that is not LIKE, a column the
	// table does not have (the filter is validated against the schema, for an
	// inequality as much as an equality), and the implicit table, which takes
	// only key equalities.
	queryErr(t, cl, `SELECT id FROM w WHERE k SIMILAR TO 'a'`, errSyntax)
	queryErr(t, cl, `SELECT id FROM w WHERE z <> 'a'`, errUndefinedColumn)
	queryErr(t, cl, `SELECT key FROM tellstone WHERE key <> 'a'`, errSyntax)
}

// TestStageBOuterJoins exercises LEFT, RIGHT and FULL JOIN against a pair of
// tables whose equality matches, misses, and meets NULL keys on both sides.
// What is pinned is PostgreSQL's row-preservation rule and the interaction that
// is easy to get backwards: the WHERE clause applies to the joined output, so a
// filter that rejects a matched pair does not re-emit it as an unmatched row,
// and `LEFT JOIN ... WHERE r.x IS NULL` reads the unmatched left rows.
func TestStageBOuterJoins(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE lo (id bigint PRIMARY KEY, k text)`, "CREATE TABLE")
	queryOK(t, cl, `CREATE TABLE ro (id bigint PRIMARY KEY, k text, n bigint)`, "CREATE TABLE")
	for _, ins := range []string{
		`INSERT INTO lo (id, k) VALUES (1, 'a')`,
		`INSERT INTO lo (id, k) VALUES (2, 'b')`,
		`INSERT INTO lo (id, k) VALUES (3, 'c')`,
		`INSERT INTO lo (id, k) VALUES (4, NULL)`,
		`INSERT INTO ro (id, k, n) VALUES (10, 'a', 100)`,
		`INSERT INTO ro (id, k, n) VALUES (20, 'b', 200)`,
		`INSERT INTO ro (id, k, n) VALUES (30, 'd', 300)`,
		`INSERT INTO ro (id, k, n) VALUES (40, NULL, 400)`,
	} {
		queryOK(t, cl, ins, "INSERT 0 1")
	}

	// pairs renders a two-cell result as "left/right", with a missing cell as
	// NULL: an outer join's rows are exactly which side has a value, so the
	// assertion has to see both at once.
	pairs := func(rows [][][]byte) []string {
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			l, rt := "NULL", "NULL"
			if r[0] != nil {
				l = string(r[0])
			}
			if r[1] != nil {
				rt = string(r[1])
			}
			out = append(out, l+"/"+rt)
		}
		return out
	}
	joinPairs := func(q string, want int) string {
		t.Helper()
		return fmt.Sprint(pairs(selectRows(t, cl, q, want)))
	}

	// INNER is the contrast for every outer case below: two matched pairs, and
	// neither the unmatched left rows nor either NULL key.
	if got := joinPairs(`SELECT lo.id, ro.id FROM lo JOIN ro ON lo.k = ro.k ORDER BY lo.id`, 2); got != "[1/10 2/20]" {
		t.Fatalf("inner rows = %s, want [1/10 2/20]", got)
	}
	// LEFT keeps every left row: the unmatched ones and the NULL key alike,
	// with the right half filled in only where the equality found a partner.
	if got := joinPairs(`SELECT lo.id, ro.id FROM lo LEFT JOIN ro ON lo.k = ro.k ORDER BY lo.id`, 4); got != "[1/10 2/20 3/NULL 4/NULL]" {
		t.Fatalf("left rows = %s, want [1/10 2/20 3/NULL 4/NULL]", got)
	}
	// RIGHT keeps every right row, including the NULL-key one the equality can
	// never match, with the left half NULLed where nothing matched.
	if got := joinPairs(`SELECT lo.id, ro.id FROM lo RIGHT JOIN ro ON lo.k = ro.k ORDER BY ro.id`, 4); got != "[1/10 2/20 NULL/30 NULL/40]" {
		t.Fatalf("right rows = %s, want [1/10 2/20 NULL/30 NULL/40]", got)
	}
	// FULL keeps both. The order of two NULL-left rows is not something to
	// pin, so the rows are compared as a set.
	full := pairs(selectRows(t, cl, `SELECT lo.id, ro.id FROM lo FULL OUTER JOIN ro ON lo.k = ro.k`, 6))
	sort.Strings(full)
	if got := fmt.Sprint(full); got != "[1/10 2/20 3/NULL 4/NULL NULL/30 NULL/40]" {
		t.Fatalf("full rows = %s, want the six preserved rows", got)
	}

	// The null-rejection idiom: WHERE over the preserved side reads the
	// unmatched left rows, because the filter sees NULLed cells rather than
	// being asked about them.
	if got := joinPairs(`SELECT lo.id, ro.id FROM lo LEFT JOIN ro ON lo.k = ro.k WHERE ro.id IS NULL ORDER BY lo.id`, 2); got != "[3/NULL 4/NULL]" {
		t.Fatalf("left-where-null rows = %s, want [3/NULL 4/NULL]", got)
	}
	// A filter that rejects a matched pair removes it; the left row does not
	// reappear as unmatched. Emitting it again would be a wrong answer that
	// looks like row preservation.
	if got := joinPairs(`SELECT lo.id, ro.id FROM lo LEFT JOIN ro ON lo.k = ro.k WHERE ro.n > 150 ORDER BY lo.id`, 1); got != "[2/20]" {
		t.Fatalf("left-where-filter rows = %s, want [2/20]", got)
	}

	// An aggregate folds the preserved rows: count(*) over a LEFT JOIN counts
	// every left row however the equality fared.
	row := selectRow(t, cl, `SELECT count(*) FROM lo LEFT JOIN ro ON lo.k = ro.k`)
	if string(row[0]) != "4" {
		t.Fatalf("left count(*) = %s, want 4", row[0])
	}
	// A grouped fold with a WHERE on the preserved side groups what survived,
	// and count(ro.n) counts only the rows a partner supplied.
	got := joinPairs(`SELECT lo.k, count(ro.n) FROM lo LEFT JOIN ro ON lo.k = ro.k WHERE lo.k IS NOT NULL GROUP BY lo.k ORDER BY lo.k`, 3)
	// pairs() renders the first cell only for these columns? No: it renders
	// both, as "key/count".
	if got != "[a/1 b/1 c/0]" {
		t.Fatalf("left grouped rows = %s, want [a/1 b/1 c/0]", got)
	}

	// EXPLAIN names the node by what it preserves, as PostgreSQL does.
	frames := cl.query(`EXPLAIN SELECT lo.id, ro.id FROM lo LEFT JOIN ro ON lo.k = ro.k`)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("explain: unexpected error %s %s", code, msg)
	}
	var explained bool
	for _, fr := range frames {
		if fr.typ == msgDataRow && bytes.Contains(fr.val, []byte("Hash Left Join")) {
			explained = true
		}
	}
	if !explained {
		t.Fatalf("explain output does not name a Hash Left Join: %q", frames)
	}

	// The shapes still refused: NATURAL and USING infer names, and CROSS JOIN
	// has no equality.
	queryErr(t, cl, `SELECT * FROM lo NATURAL JOIN ro`, errSyntax)
	queryErr(t, cl, `SELECT * FROM lo JOIN ro USING (k)`, errSyntax)
	queryErr(t, cl, `SELECT * FROM lo CROSS JOIN ro`, errSyntax)

	// A join whose left side is a join is the chain a client writes when it
	// joins three tables. It is planned as the chain it reads, and the left
	// side's preserved rows survive the second join: the rows the first join
	// preserved keep their NULL-extended partner, and the second join adds
	// its own.
	chained := selectRows(t, cl, `SELECT lo.id, ro.id, r2.id FROM lo LEFT JOIN ro ON lo.k = ro.k LEFT JOIN ro r2 ON lo.k = r2.k ORDER BY lo.id`, 4)
	want := []string{"1/10/10", "2/20/20", "3/NULL/NULL", "4/NULL/NULL"}
	for i, r := range chained {
		got := "NULL"
		if r[0] != nil {
			got = string(r[0])
		}
		for _, c := range r[1:] {
			if c == nil {
				got += "/NULL"
				continue
			}
			got += "/" + string(c)
		}
		if got != want[i] {
			t.Fatalf("chained row %d = %s, want %s", i, got, want[i])
		}
	}
}

// TestStageBCase exercises CASE target expressions: the searched and simple
// forms, each accepted literal result type, the result type PostgreSQL infers
// and reports in RowDescription, evaluation against columns the projection does
// not return, and the condition surface CASE shares with WHERE. The inferences
// pinned here are PostgreSQL 18's: an untyped string beside an integer parses
// as one (22P02 when it cannot), integer and boolean refuse to mix (42804), and
// an integer CASE is int4 unless a branch leaves int4's range.
func TestStageBCase(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE cs (id bigint PRIMARY KEY, k text, n bigint)`, "CREATE TABLE")
	for _, q := range []string{
		`INSERT INTO cs (id, k, n) VALUES (1, 'a', 10)`,
		`INSERT INTO cs (id, k, n) VALUES (2, 'b', 200)`,
		`INSERT INTO cs (id, k, n) VALUES (3, 'c', 5000000)`,
		`INSERT INTO cs (id, k, n) VALUES (4, NULL, NULL)`,
		`INSERT INTO cs (id, k, n) VALUES (5, 'a', 300)`,
	} {
		queryOK(t, cl, q, "INSERT 0 1")
	}

	// joined renders each row as its cells joined by "|", distinguishing NULL
	// from the empty string.
	joined := func(rows [][][]byte) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			parts := make([]string, len(r))
			for j, c := range r {
				if c == nil {
					parts[j] = "NULL"
				} else {
					parts[j] = string(c)
				}
			}
			out[i] = strings.Join(parts, "|")
		}
		return out
	}
	wantRows := func(q string, n int, want ...string) {
		t.Helper()
		if got := joined(selectRows(t, cl, q, n)); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s\n rows = %v, want %v", q, got, want)
		}
	}
	// describe parses and describes a statement, returning its column names and
	// type OIDs; it is how the result type PostgreSQL infers is pinned.
	describe := func(name, q string) ([]string, []int32) {
		t.Helper()
		cl.send(msgParse, concat(cstring(name), cstring(q), two(0)))
		cl.expectLine(msgParseComplete)
		cl.send(msgDescribe, concat([]byte{'S'}, cstring(name)))
		cl.expectLine(msgParameterDesc)
		rd := cl.expectLine(msgRowDescription)
		cl.send(msgSync, nil)
		cl.expectLine(msgReadyForQuery)
		return rowDescNames(t, rd.val), rowDescOIDs(t, rd.val)
	}

	// A searched CASE over a column the projection does not return: the
	// condition reads the row, the result is text, and a NULL condition falls
	// to ELSE because only a true condition selects its branch.
	wantRows(`SELECT id, CASE WHEN n > 100 THEN 'big' ELSE 'small' END AS size FROM cs ORDER BY id`, 5,
		"1|small", "2|big", "3|big", "4|small", "5|big")

	// No ELSE: an unmatched row answers NULL, not the empty string. The result
	// type still counts the THEN branch (PostgreSQL infers from every result,
	// taken or not), so this is an integer CASE.
	rows := selectRows(t, cl, `SELECT id, CASE WHEN n > 100000000 THEN 1 END FROM cs ORDER BY id`, 5)
	for i, r := range rows {
		if r[1] != nil {
			t.Fatalf("no-ELSE row %d = %q, want NULL", i, r[1])
		}
	}
	if names, oids := describe("cne", `SELECT CASE WHEN n > 100000000 THEN 1 END FROM cs`); fmt.Sprint(names) != "[case]" || fmt.Sprint(oids) != fmt.Sprint([]int32{oidInt4}) {
		t.Fatalf("no-ELSE CASE = %v %v, want [case] [23]", names, oids)
	}

	// Simple CASE: PostgreSQL defines it as the operand compared with each WHEN
	// value, so a NULL operand matches nothing and falls through to ELSE, and a
	// WHEN NULL can never match.
	wantRows(`SELECT id, CASE k WHEN 'a' THEN 'A' WHEN 'b' THEN 'B' ELSE '?' END AS v FROM cs ORDER BY id`, 5,
		"1|A", "2|B", "3|?", "4|?", "5|A")
	wantRows(`SELECT CASE k WHEN NULL THEN 'n' ELSE 'm' END FROM cs WHERE id = 4`, 1, "m")
	wantRows(`SELECT CASE k WHEN 'a' THEN 'A' END FROM cs WHERE id = 2`, 1, "NULL")

	// The conditions a client writes are WHERE's: IS NULL, LIKE, IN.
	wantRows(`SELECT id, CASE WHEN k IS NULL THEN 'n' ELSE 'p' END FROM cs ORDER BY id`, 5,
		"1|p", "2|p", "3|p", "4|n", "5|p")
	wantRows(`SELECT id, CASE WHEN k LIKE 'a%' THEN 1 ELSE 0 END FROM cs ORDER BY id`, 5,
		"1|1", "2|0", "3|0", "4|0", "5|1")
	wantRows(`SELECT id, CASE WHEN k IN ('b', 'c') THEN 'x' ELSE 'y' END FROM cs ORDER BY id`, 5,
		"1|y", "2|x", "3|x", "4|y", "5|y")

	// Boolean results encode as t/f and an ORDER BY on the CASE's alias reads
	// the computed value; the secondary key is a column the projection does not
	// return, so the chunk carries both a hidden sort key and a CASE cell.
	wantRows(`SELECT id, CASE WHEN n > 100 THEN true ELSE false END FROM cs ORDER BY id`, 5,
		"1|f", "2|t", "3|t", "4|f", "5|t")
	if names, oids := describe("cbool", `SELECT CASE WHEN n > 100 THEN true ELSE false END FROM cs`); fmt.Sprint(names) != "[case]" || fmt.Sprint(oids) != fmt.Sprint([]int32{oidBool}) {
		t.Fatalf("bool CASE = %v %v, want [case] [16]", names, oids)
	}
	if names, _ := describe("ctext", `SELECT CASE WHEN n > 100 THEN 'big' ELSE 'small' END AS size FROM cs`); fmt.Sprint(names) != "[size]" {
		t.Fatalf("aliased CASE name = %v, want [size]", names)
	}
	wantRows(`SELECT CASE WHEN n > 100 THEN 'big' ELSE 'small' END AS size FROM cs ORDER BY size, id`, 5,
		"big", "big", "big", "small", "small")

	// Integer CASE is int4 unless a branch leaves int4's range, in which case
	// PostgreSQL reports int8; an in-range 100000 stays int4.
	if _, oids := describe("cint", `SELECT CASE WHEN id = 1 THEN 100000 ELSE 0 END FROM cs`); fmt.Sprint(oids) != fmt.Sprint([]int32{oidInt4}) {
		t.Fatalf("int4 CASE oids = %v, want [23]", oids)
	}
	if _, oids := describe("cbig", `SELECT CASE WHEN id = 1 THEN 5000000000 ELSE 0 END FROM cs`); fmt.Sprint(oids) != fmt.Sprint([]int32{oidInt8}) {
		t.Fatalf("int8 CASE oids = %v, want [20]", oids)
	}
	wantRows(`SELECT CASE WHEN id = 1 THEN 2147483647 ELSE -2147483648 END FROM cs ORDER BY id`, 5,
		"2147483647", "-2147483648", "-2147483648", "-2147483648", "-2147483648")

	// The point-lookup path computes CASE too: the key equality picks one row
	// and the condition still reads a column the projection does not return.
	wantRows(`SELECT id, CASE WHEN n > 100 THEN 'big' ELSE 'small' END FROM cs WHERE id = 3`, 1, "3|big")
	wantRows(`SELECT id, CASE WHEN n > 100 THEN 'big' ELSE 'small' END FROM cs WHERE id = 4`, 1, "4|small")
	// A CASE that answers NULL does so through the point path too.
	wantRows(`SELECT id, CASE WHEN n > 100000000 THEN 1 END FROM cs WHERE id = 4`, 1, "4|NULL")
	wantRows(`SELECT id, CASE k WHEN 'zzz' THEN 'x' END FROM cs WHERE id = 1`, 1, "1|NULL")

	// Two CASEs in one projection, each with its own type, and the modifiers
	// around them.
	wantRows(`SELECT CASE WHEN id = 1 THEN 'one' ELSE 'other' END AS a, CASE WHEN id = 1 THEN 10 ELSE 20 END AS b FROM cs ORDER BY id LIMIT 2`, 2,
		"one|10", "other|20")
	wantRows(`SELECT id, CASE WHEN n > 100 THEN 'big' ELSE 'small' END AS size FROM cs WHERE id > 2 ORDER BY id DESC LIMIT 2`, 2,
		"5|big", "4|small")
	if got := joined(selectRows(t, cl, `SELECT DISTINCT CASE WHEN n > 100 THEN 'big' ELSE 'small' END AS size FROM cs`, 2)); func() bool {
		sort.Strings(got)
		return fmt.Sprint(got) != "[big small]"
	}() {
		t.Fatalf("DISTINCT CASE rows = %v, want [big small]", got)
	}

	// A bound parameter in a CASE condition is reported by Describe and
	// evaluated at Bind, exactly as it is in WHERE.
	cl.send(msgParse, concat(cstring("cparam"), cstring(`SELECT id, CASE WHEN k = $1 THEN 'y' ELSE 'n' END FROM cs ORDER BY id`), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgDescribe, concat([]byte{'S'}, cstring("cparam")))
	pd := cl.expectLine(msgParameterDesc)
	assertParameterDescription(t, pd.val, []int32{oidText})
	rd := cl.expectLine(msgRowDescription)
	assertRowDescription(t, rd.val, []string{"id", "case"}, []int32{oidInt8, oidText})
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)
	cl.send(msgBind, concat(cstring(""), cstring("cparam"), two(0), two(1), four(1), []byte("a"), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	frames := cl.recvUntil(msgCommandComplete)
	if tag := findTag(frames); tag != "SELECT 5" {
		t.Fatalf("bound CASE tag = %q, want SELECT 5", tag)
	}
	var got []string
	for _, fr := range frames {
		if fr.typ == msgDataRow {
			cells, err := parseDataRow(fr.val)
			if err != nil {
				t.Fatalf("parse data row: %v", err)
			}
			got = append(got, string(cells[0])+"|"+string(cells[1]))
		}
	}
	if fmt.Sprint(got) != "[1|y 2|n 3|n 4|n 5|y]" {
		t.Fatalf("bound CASE rows = %v, want [1|y 2|n 3|n 4|n 5|y]", got)
	}
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	// EXPLAIN answers without a CASE-specific node.
	frames = cl.query(`EXPLAIN SELECT id, CASE WHEN n > 100 THEN 'big' ELSE 'small' END FROM cs`)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("explain: unexpected error %s %s", code, msg)
	}

	// Types that cannot unify: an untyped string beside an integer must parse
	// as one at analysis time, and integer and boolean refuse in either order
	// with PostgreSQL's message naming the newly arrived type first.
	msg := queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN 1 ELSE 'x' END FROM cs`, errInvalidTextRepresentation)
	if !strings.Contains(msg, `invalid input syntax for type integer: "x"`) {
		t.Fatalf("mixed CASE message = %q", msg)
	}
	msg = queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN 1 ELSE true END FROM cs`, errDatatypeMismatch)
	if !strings.Contains(msg, "CASE types boolean and integer cannot be matched") {
		t.Fatalf("int/bool CASE message = %q", msg)
	}
	msg = queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN true ELSE 1 END FROM cs`, errDatatypeMismatch)
	if !strings.Contains(msg, "CASE types integer and boolean cannot be matched") {
		t.Fatalf("bool/int CASE message = %q", msg)
	}
	// A string beside a boolean parses as one, in either spelling PostgreSQL
	// accepts for a boolean literal.
	wantRows(`SELECT CASE WHEN id = 1 THEN true ELSE 'no' END FROM cs ORDER BY id`, 5,
		"t", "f", "f", "f", "f")

	// The shapes still refused: a non-literal result, a float result, a
	// non-column simple-CASE operand, a star beside a CASE in either order, and
	// CASE where the plan folds rows (GROUP BY).
	queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN id ELSE 0 END FROM cs`, errSyntax)
	queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN 1.5 ELSE 0 END FROM cs`, errSyntax)
	queryErr(t, cl, `SELECT CASE 1 WHEN id THEN 'x' ELSE 'y' END FROM cs`, errSyntax)
	queryErr(t, cl, `SELECT *, CASE WHEN id = 1 THEN 1 ELSE 0 END FROM cs`, errSyntax)
	queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN 1 ELSE 0 END, * FROM cs`, errSyntax)
	queryErr(t, cl, `SELECT CASE WHEN id = 1 THEN 1 ELSE 0 END FROM cs GROUP BY id`, errSyntax)
	queryErr(t, cl, `SELECT CASE WHEN true THEN 1 ELSE 0 END`, errSyntax)

	// A CASE in a join query is computed from the merged row: the condition
	// names a column of one side, and the branch that holds gives the value.
	joinCase := selectRows(t, cl, `SELECT cs.id, CASE WHEN cs.id = 1 THEN 1 ELSE 0 END FROM cs JOIN cs c2 ON cs.id = c2.id ORDER BY cs.id`, 5)
	for i, r := range joinCase {
		want := "0"
		if i == 0 {
			want = "1"
		}
		if string(r[1]) != want {
			t.Fatalf("join CASE row %d = %q, want %q", i, r[1], want)
		}
	}
}

// rowDescOIDs parses the type OIDs out of a RowDescription payload, one per
// field, in field order.
func rowDescOIDs(t testing.TB, val []byte) []int32 {
	t.Helper()
	if len(val) < 2 {
		t.Fatalf("RowDescription payload is %d bytes, want a field count", len(val))
	}
	n := int(binary.BigEndian.Uint16(val))
	p := val[2:]
	oids := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		j := bytes.IndexByte(p, 0)
		if j < 0 {
			t.Fatalf("RowDescription field %d: name is not terminated", i)
		}
		p = p[j+1:]
		if len(p) < 18 {
			t.Fatalf("RowDescription field %d: descriptor is truncated", i)
		}
		oids = append(oids, int32(binary.BigEndian.Uint32(p[6:])))
		p = p[18:]
	}
	return oids
}

// queryNames runs a SELECT and returns its RowDescription column names together
// with its rows, so a test can pin what the client is told as well as what it
// is given.
func queryNames(t testing.TB, cl *tclient, q string, want int) ([]string, [][][]byte) {
	t.Helper()
	frames := cl.query(q)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("%s: unexpected error %s %s", q, code, msg)
	}
	if tag := findTag(frames); tag != fmt.Sprintf("SELECT %d", want) {
		t.Fatalf("%s: tag = %q, want SELECT %d", q, tag, want)
	}
	fd := findFrame(frames, msgRowDescription)
	if fd == nil {
		t.Fatalf("%s: no RowDescription", q)
	}
	names := rowDescNames(t, fd.val)
	var rows [][][]byte
	for _, f := range frames {
		if f.typ != msgDataRow {
			continue
		}
		row, err := parseDataRow(f.val)
		if err != nil {
			t.Fatalf("%s: parseDataRow: %v", q, err)
		}
		rows = append(rows, row)
	}
	if len(rows) != want {
		t.Fatalf("%s: got %d rows, want %d", q, len(rows), want)
	}
	return names, rows
}

// rowDescNames parses the column names out of a RowDescription payload.
func rowDescNames(t testing.TB, val []byte) []string {
	t.Helper()
	if len(val) < 2 {
		t.Fatalf("RowDescription payload is %d bytes, want a field count", len(val))
	}
	n := int(binary.BigEndian.Uint16(val))
	p := val[2:]
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		j := bytes.IndexByte(p, 0)
		if j < 0 {
			t.Fatalf("RowDescription field %d: name is not terminated", i)
		}
		names = append(names, string(p[:j]))
		p = p[j+1:]
		// table OID int32, column number int16, type OID int32, type length
		// int16, type modifier int32, format int16.
		if len(p) < 18 {
			t.Fatalf("RowDescription field %d: descriptor is truncated", i)
		}
		p = p[18:]
	}
	return names
}

// wantIDs checks a single-column result against the values it must carry, in
// order. The expectation "NULL" is the cell a NULL column produces, which is
// how the two-column table's NULLs are told apart from an empty string.
func wantIDs(t testing.TB, rows [][][]byte, want ...string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for i, w := range want {
		if len(rows[i]) != 1 {
			t.Fatalf("row %d has %d cells, want 1", i, len(rows[i]))
		}
		if w == "NULL" {
			if rows[i][0] != nil {
				t.Fatalf("row %d = %q, want NULL", i, rows[i][0])
			}
			continue
		}
		if rows[i][0] == nil {
			t.Fatalf("row %d = NULL, want %q", i, w)
		}
		if string(rows[i][0]) != w {
			t.Fatalf("row %d = %q, want %q", i, rows[i][0], w)
		}
	}
}
