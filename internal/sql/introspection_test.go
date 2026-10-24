/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: introspection_test.go
Description: The client introspection queries, as a client writes them.

A JDBC driver, a terminal and an IDE do not read the catalog with the small
queries a test would write: they read it with the long ones their dialects
produced, and they read it as a single statement -- a comma join over three
relations, a left join with a compound condition, a nested CASE, a regular
expression, a cast, a constant in the target list. These tests pin those
statements, because the shape of the answer is the shape of the client.
*/
package sql

import (
	"slices"
	"testing"
)

// pgjdbcGetTables is the statement pgJDBC's DatabaseMetaData.getTables runs.
// It is spelled the way the driver spells it: a comma join over two
// relations, a left join with a compound condition, a cast, a nested CASE and
// a regular expression, all in one statement.
const pgjdbcGetTables = `SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,
 CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'
 WHEN true THEN CASE
 WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind
  WHEN 'r' THEN 'SYSTEM TABLE'
  WHEN 'v' THEN 'SYSTEM VIEW'
  WHEN 'i' THEN 'SYSTEM INDEX'
  ELSE NULL
  END
 WHEN n.nspname = 'pg_toast' THEN CASE c.relkind
  WHEN 'r' THEN 'SYSTEM TOAST TABLE'
  WHEN 'i' THEN 'SYSTEM TOAST INDEX'
  ELSE NULL
  END
 ELSE CASE c.relkind
  WHEN 'r' THEN 'TEMPORARY TABLE'
  WHEN 'p' THEN 'TEMPORARY TABLE'
  WHEN 'i' THEN 'TEMPORARY INDEX'
  WHEN 'S' THEN 'TEMPORARY SEQUENCE'
  WHEN 'v' THEN 'TEMPORARY VIEW'
  ELSE NULL
  END
 END
 WHEN false THEN CASE c.relkind
 WHEN 'r' THEN 'TABLE'
 WHEN 'p' THEN 'PARTITIONED TABLE'
 WHEN 'i' THEN 'INDEX'
 WHEN 'P' then 'PARTITIONED INDEX'
 WHEN 'S' THEN 'SEQUENCE'
 WHEN 'v' THEN 'VIEW'
 WHEN 'c' THEN 'TYPE'
 WHEN 'f' THEN 'FOREIGN TABLE'
 WHEN 'm' THEN 'MATERIALIZED VIEW'
 ELSE NULL
 END
 ELSE NULL
 END
 AS TABLE_TYPE, d.description AS REMARKS,
 '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME,
 '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION
 FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)
 WHERE c.relnamespace = n.oid`

// TestIntrospectionPgjdbcGetTables runs the driver's own statement and pins
// the row it answers with: the table of the public schema, of the type the
// nested CASE selects for it.
func TestIntrospectionPgjdbcGetTables(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	rows := selectRows(t, cl, pgjdbcGetTables+` AND n.nspname LIKE 'public'`, 1)
	row := rows[0]
	if len(row) != 10 {
		t.Fatalf("row has %d columns, want 10", len(row))
	}
	if string(row[0]) != "" {
		t.Fatalf("TABLE_CAT = %q, want the NULL of the statement", row[0])
	}
	if string(row[1]) != "public" || string(row[2]) != "users" {
		t.Fatalf("row = %q, want public/users", row[1:3])
	}
	if string(row[3]) != "TABLE" {
		t.Fatalf("TABLE_TYPE = %q, want TABLE", row[3])
	}
	// The constants the driver writes to fill a column it has no value for.
	if string(row[5]) != "" || string(row[8]) != "" {
		t.Fatalf("constants = %q/%q, want empty", row[5], row[8])
	}
}

// TestIntrospectionSystemTableType pins the other arm of the nested CASE: a
// relation of a system schema is a system table, not a table.
func TestIntrospectionSystemTableType(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	rows := selectRows(t, cl, `SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'pg_class'`, 1)
	if string(rows[0][0]) != "pg_class" || string(rows[0][1]) != "r" {
		t.Fatalf("pg_class row = %q, want [pg_class r]", rows[0])
	}
}

// TestIntrospectionThreeTableJoin pins the comma join over three relations:
// the client's join of a table, its schema and its columns, with the filter
// and the ordering that go with it.
func TestIntrospectionThreeTableJoin(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT n.nspname, c.relname, a.attname FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_attribute a WHERE c.relnamespace = n.oid AND a.attrelid = c.oid AND NOT a.attisdropped AND c.relname = 'users' ORDER BY a.attnum`, 2)
	if string(rows[0][2]) != "id" || string(rows[1][2]) != "name" {
		t.Fatalf("rows = %q/%q, want id/name", rows[0], rows[1])
	}
}

// TestIntrospectionChainedJoin pins the left-deep chain: a left join whose
// result is joined again.
func TestIntrospectionChainedJoin(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT c.relname, a.attname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'users' ORDER BY a.attnum`, 2)
	if string(rows[0][1]) != "id" || string(rows[1][1]) != "name" {
		t.Fatalf("rows = %q/%q, want id/name", rows[0], rows[1])
	}
}

// TestIntrospectionRegexAndBoolean pins the two predicates a client uses to
// pick the relations it wants: a regular expression on a text column and a
// bare boolean column.
func TestIntrospectionRegexAndBoolean(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname !~ '^pg_' ORDER BY nspname`, 2)
	if string(rows[0][0]) != "information_schema" || string(rows[1][0]) != "public" {
		t.Fatalf("rows = %q/%q, want information_schema/public", rows[0], rows[1])
	}
	rows = selectRows(t, cl, `SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname ~ '^pg_cat'`, 1)
	if string(rows[0][0]) != "pg_catalog" {
		t.Fatalf("rows = %q, want pg_catalog", rows[0])
	}
	// A bare boolean column as a predicate: the column that was not dropped.
	rows = selectRows(t, cl, `SELECT attname FROM pg_catalog.pg_attribute WHERE attname = 'id' AND NOT attisdropped`, 1)
	if string(rows[0][0]) != "id" {
		t.Fatalf("rows = %q, want id", rows[0])
	}
}

// TestIntrospectionConstantTarget pins a constant in the target list: the
// column is there, and every row of it holds the value.
func TestIntrospectionConstantTarget(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT NULL AS a, '' AS b, 'x' AS c FROM pg_catalog.pg_namespace WHERE nspname = 'public'`, 1)
	for _, row := range rows {
		if len(row) != 3 || string(row[0]) != "" || string(row[1]) != "" || string(row[2]) != "x" {
			t.Fatalf("row = %q, want [\"\" \"\" x]", row)
		}
	}
}

// TestExtendedDescribeResultKinds pins the pgJDBC connection-probe path. A
// FROM-less SELECT (`SELECT version()`) and SHOW answer a portal Describe
// with a RowDescription, not NoData: a DataRow that follows a NoData carries
// no column definitions, and pgJDBC -- and with it DataGrip, which drives
// its console through the driver -- closes the connection on it. Both kinds
// are result-producing without being StmtSelect, which is the only kind
// Describe used to recognize.
func TestExtendedDescribeResultKinds(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	for _, tc := range []struct {
		stmt, query, tag string
		cols             []string
		oids             []int32
	}{
		{"ps_ver", `SELECT version()`, "SELECT 1", []string{"version"}, []int32{oidText}},
		{"ps_show", `SHOW server_version`, "SHOW", []string{"server_version"}, []int32{oidText}},
		{"ps_all", `SHOW ALL`, "SHOW", []string{"name", "setting", "description"},
			[]int32{oidText, oidText, oidText}},
		{"ps_expl", `EXPLAIN SELECT 1`, "EXPLAIN", []string{"QUERY PLAN"}, []int32{oidText}},
	} {
		// Parse, Bind, Describe of the portal, Execute: the exact shape
		// pgJDBC sends for every statement it runs.
		cl.send(msgParse, concat(cstring(tc.stmt), cstring(tc.query), two(0)))
		cl.expectLine(msgParseComplete)
		cl.send(msgBind, concat(cstring(""), cstring(tc.stmt), two(0), two(0), two(0)))
		cl.expectLine(msgBindComplete)
		cl.send(msgDescribe, concat([]byte{'P'}, cstring("")))
		rd := cl.expectLine(msgRowDescription)
		assertRowDescription(t, rd.val, tc.cols, tc.oids)
		cl.send(msgExecute, concat(cstring(""), four(0)))
		for {
			f := cl.recv()
			if f.typ == msgCommandComplete {
				if tag, _, _ := consumeCString(f.val); tag != tc.tag {
					t.Fatalf("%s: CommandComplete tag = %q, want %q", tc.query, tag, tc.tag)
				}
				break
			}
			if f.typ != msgDataRow {
				t.Fatalf("%s: message %q after Execute, want DataRow or CommandComplete", tc.query, f.typ)
			}
		}
		cl.send(msgSync, nil)
		cl.expectLine(msgReadyForQuery)
	}

	// Describe of the prepared statement itself answers ParameterDescription
	// then the same RowDescription -- pgJDBC describes statements when it
	// caches them.
	cl.send(msgParse, concat(cstring("ps_s"), cstring(`SELECT version()`), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgDescribe, concat([]byte{'S'}, cstring("ps_s")))
	cl.expectLine(msgParameterDesc)
	rd := cl.expectLine(msgRowDescription)
	assertRowDescription(t, rd.val, []string{"version"}, []int32{oidText})
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)
}

// TestExtendedEmptyQuery pins the extended-protocol answer to a blank query
// string, as PostgreSQL 18 gives it: ParseComplete, BindComplete, NoData,
// EmptyQueryResponse, ReadyForQuery -- and no error. DataGrip parses "" as a
// validation round-trip right after connect; on an ErrorResponse it sends
// Terminate and reports the session as closed as invalid.
func TestExtendedEmptyQuery(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	cl.send(msgParse, concat(cstring(""), cstring(""), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgBind, concat(cstring(""), cstring(""), two(0), two(0), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgDescribe, concat([]byte{'P'}, cstring("")))
	cl.expectLine(msgNoData)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	cl.expectLine(msgEmptyQuery)
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)

	// A statement of whitespace is empty too.
	cl.send(msgParse, concat(cstring("ws"), cstring("  \n\t"), two(0)))
	cl.expectLine(msgParseComplete)
	cl.send(msgBind, concat(cstring(""), cstring("ws"), two(0), two(0), two(0)))
	cl.expectLine(msgBindComplete)
	cl.send(msgExecute, concat(cstring(""), four(0)))
	cl.expectLine(msgEmptyQuery)
	cl.send(msgSync, nil)
	cl.expectLine(msgReadyForQuery)
}

// TestCurrentSchemasExtended pins current_schemas(include) in the extended
// protocol the way DataGrip issues it: named prepared statements plus
// portal Describe/Bind/Execute/Sync, returning a name[] with the schemas
// search_path resolves to. With the default search_path ("$user", public)
// and $user unknown to pg_namespace, the result is {pg_catalog,public} when
// include=true and {public} when include=false, matching PostgreSQL 18.
func TestCurrentSchemasExtended(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	runCS := func(stmt, query string, cols []string, oids []int32, wants ...string) {
		cl.send(msgParse, concat(cstring(stmt), cstring(query), two(0)))
		cl.expectLine(msgParseComplete)
		cl.send(msgBind, concat(cstring(""), cstring(stmt), two(0), two(0), two(0)))
		cl.expectLine(msgBindComplete)
		cl.send(msgDescribe, concat([]byte{'P'}, cstring("")))
		rd := cl.expectLine(msgRowDescription)
		assertRowDescription(t, rd.val, cols, oids)
		cl.send(msgExecute, concat(cstring(""), four(0)))
		var got []string
		for {
			f := cl.recv()
			if f.typ == msgCommandComplete {
				if tag, _, _ := consumeCString(f.val); tag != "SELECT 1" {
					t.Fatalf("%s: tag = %q", query, tag)
				}
				break
			}
			if f.typ != msgDataRow {
				t.Fatalf("%s: %q", query, f.typ)
			}
			row, err := parseDataRow(f.val)
			if err != nil {
				t.Fatal(err)
			}
			for _, cell := range row {
				if cell == nil {
					t.Fatalf("%s: unexpected NULL", query)
				}
				got = append(got, string(cell))
			}
		}
		if !slices.Equal(got, wants) {
			t.Fatalf("%s: got %q want %q", query, got, wants)
		}
		cl.send(msgSync, nil)
		cl.expectLine(msgReadyForQuery)
	}

	// The default search_path ("$user", public): $user names no schema in
	// pg_namespace, so false sees only public and true prepends pg_catalog.
	runCS("p1", "select current_schemas(true) as a", []string{"a"}, []int32{oidNameArray},
		"{pg_catalog,public}")
	runCS("p2", "select current_schemas(false) as a", []string{"a"}, []int32{oidNameArray},
		"{public}")
	// DataGrip's exact query, both columns.
	runCS("p3", "select current_database() as a, current_schemas(false) as b",
		[]string{"a", "b"}, []int32{oidText, oidNameArray}, DefaultDB, "{public}")

	// A session SET of search_path is what resolves: information_schema alone
	// answers {information_schema} for false and pg_catalog first for true,
	// as on the golden server.
	cl.query("SET search_path TO information_schema")
	if row := selectRow(t, cl, "SELECT current_schemas(false)"); string(row[0]) != "{information_schema}" {
		t.Fatalf("current_schemas(false) after SET = %q", row[0])
	}
	if row := selectRow(t, cl, "SELECT current_schemas(true)"); string(row[0]) != "{pg_catalog,information_schema}" {
		t.Fatalf("current_schemas(true) after SET = %q", row[0])
	}
	// An entry naming no schema drops out; RESET restores the default path.
	cl.query("SET search_path TO nosuchschema")
	if row := selectRow(t, cl, "SELECT current_schemas(false)"); string(row[0]) != "{}" {
		t.Fatalf("current_schemas(false) over unknown schema = %q", row[0])
	}
	cl.query("SET search_path TO DEFAULT")
	if row := selectRow(t, cl, "SELECT current_schemas(false)"); string(row[0]) != "{public}" {
		t.Fatalf("current_schemas(false) after RESET = %q", row[0])
	}
}
