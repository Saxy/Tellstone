/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: stagec_test.go
Description: The Stage C tests: the virtual catalog relations (ADR-016).

A client's introspection is only as good as the catalog it reads, so these
tests pin what the relations answer: the schemas, the tables and their
columns, the constraints and the types, read through the shapes a client
actually uses -- a join, a point lookup on an object identifier, an aggregate,
and a filter on a non-key column.
*/
package sql

import (
	"fmt"
	"testing"
)

// queryNoErr runs a statement and fails only if it failed. It is for the
// statements whose row count is not the point of the test.
func queryNoErr(t testing.TB, cl *tclient, q string) []frame {
	t.Helper()
	frames := cl.query(q)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("%s: unexpected error %s %s", q, code, msg)
	}
	return frames
}

// TestStageCCatalogRelations pins the shape of the catalog: the schemas it
// lists, the tables it reports, and the columns of those tables.
func TestStageCCatalogRelations(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE users (id bigint PRIMARY KEY, name text)`, "CREATE TABLE")

	// The schemas a client sees: the one the engine's tables live in, and the
	// two system schemas.
	rows := selectRows(t, cl, `SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname`, 3)
	wantIDs(t, rows, "information_schema", "pg_catalog", "public")

	// The engine's table is reported as a table in the public schema.
	rows = selectRows(t, cl, `SELECT relname, relnamespace, relkind FROM pg_catalog.pg_class WHERE relname = 'users'`, 1)
	if len(rows[0]) != 3 || string(rows[0][0]) != "users" ||
		string(rows[0][1]) != "2200" || string(rows[0][2]) != "r" {
		t.Fatalf("pg_class row = %q, want [users 2200 r]", rows[0])
	}

	// Its columns are reported with their ordinals and their types.
	rows = selectRows(t, cl, `SELECT a.attname, a.attnum, a.atttypid, a.attnotnull FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid WHERE c.relname = 'users' ORDER BY a.attnum`, 2)
	if len(rows[0]) != 4 || string(rows[0][0]) != "id" || string(rows[0][1]) != "1" ||
		string(rows[0][2]) != "20" || string(rows[0][3]) != "t" {
		t.Fatalf("pg_attribute row = %q, want [id 1 20 t]", rows[0])
	}
	if len(rows[1]) != 4 || string(rows[1][0]) != "name" || string(rows[1][1]) != "2" ||
		string(rows[1][2]) != "25" || string(rows[1][3]) != "f" {
		t.Fatalf("pg_attribute row = %q, want [name 2 25 f]", rows[1])
	}

	// The SQL-standard view reports the same table.
	rows = selectRows(t, cl, `SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_name = 'users'`, 1)
	if len(rows[0]) != 3 || string(rows[0][0]) != "public" || string(rows[0][1]) != "users" ||
		string(rows[0][2]) != "BASE TABLE" {
		t.Fatalf("information_schema.tables row = %q, want [public users BASE TABLE]", rows[0])
	}
	rows = selectRows(t, cl, `SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position`, 2)
	if len(rows[0]) != 4 || string(rows[0][0]) != "id" || string(rows[0][1]) != "1" ||
		string(rows[0][2]) != "bigint" || string(rows[0][3]) != "NO" {
		t.Fatalf("information_schema.columns row = %q, want [id 1 bigint NO]", rows[0])
	}

	// The table's primary key is reported as a constraint and as an index.
	rows = selectRows(t, cl, `SELECT k.conname, k.contype FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_class c ON k.conrelid = c.oid WHERE c.relname = 'users'`, 1)
	if len(rows[0]) != 2 || string(rows[0][0]) != "users_pkey" || string(rows[0][1]) != "p" {
		t.Fatalf("pg_constraint row = %q, want [users_pkey p]", rows[0])
	}
	rows = selectRows(t, cl, `SELECT i.indisunique, i.indisprimary FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON i.indrelid = c.oid WHERE c.relname = 'users'`, 1)
	if len(rows[0]) != 2 || string(rows[0][0]) != "t" || string(rows[0][1]) != "t" {
		t.Fatalf("pg_index row = %q, want [t t]", rows[0])
	}
}

// TestStageCCatalogJoin pins the join a client's introspection is built on: the
// relations of a schema, joined to the schema they belong to.
func TestStageCCatalogJoin(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE orders (id bigint PRIMARY KEY)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT c.relname, n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'orders'`, 1)
	if len(rows[0]) != 2 || string(rows[0][0]) != "orders" || string(rows[0][1]) != "public" {
		t.Fatalf("join row = %q, want [orders public]", rows[0])
	}
	// A left join keeps the relation even when the other side has no match.
	rows = selectRows(t, cl, `SELECT c.relname, n.nspname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON c.oid = n.oid WHERE c.relname = 'orders'`, 1)
	if len(rows[0]) != 2 || string(rows[0][0]) != "orders" || rows[0][1] != nil {
		t.Fatalf("left join row = %q, want [orders NULL]", rows[0])
	}
}

// TestStageCCatalogIsLive pins that the catalog is projected rather than
// stored: a table created after the server started is in it, and a dropped
// one is not.
func TestStageCCatalogIsLive(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryNoErr(t, cl, `SELECT relname FROM pg_catalog.pg_class WHERE relname = 'later'`)
	queryOK(t, cl, `CREATE TABLE later (id bigint PRIMARY KEY)`, "CREATE TABLE")
	rows := selectRows(t, cl, `SELECT relname FROM pg_catalog.pg_class WHERE relname = 'later'`, 1)
	if string(rows[0][0]) != "later" {
		t.Fatalf("pg_class row = %q, want [later]", rows[0])
	}
	queryOK(t, cl, `DROP TABLE later`, "DROP TABLE")
	rows = selectRows(t, cl, `SELECT relname FROM pg_catalog.pg_class WHERE relname = 'later'`, 0)
	if len(rows) != 0 {
		t.Fatalf("pg_class still reports the dropped table: %q", rows)
	}
}

// TestStageCCatalogObjectIdentifiers pins that an object's identifier is
// stable: it is the same in two reads, and it is the same in a point lookup
// as it is in a scan.
func TestStageCCatalogObjectIdentifiers(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE stable (id bigint PRIMARY KEY)`, "CREATE TABLE")

	scanned := selectRow(t, cl, `SELECT oid FROM pg_catalog.pg_class WHERE relname = 'stable'`)
	looked := selectRow(t, cl, `SELECT oid FROM pg_catalog.pg_class WHERE oid = `+string(scanned[0]))
	if string(looked[0]) != string(scanned[0]) {
		t.Fatalf("point lookup oid = %q, want %q", looked[0], scanned[0])
	}
	again := selectRow(t, cl, `SELECT oid FROM pg_catalog.pg_class WHERE relname = 'stable'`)
	if string(again[0]) != string(scanned[0]) {
		t.Fatalf("second read oid = %q, want %q", again[0], scanned[0])
	}
}

// TestStageCCatalogAggregates pins that the catalog folds like any other
// relation, which is what a client's summary queries do.
func TestStageCCatalogAggregates(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE counted (id bigint PRIMARY KEY)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT n.nspname, count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid GROUP BY n.nspname ORDER BY n.nspname`, 3)
	for i, want := range []string{"information_schema", "pg_catalog", "public"} {
		if len(rows[i]) != 2 || string(rows[i][0]) != want {
			t.Fatalf("grouped row %d = %q, want [%s ...]", i, rows[i], want)
		}
	}
}

// TestStageCCatalogRefusals pins the answers a client gets for the catalog
// questions it must not get an answer to: a relation that does not exist, and
// a write to one that does.
func TestStageCCatalogRefusals(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	// A relation that is not in the catalog does not exist, whatever schema it
	// was named in.
	queryErr(t, cl, `SELECT * FROM pg_catalog.pg_nonexistent`, errUndefinedTable)
	queryErr(t, cl, `SELECT * FROM information_schema.nonexistent`, errUndefinedTable)
	// A table of the engine's own is not reachable through a system schema.
	queryOK(t, cl, `CREATE TABLE shadowed (id bigint PRIMARY KEY)`, "CREATE TABLE")
	queryErr(t, cl, `SELECT * FROM pg_catalog.shadowed`, errUndefinedTable)
	// The catalog relations are read-only.
	queryErr(t, cl, `INSERT INTO pg_catalog.pg_class (oid, relname) VALUES (1, 'x')`, errFeatureNotSupported)
	queryErr(t, cl, `DELETE FROM pg_catalog.pg_class WHERE oid = 1`, errFeatureNotSupported)
}

// TestStageCCatalogUnqualified pins the search order: an unqualified name that
// matches a table of the engine's is that table, and one that matches no
// table is looked for in pg_catalog.
func TestStageCCatalogUnqualified(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	rows := selectRows(t, cl, `SELECT relname FROM pg_class WHERE relname = 'pg_namespace'`, 1)
	if string(rows[0][0]) != "pg_namespace" {
		t.Fatalf("unqualified pg_class row = %q, want [pg_namespace]", rows[0])
	}
	// The information_schema is not in the search path, so its relations are
	// reachable only when they are qualified.
	queryErr(t, cl, `SELECT * FROM tables`, errUndefinedTable)
	queryNoErr(t, cl, `SELECT * FROM information_schema.tables`)
}

// TestStageCCatalogSessionState pins that the catalog reports the session it
// belongs to: the role is the session's own.
func TestStageCCatalogSessionState(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	rows := selectRows(t, cl, `SELECT rolname FROM pg_catalog.pg_roles`, 1)
	if string(rows[0][0]) != "default" {
		t.Fatalf("pg_roles row = %q, want [default]", rows[0])
	}
	rows = selectRows(t, cl, `SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'`, 1)
	if string(rows[0][0]) != "server_version" || string(rows[0][1]) == "" {
		t.Fatalf("pg_settings row = %q, want [server_version ...]", rows[0])
	}
}

// TestStageCCatalogFilter pins a filter on a column that is not the key,
// which is what a client's "tables whose name is like this" query is.
func TestStageCCatalogFilter(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE filtered (id bigint PRIMARY KEY)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE 'filter%'`, 1)
	if string(rows[0][0]) != "filtered" {
		t.Fatalf("pg_class row = %q, want [filtered]", rows[0])
	}
	rows = selectRows(t, cl, `SELECT relname FROM pg_catalog.pg_class WHERE relname <> 'filtered' AND relname = 'pg_namespace'`, 1)
	if string(rows[0][0]) != "pg_namespace" {
		t.Fatalf("pg_class row = %q, want [pg_namespace]", rows[0])
	}
}

// TestStageCCatalogColumnTypes pins the type a catalog column is reported
// with, because a client reads the identifier and renders the name itself.
func TestStageCCatalogColumnTypes(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `CREATE TABLE typed (id bigint PRIMARY KEY, v text, n int, f double precision)`, "CREATE TABLE")

	rows := selectRows(t, cl, `SELECT a.attname, a.atttypid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid WHERE c.relname = 'typed' ORDER BY a.attnum`, 4)
	want := []string{"id", "v", "n", "f"}
	wantOID := []string{"20", "25", "23", "701"}
	for i := range want {
		if string(rows[i][0]) != want[i] || string(rows[i][1]) != wantOID[i] {
			t.Fatalf("pg_attribute row %d = %q, want [%s %s]", i, rows[i], want[i], wantOID[i])
		}
	}
	// The types are in the type catalog, which is where a client looks them up.
	rows = selectRows(t, cl, `SELECT typname, typlen FROM pg_catalog.pg_type WHERE oid = 20`, 1)
	if string(rows[0][0]) != "int8" || string(rows[0][1]) != "8" {
		t.Fatalf("pg_type row = %q, want [int8 8]", rows[0])
	}
}

// TestStageCCatalogMultiRow pins that a catalog relation delivers every row
// it has, not just the first, and that the count matches the relations
// registered.
func TestStageCCatalogMultiRow(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	frames := queryNoErr(t, cl, `SELECT relname FROM pg_catalog.pg_class`)
	want := fmt.Sprintf("SELECT %d", len(catalogRelations))
	if tag := findTag(frames); tag != want {
		t.Fatalf("pg_class tag = %q, want %q", tag, want)
	}
}
