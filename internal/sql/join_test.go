/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: join_test.go
Description: The Phase 11 join tests: an INNER equality join returns the merged
row its projection asks for, a bare column only one side owns is rejected as
ambiguous, EXPLAIN describes the join and both child scans, and authorization
covers both of a join's tables so a session granted one side cannot read the
other through a join.
*/
package sql

import (
	"strings"
	"testing"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

func TestJoinInnerBasic(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	queryOK(t, cl, `CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)`, "CREATE TABLE")
	queryOK(t, cl, `CREATE TABLE orders (id BIGINT PRIMARY KEY, uid BIGINT, item VARCHAR)`, "CREATE TABLE")
	queryOK(t, cl, `INSERT INTO users (id, name) VALUES (1, 'alice')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO users (id, name) VALUES (2, 'bob')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO orders (id, uid, item) VALUES (1, 1, 'book')`, "INSERT 0 1")
	queryOK(t, cl, `INSERT INTO orders (id, uid, item) VALUES (2, 2, 'pen')`, "INSERT 0 1")
	rows := selectRows(t, cl, `SELECT u.id, u.name, o.id, o.item FROM users u JOIN orders o ON u.id = o.uid WHERE o.id = 2`, 1)
	if len(rows[0]) != 4 {
		t.Fatalf("want 4 cols")
	}
	// The cells are the join's output projection in order: the merged row's
	// left id and name, then the probe side's id and item.
	for i, want := range []string{"2", "bob", "2", "pen"} {
		if got := string(rows[0][i]); got != want {
			t.Fatalf("column %d = %q, want %q", i, got, want)
		}
	}
}

func TestJoinAmbiguousBare(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	queryOK(t, cl, `CREATE TABLE a (id BIGINT PRIMARY KEY, x BIGINT)`, "CREATE TABLE")
	queryOK(t, cl, `CREATE TABLE b (id BIGINT PRIMARY KEY, x BIGINT)`, "CREATE TABLE")
	queryErr(t, cl, `SELECT x FROM a JOIN b ON a.id=b.id`, "42702")
}

func TestJoinExplain(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	queryOK(t, cl, `CREATE TABLE a (id BIGINT PRIMARY KEY, x BIGINT)`, "CREATE TABLE")
	queryOK(t, cl, `CREATE TABLE b (id BIGINT PRIMARY KEY, a_id BIGINT)`, "CREATE TABLE")
	text := explainText(t, cl, `SELECT a.x FROM a JOIN b ON a.id = b.a_id`)
	if !strings.Contains(text, "Hash Join") || !strings.Contains(text, "Seq Scan on a") || !strings.Contains(text, "Seq Scan on b") {
		t.Fatalf("explain wrong: %s", text)
	}
}

// A join is authorized against both of its tables, so a grant on one side must
// not let a session read the other side's rows through a join. The refusal has
// to name SQLSTATE 42501 like any other RBAC denial, and it has to come from
// the right-hand table: the same session joining the granted table to itself
// plans and runs, which is what separates "the second key was checked" from
// "every join was denied".
func TestJoinDeniedWithoutRightTableGrant(t *testing.T) {
	srv, store := newTestServer(t, srvOpts{policy: newRowRBACPolicy(t), withTLS: true})

	// The tables and their rows are seeded straight into the store, the way the
	// single-table RBAC test does it, so only carol's session exists and
	// nothing in the test can bypass the check under test.
	for _, tbl := range []struct {
		name string
		cols []Column
	}{
		{"users", []Column{{Name: "id", Type: TypeBigInt}, {Name: "name", Type: TypeVarchar, Nullable: true}}},
		{"secrets", []Column{{Name: "id", Type: TypeBigInt}, {Name: "token", Type: TypeVarchar, Nullable: true}}},
	} {
		blob, err := EncodeSchema(&Schema{DB: DefaultDB, Table: tbl.name, Columns: tbl.cols, PrimaryKey: 0})
		if err != nil {
			t.Fatalf("encode schema %s: %v", tbl.name, err)
		}
		if err := store.Set(MetaTableKey(DefaultDB, tbl.name), blob, 0); err != nil {
			t.Fatalf("seed catalog %s: %v", tbl.name, err)
		}
		if err := store.Set(ColumnKey(DefaultDB, tbl.name, keyspace.EncodeIntRowID(1), "id"), EncodeOrderableInt(1), 0); err != nil {
			t.Fatalf("seed row %s: %v", tbl.name, err)
		}
	}

	carol := dialServer(t, srv.Addr())
	carol.startupPassword("carol", "secret")

	// carol's grant names tellstone/users/, so the granted side of the join is
	// readable and the join runs end to end.
	if tag := findTag(carol.query(`SELECT u.id FROM users u JOIN users v ON u.id = v.id`)); tag != "SELECT 1" {
		t.Fatalf("join of the granted table to itself: tag=%q", tag)
	}
	// The right-hand table is outside the grant, so the join is refused rather
	// than allowed because its row id matches one the grant covers.
	code, msg, ok := findError(carol.query(`SELECT u.id FROM users u JOIN secrets s ON u.id = s.id`))
	if !ok || code != errInsufficientPrivilege {
		t.Fatalf("join across the ungranted table: code=%q ok=%v msg=%q", code, ok, msg)
	}
}
