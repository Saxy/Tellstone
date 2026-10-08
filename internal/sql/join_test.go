package sql

import "testing"

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
	if !stringsContains(text, "Hash Join") || !stringsContains(text, "Seq Scan on a") || !stringsContains(text, "Seq Scan on b") {
		t.Fatalf("explain wrong: %s", text)
	}
}

func stringsContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}
