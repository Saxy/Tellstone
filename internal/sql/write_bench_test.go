package sql

import (
	"fmt"
	"testing"
)

// BenchmarkWriteTxn measures the write path of a catalog-table transaction:
// BEGIN + UPDATE + COMMIT through the wire, the core of the pgbench read-write
// workload. It isolates the commit path (conflict detection + MVCC append).
func BenchmarkWriteTxn(b *testing.B) {
	srv, _ := newTestServer(b, srvOpts{})
	cl := dialServer(b, srv.Addr())
	cl.startupTrust("bench")

	queryOK(b, cl, `CREATE TABLE w (id BIGINT PRIMARY KEY, a BIGINT)`, "CREATE TABLE")
	for i := int64(1); i <= 100; i++ {
		queryOK(b, cl, fmt.Sprintf(`INSERT INTO w (id, a) VALUES (%d, 0)`, i), "INSERT 0 1")
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := int64(i%100) + 1
		frames := cl.query(fmt.Sprintf(`BEGIN; UPDATE w SET a = %d WHERE id = %d; COMMIT;`, i, id))
		if code, msg, ok := findError(frames); ok {
			b.Fatalf("write txn failed: %s %s", code, msg)
		}
	}
}

// BenchmarkAutocommitWrite measures a single UPDATE without an explicit txn,
// which exercises the autocommit MVCC append path (per-key stripe lock).
func BenchmarkAutocommitWrite(b *testing.B) {
	srv, _ := newTestServer(b, srvOpts{})
	cl := dialServer(b, srv.Addr())
	cl.startupTrust("bench")

	queryOK(b, cl, `CREATE TABLE w2 (id BIGINT PRIMARY KEY, a BIGINT)`, "CREATE TABLE")
	for i := int64(1); i <= 100; i++ {
		queryOK(b, cl, fmt.Sprintf(`INSERT INTO w2 (id, a) VALUES (%d, 0)`, i), "INSERT 0 1")
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := int64(i%100) + 1
		frames := cl.query(fmt.Sprintf(`UPDATE w2 SET a = %d WHERE id = %d`, i, id))
		if code, msg, ok := findError(frames); ok {
			b.Fatalf("autocommit write failed: %s %s", code, msg)
		}
	}
}
