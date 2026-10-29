package sql

import "testing"

// BenchmarkPGWireSimpleSelectPK measures one simple-query primary-key SELECT
// round-trip over a real TCP socket through the pgwire server. The store is the
// in-package testStore, so the number isolates protocol and executor overhead.
// Client and server share the process, so allocs/op counts both sides.
func BenchmarkPGWireSimpleSelectPK(b *testing.B) {
	srv, _ := newTestServer(b, srvOpts{})
	cl := dialServer(b, srv.Addr())
	cl.startupTrust("bench")
	if tag := findTag(cl.query(`INSERT INTO tellstone (key, value) VALUES ('foo', '\x626172')`)); tag != "INSERT 0 1" {
		b.Fatalf("seed INSERT tag = %q", tag)
	}
	const q = `SELECT key, value FROM tellstone WHERE key = 'foo'`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cl.send(msgQuery, append([]byte(q), 0))
		cl.recvUntil(msgReadyForQuery)
	}
}
