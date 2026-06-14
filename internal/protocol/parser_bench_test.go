package protocol

import (
	"strings"
	"testing"
)

// sink prevents compiler optimization from discarding parsing work.
var sink ParsedQuery

var (
	pgSelectQuery = []byte("SELECT value FROM kv WHERE key = 'tellstone:session:cluster-node-a:active:user:9950'")
	pgInsertQuery = []byte("INSERT INTO kv (key, value, ttl_ms) VALUES ('tellstone:session:cluster-node-a:active:user:9950', 'raw_protobuf_bytes_32_bytes_long', 5000)")
	pgDeleteQuery = []byte("DELETE FROM kv WHERE key = 'tellstone:session:cluster-node-a:active:user:9950'")

	// Additional benchmark inputs
	pgSelectWhitespace  = []byte("   SELECT   value   FROM   kv   WHERE   key   =   'tellstone:session:cluster-node-a:active:user:9950'   ")
	pgSelectMixedCase   = []byte("SeLeCt value FROM kv WhErE kEy = 'tellstone:session:cluster-node-a:active:user:9950'")
	pgInsertLongValue   []byte
	pgInsertTTLOverflow = []byte("INSERT INTO kv (key, value, ttl_ms) VALUES ('k','v', 9999999999999)")
	pgSelectUTF8Key     = []byte("SELECT value FROM kv WHERE key = '用户:12345'")
)

func init() {
	// Build a long value (~2 KB) for the long‑value benchmark.
	longVal := strings.Repeat("a", 2048)
	pgInsertLongValue = []byte("INSERT INTO kv (key, value, ttl_ms) VALUES ('k','" + longVal + "', 5000)")
}

// --- SEQUENTIAL BENCHMARKS (Single-Threaded) ---

func BenchmarkParseSQL_Select_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := ParseQuery(pgSelectQuery, nil)
		if err != nil {
			b.Fatal(err)
		}
		if q.Type != CmdGet {
			b.Fatal("invalid command type parsed")
		}
		sink = q
	}
}

func BenchmarkParseSQL_Insert_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := ParseQuery(pgInsertQuery, nil)
		if err != nil {
			b.Fatal(err)
		}
		if q.Type != CmdSet {
			b.Fatal("invalid command type parsed")
		}
		sink = q
	}
}

func BenchmarkParseSQL_Select_Whitespace_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := ParseQuery(pgSelectWhitespace, nil)
		if err != nil {
			b.Fatal(err)
		}
		if q.Type != CmdGet {
			b.Fatal("invalid command type parsed")
		}
		sink = q
	}
}

func BenchmarkParseSQL_Select_MixedCase_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := ParseQuery(pgSelectMixedCase, nil)
		if err != nil {
			b.Fatal(err)
		}
		if q.Type != CmdGet {
			b.Fatal("invalid command type parsed")
		}
		sink = q
	}
}

func BenchmarkParseSQL_Insert_LongValue_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := ParseQuery(pgInsertLongValue, nil)
		if err != nil {
			b.Fatal(err)
		}
		if q.Type != CmdSet {
			b.Fatal("invalid command type parsed")
		}
		sink = q
	}
}

func BenchmarkParseSQL_Insert_TTLOverflow_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseQuery(pgInsertTTLOverflow, nil)
		if err == nil {
			b.Fatal("expected overflow error")
		}
	}
}

func BenchmarkParseSQL_Select_UTF8Key_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := ParseQuery(pgSelectUTF8Key, nil)
		if err != nil {
			b.Fatal(err)
		}
		if q.Type != CmdGet {
			b.Fatal("invalid command type parsed")
		}
		sink = q
	}
}

// --- PARALLEL BENCHMARKS (Multi-Threaded Showcase) ---

func BenchmarkParseSQL_Select_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q, err := ParseQuery(pgSelectQuery, nil)
			if err != nil {
				b.Fatal(err)
			}
			if q.Type != CmdGet {
				b.Fatal("invalid command type parsed")
			}
			sink = q
		}
	})
}

func BenchmarkParseSQL_Insert_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q, err := ParseQuery(pgInsertQuery, nil)
			if err != nil {
				b.Fatal(err)
			}
			if q.Type != CmdSet {
				b.Fatal("invalid command type parsed")
			}
			sink = q
		}
	})
}

func BenchmarkParseSQL_MixedParallel_Mix_Parallel(b *testing.B) {
	queries := [][]byte{pgSelectQuery, pgInsertQuery, pgDeleteQuery}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			q, err := ParseQuery(queries[idx], nil)
			if err != nil {
				b.Fatal(err)
			}
			// verify type matches expected for this query
			expected := []CommandType{CmdGet, CmdSet, CmdDelete}[idx]
			if q.Type != expected {
				b.Fatalf("expected %v, got %v", expected, q.Type)
			}
			sink = q
			idx = (idx + 1) % len(queries)
		}
	})
}
