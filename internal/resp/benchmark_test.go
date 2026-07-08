package resp

import (
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

// BenchmarkParseGet measures the cost of decoding a single GET command out of a buffer,
// isolated from dispatch and I/O. args slices point into in, so this should be allocation-free.
func BenchmarkParseGet(b *testing.B) {
	in := []byte("*2\r\n$3\r\nGET\r\n$16\r\nbenchmark_key_01\r\n")
	dst := make([][]byte, 0, 8)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, _, err := Parse(in, dst)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		dst = args[:0]
	}
}

// BenchmarkParseSet measures the cost of decoding a single 3-arg SET command.
func BenchmarkParseSet(b *testing.B) {
	in := []byte("*3\r\n$3\r\nSET\r\n$16\r\nbenchmark_key_01\r\n$16\r\nbenchmark_value1\r\n")
	dst := make([][]byte, 0, 8)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, _, err := Parse(in, dst)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		dst = args[:0]
	}
}

// BenchmarkParsePipeline measures decoding a batch of pipelined commands the way memtier's
// --pipeline flag or redis-benchmark -P submit them: many commands arrive in a single read.
func BenchmarkParsePipeline(b *testing.B) {
	var buf []byte
	const batch = 16
	for i := 0; i < batch; i++ {
		buf = append(buf, []byte("*2\r\n$3\r\nGET\r\n$16\r\nbenchmark_key_01\r\n")...)
	}
	dst := make([][]byte, 0, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumed := 0
		for consumed < len(buf) {
			args, n, err := Parse(buf[consumed:], dst)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			dst = args[:0]
			consumed += n
		}
	}
}

// BenchmarkDispatchGetHit measures Server.dispatch for a GET that hits.
func BenchmarkDispatchGetHit(b *testing.B) {
	store := newFakeStore()
	_ = store.Set("k", []byte("benchmark_value"), 0)
	srv := &Server{store: store, logger: log.NewNoOpLogger()}
	args := [][]byte{[]byte("GET"), []byte("k")}
	out := make([]byte, 0, 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out = srv.dispatch(args, out[:0])
	}
}

// BenchmarkDispatchSet measures Server.dispatch for a plain (no-TTL) SET.
func BenchmarkDispatchSet(b *testing.B) {
	store := newFakeStore()
	srv := &Server{store: store, logger: log.NewNoOpLogger()}
	args := [][]byte{[]byte("SET"), []byte("k"), []byte("benchmark_value")}
	out := make([]byte, 0, 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out = srv.dispatch(args, out[:0])
	}
}

// BenchmarkDispatchSetParallel measures dispatch under concurrent access to the fakeStore's
// single mutex, mirroring how many gnet event-loop goroutines would hammer a shared store.
func BenchmarkDispatchSetParallel(b *testing.B) {
	store := newFakeStore()
	srv := &Server{store: store, logger: log.NewNoOpLogger()}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		args := [][]byte{[]byte("SET"), []byte("k"), []byte("benchmark_value")}
		out := make([]byte, 0, 128)
		for pb.Next() {
			out = srv.dispatch(args, out[:0])
		}
	})
}
