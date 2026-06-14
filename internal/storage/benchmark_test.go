package storage

import (
	"testing"
	"time"
)

// BenchmarkEngineGetNoAlloc measures the allocation cost of Engine.Get when the key is already present.
// The expectation is zero heap allocations per Get call because the value is stored as a []byte slice
// and the Get operation only reads from the map under a read lock.
func BenchmarkEngineGetNoAlloc(b *testing.B) {
	// Create an engine with a tiny tick interval (not used in this benchmark) and a modest number of slots.
	eng := NewEngine(1*time.Millisecond, 64, nil)
	defer eng.Close()

	// Pre‑populate the engine with a key/value pair.
	key := "benchmark_key"
	val := []byte("benchmark_value")
	eng.Set(key, val, 0) // no TTL to avoid chronometer involvement

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got, ok := eng.Get(key); !ok || string(got) != string(val) {
			b.Fatalf("unexpected result from Get: ok=%v, got=%s", ok, string(got))
		}
	}
}
