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
	eng := NewEngine(1*time.Millisecond, 64, 0, nil, nil)
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

// BenchmarkEngineGetDeleteHit measures the DEL pattern the command layer used
// before the Delete seam: Get to test existence, then Delete. The key is
// re-set each iteration so every iteration sees a hit, the same shape as
// BenchmarkEngineSetGetParallelNoTTL.
func BenchmarkEngineGetDeleteHit(b *testing.B) {
	eng := NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()

	key := "benchmark_key"
	val := []byte("benchmark_value")
	eng.Set(key, val, 0)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eng.Set(key, val, 0)
		if _, ok := eng.Get(key); ok {
			eng.Delete(key)
		}
	}
}

// BenchmarkEngineDeleteHit measures the same DEL cycle with the single Delete
// call that reports existence, i.e. one map lookup instead of two.
func BenchmarkEngineDeleteHit(b *testing.B) {
	eng := NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()

	key := "benchmark_key"
	val := []byte("benchmark_value")
	eng.Set(key, val, 0)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eng.Set(key, val, 0)
		if !eng.Delete(key) {
			b.Fatalf("Delete reported a miss on a present key")
		}
	}
}

// BenchmarkEngineDeleteMiss measures the cost of DEL on an absent key.
func BenchmarkEngineDeleteMiss(b *testing.B) {
	eng := NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()

	key := "benchmark_key"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if eng.Delete(key) {
			b.Fatalf("Delete reported a hit on an absent key")
		}
	}
}
