package storage

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkEngineSetNoTTL measures a sequential Set with no TTL (chronometer not involved).
func BenchmarkEngineSetNoTTL(b *testing.B) {
	eng := NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	val := []byte("benchmark_value")
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("benchmark_key_%d", i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(keys[i%1000], val, 0); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkEngineSetSetGetParallelNoTTL is the no-TTL counterpart to
// BenchmarkChronometerEvictionPipeline (chronometer_bench_test.go): same shape of workload
// (Set followed by Get across a large keyspace, parallel), but ttl=0 so Engine.Set never calls
// Chronometer.Register. Comparing the two isolates how much of the parallel Set/Get cost comes
// from the chronometer's single global mutex versus the per-shard locking itself.
func BenchmarkEngineSetGetParallelNoTTL(b *testing.B) {
	engine := NewEngine(1*time.Millisecond, 1000, 0, nil, nil)
	defer engine.Close()
	payload := []byte("raw_protobuf_bytes_32_bytes_long")
	numKeys := 50000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("tellstone:session:cluster-node-a:active:user:%d", i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%numKeys]
			engine.Set(key, payload, 0)
			_, _ = engine.Get(key)
			i++
		}
	})
}

