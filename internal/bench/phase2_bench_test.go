package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/shard"
)

// Phase 2 benchmarks to measure the impact of removing lock convoys and scaling with cores.
// These benchmarks will be run before and after implementing Phase 2 optimizations.

func BenchmarkActorModelGet(b *testing.B) {
	// Create a shard with actor model
	sh := shard.New(1, time.Millisecond, 64, 0, nil, nil)
	defer sh.Close()

	// Set up test data
	key := "actorbench:key:1"
	value := []byte("actorbench-value-32-bytes-long-xxxx")
	if resp := sh.Execute("SET", key, value, 0); resp.Err != nil {
		b.Fatal(resp.Err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		response := sh.Execute("GET", key, nil, 0)
		if response.Err != nil {
			b.Fatal("miss")
		}
	}
}

func BenchmarkActorModelSet(b *testing.B) {
	// Create a shard with actor model
	sh := shard.New(1, time.Millisecond, 64, 0, nil, nil)
	defer sh.Close()

	// Set up test data
	key := "actorbench:key:1"
	value := []byte("actorbench-value-32-bytes-long-xxxx")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if resp := sh.Execute("SET", key, value, 0); resp.Err != nil {
			b.Fatal(resp.Err)
		}
	}
}

func BenchmarkActorModelBatch(b *testing.B) {
	// Create a shard with actor model
	sh := shard.New(1, time.Millisecond, 64, 0, nil, nil)
	defer sh.Close()

	// Set up test data
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("actorbench:key:%d", i)
	}
	value := []byte("actorbench-value-32-bytes-long-xxxx")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			if resp := sh.Execute("SET", keys[j], value, 0); resp.Err != nil {
				b.Fatal(resp.Err)
			}
		}
	}
}