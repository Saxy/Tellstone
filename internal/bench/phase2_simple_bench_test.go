package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
)

// Phase 2 benchmarks to measure the impact of removing lock convoys and scaling with cores.
// These benchmarks will be run before and after implementing Phase 2 optimizations.

func BenchmarkPhase2Get(b *testing.B) {
	// Create a storage engine
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	
	// Set up test data
	key := "phase2bench:key:1"
	value := []byte("phase2bench-value-32-bytes-long-xxxx")
	if err := eng.Set(key, value, 0); err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := eng.Get(key); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkPhase2Set(b *testing.B) {
	// Create a storage engine
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	
	// Set up test data
	key := "phase2bench:key:1"
	value := []byte("phase2bench-value-32-bytes-long-xxxx")
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(key, value, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPhase2Batch(b *testing.B) {
	// Create a storage engine
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	
	// Set up test data
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("phase2bench:key:%d", i)
	}
	value := []byte("phase2bench-value-32-bytes-long-xxxx")
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			if err := eng.Set(keys[j], value, 0); err != nil {
				b.Fatal(err)
			}
		}
	}
}