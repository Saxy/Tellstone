package bench

import (
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
)

func BenchmarkKVGet(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "kvbench:key:1"
	val := []byte("kvbench-value-32-bytes-long-xxxx")
	if err := eng.Set(key, val, 0); err != nil {
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

func BenchmarkKVSet(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "kvbench:key:1"
	val := []byte("kvbench-value-32-bytes-long-xxxx")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(key, val, 0); err != nil {
			b.Fatal(err)
		}
	}
}
