package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
)

func BenchmarkCoreGet(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "corebench:key:1"
	val := []byte("corebench-value-32-bytes-long-xxxx")
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

func BenchmarkCoreSet(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "corebench:key:1"
	val := []byte("corebench-value-32-bytes-long-xxxx")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(key, val, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCoreBatch(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("corebench:key:%d", i)
	}
	val := []byte("corebench-value-32-bytes-long-xxxx")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(keys[i%1000], val, 0); err != nil {
			b.Fatal(err)
		}
		if _, ok := eng.Get(keys[i%1000]); !ok {
			b.Fatal("miss")
		}
	}
}