package bench

import (
	"strconv"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
)

func BenchmarkWriteSequential(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "persistencebench:key:1"
	val := []byte("persistencebench-value-32-bytes-long-xxxx")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(key, val, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "persistencebench:key:" + strconv.Itoa(i)
	}
	val := []byte("persistencebench-value-32-bytes-long-xxxx")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := eng.Set(keys[i%1000], val, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSequential(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "persistencebench:key:1"
	val := []byte("persistencebench-value-32-bytes-long-xxxx")
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

func BenchmarkReadBatch(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "persistencebench:key:" + strconv.Itoa(i)
	}
	val := []byte("persistencebench-value-32-bytes-long-xxxx")
	for i := range keys {
		if err := eng.Set(keys[i], val, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := eng.Get(keys[i%1000]); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkScanPrefix(b *testing.B) {
	eng := storage.NewEngine(1*time.Millisecond, 64, 0, nil, nil)
	defer eng.Close()
	key := "persistencebench:scan:1"
	val := []byte("persistencebench-value-32-bytes-long-xxxx")
	if err := eng.Set(key, val, 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eng.ScanPrefix("persistencebench:scan:", func(k, v []byte) bool {
			return true
		})
	}
}
