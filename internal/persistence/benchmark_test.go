package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
)

// BenchmarkWriteSequential measures the cost of a single Write (append to WAL).
func BenchmarkWriteSequential(b *testing.B) {
	dir := b.TempDir()
	s := NewStorage(true, nil, dir)
	if err := s.OpenShard(0); err != nil {
		b.Fatal(err)
	}
	val := []byte("benchmark_value_32_bytes_long_xxxxx")
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench_key_%d", i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.Write(0, keys[i%1000], val, time.Time{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteParallel measures concurrent Write throughput across goroutines.
func BenchmarkWriteParallel(b *testing.B) {
	dir := b.TempDir()
	s := NewStorage(true, nil, dir)
	if err := s.OpenShard(0); err != nil {
		b.Fatal(err)
	}
	val := []byte("bench_val")
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("k%d", i)
			if err := s.Write(0, key, val, time.Time{}); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkWriteWithTTL measures Write cost when TTL is set.
func BenchmarkWriteWithTTL(b *testing.B) {
	dir := b.TempDir()
	s := NewStorage(true, nil, dir)
	if err := s.OpenShard(0); err != nil {
		b.Fatal(err)
	}
	val := []byte("benchmark_value_32_bytes_long_xxxxx")
	ttl := time.Now().Add(time.Hour)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench_key_%d", i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.Write(0, keys[i%1000], val, ttl); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLoadShardSequential measures the cost of replaying a WAL file.
func BenchmarkLoadShardSequential(b *testing.B) {
	dir := b.TempDir()
	s := NewStorage(true, nil, dir)
	if err := s.OpenShard(0); err != nil {
		b.Fatal(err)
	}
	numRecords := 10000
	val := []byte("load_bench_value_32_bytes_long_xxx")
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("load_key_%d", i)
		if err := s.Write(0, key, val, time.Time{}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		engine := storage.NewEngine(0, 0, 0, nil, nil)
		if err := s.LoadShard(0, engine); err != nil {
			b.Fatal(err)
		}
		engine.Close()
	}
}

// BenchmarkLoadShardSmall measures load performance for small datasets (typical restart).
func BenchmarkLoadShardSmall(b *testing.B) {
	dir := b.TempDir()
	s := NewStorage(true, nil, dir)
	if err := s.OpenShard(0); err != nil {
		b.Fatal(err)
	}
	numRecords := 100
	val := []byte("small_load_value")
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("sk%d", i)
		if err := s.Write(0, key, val, time.Time{}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		engine := storage.NewEngine(0, 0, 0, nil, nil)
		if err := s.LoadShard(0, engine); err != nil {
			b.Fatal(err)
		}
		engine.Close()
	}
}

// BenchmarkWriteThenLoad measures the full lifecycle: write N records then load them all.
func BenchmarkWriteThenLoad(b *testing.B) {
	dir := b.TempDir()
	s := NewStorage(true, nil, dir)
	if err := s.OpenShard(0); err != nil {
		b.Fatal(err)
	}
	val := []byte("lifecycle_value_32_bytes_long_xxxxx")
	numRecords := 1000

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s.file[0].Close()
		os.Remove(filepath.Join(dir, fmt.Sprintf("shard_%03d.db", 0)))
		if err := s.OpenShard(0); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < numRecords; j++ {
			key := fmt.Sprintf("lk%d", j)
			if err := s.Write(0, key, val, time.Time{}); err != nil {
				b.Fatal(err)
			}
		}
		engine := storage.NewEngine(0, 0, 0, nil, nil)
		b.StartTimer()
		if err := s.LoadShard(0, engine); err != nil {
			b.Fatal(err)
		}
		engine.Close()
	}
}
