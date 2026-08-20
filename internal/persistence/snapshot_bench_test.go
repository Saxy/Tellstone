package persistence

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/storage"
)

// populateEngine fills an engine with n keys of the given size.
func populateEngine(b testing.TB, n int, valSize int) *storage.Engine {
	b.Helper()
	e := storage.NewEngine(0, 0, 0, nil, nil)
	val := make([]byte, valSize)
	for i := 0; i < valSize; i++ {
		val[i] = byte('A' + i%26)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%08d", i)
		if err := e.Set(key, val, 0); err != nil {
			b.Fatal(err)
		}
	}
	return e
}

// --- snapshotWrite benchmarks ---

func BenchmarkSnapshotWrite100(b *testing.B) {
	benchmarkSnapshotWrite(b, 100, 64)
}

func BenchmarkSnapshotWrite1K(b *testing.B) {
	benchmarkSnapshotWrite(b, 1000, 64)
}

func BenchmarkSnapshotWrite10K(b *testing.B) {
	benchmarkSnapshotWrite(b, 10000, 64)
}

func BenchmarkSnapshotWrite100K(b *testing.B) {
	benchmarkSnapshotWrite(b, 100000, 64)
}

func benchmarkSnapshotWrite(b *testing.B, nKeys, valSize int) {
	dir := b.TempDir()
	engine := populateEngine(b, nKeys, valSize)
	defer engine.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		os.Remove(filepath.Join(dir, "shard_000.snap"))
		b.StartTimer()
	}
}

// --- snapshotRead benchmarks ---

func BenchmarkSnapshotRead100(b *testing.B) {
	benchmarkSnapshotRead(b, 100, 64)
}

func BenchmarkSnapshotRead1K(b *testing.B) {
	benchmarkSnapshotRead(b, 1000, 64)
}

func BenchmarkSnapshotRead10K(b *testing.B) {
	benchmarkSnapshotRead(b, 10000, 64)
}

func BenchmarkSnapshotRead100K(b *testing.B) {
	benchmarkSnapshotRead(b, 100000, 64)
}

func benchmarkSnapshotRead(b *testing.B, nKeys, valSize int) {
	dir := b.TempDir()
	engine := populateEngine(b, nKeys, valSize)
	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		b.Fatal(err)
	}
	engine.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e := storage.NewEngine(0, 0, 0, nil, nil)
		if _, err := snapshotRead(dir, 0, e, [16]byte{}, nil); err != nil {
			b.Fatal(err)
		}
		e.Close()
	}
}

// --- full round-trip: write snapshot + read it back ---

func BenchmarkSnapshotRoundTrip1K(b *testing.B) {
	benchmarkSnapshotRoundTrip(b, 1000, 64)
}

func BenchmarkSnapshotRoundTrip10K(b *testing.B) {
	benchmarkSnapshotRoundTrip(b, 10000, 64)
}

func BenchmarkSnapshotRoundTrip100K(b *testing.B) {
	benchmarkSnapshotRoundTrip(b, 100000, 64)
}

func benchmarkSnapshotRoundTrip(b *testing.B, nKeys, valSize int) {
	dir := b.TempDir()
	engine := populateEngine(b, nKeys, valSize)
	defer engine.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		e := storage.NewEngine(0, 0, 0, nil, nil)
		if _, err := snapshotRead(dir, 0, e, [16]byte{}, nil); err != nil {
			b.Fatal(err)
		}
		e.Close()
		os.Remove(filepath.Join(dir, "shard_000.snap"))
	}
}

// --- serializeEngineToWriter benchmarks ---

func BenchmarkSerializeEngineToWriter1K(b *testing.B) {
	benchmarkSerialize(b, 1000, 64)
}

func BenchmarkSerializeEngineToWriter10K(b *testing.B) {
	benchmarkSerialize(b, 10000, 64)
}

func BenchmarkSerializeEngineToWriter100K(b *testing.B) {
	benchmarkSerialize(b, 100000, 64)
}

func benchmarkSerialize(b *testing.B, nKeys, valSize int) {
	engine := populateEngine(b, nKeys, valSize)
	defer engine.Close()
	f, err := os.CreateTemp(b.TempDir(), "bench-ser-*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f.Truncate(0)
		f.Seek(0, 0)
		serializeEngineToWriter(f, engine)
	}
}

// --- snapshotChildWrite benchmarks ---

func BenchmarkSnapshotChildWrite1K(b *testing.B) {
	benchmarkChildWrite(b, 1000, 64)
}

func BenchmarkSnapshotChildWrite10K(b *testing.B) {
	benchmarkChildWrite(b, 10000, 64)
}

func BenchmarkSnapshotChildWrite100K(b *testing.B) {
	benchmarkChildWrite(b, 100000, 64)
}

func benchmarkChildWrite(b *testing.B, nKeys, valSize int) {
	dir := b.TempDir()
	// Build a pipe with serialized engine data, then benchmark childWrite.
	engine := populateEngine(b, nKeys, valSize)
	defer engine.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pr, pw, err := os.Pipe()
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			serializeEngineToWriter(pw, engine)
			pw.Close()
		}()
		b.StartTimer()
		if err := snapshotChildWrite(dir, 0, pr, [16]byte{}); err != nil {
			b.Fatal(err)
		}
		pr.Close()
		b.StopTimer()
		os.Remove(filepath.Join(dir, "shard_000.snap"))
		b.StartTimer()
	}
}

// --- per-entry alloc isolation: only the write loop ---

func BenchmarkSnapshotWriteEntryAllocs1K(b *testing.B) {
	benchmarkWriteEntryOnly(b, 1000, 64)
}

func BenchmarkSnapshotWriteEntryAllocs10K(b *testing.B) {
	benchmarkWriteEntryOnly(b, 10000, 64)
}

// benchmarkWriteEntryOnly isolates the per-entry allocation cost of the
// snapshotWrite inner loop (hashing + writing entries) from the one-time
// setup cost (file open, header write).
func benchmarkWriteEntryOnly(b *testing.B, nKeys, valSize int) {
	dir := b.TempDir()
	engine := populateEngine(b, nKeys, valSize)
	defer engine.Close()
	path := filepath.Join(dir, "shard_000.snap")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			b.Fatal(err)
		}
		engine.ForEach(func(key string, value []byte, expiration time.Time) {
			f.WriteString(key)
			f.Write(value)
		})
		f.Close()
		os.Remove(path)
	}
}

// --- large value benchmarks ---

func BenchmarkSnapshotWriteLargeValues1K(b *testing.B) {
	benchmarkSnapshotWrite(b, 1000, 4096)
}

func BenchmarkSnapshotReadLargeValues1K(b *testing.B) {
	benchmarkSnapshotRead(b, 1000, 4096)
}

// --- snapshotExists benchmark ---

func BenchmarkSnapshotExists(b *testing.B) {
	dir := b.TempDir()
	os.WriteFile(filepath.Join(dir, "shard_000.snap"), make([]byte, snapHeader), 0600)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snapshotExists(dir, 0)
	}
}

// --- benchmark the full LoadShard path (snapshot + WAL) ---

func BenchmarkLoadShardWithSnapshot1K(b *testing.B) {
	dir := b.TempDir()
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		b.Fatal(err)
	}
	s.OpenShard(0, nil)

	engine := populateEngine(b, 1000, 64)
	// Write snapshot from engine state.
	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		b.Fatal(err)
	}
	// Truncate WAL and write some post-snapshot WAL records.
	s.TruncateWALTo(0, 0)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("wal_%08d", i)
		if err := s.Write(0, key, []byte("wal_value"), time.Time{}); err != nil {
			b.Fatal(err)
		}
	}
	engine.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e := storage.NewEngine(0, 0, 0, nil, nil)
		if err := s.LoadShard(0, e); err != nil {
			b.Fatal(err)
		}
		e.Close()
	}
}

// io.WriteString uses this path — benchmarks the string→[]byte conversion cost.
func BenchmarkIoWriteStringConversion(b *testing.B) {
	dir := b.TempDir()
	f, err := os.CreateTemp(dir, "bench-ws-*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	key := "bench_key_name_that_is_typical_length"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f.Truncate(0)
		f.Seek(0, 0)
		io.WriteString(f, key)
	}
}

// The same write but with an unsafe []byte(key) conversion — should be zero-alloc.
func BenchmarkWriteUnsafeConversion(b *testing.B) {
	dir := b.TempDir()
	f, err := os.CreateTemp(dir, "bench-wu-*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	key := "bench_key_name_that_is_typical_length"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f.Truncate(0)
		f.Seek(0, 0)
		f.Write(unsafeBytes(key))
	}
}

func unsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
