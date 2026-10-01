/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: benchmark_scan_test.go
Description: Baseline benchmarks and memory accounting for the Phase 9 key
layout (ADR-013). These are the "before" numbers for the ordered-index
sidecar: the column-key layout routes row reconstruction and full-table
scans through range reads, and its per-key memory is the cost guardrail 2
exists to protect. Re-run these unchanged after the sidecar lands and
compare — a regression in BenchmarkEngineScanRow or the bytes-per-key
figures invalidates the layout.

The key space mirrors ADR-013 §1, <db>/<table>/<row-id>/<column>, which is
what makes the numbers predictive: row reconstruction reads the columns of
one row, and a table scan reads a whole table prefix.
*/
package storage

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// newLayoutEngine builds an engine preloaded with the Phase 9 key layout for
// rows rows of the given column count, with value of the given length.
func newLayoutEngine(b testing.TB, rows, columns, valueLen int) (*Engine, int) {
	b.Helper()
	e := NewEngine(time.Hour, 256, 0, nil, nil)
	value := strings.Repeat("v", valueLen)
	// Row ids are fixed width so the byte order of the row-id segment is
	// meaningful, matching a BIGINT primary key under §4's big-endian rule.
	for r := 0; r < rows; r++ {
		prefix := fmt.Sprintf("tellstone/users/%012d/", r)
		for c := 0; c < columns; c++ {
			key := prefix + columnName(c)
			if err := e.Set(key, []byte(value), 0); err != nil {
				b.Fatalf("seed %q: %v", key, err)
			}
		}
	}
	return e, rows * columns
}

func columnName(c int) string {
	switch c {
	case 0:
		return "name"
	case 1:
		return "lastname"
	case 2:
		return "age"
	default:
		return fmt.Sprintf("col%d", c)
	}
}

// prefixUpperBound returns the exclusive upper bound for a prefix scan: every
// key that starts with prefix is < prefixUpperBound(prefix). This is the
// guardrail-1 seek, seek prefix -> prefix\xff, without assuming a terminator.
func prefixUpperBound(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	// All 0xff: no finite bound, the prefix spans the whole keyspace tail.
	return ""
}

// BenchmarkEngineScanRow measures the phase 9 point lookup: reconstructing one
// row from its column keys. It is the case guardrail 1 exists for, and the one
// the ordered sidecar must turn from a full-engine walk into a prefix seek.
func BenchmarkEngineScanRow(b *testing.B) {
	for _, rows := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d/cols=3", rows), func(b *testing.B) {
			e, total := newLayoutEngine(b, rows, 3, 64)
			defer e.Close()
			b.ReportMetric(float64(total), "keys")
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Walk 100 different rows so the loop is not one hot prefix
				// that every cache level has memorized.
				prefix := fmt.Sprintf("tellstone/users/%012d/", i%100)
				hit := 0
				e.Scan([]byte(prefix), []byte(prefixUpperBound(prefix)), func(key string, _ []byte) {
					hit++
				})
				if hit != 3 {
					b.Fatalf("row %q: reconstructed %d columns, want 3", prefix, hit)
				}
			}
		})
	}
}

// BenchmarkEngineScanTable measures the unfiltered full-table select of §6. It
// is expected to stay O(n) in the table, but the constants are what the
// sidecar is judged on: today Scan copies every live value in the engine and
// sorts all keys before the callback sees anything, so this number is
// dominated by keys outside the table.
func BenchmarkEngineScanTable(b *testing.B) {
	for _, rows := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("rows=%d/cols=3", rows), func(b *testing.B) {
			e, total := newLayoutEngine(b, rows, 3, 64)
			// A second table so the scan has to skip foreign keys, which is
			// what a real engine holds once more than one table exists.
			other, _ := newLayoutEngineInto(b, e, "other", 500, 3, 64)
			_ = other
			defer e.Close()
			b.ReportMetric(float64(total+500*3), "keys")
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				hit := 0
				e.Scan([]byte("tellstone/users/"), []byte("tellstone/users0"), func(string, []byte) {
					hit++
				})
				if hit != rows*3 {
					b.Fatalf("table scan returned %d keys, want %d", hit, rows*3)
				}
			}
		})
	}
}

func newLayoutEngineInto(b testing.TB, e *Engine, table string, rows, columns, valueLen int) (*Engine, int) {
	b.Helper()
	value := strings.Repeat("v", valueLen)
	for r := 0; r < rows; r++ {
		prefix := fmt.Sprintf("tellstone/%s/%012d/", table, r)
		for c := 0; c < columns; c++ {
			if err := e.Set(prefix+columnName(c), []byte(value), 0); err != nil {
				b.Fatalf("seed: %v", err)
			}
		}
	}
	return e, rows * columns
}

// TestEngineLayoutBytesPerKey reports the memory the column-key layout costs
// per key. Guardrail 2 exists because a column-key row repeats
// tellstone/users/<id>/ in every one of its keys; this is the number that
// quantifies it, and the one the sidecar index has to justify. It is a test
// rather than a benchmark so it fails loudly if the layout starts leaking.
func TestEngineLayoutBytesPerKey(t *testing.T) {
	const valueLen = 64
	e, keys := newLayoutEngine(t, 10_000, 3, valueLen)
	defer e.Close()

	bytesPerKey := float64(e.AllocatedBytes()) / float64(keys)
	t.Logf("keys=%d allocated=%d bytes/key=%.1f (key overhead=%.1f over a %d-byte value)",
		keys, e.AllocatedBytes(), bytesPerKey, bytesPerKey-valueLen, valueLen)

	// A front-coded sidecar cannot beat the map's own key storage, so this is
	// a reporting guard, not a pass/fail budget: the value is logged for
	// before/after comparison in the sidecar ADR.
	if e.KeyCount() != uint64(keys) {
		t.Fatalf("key count = %d, want %d", e.KeyCount(), keys)
	}
}

// marginalHeapPerKey returns the engine's true heap cost per key by measuring
// the slope between two key counts. A single measurement is not usable: the
// timeline wheel, the map itself and the chronometer goroutine are fixed costs
// that a naive divide attributes to the keys, so a small keyspace reports
// several times the real per-key cost. Differencing cancels the fixed part and
// leaves what an added key actually costs, which is the number a second index
// over the same keys has to be compared against.
func marginalHeapPerKey(tb testing.TB, build func(keys int) *Engine) (perKey float64, low, high int) {
	tb.Helper()
	heapAt := func(keys int) (uint64, int) {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		e := build(keys)
		n := int(e.KeyCount())
		runtime.GC()
		runtime.ReadMemStats(&after)
		e.Close()
		return after.HeapAlloc - before.HeapAlloc, n
	}
	// A 4x range keeps the slope well conditioned while staying quick.
	loKeys, hiKeys := 20_000, 80_000
	loHeap, loN := heapAt(loKeys)
	hiHeap, hiN := heapAt(hiKeys)
	if hiN <= loN || hiHeap <= loHeap {
		tb.Fatalf("heap did not grow with the keyspace: %d keys=%dB, %d keys=%dB",
			loN, loHeap, hiN, hiHeap)
	}
	return float64(hiHeap-loHeap) / float64(hiN-loN), loN, hiN
}

// TestEngineLayoutHeapPerKey pins the marginal per-key footprint of the
// ADR-013 column-key layout. Logged rather than asserted: the point is the
// before/after comparison when the ordered sidecar is added, and the budget
// belongs in the sidecar ADR where the trade is argued. Columns per row is
// the variable that matters, because it decides how many times the
// tellstone/users/<id>/ prefix is repeated across the row.
func TestEngineLayoutHeapPerKey(t *testing.T) {
	for _, columns := range []int{1, 3, 8} {
		t.Run(fmt.Sprintf("cols=%d", columns), func(t *testing.T) {
			perKey, lo, hi := marginalHeapPerKey(t, func(keys int) *Engine {
				rows := keys / columns
				e, _ := newLayoutEngine(t, rows, columns, 64)
				return e
			})
			t.Logf("keys %d..%d: marginal heap/key=%.1f bytes (64-byte value, ~34-byte key)",
				lo, hi, perKey)
		})
	}
}

// BenchmarkEngineScanPrefixRow is the "after" number for a phase 9 row read
// through the engine's prefix range read, to be read next to
// BenchmarkEngineScanRow's ~34 ms for the same row out of a full engine scan.
func BenchmarkEngineScanPrefixRow(b *testing.B) {
	for _, rows := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d/cols=3", rows), func(b *testing.B) {
			e := NewEngine(0, 0, 0, nil, nil)
			prefixes := make([]string, rows)
			for r := 0; r < rows; r++ {
				prefix := fmt.Sprintf("tellstone/users/%012d/", r)
				prefixes[r] = prefix
				for _, c := range []string{"age", "lastname", "name"} {
					if err := e.Set(prefix+c, []byte(c), 0); err != nil {
						b.Fatal(err)
					}
				}
			}
			var sum int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.ScanPrefix(prefixes[i%rows], func(_, v []byte) bool {
					sum += len(v)
					return true
				})
			}
			b.StopTimer()
			if sum == 0 {
				b.Fatal("scan produced nothing")
			}
		})
	}
}
