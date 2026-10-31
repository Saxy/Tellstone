/*
Package router
Tellstone Request Router
File: benchmark_scan_test.go
Description: Allocation benchmarks for the cross-shard range read. These are the
Phase 1 "before" numbers for Router.ScanPrefix: the engine serves a prefix walk
without allocating (internal/storage.Engine.ScanPrefix), but the router hashes
whole keys, so a row's columns scatter across every shard and the merge has to
collect them. What that collection costs per key is what these measure.

The key layout mirrors ADR-013, <db>/<table>/<row-id>/<column>, because that is
what makes the numbers predictive: a SQL row read is a prefix walk over one
row's column keys, and a table scan is a walk over a whole table prefix.

Authors:

	Maximilian Hagen
*/
package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/shard"
)

// newBenchRouter builds a router over numShards real shards, seeded with rows
// rows of cols columns each in the Phase 9 key layout. Writes go through
// Dispatch so each key lands on the shard that owns it in production.
func newBenchRouter(b *testing.B, numShards, rows, cols int) *Router {
	b.Helper()
	cfg := config.LoadConfig([]string{fmt.Sprintf("-shards=%d", numShards)})
	shards := make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		s, err := shard.Run(shard.ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
		if err != nil {
			b.Fatalf("shard %d: %v", i, err)
		}
		b.Cleanup(func() { _ = s.Stop(context.Background()) })
		shards[i] = s
	}
	r := New(shards)
	value := make([]byte, 64)
	for i := range value {
		value[i] = 'v'
	}
	for row := 0; row < rows; row++ {
		// Fixed-width row ids keep the byte order of the row-id segment
		// meaningful, matching a BIGINT primary key under ADR-013 §4.
		prefix := fmt.Sprintf("tellstone/users/%012d/", row)
		for c := 0; c < cols; c++ {
			resp := r.Dispatch(shard.CmdSet, prefix+benchColumn(c), value, 0)
			if resp.Err != nil {
				b.Fatalf("seed row %d col %d: %v", row, c, resp.Err)
			}
		}
	}
	return r
}

func benchColumn(c int) string {
	switch c {
	case 0:
		return "id"
	case 1:
		return "name"
	case 2:
		return "age"
	default:
		return fmt.Sprintf("col%d", c)
	}
}

// BenchmarkRouterScanPrefixRow is the SQL point-read shape: reconstruct one row
// from its column keys. Every production SQL read reaches the store through
// this path (server.RouterStore.ScanPrefix), so its per-key cost is multiplied
// by every cell of every row a query touches.
func BenchmarkRouterScanPrefixRow(b *testing.B) {
	for _, shards := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("shards=%d/cols=3", shards), func(b *testing.B) {
			r := newBenchRouter(b, shards, 1000, 3)
			// Walk 100 different rows so the loop is not one hot prefix that
			// every cache level has memorized. The prefixes are built up front
			// because a Sprintf in the loop allocates, and this benchmark
			// exists to report the router's allocations, not the harness's.
			prefixes := make([]string, 100)
			for i := range prefixes {
				prefixes[i] = fmt.Sprintf("tellstone/users/%012d/", i)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				prefix := prefixes[i%len(prefixes)]
				hit := 0
				r.ScanPrefix(prefix, func(_, _ []byte) bool {
					hit++
					return true
				})
				if hit != 3 {
					b.Fatalf("row %q: reconstructed %d columns, want 3", prefix, hit)
				}
			}
		})
	}
}

// BenchmarkRouterScanPrefixTable is the full-table scan shape: every column key
// of every row, which is what a sequential SELECT drives. It is the case where
// the per-key collection cost dominates, because the merge holds one copy of
// every key in the table before the first row reaches the caller.
func BenchmarkRouterScanPrefixTable(b *testing.B) {
	for _, rows := range []int{100, 1000} {
		b.Run(fmt.Sprintf("shards=16/rows=%d/cols=3", rows), func(b *testing.B) {
			r := newBenchRouter(b, 16, rows, 3)
			want := rows * 3
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				hit := 0
				r.ScanPrefix("tellstone/users/", func(_, _ []byte) bool {
					hit++
					return true
				})
				if hit != want {
					b.Fatalf("table scan delivered %d keys, want %d", hit, want)
				}
			}
		})
	}
}
