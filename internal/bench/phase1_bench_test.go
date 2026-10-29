package bench

import (
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
	"github.com/Saxy/Tellstone/internal/shard"
	"github.com/Saxy/Tellstone/internal/router"
)

// Phase1Benchmarks captures the "before" numbers for Phase 1 optimizations
// These benchmarks should be run before any Phase 1 optimizations are applied
// to establish a baseline for performance improvements.

func BenchmarkPhase1All(b *testing.B) {
	// Run all Phase 1 benchmarks
	b.Run("Persistence", func(b *testing.B) {
		BenchmarkWriteSequential(b)
		BenchmarkWriteBatch(b)
		BenchmarkReadSequential(b)
		BenchmarkReadBatch(b)
		BenchmarkScanPrefix(b)
	})
	
	b.Run("Router", func(b *testing.B) {
		BenchmarkRouterScanPrefix(b)
		BenchmarkRouterGet(b)
		BenchmarkRouterSet(b)
	})
	
}

// Helper function to create a router with 4 shards for testing
func createTestRouter() *router.Router {
	shards := make([]*shard.Shard, 4)
	for i := range shards {
		shards[i] = shard.New(i, time.Millisecond, 64, 0, nil, nil)
	}
	return router.New(shards)
}

// Helper function to set up test data for router benchmarks
func setupRouterTestData(r *router.Router) {
	// Insert some test data
	for i := 0; i < 100; i++ {
		key := "routerbench:scan:row" + string(rune('0'+i/10)) + ":col" + string(rune('0'+i%10))
		val := []byte("routerbench-value-32-bytes-long-xxxx")
		r.Dispatch("SET", key, val, 0)
	}
}

// Helper function to set up test data for persistence benchmarks
func setupPersistenceTestData(eng *storage.Engine) {
	// Insert some test data
	for i := 0; i < 100; i++ {
		key := "persistencebench:scan:row" + string(rune('0'+i/10)) + ":col" + string(rune('0'+i%10))
		val := []byte("persistencebench-value-32-bytes-long-xxxx")
		if err := eng.Set(key, val, 0); err != nil {
			panic(err)
		}
	}
}