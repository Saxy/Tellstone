package bench

import (
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/shard"
	"github.com/Saxy/Tellstone/internal/router"
)

func BenchmarkRouterScanPrefix(b *testing.B) {
	// Create a router with 4 shards
	shards := make([]*shard.Shard, 4)
	for i := range shards {
		shards[i] = shard.New(i, time.Millisecond, 64, 0, nil, nil)
	}
	r := router.New(shards)
	
	// Set up some test data
	for i := 0; i < 100; i++ {
		key := "routerbench:scan:row" + string(rune('0'+i/10)) + ":col" + string(rune('0'+i%10))
		val := []byte("routerbench-value-32-bytes-long-xxxx")
		r.Dispatch("SET", key, val, 0)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.ScanPrefix("routerbench:scan:", func(key, value []byte) bool {
			return true
		})
	}
}

func BenchmarkRouterGet(b *testing.B) {
	// Create a router with 4 shards
	shards := make([]*shard.Shard, 4)
	for i := range shards {
		shards[i] = shard.New(i, time.Millisecond, 64, 0, nil, nil)
	}
	r := router.New(shards)
	
	// Set up some test data
	key := "routerbench:get:1"
	val := []byte("routerbench-value-32-bytes-long-xxxx")
	r.Dispatch("SET", key, val, 0)
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Dispatch("GET", key, nil, 0)
	}
}

func BenchmarkRouterSet(b *testing.B) {
	// Create a router with 4 shards
	shards := make([]*shard.Shard, 4)
	for i := range shards {
		shards[i] = shard.New(i, time.Millisecond, 64, 0, nil, nil)
	}
	r := router.New(shards)
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := "routerbench:set:" + string(rune('0'+i%10))
		val := []byte("routerbench-value-32-bytes-long-xxxx")
		r.Dispatch("SET", key, val, 0)
	}
}