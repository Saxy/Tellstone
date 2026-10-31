package bench

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Saxy/Tellstone/internal/router"
	"github.com/Saxy/Tellstone/internal/shard"
	"runtime"
	"time"
)

// Parallel router benchmarks compare direct execution (each caller mutates the
// engine under its lock) with the actor (mutations are queued to one goroutine
// per shard). Run with -cpu=1,4,16 to see how each scales with cores. The shard
// count follows GOMAXPROCS, matching the --shards default.

const benchKeyCount = 4096

var benchKeys = func() []string {
	k := make([]string, benchKeyCount)
	for i := range k {
		k[i] = fmt.Sprintf("bench:key:%d", i)
	}
	return k
}()

func newBenchRouter(b *testing.B, actor bool) *router.Router {
	b.Helper()
	n := runtime.GOMAXPROCS(0)
	shards := make([]*shard.Shard, n)
	for i := range shards {
		shards[i] = shard.New(i, time.Millisecond, 64, 0, nil, nil)
		if actor {
			shards[i].EnableActor(false)
		}
		s := shards[i]
		b.Cleanup(s.Close)
	}
	r := router.New(shards)
	for _, k := range benchKeys {
		r.Dispatch("SET", k, []byte("bench-value-32-bytes-long-xxxxxx"), 0)
	}
	return r
}

// keyCursor gives each parallel goroutine its own walk over benchKeys. A
// counter shared by all goroutines would be the bottleneck being measured.
type keyCursor struct {
	seed atomic.Uint64
}

func (c *keyCursor) newWalker() func() string {
	off := int(c.seed.Add(1) * 7919)
	i := 0
	return func() string {
		i++
		return benchKeys[(off+i)%benchKeyCount]
	}
}

func benchRouterParallel(b *testing.B, actor bool, op string) {
	r := newBenchRouter(b, actor)
	value := []byte("bench-value-32-bytes-long-xxxxxx")
	var cur keyCursor
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		next := cur.newWalker()
		for pb.Next() {
			k := next()
			if op == "GET" {
				r.Dispatch("GET", k, nil, 0)
			} else {
				r.Dispatch("SET", k, value, 0)
			}
		}
	})
}

func BenchmarkRouterParallelGetDirect(b *testing.B) { benchRouterParallel(b, false, "GET") }
func BenchmarkRouterParallelGetActor(b *testing.B)  { benchRouterParallel(b, true, "GET") }
func BenchmarkRouterParallelSetDirect(b *testing.B) { benchRouterParallel(b, false, "SET") }
func BenchmarkRouterParallelSetActor(b *testing.B)  { benchRouterParallel(b, true, "SET") }

// mutexMap is the baseline for the lock-free map: the same operations behind a
// single RWMutex.
type mutexMap struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func newMutexMap(n int) *mutexMap {
	mm := &mutexMap{m: make(map[string][]byte)}
	for i := 0; i < n; i++ {
		mm.m[benchKeys[i%benchKeyCount]] = []byte("v")
	}
	return mm
}

func (mm *mutexMap) get(k string) ([]byte, bool) {
	mm.mu.RLock()
	v, ok := mm.m[k]
	mm.mu.RUnlock()
	return v, ok
}

func BenchmarkMapParallelGetRWMutex(b *testing.B) {
	mm := newMutexMap(benchKeyCount)
	var cur keyCursor
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		next := cur.newWalker()
		for pb.Next() {
			mm.get(next())
		}
	})
}

func BenchmarkMapParallelGetLockFree(b *testing.B) {
	m := shard.NewLockFreeMap(benchKeyCount)
	for _, k := range benchKeys {
		m.Put([]byte(k), []byte("v"))
	}
	var cur keyCursor
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		next := cur.newWalker()
		for pb.Next() {
			m.Get([]byte(next()))
		}
	})
}

func BenchmarkLockFreeMapGet(b *testing.B) {
	m := shard.NewLockFreeMap(1024)
	for i := 0; i < 100; i++ {
		m.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get([]byte(fmt.Sprintf("key-%d", i%100)))
	}
}

func BenchmarkLockFreeMapPut(b *testing.B) {
	m := shard.NewLockFreeMap(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
}

func BenchmarkRCUMapGet(b *testing.B) {
	m := shard.NewRCUMap()
	for i := 0; i < 100; i++ {
		m.Put(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(fmt.Sprintf("key-%d", i%100))
	}
}

func BenchmarkRCUMapPut(b *testing.B) {
	m := shard.NewRCUMap()
	const keyCount = 100
	keys := make([]string, keyCount)
	values := make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = []byte(fmt.Sprintf("value-%d", i))
		m.Put(keys[i], values[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % keyCount
		m.Put(keys[j], values[j])
	}
}
