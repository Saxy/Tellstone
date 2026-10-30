/*
Package router
Tellstone Request Router
File: router.go
Description: Routes incoming requests to the correct shard using FNV-1a hashing. Uses modulo against the shard count so any positive shard count works, not only powers of two.

Authors:

	Maximilian Hagen
*/
package router

import (
	"bytes"
	"container/heap"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/shard"
)

type Router struct {
	shards    []*shard.Shard
	numShards uint32
}

func New(shards []*shard.Shard) *Router {
	return &Router{
		shards:    shards,
		numShards: uint32(len(shards)),
	}
}

func hashKey(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func (r *Router) Dispatch(op string, key string, value []byte, ttl time.Duration) shard.Response {
	sid := hashKey(key) % r.numShards
	return r.shards[sid].Execute(op, key, value, ttl)
}

// batchGroup collects the positions in the input that one shard owns.
type batchGroup struct {
	shardID uint32
	idx     []int
}

// DispatchBatch applies a batch of keyed mutations, grouping them by owning
// shard and dispatching each shard's group in parallel. Shards are independent,
// so their WAL flushes overlap: a transaction spanning three shards waits for
// one round of flushes rather than three sequential ones.
//
// Responses are returned in input order regardless of completion order. A batch
// that touches one shard is dispatched inline, without a goroutine.
func (r *Router) DispatchBatch(ops []shard.BatchOp) []shard.Response {
	if len(ops) == 0 {
		return nil
	}
	out := make([]shard.Response, len(ops))

	groups := make([]batchGroup, 0, r.numShards)
	slot := make([]int, r.numShards)
	for i := range slot {
		slot[i] = -1
	}
	for i, op := range ops {
		sid := hashKey(op.Key) % r.numShards
		g := slot[sid]
		if g < 0 {
			g = len(groups)
			slot[sid] = g
			groups = append(groups, batchGroup{shardID: sid})
		}
		groups[g].idx = append(groups[g].idx, i)
	}

	run := func(g batchGroup) {
		batch := make([]shard.BatchOp, len(g.idx))
		for j, idx := range g.idx {
			batch[j] = ops[idx]
		}
		resps := r.shards[g.shardID].ExecuteBatch(batch)
		for j, idx := range g.idx {
			out[idx] = resps[j]
		}
	}

	if len(groups) == 1 {
		run(groups[0])
		return out
	}
	var wg sync.WaitGroup
	wg.Add(len(groups))
	for _, g := range groups {
		go func(g batchGroup) {
			defer wg.Done()
			run(g)
		}(g)
	}
	wg.Wait()
	return out
}

func (r *Router) NumShards() int {
	return int(r.numShards)
}

// ShardID returns the shard index that owns a key, for diagnostic logging.
func (r *Router) ShardID(key string) uint32 {
	return hashKey(key) % r.numShards
}

// scanKV is one collected key/value pair. Values are cloned out of the engine
// because each shard's walk hands out slices valid only for the duration of its
// own call, and a merge outlives every one of those calls.
type scanKV struct {
	key string
	val []byte
}

// ScanPrefix walks every shard's ordered index for keys under prefix and
// delivers them to fn in key order, returning how many keys were delivered.
//
// A prefix is not owned by one shard: the router hashes the complete key, so a
// row's column keys scatter across shards and a row read must consult all of
// them. Each shard's walk is already in key order, so the results are merged
// rather than re-sorted.
//
// fn receives owned copies and may retain them.
func (r *Router) ScanPrefix(prefix string, fn func(key, value []byte) bool) int {
	runs := make([][]scanKV, 0, len(r.shards))
	for _, sh := range r.shards {
		run := make([]scanKV, 0, 16)
		sh.Engine.ScanPrefix(prefix, func(k, v []byte) bool {
			run = append(run, scanKV{key: string(k), val: bytes.Clone(v)})
			return true
		})
		if len(run) > 0 {
			runs = append(runs, run)
		}
	}
	if len(runs) == 0 {
		return 0
	}
	// A k-way merge over already-sorted runs, which is the shape a comparison
	// sort cannot exploit. One cursor per run, not per key.
	h := make(mergeHeap, 0, len(runs))
	for i := range runs {
		h = append(h, &mergeCursor{run: runs[i]})
	}
	heap.Init(&h)
	delivered := 0
	for h.Len() > 0 {
		cur := heap.Pop(&h).(*mergeCursor)
		kv := &cur.run[cur.pos]
		if !fn([]byte(kv.key), kv.val) {
			break
		}
		delivered++
		cur.pos++
		if cur.pos < len(cur.run) {
			heap.Push(&h, cur)
		}
	}
	return delivered
}

// mergeCursor is one run's current position in the merged sequence.
type mergeCursor struct {
	run []scanKV
	pos int
}

// key returns the key the cursor currently points at.
func (c *mergeCursor) key() string { return c.run[c.pos].key }

// mergeHeap is a min-heap of cursors ordered by the key under each cursor.
type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].key() < h[j].key() }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(*mergeCursor)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
