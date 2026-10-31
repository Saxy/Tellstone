package shard

import (
	"sync"
	"sync/atomic"
)

// LockFreeMap is a hash map for read-heavy hot paths. Get never takes a lock:
// it loads the current table and walks an immutable-once-published chain. Put
// serializes writers on a mutex, so writes are not lock-free, but a reader is
// never blocked by a writer or by a resize.
//
// A resize builds a new table from copies of the nodes and then publishes it
// with one atomic store. A reader that loaded the old table keeps a consistent
// view of it, because the old table is never modified again.
type LockFreeMap struct {
	table atomic.Pointer[lfTable]

	writeMu sync.Mutex
	count   int // guarded by writeMu
}

type lfTable struct {
	buckets []atomic.Pointer[Bucket]
	mask    int
}

// Bucket is one node in a bucket chain.
type Bucket struct {
	key  []byte
	val  atomic.Pointer[valueBox]
	next atomic.Pointer[Bucket]
}

// valueBox wraps a value so an update is a single atomic pointer store.
type valueBox struct {
	v []byte
}

const lfMinBuckets = 16

// NewLockFreeMap creates a map with room for about size keys before its first
// resize. The bucket count is rounded up to a power of two.
func NewLockFreeMap(size int) *LockFreeMap {
	n := lfMinBuckets
	for n < size {
		n <<= 1
	}
	m := &LockFreeMap{}
	m.table.Store(newLFTable(n))
	return m
}

func newLFTable(n int) *lfTable {
	return &lfTable{buckets: make([]atomic.Pointer[Bucket], n), mask: n - 1}
}

// Get returns the value stored under key. It takes no lock. The returned slice
// must not be modified.
func (m *LockFreeMap) Get(key []byte) ([]byte, bool) {
	t := m.table.Load()
	for b := t.buckets[hash(key)&t.mask].Load(); b != nil; b = b.next.Load() {
		if equal(b.key, key) {
			vb := b.val.Load()
			if vb == nil {
				return nil, false
			}
			return vb.v, true
		}
	}
	return nil, false
}

// Put stores value under key, replacing any previous value. The map keeps its
// own copies of key and value.
func (m *LockFreeMap) Put(key []byte, value []byte) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	t := m.table.Load()
	idx := hash(key) & t.mask
	for b := t.buckets[idx].Load(); b != nil; b = b.next.Load() {
		if equal(b.key, key) {
			b.val.Store(&valueBox{v: cloneBytes(value)})
			return
		}
	}

	n := &Bucket{key: cloneBytes(key)}
	n.val.Store(&valueBox{v: cloneBytes(value)})
	n.next.Store(t.buckets[idx].Load())
	t.buckets[idx].Store(n)
	m.count++

	if m.count*4 > len(t.buckets)*3 {
		m.table.Store(resize(t, len(t.buckets)*2))
	}
}

// resize rehashes every node of old into a new table of n buckets. The nodes
// are copied, so no node reachable from old is ever relinked.
func resize(old *lfTable, n int) *lfTable {
	nt := newLFTable(n)
	for i := range old.buckets {
		for b := old.buckets[i].Load(); b != nil; b = b.next.Load() {
			c := &Bucket{key: b.key}
			c.val.Store(b.val.Load())
			j := hash(b.key) & nt.mask
			c.next.Store(nt.buckets[j].Load())
			nt.buckets[j].Store(c)
		}
	}
	return nt
}

// RCUMap is a read-copy-update map for read-heavy workloads where updates are
// rare. Get is lock-free. Each Put copies the whole map, so writes cost O(n).
type RCUMap struct {
	data atomic.Pointer[MapData]
	mu   sync.Mutex
}

// MapData is the immutable snapshot an RCUMap publishes.
type MapData struct {
	items map[string][]byte
}

// NewRCUMap creates an empty RCU map.
func NewRCUMap() *RCUMap {
	return &RCUMap{}
}

// Get returns a copy of the value stored under key. It takes no lock.
func (m *RCUMap) Get(key string) ([]byte, bool) {
	data := m.data.Load()
	if data == nil {
		return nil, false
	}
	value, ok := data.items[key]
	if !ok {
		return nil, false
	}
	return cloneBytes(value), true
}

// Put stores value under key by publishing a new copy of the map.
func (m *RCUMap) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.data.Load()
	var items map[string][]byte
	if current != nil {
		items = make(map[string][]byte, len(current.items)+1)
		for k, v := range current.items {
			items[k] = v
		}
	} else {
		items = make(map[string][]byte)
	}
	items[key] = cloneBytes(value)
	m.data.Store(&MapData{items: items})
}

// hash is FNV-1a over the key, so the low bits used by the mask are well mixed.
func hash(key []byte) int {
	h := uint32(2166136261)
	for _, b := range key {
		h ^= uint32(b)
		h *= 16777619
	}
	return int(h & 0x7fffffff)
}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
