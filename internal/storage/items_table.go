package storage

import "sync/atomic"

// itemTable maps keys to items. Load takes no lock: it walks a bucket chain
// whose nodes are never re-linked through a removed node, and it reads each
// item through one atomic pointer. Every write goes through put, remove or grow,
// which the caller must serialize (the engine's write lock does this).
//
// An item is never modified after it is published. An update stores a fresh
// Item behind the node's pointer, so a reader sees either the old item or the
// new one, never a mix of the two.
//
// grow builds a new bucket array from copies of the nodes and publishes it
// with one atomic store. A reader that loaded the old array keeps a consistent
// view of it, because nothing writes to the old array after the swap.
type itemTable struct {
	arr atomic.Pointer[bucketArray]
	n   int // guarded by the caller's write lock
}

type bucketArray struct {
	heads []atomic.Pointer[itemEntry]
	mask  uint64
}

type itemEntry struct {
	key  string
	item atomic.Pointer[Item]
	next atomic.Pointer[itemEntry]
}

// itemSlot co-locates an entry with the first Item stored for it, so an insert
// costs one allocation for both.
type itemSlot struct {
	itemEntry
	item Item
}

const itemTableMinBuckets = 16

func newBucketArray(n int) *bucketArray {
	return &bucketArray{heads: make([]atomic.Pointer[itemEntry], n), mask: uint64(n - 1)}
}

// newItemTable returns an empty table sized for about hint keys before its
// first grow.
func newItemTable(hint int) *itemTable {
	n := itemTableMinBuckets
	for n < hint {
		n <<= 1
	}
	t := &itemTable{}
	t.arr.Store(newBucketArray(n))
	return t
}

// hashString is FNV-1a, computed without converting the key to bytes.
func hashString(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// load returns the item stored under key. It takes no lock.
func (t *itemTable) load(key string) (Item, bool) {
	a := t.arr.Load()
	for en := a.heads[hashString(key)&a.mask].Load(); en != nil; en = en.next.Load() {
		if en.key == key {
			return *en.item.Load(), true
		}
	}
	return Item{}, false
}

// put stores it under key and returns the item it replaced, if any. Caller must
// hold the write lock.
func (t *itemTable) put(key string, it Item) (Item, bool) {
	a := t.arr.Load()
	idx := hashString(key) & a.mask
	for en := a.heads[idx].Load(); en != nil; en = en.next.Load() {
		if en.key == key {
			old := *en.item.Load()
			ni := new(Item)
			*ni = it
			en.item.Store(ni)
			return old, true
		}
	}
	s := &itemSlot{item: it}
	s.key = key
	s.itemEntry.item.Store(&s.item)
	s.next.Store(a.heads[idx].Load())
	a.heads[idx].Store(&s.itemEntry)
	t.n++
	if t.n*4 > len(a.heads)*3 {
		t.grow()
	}
	return Item{}, false
}

// remove deletes key and returns the item it held. Caller must hold the write
// lock. The removed node keeps its next pointer, so a reader already standing
// on it can finish its walk.
func (t *itemTable) remove(key string) (Item, bool) {
	a := t.arr.Load()
	idx := hashString(key) & a.mask
	var prev *itemEntry
	for en := a.heads[idx].Load(); en != nil; en = en.next.Load() {
		if en.key == key {
			next := en.next.Load()
			if prev == nil {
				a.heads[idx].Store(next)
			} else {
				prev.next.Store(next)
			}
			t.n--
			return *en.item.Load(), true
		}
		prev = en
	}
	return Item{}, false
}

func (t *itemTable) grow() {
	old := t.arr.Load()
	na := newBucketArray(len(old.heads) * 2)
	for i := range old.heads {
		for en := old.heads[i].Load(); en != nil; en = en.next.Load() {
			c := &itemEntry{key: en.key}
			c.item.Store(en.item.Load())
			j := hashString(en.key) & na.mask
			c.next.Store(na.heads[j].Load())
			na.heads[j].Store(c)
		}
	}
	t.arr.Store(na)
}

// each calls fn for every entry. Caller must hold the write lock so the walk
// sees one consistent state.
func (t *itemTable) each(fn func(key string, it Item)) {
	a := t.arr.Load()
	for i := range a.heads {
		for en := a.heads[i].Load(); en != nil; en = en.next.Load() {
			fn(en.key, *en.item.Load())
		}
	}
}
