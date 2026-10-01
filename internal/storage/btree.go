/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: btree.go
Description: Ordered, prefix-compressed key index backing Engine range scans
(ADR-013). A B+Tree: leaves are linked so a range scan after one seek walks leaf
to leaf with no re-descent, and internal nodes hold separator keys rather than
values. Keys are front coded, each entry storing only the bytes it does not
share with the previous key in the same node. Under the column-key layout every
key in a table shares "tellstone/users/", so neighbouring keys compress down to
the part that actually differs, which is what guardrail 2 is about.

Every key suffix lives in the owning node's arena rather than in a slice per
entry, which drops a 24 byte slice header and an allocation from each key. A
scan is served through a callback over pooled buffers so that reading a range
allocates nothing at all: the key is decoded into a buffer owned by the scanner
and handed to the callback, which is what lets a scan stream straight into a
wire frame without an intermediate copy.
Copyright (C) 2024 Saxy. All rights reserved.
*/
package storage

import (
	"sync"
	"time"
	"unsafe"
)

// Fanout bounds. 32 entries per node balances pointer chasing against the cost
// of decoding a node's entries, which is linear because each entry is front
// coded against its predecessor.
const (
	btreeMaxLeafEntries     = 32
	btreeMaxInternalEntries = 32
)

// btreeMaxEntries is the smaller of the two fanouts, which are kept equal. A
// node is sized once for a full load rather than grown by doubling, because the
// slack a doubled slice leaves is dead weight on every key in the node.
const btreeMaxEntries = btreeMaxLeafEntries

// reserve makes room for one more entry without letting append double the
// slice, which would strand a full node's worth of capacity.
func (n *btreeNode) reserve() {
	if len(n.keys) < cap(n.keys) || len(n.keys) >= btreeMaxEntries+1 {
		return
	}
	grown := make([]btreeEntry, len(n.keys), len(n.keys)+1)
	copy(grown, n.keys)
	n.keys = grown
}

// btreeKeyBuf is the stack buffer a search decodes keys into. Keys longer than
// this fall back to the heap, which is rare: the column-key layout keeps keys
// well inside it.
const btreeKeyBuf = 256

// btreeArenaSlack is the dead space a node's arena may carry before it is
// rebuilt. Recoding an entry re-appends its suffix, so a node drifts until it is
// compacted.
const btreeArenaSlack = 64

// btreeEntry is one key in a node, front coded against the entry before it.
// shared is how many leading bytes this key shares with the previous key in the
// node, and the remaining bytes live in the node's arena at suffixOff. On a leaf
// it also carries the value and the expiration, so a scan reads a row without
// consulting the items map: the duplicated expiration is deliberate, trading
// bytes to keep a scan free of per-key lookups.
//
// The value stays a slice because it is shared with the items map, so the index
// holds a header rather than a copy of the payload. Everything else is packed
// into offsets, which is what brings the index to roughly 40 bytes per key.
type btreeEntry struct {
	suffixOff uint32
	suffixLen uint16
	shared    uint16
	val       []byte
	exp       int64
}

// btreeNode is a leaf or an internal node. On an internal node children
// outnumber separator keys by one: kids[0] holds keys below keys[0], and
// kids[j+1] holds keys in [keys[j], keys[j+1]). Leaves chain through next and
// prev so a scan walks them in order and a removal can unlink a dropped node.
//
// Entries are decoded in order, so the first entry in a node always carries
// shared = 0 and holds its key in full.
type btreeNode struct {
	leaf  bool
	keys  []btreeEntry
	kids  []*btreeNode
	arena []byte

	next *btreeNode
	prev *btreeNode
}

// btree is the ordered index over live keys. It holds no lock of its own: the
// owning Engine guards it with the same lock that guards the items map.
type btree struct {
	root   *btreeNode
	length int
}

// newBTree returns an index holding one empty leaf, so a seek on an empty
// index needs no special case.
func newBTree() *btree {
	return &btree{root: &btreeNode{leaf: true}}
}

// btreeSplit is the result of an insertion that overflowed a node: the smallest
// key in right, and right itself, both to be adopted by the parent.
type btreeSplit struct {
	sep   string
	right *btreeNode
}

// encodeExp maps a zero expiration to the zero int64, so the leaf can test
// emptiness with a single comparison.
func encodeExp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// decodeExp is the inverse of encodeExp.
func decodeExp(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// suffixAt returns the front-coded remainder of entry i.
func (n *btreeNode) suffixAt(i int) []byte {
	e := &n.keys[i]
	return n.arena[e.suffixOff : e.suffixOff+uint32(e.suffixLen)]
}

// putSuffix appends s to the node's arena and returns where it landed. Suffixes
// are packed in insertion order and referenced by offset, so inserting a key
// never has to move the suffixes already in the arena. The argument is a string
// because appending one to a byte slice is compiled without the temporary copy
// that []byte(s) would allocate.
func (n *btreeNode) putSuffix(s string) uint32 {
	off := uint32(len(n.arena))
	n.arena = append(n.arena, s...)
	return off
}

// sharedPrefix returns how many leading bytes a and b have in common.
func sharedPrefix(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// compareBytes returns -1, 0 or 1 comparing a to b lexicographically.
func compareBytes(a []byte, b string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// decodeInto rebuilds entry e's full key into buf, given the key of the entry
// before it. Entries are front coded, so a search has to rebuild each key in
// order, and buf must not alias prev.
func decodeInto(prev []byte, e *btreeEntry, n *btreeNode, buf []byte) []byte {
	shared := int(e.shared)
	if shared > len(prev) {
		shared = len(prev)
	}
	buf = buf[:0]
	buf = append(buf, prev[:shared]...)
	return append(buf, n.arena[e.suffixOff:e.suffixOff+uint32(e.suffixLen)]...)
}

// search returns the first index in this node whose key is >= key, and whether
// that key is exactly key. The descent needs both: a child boundary advances
// only on an exact match. The two stack buffers alternate so a decode never
// overwrites the predecessor it reads from, and nothing here allocates.
func (n *btreeNode) search(key string) (int, bool) {
	var prevArr, curArr [btreeKeyBuf]byte
	prev, cur := prevArr[:0], curArr[:0]
	for i := range n.keys {
		cur = decodeInto(prev, &n.keys[i], n, cur)
		switch compareBytes(cur, key) {
		case -1:
			// Advance the cursor, reusing the older buffer for the next decode.
			prev, cur = cur, prev
		case 0:
			return i, true
		default:
			return i, false
		}
	}
	return len(n.keys), false
}

// lowerBound returns the first index in this node whose key is >= key.
func (n *btreeNode) lowerBound(key string) int {
	i, _ := n.search(key)
	return i
}

// childIndex returns the child whose key range can contain key.
func (n *btreeNode) childIndex(key string) int {
	i, exact := n.search(key)
	// kids[j+1] owns keys[j] itself, so the boundary advances only on an
	// exact match; a key merely below keys[i] still belongs to kids[i].
	if exact {
		return i + 1
	}
	return i
}

// entryKey materializes the key of entry i in node n. It walks from the start
// because entries are front coded, but it keeps one buffer rather than a string
// per step.
func (n *btreeNode) entryKey(i int) string {
	buf := make([]byte, 0, btreeKeyBuf)
	for j := 0; j <= i; j++ {
		buf = decodeInto(buf, &n.keys[j], n, buf[:0])
	}
	return string(buf)
}

// firstKey materializes the lowest key in the subtree.
func (n *btreeNode) firstKey() string {
	for !n.leaf {
		n = n.kids[0]
	}
	if len(n.keys) == 0 {
		return ""
	}
	return n.entryKey(0)
}

// btreeRec is a fully decoded entry. Re-encoding a node works from these
// because an entry's shared count is only meaningful next to its predecessor.
type btreeRec struct {
	key string
	val []byte
	exp int64
}

// fullRecs decodes every entry in the node in order, carrying values along.
func (n *btreeNode) fullRecs() []btreeRec {
	recs := make([]btreeRec, len(n.keys))
	prev := []byte(nil)
	for i := range n.keys {
		key := string(decodeInto(prev, &n.keys[i], n, prev[:0]))
		recs[i] = btreeRec{key: key, val: n.keys[i].val, exp: n.keys[i].exp}
		prev = []byte(key)
	}
	return recs
}

// fullKeys decodes every separator in the node, for nodes that hold no values.
func (n *btreeNode) fullKeys() []string {
	keys := make([]string, len(n.keys))
	prev := []byte(nil)
	for i := range n.keys {
		key := string(decodeInto(prev, &n.keys[i], n, prev[:0]))
		keys[i] = key
		prev = []byte(key)
	}
	return keys
}

// encodeRecs rewrites the node from decoded entries, front coding each against
// its predecessor and repacking the arena. The arena is rebuilt rather than
// patched, which is what reclaims the space left by dropped suffixes; it reuses
// its capacity, so a steady-state node stops allocating.
func (n *btreeNode) encodeRecs(recs []btreeRec) {
	// Both buffers are sized to what the node actually holds. Growing either by
	// doubling would strand roughly half of the allocation on every key in the
	// node, and a node keeps whatever capacity it reaches for its whole life, so
	// one insertion spike would otherwise be paid for by every key forever.
	var sharedArr [btreeMaxEntries + 1]int
	shared := sharedArr[:0]
	live := 0
	prev := ""
	for i, r := range recs {
		sh := 0
		if i > 0 {
			sh = sharedPrefix(prev, r.key)
		}
		shared = append(shared, sh)
		live += len(r.key) - sh
		prev = r.key
	}
	arena := live + btreeArenaSlack/4
	if cap(n.arena) < arena || cap(n.arena) > 2*arena {
		n.arena = make([]byte, 0, arena)
	} else {
		n.arena = n.arena[:0]
	}
	if cap(n.keys) < len(recs) || cap(n.keys) > len(recs)+2 {
		n.keys = make([]btreeEntry, len(recs), len(recs)+1)
	} else {
		n.keys = n.keys[:len(recs)]
	}
	prev = ""
	for i, r := range recs {
		off := n.putSuffix(r.key[shared[i]:])
		n.keys[i] = btreeEntry{
			suffixOff: off,
			suffixLen: uint16(len(r.key) - shared[i]),
			shared:    uint16(shared[i]),
			val:       r.val,
			exp:       r.exp,
		}
		prev = r.key
	}
}

// encodeKeys rewrites an internal node from decoded separators.
func (n *btreeNode) encodeKeys(keys []string) {
	recs := make([]btreeRec, len(keys))
	for i, k := range keys {
		recs[i] = btreeRec{key: k}
	}
	n.encodeRecs(recs)
}

// liveArena returns the bytes the node's suffixes actually occupy.
func (n *btreeNode) liveArena() int {
	total := 0
	for i := range n.keys {
		total += int(n.keys[i].suffixLen)
	}
	return total
}

// maybeCompact rebuilds the arena once dead suffixes outnumber live ones.
func (n *btreeNode) maybeCompact() {
	if len(n.arena) > n.liveArena()+btreeArenaSlack {
		n.encodeRecs(n.fullRecs())
	}
}

// recodeAt recomputes the front coding of the entry at i and its successor.
// succ is the key the successor is about to hold: it has to be decoded against
// its old predecessor, because after the insertion its stored suffix refers to
// a key that is no longer there.
func (n *btreeNode) recodeAt(i int, succ string) {
	if i == 0 {
		// The first entry of a node always holds its key in full.
		n.keys[0].shared = 0
	} else {
		key := n.entryKey(i)
		shared := sharedPrefix(n.entryKey(i-1), key)
		off := n.putSuffix(key[shared:])
		n.keys[i].shared = uint16(shared)
		n.keys[i].suffixOff, n.keys[i].suffixLen = off, uint16(len(key)-shared)
	}
	if j := i + 1; j < len(n.keys) {
		shared := sharedPrefix(n.entryKey(i), succ)
		off := n.putSuffix(succ[shared:])
		n.keys[j].shared = uint16(shared)
		n.keys[j].suffixOff, n.keys[j].suffixLen = off, uint16(len(succ)-shared)
	}
	n.maybeCompact()
}

// set inserts key with val, or replaces the value if key is already present.
func (t *btree) set(key string, val []byte, exp time.Time) {
	if t.root.contains(key) {
		t.root.insertNode(key, val, exp)
		return
	}
	t.length++
	if split := t.root.insertNode(key, val, exp); split != nil {
		// The root split, so grow the tree by one level. The promoted
		// separator is the smallest key of the new right subtree.
		t.root = &btreeNode{kids: []*btreeNode{t.root, split.right}}
		t.root.encodeKeys([]string{split.sep})
	}
}

// contains reports whether the subtree holds key.
func (n *btreeNode) contains(key string) bool {
	if n.leaf {
		i := n.lowerBound(key)
		return i < len(n.keys) && n.entryKey(i) == key
	}
	return n.kids[n.childIndex(key)].contains(key)
}

// remove deletes key, reporting whether it was present.
func (t *btree) remove(key string) bool {
	found, drop := t.root.removeNode(key)
	if !found {
		return false
	}
	t.length--
	if drop {
		// The last subtree emptied, so the tree is now empty. Reset to a
		// single empty leaf: a childless internal node would panic on the next
		// seek, and a detached leaf would be a root with no chain head.
		t.root = &btreeNode{leaf: true}
		return true
	}
	// Collapse a root left with a single child so the tree does not keep a
	// permanent extra level of indirection after deletions.
	for !t.root.leaf && len(t.root.kids) == 1 {
		t.root = t.root.kids[0]
	}
	return true
}

// insertNode inserts into the subtree and returns a split to propagate upward,
// or nil if this node absorbed the key.
func (n *btreeNode) insertNode(key string, val []byte, exp time.Time) *btreeSplit {
	if n.leaf {
		i := n.lowerBound(key)
		if i < len(n.keys) && n.entryKey(i) == key {
			n.keys[i].val = val
			n.keys[i].exp = encodeExp(exp)
			return nil
		}
		// The entry about to be pushed right keeps its own key, which is
		// measured against the key now landing at i.
		succ := ""
		if i < len(n.keys) {
			succ = n.entryKey(i)
		}
		n.reserve()
		n.keys = append(n.keys, btreeEntry{})
		copy(n.keys[i+1:], n.keys[i:])
		off := n.putSuffix(key)
		n.keys[i] = btreeEntry{
			suffixOff: off,
			suffixLen: uint16(len(key)),
			shared:    0,
			val:       val,
			exp:       encodeExp(exp),
		}
		// Only the inserted entry and the one it displaced have a predecessor
		// that moved, so only they can have a stale shared count.
		n.recodeAt(i, succ)
		if len(n.keys) <= btreeMaxLeafEntries {
			return nil
		}
		return n.splitLeaf()
	}

	i := n.childIndex(key)
	if split := n.kids[i].insertNode(key, val, exp); split != nil {
		n.adopt(split, i)
		if len(n.keys) <= btreeMaxInternalEntries {
			return nil
		}
		return n.splitInternal()
	}
	return nil
}

// adopt inserts a split separator and its right sibling at child position i.
func (n *btreeNode) adopt(split *btreeSplit, i int) {
	succ := ""
	if i < len(n.keys) {
		succ = n.entryKey(i)
	}
	n.reserve()
	n.keys = append(n.keys, btreeEntry{})
	copy(n.keys[i+1:], n.keys[i:])
	off := n.putSuffix(split.sep)
	n.keys[i] = btreeEntry{suffixOff: off, suffixLen: uint16(len(split.sep))}
	n.kids = append(n.kids, nil)
	copy(n.kids[i+2:], n.kids[i+1:])
	n.kids[i+1] = split.right
	n.recodeAt(i, succ)
}

// splitLeaf divides an overfull leaf in half and links the new one in. The
// entries are decoded before the node is repartitioned, because an entry's
// shared count is only valid next to its predecessor.
func (n *btreeNode) splitLeaf() *btreeSplit {
	recs := n.fullRecs()
	mid := len(recs) / 2
	right := &btreeNode{leaf: true}
	n.encodeRecs(recs[:mid])
	right.encodeRecs(recs[mid:])

	// Insert right after n in the leaf chain.
	right.next, right.prev = n.next, n
	if n.next != nil {
		n.next.prev = right
	}
	n.next = right
	return &btreeSplit{sep: right.firstKey(), right: right}
}

// splitInternal divides an overfull internal node, promoting one separator to
// the parent. The invariant kids[j+1] holds [keys[j], keys[j+1]) means the
// promoted key is the boundary between the two halves.
func (n *btreeNode) splitInternal() *btreeSplit {
	keys := n.fullKeys()
	mid := (len(keys) + 1) / 2
	// keys[mid-1] separates the left kids from the right kids, so it leaves
	// this node entirely and travels up.
	sep := keys[mid-1]

	right := &btreeNode{}
	n.encodeKeys(keys[:mid-1])
	right.encodeKeys(keys[mid:])
	right.kids = append(right.kids, n.kids[mid:]...)
	n.kids = n.kids[:mid]
	return &btreeSplit{sep: sep, right: right}
}

// removeNode deletes key from the subtree. It reports whether the key was
// found, and separately whether this node emptied and must be dropped by its
// parent. A leaf that empties unlinks itself from the leaf chain, but it stays
// reachable through the parent's children until that parent removes it, so the
// two outcomes cannot be collapsed into one flag.
func (n *btreeNode) removeNode(key string) (found, drop bool) {
	if n.leaf {
		i := n.lowerBound(key)
		if i >= len(n.keys) || n.entryKey(i) != key {
			return false, false
		}
		// The entries are decoded before the node is compacted and re-encoded
		// afterwards. A front-coded entry stores only the bytes past its
		// predecessor, so shifting entries in place would leave each survivor
		// holding a suffix that no longer has a predecessor to attach to.
		recs := n.fullRecs()
		recs = append(recs[:i], recs[i+1:]...)
		if len(recs) == 0 {
			n.unlink()
			return true, true
		}
		n.encodeRecs(recs)
		return true, false
	}

	i := n.childIndex(key)
	found, drop = n.kids[i].removeNode(key)
	if !found {
		return false, false
	}
	if drop {
		// The child emptied. kids[0] is introduced by no separator; any other
		// child shares the separator at i-1, which goes with it.
		sep := i - 1
		if sep < 0 {
			sep = 0
		}
		n.kids = append(n.kids[:i], n.kids[i+1:]...)
		// Same reasoning as the leaf: decode, drop the separator, re-encode.
		keys := n.fullKeys()
		if len(keys) > 0 {
			n.encodeKeys(append(keys[:sep], keys[sep+1:]...))
		}
		if len(n.kids) == 0 {
			return true, true
		}
		return true, false
	}
	return true, false
}

// unlink removes a leaf from the chain. A leaf with no predecessor is the head,
// in which case its successor becomes the new head with no predecessor.
func (n *btreeNode) unlink() {
	if n.prev != nil {
		n.prev.next = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	}
	n.prev, n.next = nil, nil
}

// btreeIter walks entries in order. A key is decoded exactly once, when next
// reaches it, and cached in cur: entries are front coded against their
// predecessor, so decoding the same entry twice would read it against a cursor
// that has already moved past it. The cached key aliases a reused buffer and is
// valid only until the next call to next.
type btreeIter struct {
	leaf *btreeNode
	cur  string
	// pos is the next entry to decode. It only ever moves forward, which is
	// what keeps the front-coding cursor consistent.
	pos int
	end string

	scratch []byte
	// prevKey holds the key of the last decoded entry, which is what the next
	// entry's shared count is measured against.
	prevKey []byte
}

// seek returns an iterator at the first key >= lower, bounded above by end
// (empty for unbounded). It is the phase 9 row and table scan entry point: one
// descent, then a walk over linked leaves.
func (t *btree) seek(lower, end string) *btreeIter {
	n := t.root
	for !n.leaf {
		n = n.kids[n.childIndex(lower)]
	}
	// The front-coding cursor has to be primed with every entry before the
	// start, because each entry is decoded against its predecessor.
	start := n.lowerBound(lower)
	it := &btreeIter{leaf: n, end: end}
	for i := 0; i < start; i++ {
		it.decode(i)
	}
	it.pos = start
	return it
}

// next advances the iterator, reporting whether an entry remains.
func (it *btreeIter) next() bool {
	for {
		if it.leaf == nil {
			return false
		}
		if it.pos < len(it.leaf.keys) {
			key := it.decode(it.pos)
			if it.end != "" && key >= it.end {
				it.cur = ""
				return false
			}
			it.pos++
			it.cur = key
			return true
		}
		it.leaf = it.leaf.next
		it.pos = 0
		it.prevKey = it.prevKey[:0]
	}
}

// decode rebuilds the entry at position i and advances the front-coding cursor.
func (it *btreeIter) decode(i int) string {
	buf := it.scratch[:0]
	if i == 0 {
		// The first entry of a node always holds its key in full.
		buf = append(buf, it.leaf.suffixAt(0)...)
	} else {
		e := &it.leaf.keys[i]
		shared := int(e.shared)
		if shared > len(it.prevKey) {
			shared = len(it.prevKey)
		}
		buf = append(buf, it.prevKey[:shared]...)
		buf = append(buf, it.leaf.suffixAt(i)...)
	}
	it.scratch = buf
	it.prevKey = append(it.prevKey[:0], buf...)
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

// key returns the current entry's key.
func (it *btreeIter) key() string { return it.cur }

// value returns the current entry's value and expiration.
func (it *btreeIter) value() ([]byte, time.Time) {
	if it.leaf == nil {
		return nil, time.Time{}
	}
	e := &it.leaf.keys[it.pos-1]
	return e.val, decodeExp(e.exp)
}

// btreeScanBuf holds the decode buffers a scan walks a range with. The buffers
// belong to the scanner rather than to the index because a scan is the hot path
// for a range read, and the key handed to the callback has to be stable for the
// length of the call without anything being allocated per entry.
type btreeScanBuf struct {
	prev []byte
	cur  []byte
}

// btreeScanPool recycles scan buffers. A callback of unknown shape forces Go to
// assume its arguments escape, so a stack buffer would be heap allocated no
// matter how the scan is written; pooling is what actually makes a scan free.
var btreeScanPool = sync.Pool{New: func() any { return new(btreeScanBuf) }}

// ScanPrefix calls fn for every key beginning with prefix, in order, stopping
// early if fn returns false. key and val are only valid for the duration of the
// call: they alias buffers the scanner recycles, which is what lets a scan hand
// rows straight to a writer without copying them.
//
// A scan allocates nothing once the pool is warm.
func (t *btree) ScanPrefix(prefix string, fn func(key, val []byte, exp time.Time) bool) {
	buf := btreeScanPool.Get().(*btreeScanBuf)
	t.scanPrefixBuf(prefix, buf, fn)
	btreeScanPool.Put(buf)
}

// scanPrefixBuf is ScanPrefix over caller-owned buffers, for a caller that would
// rather keep them in its own frame.
func (t *btree) scanPrefixBuf(prefix string, sb *btreeScanBuf, fn func(key, val []byte, exp time.Time) bool) {
	n := t.root
	for !n.leaf {
		n = n.kids[n.childIndex(prefix)]
	}
	for ; n != nil; n = n.next {
		// Each leaf starts a fresh front-coding run: its first entry is always
		// in full, so the cursor resets here rather than carrying across.
		sb.prev = sb.prev[:0]
		for i := range n.keys {
			cur := decodeInto(sb.prev, &n.keys[i], n, sb.cur)
			switch {
			case compareBytes(cur, prefix) < 0:
				// The leaf the descent landed in covers a range that begins at
				// or below the prefix, so its first entries can sort before it
				// while a later entry in the same leaf still matches. Skipping
				// one is not the same as stopping.
			case !hasPrefixBytes(cur, prefix):
				// At or past the prefix but not carrying it, so this key
				// diverges above every prefixed key. The keys are ordered and
				// the leaves are chained in order, so nothing further matches.
				return
			default:
				e := &n.keys[i]
				if !fn(cur, e.val, decodeExp(e.exp)) {
					return
				}
			}
			sb.prev, sb.cur = cur, sb.prev
		}
	}
}

// hasPrefixBytes reports whether b begins with prefix, without allocating the
// conversion that string(b) would otherwise imply.
func hasPrefixBytes(b []byte, prefix string) bool {
	return len(b) >= len(prefix) && string(b[:len(prefix)]) == prefix
}
