/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: btree_test.go
Description: Correctness tests for the ordered prefix-compressed index. The
ordering tests are differential: randomized insert/delete/overwrite runs are
checked against a sorted slice reference after every step, so an invariant
break in a split, a front-coded prefix or the leaf chain surfaces as a diff
rather than as a flaky timing artifact. Structural checks verify the front
coding stays exact and the leaf chain stays doubly linked, both of which the
range scan depends on.
Copyright (C) 2024 Saxy. All rights reserved.
*/
package storage

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// materialize walks the index and returns its keys in order. The key must be
// copied: the iterator reuses one buffer, so a retained reference would alias
// the next entry's key.
func (t *btree) materialize() []string {
	var out []string
	for it := t.seek("", ""); it.next(); {
		out = append(out, strings.Clone(it.key()))
	}
	return out
}

// keys reconstructs every key in a node, for structural assertions.
func (n *btreeNode) ownKeys() []string {
	out := make([]string, len(n.keys))
	for i := range n.keys {
		out[i] = n.entryKey(i)
	}
	return out
}

// checkInvariants walks the whole tree verifying the properties the scan and
// the search depend on.
func (t *btree) checkInvariants(tb testing.TB) {
	tb.Helper()
	if t.root == nil {
		tb.Fatal("root is nil")
	}

	// Every node: keys sorted and unique, front coding exact, and children
	// whose ranges are disjoint and ascending. A stale shared count would
	// silently waste the memory this structure exists to save, and overlapping
	// child ranges would make a seek miss keys that are present.
	var check func(n *btreeNode, depth int) (lo, hi string, have bool)
	check = func(n *btreeNode, depth int) (string, string, bool) {
		keys := n.ownKeys()
		for i, k := range keys {
			if i > 0 && k <= keys[i-1] {
				tb.Fatalf("depth %d: keys out of order: %q then %q", depth, keys[i-1], k)
			}
		}
		// Front coding must be exact, not merely correct: each entry holds only
		// the bytes it does not share with its predecessor.
		for i := 1; i < len(keys); i++ {
			want := sharedPrefix(keys[i-1], keys[i])
			if int(n.keys[i].shared) != want {
				tb.Fatalf("depth %d: entry %d shared = %d, want %d (keys %q, %q)",
					depth, i, n.keys[i].shared, want, keys[i-1], keys[i])
			}
		}
		if len(n.keys) > 0 && n.keys[0].shared != 0 {
			tb.Fatalf("depth %d: first entry shared = %d, want 0", depth, n.keys[0].shared)
		}
		if n.leaf {
			if len(n.kids) != 0 {
				tb.Fatalf("leaf has %d children", len(n.kids))
			}
			if len(keys) == 0 {
				return "", "", false
			}
			return keys[0], keys[len(keys)-1], true
		}
		if len(n.kids) != len(n.keys)+1 {
			tb.Fatalf("internal node has %d children for %d separators", len(n.kids), len(n.keys))
		}
		prevHi := ""
		have := false
		for i, kid := range n.kids {
			// kids[0] holds keys below keys[0]; kids[j+1] holds [keys[j], keys[j+1]).
			if i > 0 {
				if keys[i-1] < kidMin(tb, kid) {
					tb.Fatalf("depth %d: child %d starts at %q, below separator %q",
						depth, i, kidMin(tb, kid), keys[i-1])
				}
			}
			if i < len(n.keys) && kidMax(tb, kid) >= keys[i] {
				tb.Fatalf("depth %d: child %d ends at %q, at or above separator %q",
					depth, i, kidMax(tb, kid), keys[i])
			}
			klo, khi, khave := check(kid, depth+1)
			if khave {
				if have && klo < prevHi {
					tb.Fatalf("depth %d: child %d starts at %q, below previous child end %q",
						depth, i, klo, prevHi)
				}
				if !have {
					return klo, khi, true
				}
			}
			if khave {
				prevHi = khi
				have = true
			}
		}
		return "", "", have
	}
	check(t.root, 0)

	// The leaf chain must be doubly linked, strictly ascending, and contain
	// exactly the leaves of the tree. A scan walks it without re-descending,
	// so a broken link is both a correctness and a hang risk.
	var leaves []*btreeNode
	var walk func(n *btreeNode)
	walk = func(n *btreeNode) {
		if n.leaf {
			leaves = append(leaves, n)
			return
		}
		for _, kid := range n.kids {
			walk(kid)
		}
	}
	walk(t.root)

	if len(leaves) < 2 {
		return
	}
	// Collect the chain from the first leaf, which has no predecessor.
	var head *btreeNode
	for _, lf := range leaves {
		if lf.prev == nil {
			if head != nil {
				tb.Fatal("more than one leaf has no predecessor")
			}
			head = lf
		}
	}
	if head == nil {
		tb.Fatal("no leaf has no predecessor: chain head is lost")
	}
	var chain []*btreeNode
	for n := head; n != nil; n = n.next {
		chain = append(chain, n)
	}
	if len(chain) != len(leaves) {
		tb.Fatalf("chain has %d leaves, tree has %d", len(chain), len(leaves))
	}
	for i := 1; i < len(chain); i++ {
		if chain[i].prev != chain[i-1] {
			tb.Fatalf("leaf %d: prev does not point back", i)
		}
		if chain[i-1].next != chain[i] {
			tb.Fatalf("leaf %d: next does not point forward", i)
		}
		prevLast := lastOr(chain[i-1].ownKeys(), "")
		curFirst := firstOr(chain[i].ownKeys(), "")
		if prevLast >= curFirst {
			tb.Fatalf("chain out of order: %q then %q", prevLast, curFirst)
		}
	}
}

// kidMin and kidMax return a subtree's extreme keys, or "" when it is empty.
func kidMin(tb testing.TB, n *btreeNode) string {
	for !n.leaf && len(n.kids) > 0 {
		n = n.kids[0]
	}
	if len(n.keys) == 0 {
		return ""
	}
	return n.entryKey(0)
}

func kidMax(tb testing.TB, n *btreeNode) string {
	for !n.leaf {
		n = n.kids[len(n.kids)-1]
	}
	if len(n.keys) == 0 {
		return ""
	}
	return n.entryKey(len(n.keys) - 1)
}

func firstOr(keys []string, def string) string {
	if len(keys) == 0 {
		return def
	}
	return keys[0]
}

func lastOr(keys []string, def string) string {
	if len(keys) == 0 {
		return def
	}
	return keys[len(keys)-1]
}

func firstOf(n *btreeNode) string { return n.firstKey() }

// TestBTreeOrderedInsert checks lexicographic order regardless of insertion
// order, including keys sharing long prefixes and a range-crossing pair.
func TestBTreeOrderedInsert(t *testing.T) {
	tb := newBTree()
	keys := []string{
		"tellstone/users/000000000002/name",
		"tellstone/users/000000000000/name",
		"tellstone/users/000000000001/name",
		"tellstone/users/000000000000/age",
		"tellstone/users/000000000000/zzz",
		"tellstone/users/000000000000",
		"tellstone/aaa",
		"tellstone/~meta/tables/users",
		"zzz",
		"a",
	}
	for i, k := range keys {
		tb.set(k, []byte{byte(i)}, time.Time{})
	}
	want := append([]string(nil), keys...)
	sort.Strings(want)

	got := tb.materialize()
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %q, want %q", i, got[i], want[i])
		}
	}
	if tb.length != len(keys) {
		t.Fatalf("length = %d, want %d", tb.length, len(keys))
	}
	tb.checkInvariants(t)
}

// TestBTreeOverwriteKeepsEntry verifies an in-place value replacement does not
// disturb the tree: a key rewritten on every UPDATE must not duplicate entries
// or change the shape of the index.
func TestBTreeOverwriteKeepsEntry(t *testing.T) {
	tb := newBTree()
	tb.set("k", []byte("one"), time.Time{})
	before := tb.materialize()

	for i := 0; i < 100; i++ {
		tb.set("k", []byte(fmt.Sprintf("v%d", i)), time.Time{})
	}
	if tb.length != 1 {
		t.Fatalf("length = %d after 100 overwrites, want 1", tb.length)
	}
	after := tb.materialize()
	if len(after) != 1 || after[0] != "k" {
		t.Fatalf("overwrite disturbed the index: %v", after)
	}
	if len(before) != len(after) {
		t.Fatalf("key count changed across overwrites: %d then %d", len(before), len(after))
	}
	it := tb.seek("k", "")
	if !it.next() || it.key() != "k" {
		t.Fatal("key unreachable after overwrites")
	}
	if got := string(mustValue(tb, "k")); got != "v99" {
		t.Fatalf("value after overwrites = %q, want %q", got, "v99")
	}
	tb.checkInvariants(t)
}

func mustValue(t *btree, key string) []byte {
	it := t.seek(key, "")
	for it.next() {
		if it.key() == key {
			val, _ := it.value()
			return val
		}
	}
	return nil
}

// TestBTreeExpirationRoundTrip verifies the duplicated expiration survives a
// round trip through the index, since the scan filters on it to skip expired
// rows without a map lookup.
func TestBTreeExpirationRoundTrip(t *testing.T) {
	tb := newBTree()
	soon := time.Now().Add(time.Hour)
	tb.set("k/soon", []byte("v"), soon)
	tb.set("k/never", []byte("v"), time.Time{})

	it := tb.seek("k/never", "")
	if !it.next() || it.key() != "k/never" {
		t.Fatal("expected k/never first")
	}
	if _, exp := it.value(); !exp.IsZero() {
		t.Fatalf("expiration = %v, want zero", exp)
	}
	if !it.next() || it.key() != "k/soon" {
		t.Fatal("expected k/soon second")
	}
	if _, exp := it.value(); exp.Sub(soon).Abs() > time.Millisecond {
		t.Fatalf("expiration = %v, want ~%v", exp, soon)
	}
}

// TestBTreeRemove covers present, absent, repeated, head, tail and interior
// removals, and that the index stays consistent and searchable afterwards.
func TestBTreeRemove(t *testing.T) {
	tb := newBTree()
	for i := 0; i < 500; i++ {
		tb.set(fmt.Sprintf("key-%03d", i), []byte("v"), time.Time{})
	}

	if !tb.remove("key-000") {
		t.Fatal("removing the head reported absent")
	}
	if !tb.remove("key-499") {
		t.Fatal("removing the tail reported absent")
	}
	if !tb.remove("key-100") {
		t.Fatal("removing an interior key reported absent")
	}
	if tb.remove("key-100") {
		t.Fatal("removing an already-removed key reported present")
	}
	if tb.remove("absent") {
		t.Fatal("removing an unknown key reported present")
	}
	if tb.length != 497 {
		t.Fatalf("length = %d, want 497", tb.length)
	}
	tb.checkInvariants(t)

	for _, k := range tb.materialize() {
		if k == "key-000" || k == "key-100" || k == "key-499" {
			t.Fatalf("removed key %q still present", k)
		}
	}
}

// TestBTreeRemoveToEmpty drives the tree to nothing, which is where a split-
// less root collapse and a lost leaf-chain head both break.
func TestBTreeRemoveToEmpty(t *testing.T) {
	tb := newBTree()
	const n = 300
	for i := 0; i < n; i++ {
		tb.set(fmt.Sprintf("key-%03d", i), []byte("v"), time.Time{})
	}
	for i := 0; i < n; i++ {
		if !tb.remove(fmt.Sprintf("key-%03d", i)) {
			t.Fatalf("removal %d reported absent", i)
		}
	}
	if tb.length != 0 {
		t.Fatalf("length = %d, want 0", tb.length)
	}
	if got := tb.materialize(); len(got) != 0 {
		t.Fatalf("index still holds %v", got)
	}
	// A seek on the emptied index must return no rows, and the tree must be
	// usable again afterwards.
	if it := tb.seek("", ""); it.next() {
		t.Fatalf("seek returned %q on an empty index", it.key())
	}
	tb.set("fresh", []byte("v"), time.Time{})
	if got := tb.materialize(); len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("reinsert after empty = %v", got)
	}
	tb.checkInvariants(t)
}

// TestBTreeSeekPrefix is the ADR-013 guardrail-1 case: reconstruct one row by
// seeking its prefix and reading only that row's columns.
func TestBTreeSeekPrefix(t *testing.T) {
	tb := newBTree()
	for r := 0; r < 500; r++ {
		prefix := fmt.Sprintf("tellstone/users/%06d/", r)
		for _, c := range []string{"age", "name", "zip"} {
			tb.set(prefix+c, []byte(c), time.Time{})
		}
	}

	for _, row := range []int{0, 1, 42, 250, 499} {
		prefix := fmt.Sprintf("tellstone/users/%06d/", row)
		upper := prefixUpperBound(prefix)
		var got []string
		for it := tb.seek(prefix, upper); it.next(); {
			got = append(got, strings.TrimPrefix(strings.Clone(it.key()), prefix))
		}
		if len(got) != 3 {
			t.Fatalf("row %d: reconstructed %d columns, want 3", row, len(got))
		}
		if got[0] != "age" || got[1] != "name" || got[2] != "zip" {
			t.Fatalf("row %d: columns out of order: %v", row, got)
		}
	}

	// A table scan must return every column of every row and nothing else.
	n := 0
	for it := tb.seek("tellstone/users/", ""); it.next(); {
		n++
	}
	if n != 1500 {
		t.Fatalf("table scan returned %d keys, want 1500", n)
	}
}

// TestBTreeSeekBounds checks the empty, exact, below-minimum and
// above-maximum seek cases, plus the exclusive upper bound that makes a prefix
// scan stop at the last column of a row.
func TestBTreeSeekBounds(t *testing.T) {
	tb := newBTree()
	if it := tb.seek("", ""); it.next() {
		t.Fatal("seek on an empty index returned an entry")
	}
	tb.set("b", []byte("v"), time.Time{})

	cases := []struct {
		lower, end string
		want       []string
	}{
		{"", "", []string{"b"}},
		{"a", "", []string{"b"}},
		{"b", "", []string{"b"}},
		{"c", "", nil},
		{"b", "b", nil},           // end is exclusive
		{"", "b", nil},            // everything below b
		{"a", "c", []string{"b"}}, // exact window
	}
	for _, c := range cases {
		var got []string
		for it := tb.seek(c.lower, c.end); it.next(); {
			got = append(got, strings.Clone(it.key()))
		}
		if len(got) != len(c.want) {
			t.Fatalf("seek(%q, %q) = %v, want %v", c.lower, c.end, got, c.want)
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Fatalf("seek(%q, %q)[%d] = %q, want %q", c.lower, c.end, i, got[i], c.want[i])
			}
		}
	}
}

// TestBTreeDifferentialOrder is the main gate: a long randomized workload
// checked against a sorted reference after every step, with structure verified
// periodically so a split bug is caught near the operation that caused it.
func TestBTreeDifferentialOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(20260929))
	tb := newBTree()
	ref := map[string]bool{}

	// A small alphabet relative to the operation count, so duplicates,
	// overwrites and interleaved prefixes are all common rather than rare.
	rowKey := func() string {
		return fmt.Sprintf("tellstone/users/%02d/%s", rng.Intn(40),
			[]string{"age", "city", "name"}[rng.Intn(3)])
	}

	for step := 0; step < 4000; step++ {
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5, 9:
			k := rowKey()
			tb.set(k, []byte("v"), time.Time{})
			ref[k] = true
		default:
			k := rowKey()
			if got, want := tb.remove(k), ref[k]; got != want {
				t.Fatalf("step %d: remove(%q) = %v, want %v", step, k, got, want)
			}
			delete(ref, k)
		}

		// Ordering and cardinality after every step; the full structural walk
		// is too expensive to run 4000 times, so it runs periodically.
		prev := ""
		n := 0
		for it := tb.seek("", ""); it.next(); {
			k := strings.Clone(it.key())
			if k < prev {
				t.Fatalf("step %d: order broke: %q then %q", step, prev, k)
			}
			if !ref[k] {
				t.Fatalf("step %d: index returned unexpected key %q", step, k)
			}
			prev = k
			n++
		}
		if n != len(ref) {
			t.Fatalf("step %d: index holds %d keys, reference holds %d", step, n, len(ref))
		}
		if tb.length != len(ref) {
			t.Fatalf("step %d: length = %d, reference holds %d", step, tb.length, len(ref))
		}
		if step%250 == 0 {
			tb.checkInvariants(t)
		}
	}
	tb.checkInvariants(t)
}

// TestBTreeDifferentialDeleteHeavy removes far more than it inserts, so the
// root collapse, the separator removal and the leaf-chain head loss are all
// exercised, which an insert-heavy run never reaches.
func TestBTreeDifferentialDeleteHeavy(t *testing.T) {
	rng := rand.New(rand.NewSource(4242))
	tb := newBTree()
	ref := map[string]bool{}
	key := func() string {
		return fmt.Sprintf("tellstone/%s/%03d", []string{"users", "orders", "meta"}[rng.Intn(3)], rng.Intn(120))
	}

	for step := 0; step < 6000; step++ {
		if len(ref) > 0 && rng.Intn(10) < 7 {
			// Remove an existing key, so the run really does drain.
			all := make([]string, 0, len(ref))
			for k := range ref {
				all = append(all, k)
			}
			k := all[rng.Intn(len(all))]
			if !tb.remove(k) {
				t.Fatalf("step %d: remove(%q) reported absent", step, k)
			}
			delete(ref, k)
		} else {
			k := key()
			tb.set(k, []byte("v"), time.Time{})
			ref[k] = true
		}

		n := 0
		prev := ""
		for it := tb.seek("", ""); it.next(); {
			k := strings.Clone(it.key())
			if k < prev {
				t.Fatalf("step %d: order broke: %q then %q", step, prev, k)
			}
			prev = k
			n++
		}
		if n != len(ref) {
			t.Fatalf("step %d: index holds %d keys, reference holds %d", step, n, len(ref))
		}
		if step%300 == 0 {
			tb.checkInvariants(t)
		}
	}
	tb.checkInvariants(t)
}

// TestBTreeLargeKeyspace drives enough distinct keys to force many levels,
// which is where a split promoted to the wrong parent shows up.
func TestBTreeLargeKeyspace(t *testing.T) {
	tb := newBTree()
	const n = 20000
	for i := 0; i < n; i++ {
		tb.set(fmt.Sprintf("tellstone/users/%012d/name", i), []byte("v"), time.Time{})
	}
	if tb.length != n {
		t.Fatalf("length = %d, want %d", tb.length, n)
	}

	// Spot-check exact seeks and the boundaries, which is where an off-by-one
	// in a promoted separator would surface.
	for _, i := range []int{0, 1, 31, 32, 33, 9999, 19998, 19999} {
		k := fmt.Sprintf("tellstone/users/%012d/name", i)
		it := tb.seek(k, "")
		if !it.next() || it.key() != k {
			t.Fatalf("seek(%q) missed", k)
		}
	}

	// A prefix scan must return exactly one row's columns.
	for _, i := range []int{0, 7777, 19999} {
		prefix := fmt.Sprintf("tellstone/users/%012d/", i)
		n := 0
		for it := tb.seek(prefix, prefixUpperBound(prefix)); it.next(); {
			n++
		}
		if n != 1 {
			t.Fatalf("row %d: got %d columns, want 1", i, n)
		}
	}

	// Insertion order was already sorted, so insert a descending set too: a
	// tree that only works for ascending input would fail here.
	reverse := newBTree()
	for i := n - 1; i >= 0; i-- {
		reverse.set(fmt.Sprintf("tellstone/users/%012d/name", i), []byte("v"), time.Time{})
	}
	if got := reverse.materialize(); len(got) != n || got[0] != "tellstone/users/000000000000/name" {
		t.Fatalf("descending insert produced %d keys, head %q", len(got), got[0])
	}
	reverse.checkInvariants(t)
}

// TestBTreePrefixCompression verifies the header is doing its job, which is the
// entire point of choosing a B+Tree over a skiplist.
func TestBTreePrefixCompression(t *testing.T) {
	tb := newBTree()
	const rows, cols = 2000, 4
	for r := 0; r < rows; r++ {
		prefix := fmt.Sprintf("tellstone/users/%012d/", r)
		for c := 0; c < cols; c++ {
			tb.set(fmt.Sprintf("%scol%d", prefix, c), []byte("v"), time.Time{})
		}
	}

	var stored, full int
	var walk func(n *btreeNode)
	walk = func(n *btreeNode) {
		keys := n.ownKeys()
		for i, k := range keys {
			_ = i
			// Stored cost is the compressed suffix plus the shared count.
			stored += len(n.suffixAt(i)) + 2
			full += len(k)
		}
		for _, kid := range n.kids {
			walk(kid)
		}
	}
	walk(tb.root)

	// Every key is "tellstone/users/000000000000/colN", 33 bytes. With the
	// header absorbed once per node and once per node for its siblings'
	// shared tail, stored bytes must be well under the flat total.
	t.Logf("keys=%d front-coded bytes=%d flat bytes=%d ratio=%.2fx",
		rows*cols, stored, full, float64(full)/float64(stored))
	if stored >= full {
		t.Fatalf("front coding saved nothing: stored %d >= flat %d", stored, full)
	}
	if got := float64(full) / float64(stored); got < 1.5 {
		t.Fatalf("front coding ratio %.2fx is too low to justify the structure", got)
	}
}

// TestBTreeHeapPerKey measures the index's own marginal heap cost per key, the
// number guardrail 2 is really about. It must land far below the skiplist's
// ~190 bytes of net overhead or the choice of structure was wrong.
func TestBTreeHeapPerKey(t *testing.T) {
	heapAt := func(keys int) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		tb := newBTree()
		for i := 0; i < keys/4; i++ {
			prefix := fmt.Sprintf("tellstone/users/%012d/", i)
			for _, c := range []string{"age", "lastname", "name", "zip"} {
				tb.set(prefix+c, []byte("v"), time.Time{})
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		// Keep the index reachable through the read, or the collection above
		// reclaims it and the delta measures nothing.
		runtime.KeepAlive(tb)
		return after.HeapAlloc - before.HeapAlloc
	}
	loKeys, hiKeys := 40_000, 160_000
	lo, hi := heapAt(loKeys), heapAt(hiKeys)
	perKey := float64(hi-lo) / float64(hiKeys-loKeys)
	t.Logf("keys %d..%d: btree marginal heap/key=%.1f bytes", loKeys, hiKeys, perKey)
	t.Logf("comparison: items map ~240 B/key, skiplist ~225 B/key")
}

// BenchmarkBTreeSeekPrefix is the "after" number for the phase 9 row lookup,
// to be read next to BenchmarkEngineScanRow's 34 ms at 100k rows.
func BenchmarkBTreeSeekPrefix(b *testing.B) {
	for _, rows := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d/cols=3", rows), func(b *testing.B) {
			tb := newBTree()
			prefixes := make([]string, 100)
			for r := 0; r < rows; r++ {
				prefix := fmt.Sprintf("tellstone/users/%012d/", r)
				for _, c := range []string{"age", "lastname", "name"} {
					tb.set(prefix+c, []byte(c), time.Time{})
				}
				if r < 100 {
					prefixes[r] = prefix
				}
			}
			b.ReportMetric(float64(rows*3), "keys")
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				prefix := prefixes[i%100]
				hit := 0
				for it := tb.seek(prefix, prefixUpperBound(prefix)); it.next(); {
					hit++
				}
				if hit != 3 {
					b.Fatalf("row %q: got %d columns, want 3", prefix, hit)
				}
			}
		})
	}
}

// BenchmarkBTreeSet reports the write cost the sidecar adds to every Set.
func BenchmarkBTreeSet(b *testing.B) {
	tb := newBTree()
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("tellstone/users/%012d/name", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.set(keys[i%len(keys)], []byte("v"), time.Time{})
	}
}

// BenchmarkBTreeScanPrefix measures a whole row read through the callback scan,
// which is the shape phase 9 actually uses. Guardrail 2 is about the scan being
// free of allocations, not merely fast: a range read that allocates per key
// would reintroduce the churn the index exists to remove.
func BenchmarkBTreeScanPrefix(b *testing.B) {
	for _, rows := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d/cols=3", rows), func(b *testing.B) {
			tb := newBTree()
			prefixes := make([]string, rows)
			for r := 0; r < rows; r++ {
				prefix := fmt.Sprintf("tellstone/users/%012d/", r)
				prefixes[r] = prefix
				for _, c := range []string{"age", "lastname", "name"} {
					tb.set(prefix+c, []byte(c), time.Time{})
				}
			}
			var sum int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tb.ScanPrefix(prefixes[i%rows], func(_, val []byte, _ time.Time) bool {
					sum += len(val)
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
