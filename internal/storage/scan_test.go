/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: scan_test.go
Description: Tests for the ordered index behind prefix range reads. The index
duplicates the items map, so the load-bearing property is that the two never
disagree: every mutation site has to update both, and a missed site shows up as
a row that reads back with a stale column, a deleted key that reappears, or a
scan that skips live data. The differential test asserts the two agree after
every operation rather than only at the end, so a drift is attributed to the
operation that caused it.
*/
package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// newScanEngine returns an engine suitable for index tests: no chronometer, so
// expiry is driven explicitly by the test rather than by a background loop.
func newScanEngine() *Engine {
	return NewEngine(0, 0, 0, nil, nil)
}

// indexAgrees reports whether the ordered index holds exactly the live entries
// of the items map, with the same values and expirations. It is the invariant
// every mutation site has to preserve.
func indexAgrees(t *testing.T, e *Engine, stage string) {
	t.Helper()
	e.mu.RLock()
	want := make(map[string]Item, len(e.items))
	for k, v := range e.items {
		want[k] = v
	}
	e.mu.RUnlock()

	got := make(map[string]Item)
	var gotKeys []string
	e.index.ScanPrefix("", func(k, v []byte, exp time.Time) bool {
		got[string(k)] = Item{Value: bytes.Clone(v), Expiration: exp}
		gotKeys = append(gotKeys, string(k))
		return true
	})
	if len(gotKeys) != len(got) {
		t.Fatalf("%s: index produced duplicate keys", stage)
	}
	if !sort.StringsAreSorted(gotKeys) {
		t.Fatalf("%s: index walk is not in key order: %v", stage, gotKeys)
	}
	if len(got) != len(want) {
		t.Fatalf("%s: index has %d keys, map has %d", stage, len(got), len(want))
	}
	for k, wantItem := range want {
		gotItem, ok := got[k]
		if !ok {
			t.Fatalf("%s: key %q is in the map but not the index", stage, k)
		}
		if !bytes.Equal(gotItem.Value, wantItem.Value) {
			t.Fatalf("%s: key %q value diverged: index %q, map %q",
				stage, k, gotItem.Value, wantItem.Value)
		}
		if !gotItem.Expiration.Equal(wantItem.Expiration) {
			t.Fatalf("%s: key %q expiration diverged: index %v, map %v",
				stage, k, gotItem.Expiration, wantItem.Expiration)
		}
	}
}

// TestIndexTracksEveryMutationSite drives every path that writes or removes an
// item and checks the index agrees with the map after each one. A mutation site
// added later without index maintenance shows up here as a failure.
func TestIndexTracksEveryMutationSite(t *testing.T) {
	e := newScanEngine()
	k := func(name string) string { return "tellstone/users/" + name }

	// Plain write.
	if err := e.Set(k("1/age"), []byte("18"), 0); err != nil {
		t.Fatal(err)
	}
	indexAgrees(t, e, "Set")

	// Overwrite: the index must not keep the old value.
	if err := e.Set(k("1/age"), []byte("19"), 0); err != nil {
		t.Fatal(err)
	}
	indexAgrees(t, e, "Set overwrite")

	// Conditional create.
	out, err := e.SetIfAbsent(k("2/name"), []byte("max"), 0)
	if err != nil || !out.Applied {
		t.Fatalf("SetIfAbsent: %+v err=%v", out, err)
	}
	indexAgrees(t, e, "SetIfAbsent")

	// Conditional create that must not apply, and so must not touch the index.
	out, err = e.SetIfAbsent(k("2/name"), []byte("nope"), 0)
	if err != nil || out.Applied {
		t.Fatalf("SetIfAbsent on an existing key: %+v err=%v", out, err)
	}
	indexAgrees(t, e, "SetIfAbsent rejected")

	// Conditional update.
	out, err = e.SetIfPresent(k("2/name"), []byte("updated"), 0)
	if err != nil || !out.Applied {
		t.Fatalf("SetIfPresent: %+v err=%v", out, err)
	}
	indexAgrees(t, e, "SetIfPresent")

	// Conditional update that must not apply.
	out, err = e.SetIfPresent(k("absent/name"), []byte("nope"), 0)
	if err != nil || out.Applied {
		t.Fatalf("SetIfPresent on a missing key: %+v err=%v", out, err)
	}
	indexAgrees(t, e, "SetIfPresent rejected")

	// SetFromBuffer, the zero-alloc write path.
	buf := append([]byte(k("3/name")), []byte("buffered")...)
	if err := e.SetFromBuffer(buf, len(k("3/name")), 0); err != nil {
		t.Fatal(err)
	}
	indexAgrees(t, e, "SetFromBuffer")

	// SetRaw, the snapshot restore path.
	if err := e.SetRaw(k("4/name"), []byte("raw"), 0); err != nil {
		t.Fatal(err)
	}
	indexAgrees(t, e, "SetRaw")

	// Delete.
	if present := e.Delete(k("1/age")); !present {
		t.Fatal("Delete reported an existing key as absent")
	}
	indexAgrees(t, e, "Delete")

	// Delete of a missing key must leave the index alone.
	if present := e.Delete(k("1/age")); present {
		t.Fatal("Delete reported a missing key as present")
	}
	indexAgrees(t, e, "Delete missing")

	// Rollback of a create.
	out, err2 := e.SetIfAbsent(k("5/name"), []byte("rolled-back"), 0)
	err = err2
	if err != nil || !out.Applied {
		t.Fatalf("create for rollback: %+v err=%v", out, err)
	}
	indexAgrees(t, e, "create before rollback")
	if !e.RestoreIf(k("5/name"), out) {
		t.Fatal("rollback of a create did not happen")
	}
	indexAgrees(t, e, "rollback create")

	// Rollback of an update, which must put the previous value back.
	before, _ := e.Get(k("2/name"))
	out, err = e.SetIfPresent(k("2/name"), []byte("replaced"), 0)
	if err != nil || !out.Applied {
		t.Fatalf("update for rollback: %+v err=%v", out, err)
	}
	indexAgrees(t, e, "update before rollback")
	if !e.RestoreIf(k("2/name"), out) {
		t.Fatal("rollback of an update did not happen")
	}
	after, _ := e.Get(k("2/name"))
	if !bytes.Equal(before, after) {
		t.Fatalf("rollback did not restore the value: %q -> %q", before, after)
	}
	indexAgrees(t, e, "rollback update")
}

func TestIndexTracksExpiry(t *testing.T) {
	e := newScanEngine()
	if err := e.Set("k/live", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	// The engine only sets an expiration for a positive TTL, so an entry is
	// made expired by giving it a short one and letting it lapse.
	if err := e.Set("k/dying", []byte("v"), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	indexAgrees(t, e, "expired entry stored")

	// A scan must skip an expired key without reporting it as data.
	var seen []string
	e.ScanPrefix("k/", func(k, _ []byte) bool {
		seen = append(seen, string(k))
		return true
	})
	if len(seen) != 1 || seen[0] != "k/live" {
		t.Fatalf("scan returned %v, want only the live key", seen)
	}
	// The expired key is skipped, not evicted, so the index still agrees.
	indexAgrees(t, e, "scan over an expired key")

	// Evicting it must remove it from the index too.
	e.deleteIfExpired("k/dying")
	indexAgrees(t, e, "eviction")
}

func TestScanPrefixFiltersAndOrders(t *testing.T) {
	e := newScanEngine()
	// Deliberately inserted out of order, and interleaved with other prefixes.
	keys := []string{
		"tellstone/users/2/name", "tellstone/users/1/age", "tellstone/orders/1/id",
		"tellstone/users/2/age", "tellstone/users/1/name", "tellstone/usersX/1/name",
	}
	for i, k := range keys {
		if err := e.Set(k, []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	n := e.ScanPrefix("tellstone/users/", func(k, _ []byte) bool {
		got = append(got, string(k))
		return true
	})
	want := []string{
		"tellstone/users/1/age", "tellstone/users/1/name",
		"tellstone/users/2/age", "tellstone/users/2/name",
	}
	if n != len(want) {
		t.Fatalf("delivered %d keys, want %d: %v", n, len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan order %v, want %v", got, want)
		}
	}
}

func TestScanPrefixStopsEarly(t *testing.T) {
	e := newScanEngine()
	for i := 0; i < 10; i++ {
		if err := e.Set(fmt.Sprintf("p/%02d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	n := e.ScanPrefix("p/", func(k, _ []byte) bool {
		got = append(got, string(k))
		return len(got) < 3
	})
	if n != 3 || len(got) != 3 {
		t.Fatalf("early stop delivered %d keys, want 3", n)
	}
	if got[2] != "p/02" {
		t.Fatalf("early stop returned %v, want to stop after p/02", got)
	}
}

func TestScanPrefixEmptyMatch(t *testing.T) {
	e := newScanEngine()
	if err := e.Set("a/1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if n := e.ScanPrefix("nonexistent/", func(_, _ []byte) bool { return true }); n != 0 {
		t.Fatalf("unmatched prefix delivered %d keys", n)
	}
	if n := e.ScanPrefix("", func(_, _ []byte) bool { return true }); n != 1 {
		t.Fatalf("empty prefix delivered %d keys, want 1", n)
	}
}

// TestScanPrefixReconstructsARow is the phase 9 shape: one range read yields a
// whole row rather than one lookup per column (ADR-013 guardrail 1).
func TestScanPrefixReconstructsARow(t *testing.T) {
	e := newScanEngine()
	row := map[string][]byte{
		"tellstone/users/42/age":      []byte("18"),
		"tellstone/users/42/lastname": []byte("mustermann"),
		"tellstone/users/42/name":     []byte("max"),
		"tellstone/users/43/name":     []byte("other"),
	}
	for k, v := range row {
		if err := e.Set(k, v, 0); err != nil {
			t.Fatal(err)
		}
	}
	got := make(map[string][]byte)
	n := e.ScanPrefix("tellstone/users/42/", func(k, v []byte) bool {
		got[string(k)] = bytes.Clone(v)
		return true
	})
	if n != 3 {
		t.Fatalf("row read delivered %d columns, want 3: %v", n, got)
	}
	if len(got) != len(row)/1-1 {
		t.Fatalf("row read returned %v", got)
	}
	for k, want := range row {
		if k == "tellstone/users/43/name" {
			continue
		}
		if !bytes.Equal(got[k], want) {
			t.Fatalf("column %q: got %q, want %q", k, got[k], want)
		}
	}
}

// TestIndexStaysConsistentUnderRandomOperations is the differential test: a
// mixed workload with a reference map, checking agreement after every step. It
// is the test that would catch a mutation site added without index maintenance.
func TestIndexStaysConsistentUnderRandomOperations(t *testing.T) {
	e := newScanEngine()
	ref := make(map[string][]byte)
	refExp := make(map[string]time.Time)
	rng := rand.New(rand.NewSource(20240317))
	names := make([]string, 0, 400)
	for i := 0; i < 400; i++ {
		names = append(names, fmt.Sprintf("tellstone/users/%04d/col%d", rng.Intn(200), rng.Intn(3)))
	}

	for step := 0; step < 6000; step++ {
		key := names[rng.Intn(len(names))]
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5: // write
			val := []byte(fmt.Sprintf("value-%d-%d", step, rng.Intn(1000)))
			var exp time.Time
			if rng.Intn(8) == 0 {
				exp = time.Now().Add(time.Duration(rng.Intn(2)) * time.Hour)
			}
			var ttl time.Duration
			if !exp.IsZero() {
				ttl = time.Until(exp)
			}
			if err := e.Set(key, val, ttl); err != nil {
				t.Fatalf("step %d: Set: %v", step, err)
			}
			ref[key] = val
			refExp[key] = exp
		case 6, 7: // conditional create
			val := []byte(fmt.Sprintf("nx-%d", step))
			out, err := e.SetIfAbsent(key, val, 0)
			if err != nil {
				t.Fatalf("step %d: SetIfAbsent: %v", step, err)
			}
			_, existed := ref[key]
			if out.Applied == existed {
				t.Fatalf("step %d: SetIfAbsent applied=%v but key existed=%v", step, out.Applied, existed)
			}
			if out.Applied {
				ref[key] = val
				refExp[key] = time.Time{}
			}
		case 8: // conditional update
			val := []byte(fmt.Sprintf("xx-%d", step))
			out, err := e.SetIfPresent(key, val, 0)
			if err != nil {
				t.Fatalf("step %d: SetIfPresent: %v", step, err)
			}
			_, existed := ref[key]
			if out.Applied != existed {
				t.Fatalf("step %d: SetIfPresent applied=%v but key existed=%v", step, out.Applied, existed)
			}
			if out.Applied {
				ref[key] = val
				refExp[key] = time.Time{}
			}
		case 9: // delete
			present := e.Delete(key)
			_, existed := ref[key]
			if present != existed {
				t.Fatalf("step %d: Delete returned %v but key existed=%v", step, present, existed)
			}
			delete(ref, key)
			delete(refExp, key)
		}
		if step%250 == 0 {
			indexAgrees(t, e, fmt.Sprintf("step %d", step))
		}
	}
	indexAgrees(t, e, "final")

	// A prefix scan has to agree with the reference map restricted to the same
	// prefix, in the same order.
	for _, prefix := range []string{"tellstone/users/0007/", "tellstone/users/01", "tellstone/"} {
		var gotKeys []string
		e.ScanPrefix(prefix, func(k, _ []byte) bool {
			gotKeys = append(gotKeys, string(k))
			return true
		})
		var wantKeys []string
		for k := range ref {
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				wantKeys = append(wantKeys, k)
			}
		}
		sort.Strings(wantKeys)
		if len(gotKeys) != len(wantKeys) {
			t.Fatalf("prefix %q: scan returned %d keys, want %d", prefix, len(gotKeys), len(wantKeys))
		}
		for i := range wantKeys {
			if gotKeys[i] != wantKeys[i] {
				t.Fatalf("prefix %q: scan order %v, want %v", prefix, gotKeys, wantKeys)
			}
		}
	}
}
