/*
Package server
Tellstone Cloud-Native In-Memory Database
File: cluster_prefix_exists_test.go
Description: Tests for the cluster-wide emptiness check that guards DROP TABLE.

The guard is the one place where answering "is this table empty?" incorrectly
corrupts data rather than merely returning a wrong row: a false "empty" deletes a
table's definition while its rows remain, and nothing describes them afterwards.
These tests therefore concentrate on the ways the check can be wrong in the
permissive direction — silently skipping a region, or scanning outside it — and
on the guarantee that an unanswerable question refuses the drop instead of
guessing.

The tests drive the check through a real storage engine and a real routing table
rather than mocks, because the bugs worth catching here live in how a prefix
scan and a key range interact, which a mock cannot reproduce.

Authors:

	Maximilian Hagen
*/
package server

import (
	"testing"

	"github.com/Saxy/Tellstone/internal/cluster"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
)

// stubLocal is a localReader over a real engine, with a hook to observe which
// prefixes were scanned so a test can assert the scan stayed in range.
type stubLocal struct {
	e *storage.Engine
	// scanned records every prefix passed to ScanPrefix.
	scanned []string
	// err, when set, is returned instead of scanning.
	err error
}

func newStubLocal(t *testing.T, keys ...string) *stubLocal {
	t.Helper()
	e := storage.NewEngine(0, 0, 0, log.NewNoOpLogger(), nil)
	for _, k := range keys {
		if err := e.Set(k, []byte("v"), 0); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
	}
	return &stubLocal{e: e}
}

func (s *stubLocal) Get(key string) ([]byte, bool) {
	v, ok := s.e.Get(key)
	return v, ok
}

func (s *stubLocal) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	s.scanned = append(s.scanned, prefix)
	if s.err != nil {
		return 0, s.err
	}
	return s.e.ScanPrefix(prefix, fn), nil
}

// newStore builds a clusterStore over one routing table and no hosted regions,
// which is the state a node is in before it takes hosting responsibility. The
// strict guard refuses from here, so tests that need a successful answer drive
// scanRegionPrefix directly — that is where the prefix-versus-bounds logic lives.
func newStore(local localReader, regions ...cluster.Region) *clusterStore {
	rt := cluster.NewRoutingTable()
	for _, r := range regions {
		rt.Update(r)
	}
	return &clusterStore{local: local, rt: rt, logger: log.NewNoOpLogger()}
}

// The scan must be rooted at the table prefix and the region bounds applied
// inside the callback. A region boundary that falls between two columns of the
// same row (start key ".../2/id") is not a prefix of that row's other columns,
// so scanning with the clamped start key would silently skip ".../2/name" and the
// row would disappear from the result.
func TestScanRegionPrefixFindsSiblingsOfAMidRowBoundary(t *testing.T) {
	local := newStubLocal(t,
		"tellstone/users/1/id", "tellstone/users/1/name",
		"tellstone/users/2/id", "tellstone/users/2/name", "tellstone/users/2/email",
		"tellstone/users/3/id",
	)
	cs := newStore(local)

	var got []string
	err := cs.scanRegionPrefix("tellstone/users/",
		[]byte("tellstone/users/2/"), // a boundary landing inside row 2
		[]byte("tellstone/users/3/"),
		func(k, _ []byte) {
			got = append(got, string(k))
		})
	if err != nil {
		t.Fatalf("scanRegionPrefix: %v", err)
	}

	// Key order is the engine's, so compare as a set: the point is that no
	// column of the straddling row is dropped, not where it lands in the walk.
	want := map[string]bool{
		"tellstone/users/2/id":    true,
		"tellstone/users/2/name":  true,
		"tellstone/users/2/email": true,
	}
	if len(got) != len(want) {
		t.Fatalf("scanned %v, want all of %v (a skipped column is a lost row)", got, want)
	}
	for _, k := range got {
		if !want[k] {
			t.Fatalf("scanned %q, which is outside the region bounds", k)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Fatalf("missed %v (a skipped column is a lost row)", want)
	}
	// The scan must be rooted at the table prefix, never at a region boundary.
	for _, p := range local.scanned {
		if p != "tellstone/users/" {
			t.Fatalf("scanned with %q, want the table prefix", p)
		}
	}
}

// Keys before the region belong to an earlier region and must be filtered out
// rather than delivered, or the merge would see overlapping copies.
func TestScanRegionPrefixSkipsKeysBeforeTheRegion(t *testing.T) {
	local := newStubLocal(t, "tellstone/users/1/id", "tellstone/users/2/id", "tellstone/users/3/id")
	cs := newStore(local)

	var got []string
	err := cs.scanRegionPrefix("tellstone/users/",
		[]byte("tellstone/users/2/"),
		[]byte("tellstone/users/3/"),
		func(k, _ []byte) {
			got = append(got, string(k))
		})
	if err != nil {
		t.Fatalf("scanRegionPrefix: %v", err)
	}
	if len(got) != 1 || got[0] != "tellstone/users/2/id" {
		t.Fatalf("delivered %v, want only the in-region key", got)
	}
}

// The upper bound is exclusive and the walk stops there, so the next region's
// keys are never scanned.
func TestScanRegionPrefixStopsAtUpperBound(t *testing.T) {
	local := newStubLocal(t, "aaa/1/id", "mmm/2/id", "zzz/3/id")
	cs := newStore(local)

	var got []string
	err := cs.scanRegionPrefix("",
		[]byte("mmm"),
		[]byte("zzz"),
		func(k, _ []byte) {
			got = append(got, string(k))
		})
	if err != nil {
		t.Fatalf("scanRegionPrefix: %v", err)
	}
	if len(got) != 1 || got[0] != "mmm/2/id" {
		t.Fatalf("delivered %v, want only [mmm, zzz)", got)
	}
}

// A guard must refuse a region it does not host. Falling back to the bootstrap
// node would linearize a different Raft group and read an engine holding
// unrelated regions' data — the exact false "empty" this guard exists to prevent.
func TestPrefixExistsRefusesWhenRegionHasNoLocalNode(t *testing.T) {
	local := newStubLocal(t, "tellstone/users/1/id")
	cs := newStore(local, cluster.Region{ID: 7, StartKey: []byte(""), EndKey: []byte("\xff")})

	if _, err := cs.PrefixExists("tellstone/users/"); err == nil {
		t.Fatal("PrefixExists answered without hosting the region; the guard must fail closed")
	}
}

// A prefix that no region covers cannot be verified. Reporting "empty" here is
// the failure mode the whole guard exists to prevent.
func TestPrefixExistsFailsWhenNoRegionCoversPrefix(t *testing.T) {
	local := newStubLocal(t)
	cs := newStore(local, cluster.Region{ID: 1, StartKey: []byte("zzz/"), EndKey: []byte("zzzz")})

	if _, err := cs.PrefixExists("tellstone/users/"); err == nil {
		t.Fatal("PrefixExists returned no error for an uncovered prefix; an unverified table must not look empty")
	}
}

// The refusal must be an error rather than a silent false, and it must not be
// reported as "the table has rows" either — those are different answers.
func TestPrefixExistsRefusalIsNotAnEmptyAnswer(t *testing.T) {
	local := newStubLocal(t)
	cs := newStore(local, cluster.Region{ID: 1, StartKey: []byte(""), EndKey: []byte("\xff")})

	found, err := cs.PrefixExists("tellstone/users/")
	if err == nil {
		t.Fatal("expected an error")
	}
	if found {
		t.Fatal("reported rows alongside an error; the two must not be conflated")
	}
	if err := cs.scanRegionStrict(&cluster.RegionRoute{ID: 1}, "tellstone/users/", []byte("tellstone/users/"), []byte("\xff"), func(_, _ []byte) {}); err == nil {
		t.Fatal("scanRegionStrict accepted an unhosted region")
	}
}
