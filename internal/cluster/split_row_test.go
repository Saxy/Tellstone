package cluster

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

/*
Tests for row-boundary split snapping.

The hazard these exist for: a byte-wise midpoint of a key range is blind to row
structure, so it lands inside a row with probability approaching 1. A row is one
contiguous range of column keys; a boundary inside it means the columns of a
single logical row are owned by two different Raft groups, and every later read
or write of that row becomes a cross-region operation. The test that matters is
TestSnapNeverCutsARow, which asserts the invariant over many layouts rather
than a handful of hand-picked keys.
*/

// rowsOfTable returns every column key of every row, i.e. the real key contents
// a region would hold for that table.
func rowsOfTable(db, table string, rowIDs, columns []string) [][]byte {
	var keys [][]byte
	for _, id := range rowIDs {
		for _, c := range columns {
			keys = append(keys, []byte(keyspace.ColumnKey(db, table, id, c)))
		}
	}
	return keys
}

// splitsRow reports whether a boundary separates the columns of one row, which
// is the failure this whole change exists to prevent.
//
// A row is cut when its lowest-sorting column key is below the boundary and its
// highest is not, i.e. the row straddles the boundary. The caller passes columns
// in sorted order, so the first and last entries bound the row.
func splitsRow(boundary []byte, db, table, rowID string, columns []string) bool {
	sorted := append([]string(nil), columns...)
	sort.Strings(sorted)
	lowest := []byte(keyspace.ColumnKey(db, table, rowID, sorted[0]))
	highest := []byte(keyspace.ColumnKey(db, table, rowID, sorted[len(sorted)-1]))
	return bytes.Compare(lowest, boundary) < 0 && bytes.Compare(highest, boundary) >= 0
}

// TestSnapNeverCutsARow is the invariant test. For a range covering many rows,
// the chosen split point must never fall strictly inside any row's column span.
func TestSnapNeverCutsARow(t *testing.T) {
	columns := []string{"a", "b", "id", "name", "zz"}
	rowIDSets := [][]string{
		{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		{"1", "2", "3", "4", "5"},
		{"user:1", "user:2", "user:3", "user:4", "user:5", "user:6", "user:7", "user:8"},
		{"a", "b", "c", "d", "e", "f", "g", "h"},
		{"0000000001", "0000000002", "0000000003", "0000000004", "0000000005"},
	}
	for _, rowIDs := range rowIDSets {
		keys := rowsOfTable("tellstone", "t", rowIDs, columns)
		lo, hi := keys[0], keys[len(keys)-1]
		cur := &Region{ID: 1, StartKey: lo, EndKey: hi}

		sc := &SplitCoordinator{}
		got, err := sc.chooseSplitKey(cur, nil)
		if err != nil {
			t.Fatalf("chooseSplitKey: %v", err)
		}
		// The boundary must stay strictly inside the region.
		if bytes.Compare(got, cur.StartKey) <= 0 || bytes.Compare(got, cur.EndKey) >= 0 {
			t.Fatalf("split key %q not strictly inside [%q, %q)", got, cur.StartKey, cur.EndKey)
		}
		// And it must not cut any row.
		for _, id := range rowIDs {
			if splitsRow(got, "tellstone", "t", id, columns) {
				t.Fatalf("split key %q cuts row %q (rows %v)", got, id, rowIDs)
			}
		}
	}
}

// A region holding a single row has no boundary inside it, so the split must be
// refused rather than performed at some byte inside that row.
func TestSnapRefusesSingleRowRegion(t *testing.T) {
	columns := []string{"a", "b", "id"}
	rowIDs := []string{"7"}
	keys := rowsOfTable("tellstone", "t", rowIDs, columns)
	cur := &Region{ID: 1, StartKey: keys[0], EndKey: keys[len(keys)-1]}

	sc := &SplitCoordinator{}
	_, err := sc.chooseSplitKey(cur, nil)
	if !errors.Is(err, ErrSplitWouldCutRow) {
		t.Fatalf("chooseSplitKey error = %v, want ErrSplitWouldCutRow", err)
	}
}

// A region whose start is already mid-row is a boundary this code did not
// create. When the midpoint lands in a *different* row, snapping still yields a
// legal boundary, so this case does not refuse -- the previous test wrongly
// assumed it did. What must never happen is the split being accepted at a point
// that cuts either row.
func TestSnapHandlesMidRowRegionStart(t *testing.T) {
	cur := &Region{
		ID:       1,
		StartKey: []byte(keyspace.ColumnKey("tellstone", "t", "7", "id")),
		EndKey:   []byte(keyspace.ColumnKey("tellstone", "t", "9", "zzz")),
	}
	sc := &SplitCoordinator{}
	got, err := sc.chooseSplitKey(cur, nil)
	if err != nil {
		t.Fatalf("chooseSplitKey: %v", err)
	}
	if bytes.Compare(got, cur.StartKey) <= 0 || bytes.Compare(got, cur.EndKey) >= 0 {
		t.Fatalf("split key %q not strictly inside [%q, %q)", got, cur.StartKey, cur.EndKey)
	}
	// The region starts inside row 7, so row 7 is already being cut by a
	// pre-existing boundary. The new boundary must not make that worse by also
	// landing inside another row.
	for _, id := range []string{"8", "9"} {
		if splitsRow(got, "tellstone", "t", id, []string{"a", "id", "name", "zz"}) {
			t.Fatalf("split key %q cuts row %q", got, id)
		}
	}
}

// A region whose start is mid-row and which contains exactly one row has no
// legal boundary at all, so it must refuse.
func TestSnapRefusesMidRowStartSingleRow(t *testing.T) {
	cur := &Region{
		ID:       1,
		StartKey: []byte(keyspace.ColumnKey("tellstone", "t", "7", "id")),
		EndKey:   []byte(keyspace.ColumnKey("tellstone", "t", "7", "zzz")),
	}
	sc := &SplitCoordinator{}
	_, err := sc.chooseSplitKey(cur, nil)
	if !errors.Is(err, ErrSplitWouldCutRow) {
		t.Fatalf("chooseSplitKey error = %v, want ErrSplitWouldCutRow", err)
	}
}

// Keys outside the row grammar -- catalog keys, the legacy flat table, short
// keys -- have no row to cut, so the midpoint is left where it landed.
func TestSnapLeavesNonRowKeysAlone(t *testing.T) {
	cases := []struct {
		name       string
		start, end string
	}{
		{"catalog subtree", "db/~meta/ta", "db/~mezz"},
		{"legacy flat keys", "aaaa", "zzzz"},
		{"two segment keys", "db/aa", "db/zz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := &Region{ID: 1, StartKey: []byte(tc.start), EndKey: []byte(tc.end)}
			sc := &SplitCoordinator{}
			got, err := sc.chooseSplitKey(cur, nil)
			if err != nil {
				t.Fatalf("chooseSplitKey: %v", err)
			}
			if want := midpointKey(cur.StartKey, cur.EndKey); !bytes.Equal(got, want) {
				t.Fatalf("split key = %q, want the unsnapped midpoint %q", got, want)
			}
		})
	}
}

// An explicit split key is the operator's decision and is honoured verbatim,
// including mid-row, since that is the only way to fix a region whose
// boundaries are already wrong. It has to stay inside the region.
func TestExplicitSplitKeyIsHonouredVerbatim(t *testing.T) {
	cur := &Region{
		ID:       1,
		StartKey: []byte("db/tbl/1/a"),
		EndKey:   []byte("db/tbl/9/z"),
	}
	sc := &SplitCoordinator{logger: &testLogger{t}}

	// Mid-row, but explicitly requested.
	req := []byte("db/tbl/5/name")
	got, err := sc.chooseSplitKey(cur, req)
	if err != nil {
		t.Fatalf("chooseSplitKey: %v", err)
	}
	if !bytes.Equal(got, req) {
		t.Fatalf("split key = %q, want the requested %q verbatim", got, req)
	}

	// The result must be an independent copy: a caller mutating req afterwards
	// must not be able to reach into the stored region boundary.
	req[0] = 'X'
	if bytes.Equal(got, req) {
		t.Fatal("chooseSplitKey aliased the caller's buffer")
	}
}

// When the midpoint lands strictly inside a row, the chosen key must be that
// row's prefix, so every column key of the row lands on the same side.
//
// The region bounds here are chosen so the byte midpoint provably falls between
// two column keys of one row: they share a long common prefix, so the midpoint
// is an average of two bytes in the column position.
func TestSnappedKeyIsTheContainingRowPrefix(t *testing.T) {
	// start = "db/t/0001/aaaa", end = "db/t/0001/zzzz": the common prefix is
	// "db/t/0001/" and the differing bytes are 'a' and 'z', so the midpoint
	// falls strictly between them, inside row 0001.
	start := "db/t/0001/aaaa"
	end := "db/t/0001/zzzz"
	mid := midpointKey([]byte(start), []byte(end))
	if _, inRow := keyspace.RowPrefixOf(mid); !inRow {
		t.Skipf("midpoint %q is not inside a row; nothing to snap", mid)
	}
	cur := &Region{ID: 1, StartKey: []byte(start), EndKey: []byte(end)}
	sc := &SplitCoordinator{}
	got, err := sc.chooseSplitKey(cur, nil)
	// This region holds a single row, so refusing is the expected outcome.
	if err != nil {
		if !errors.Is(err, ErrSplitWouldCutRow) {
			t.Fatalf("chooseSplitKey error = %v, want ErrSplitWouldCutRow", err)
		}
		return
	}
	if want := keyspace.RowPrefix("db", "t", "0001"); string(got) != want {
		t.Fatalf("snapped key = %q, want the containing row prefix %q", got, want)
	}
}

// A midpoint that lands between two rows is already a legal boundary, so it must
// be left exactly where midpointKey put it rather than moved.
func TestBetweenRowMidpointIsNotMoved(t *testing.T) {
	cur := &Region{
		ID:       1,
		StartKey: []byte(keyspace.ColumnKey("db", "t", "0000", "a")),
		EndKey:   []byte(keyspace.ColumnKey("db", "t", "9999", "z")),
	}
	mid := midpointKey(cur.StartKey, cur.EndKey)
	if _, inRow := keyspace.RowPrefixOf(mid); inRow {
		t.Skipf("midpoint %q unexpectedly falls inside a row", mid)
	}
	sc := &SplitCoordinator{}
	got, err := sc.chooseSplitKey(cur, nil)
	if err != nil {
		t.Fatalf("chooseSplitKey: %v", err)
	}
	if !bytes.Equal(got, mid) {
		t.Fatalf("split key = %q, want the unsnapped midpoint %q", got, mid)
	}
}

// A row id containing an escaped separator must be treated as one segment, so a
// split never lands between the two halves of it.
func TestSnapTreatsEscapedRowIDAsOneSegment(t *testing.T) {
	// Row ids "a/b" and "a-c": the first has its separator escaped, so a naive
	// segment count would think it is two rows.
	rowIDs := []string{"a/b", "a/b/c", "a-c", "a-d", "a-e", "a-f"}
	columns := []string{"x", "y"}
	keys := rowsOfTable("db", "t", rowIDs, columns)
	cur := &Region{ID: 1, StartKey: keys[0], EndKey: keys[len(keys)-1]}

	sc := &SplitCoordinator{}
	got, err := sc.chooseSplitKey(cur, nil)
	if err != nil {
		t.Fatalf("chooseSplitKey: %v", err)
	}
	for _, id := range rowIDs {
		if splitsRow(got, "db", "t", id, columns) {
			t.Fatalf("split key %q cuts row %q (row ids %v)", got, id, rowIDs)
		}
	}
}

// The snapped boundary must be usable as a region boundary: both halves have to
// be well-formed, and the halves must be non-degenerate.
func TestSplitThroughCoordinatorSnapsToRowBoundary(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// Replace the bootstrap region with one that holds several rows of one
	// table, so the default midpoint is guaranteed to fall inside a row.
	alloc := NewRegionIDAllocator(cli)
	rows := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	cols := []string{"a", "id", "name"}
	regionID, err := alloc.NextID(ctx)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	explicit := Region{
		ID:       regionID,
		StartKey: []byte(keyspace.ColumnKey("tellstone", "t", rows[0], cols[0])),
		EndKey:   []byte(keyspace.ColumnKey("tellstone", "t", rows[len(rows)-1], cols[len(cols)-1])),
		Peers:    []uint64{1},
		Epoch:    1,
	}
	if _, err := cli.Put(ctx, regionKey(explicit.ID), string(encodeRegion(explicit))); err != nil {
		t.Fatalf("put region: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	sc := NewSplitCoordinator(mgr, &testLogger{t})
	result, err := sc.Split(ctx, SplitRequest{RegionID: explicit.ID})
	if err != nil {
		t.Fatalf("Split: %v", err)
	}

	left, _ := mgr.getRegion(ctx, result.LeftID)
	right, _ := mgr.getRegion(ctx, result.RightID)
	boundary := left.EndKey
	if !bytes.Equal(boundary, right.StartKey) {
		t.Fatalf("halves disagree: left.EndKey=%q right.StartKey=%q", boundary, right.StartKey)
	}
	for _, id := range rows {
		if splitsRow(boundary, "tellstone", "t", id, cols) {
			t.Fatalf("coordinator split at %q cuts row %q", boundary, id)
		}
	}
	// A boundary is legal when it is either a row prefix or a point between two
	// rows. Anything else means the snap did not run.
	_, inRow := keyspace.RowPrefixOf(midpointKey(explicit.StartKey, explicit.EndKey))
	if inRow {
		if _, ok := keyspace.RowPrefixOf(boundary); !ok {
			t.Fatalf("midpoint was inside a row but boundary %q is not a row prefix", boundary)
		}
	}
}

// The split must fail cleanly, leaving no partial regions, when no row boundary
// is available.
func TestSplitRefusedLeavesRegionsUntouched(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()
	defer cli.Close()

	mgr := NewRegionManager(cli, 1, stubLP{leader: true}, []uint64{1})
	if err := mgr.BootstrapDefaultRegion(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	alloc := NewRegionIDAllocator(cli)
	cols := []string{"a", "id"}
	regionID, err := alloc.NextID(ctx)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	// A region holding exactly one row.
	single := Region{
		ID:       regionID,
		StartKey: []byte(keyspace.ColumnKey("tellstone", "t", "7", cols[0])),
		EndKey:   []byte(keyspace.ColumnKey("tellstone", "t", "7", cols[1])),
		Peers:    []uint64{1},
		Epoch:    1,
	}
	if _, err := cli.Put(ctx, regionKey(single.ID), string(encodeRegion(single))); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := mgr.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	before := countRegionKeys(t, cli)

	sc := NewSplitCoordinator(mgr, &testLogger{t})
	_, err = sc.Split(ctx, SplitRequest{RegionID: single.ID})
	if !errors.Is(err, ErrSplitWouldCutRow) {
		t.Fatalf("Split error = %v, want ErrSplitWouldCutRow", err)
	}
	// A refused split must not have allocated a region ID or written a half.
	after, _ := mgr.getRegion(ctx, single.ID)
	if after == nil || !bytes.Equal(after.StartKey, single.StartKey) || !bytes.Equal(after.EndKey, single.EndKey) {
		t.Fatalf("refused split mutated the region: %+v", after)
	}
	if got := countRegionKeys(t, cli); got != before {
		t.Fatalf("refused split changed the region count from %d to %d", before, got)
	}
}

// countRegionKeys reads the region count from etcd directly, so the assertion
// does not depend on RegionManager's internal caching or field layout.
func countRegionKeys(t *testing.T, cli *clientv3.Client) int {
	t.Helper()
	resp, err := cli.Get(context.Background(), regionKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("list regions: %v", err)
	}
	return len(resp.Kvs)
}

// TestChooseSplitKeyNeverCutsARowExhaustively checks the invariant over every
// region that can be formed from a realistic key set, rather than over a few
// hand-picked bounds.
//
// The hand-written cases found three wrong *expectations* during development;
// this one is what actually establishes the guarantee. For every ordered pair
// of keys it forms the region they delimit, asks for a split, and either checks
// the refusal is a legitimate "no row boundary" or checks that the accepted
// boundary is strictly interior and cuts no row. Row ids include ones needing
// escaping, so a row id can never be mistaken for two rows.
func TestChooseSplitKeyNeverCutsARowExhaustively(t *testing.T) {
	rowIDs := []string{"1", "2", "9", "10", "a", "b", "z", "a/b", "a%b", "user:1", "0000", "x y", "~meta"}
	cols := []string{"a", "id", "name", "z"}
	sortedCols := append([]string(nil), cols...)
	sort.Strings(sortedCols)

	var keys []string
	for _, id := range rowIDs {
		for _, c := range cols {
			keys = append(keys, keyspace.ColumnKey("tellstone", "t", id, c))
		}
	}
	sort.Strings(keys)

	sc := &SplitCoordinator{}
	accepted, refused := 0, 0
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			cur := &Region{ID: 1, StartKey: []byte(keys[i]), EndKey: []byte(keys[j])}
			got, err := sc.chooseSplitKey(cur, nil)
			if err != nil {
				if !errors.Is(err, ErrSplitWouldCutRow) {
					t.Fatalf("region [%q,%q): unexpected error %v", keys[i], keys[j], err)
				}
				refused++
				continue
			}
			accepted++
			if bytes.Compare(got, cur.StartKey) <= 0 || bytes.Compare(got, cur.EndKey) >= 0 {
				t.Fatalf("region [%q,%q): boundary %q is not strictly interior",
					keys[i], keys[j], got)
			}
			for _, id := range rowIDs {
				if splitsRow(got, "tellstone", "t", id, sortedCols) {
					t.Fatalf("region [%q,%q): boundary %q cuts row %q",
						keys[i], keys[j], got, id)
				}
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no region was split, so the invariant was never exercised")
	}
	t.Logf("exhaustive: %d regions split, %d refused, 0 rows cut", accepted, refused)
}
