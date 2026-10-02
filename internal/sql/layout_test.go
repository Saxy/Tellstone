/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: layout_test.go
Description: Tests for the ADR-013 key layout. The load-bearing properties are
that the layout is injective (two row ids never share a key prefix), that the
integer encodings really are order preserving, and that a row prefix converts to
a correct bounded range. An ordering claim that is only spot-checked is the kind
of bug that surfaces as a wrong query result much later, so the encodings are
checked against a sort of the values themselves.
*/
package sql

import (
	"bytes"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestRowIDEscapingIsInjective(t *testing.T) {
	// The whole point of escaping: a row id containing the separator must not
	// be able to impersonate another row's key prefix.
	ids := []string{
		"42", "a/b", "a%2Fb", "a%b", "a", "", "/", "//", "%", "a/b/c", "a%2fb",
	}
	seen := make(map[string]string, len(ids))
	for _, id := range ids {
		key := RowPrefix(DefaultDB, "users", id)
		if prev, dup := seen[key]; dup {
			t.Fatalf("row ids %q and %q both map to %q", prev, id, key)
		}
		seen[key] = id
		got, err := UnescapeRowID(key[len(DefaultDB)+len("/users/"):][:len(key)-(len(DefaultDB)+len("/users/")+1)])
		if err != nil {
			t.Fatalf("unescaping %q: %v", id, err)
		}
		if got != id {
			t.Fatalf("round trip: %q -> %q -> %q", id, key, got)
		}
	}
}

func TestUnescapeRowIDRejectsGarbage(t *testing.T) {
	for _, bad := range []string{"a%zz", "a%2", "trailing%"} {
		if _, err := UnescapeRowID(bad); err == nil {
			t.Fatalf("UnescapeRowID(%q) accepted a malformed escape", bad)
		}
	}
}

func TestValidateSegment(t *testing.T) {
	for _, bad := range []string{"", "a/b", "~meta", "*hint"} {
		if err := ValidateTableName(bad); err == nil {
			t.Fatalf("ValidateTableName(%q) accepted an unusable name", bad)
		}
	}
	for _, ok := range []string{"users", "user_data", "Users2", "a.b"} {
		if err := ValidateTableName(ok); err != nil {
			t.Fatalf("ValidateTableName(%q) = %v", ok, err)
		}
	}
}

func TestRowRangeBoundsAreCorrect(t *testing.T) {
	// A row read is one bounded range. The bound has to include every column of
	// the row and exclude the next row, or a scan silently drops or duplicates
	// rows.
	cols := []string{"name", "lastname", "age", "zzz"}
	lower := RowPrefix(DefaultDB, "users", "42")
	upper := RowIDPrefixEnd(DefaultDB, "users", "42")
	for _, c := range cols {
		k := ColumnKey(DefaultDB, "users", "42", c)
		if k < lower || k >= upper {
			t.Fatalf("column key %q outside [%q,%q)", k, lower, upper)
		}
	}
	// "43" is the next row; "420" sorts after "42" but is a different row, and
	// "4" sorts before it. Both must fall outside the range.
	for _, other := range []string{"43", "420", "4", "5", "100"} {
		k := RowPrefix(DefaultDB, "users", other)
		if k >= lower && k < upper {
			t.Fatalf("row %q fell inside row 42's range [%q,%q)", other, lower, upper)
		}
	}
}

func TestTableRangeBoundsAreCorrect(t *testing.T) {
	lower := TablePrefix(DefaultDB, "users")
	upper := TablePrefixEnd(DefaultDB, "users")
	if k := RowPrefix(DefaultDB, "users", "1"); k < lower || k >= upper {
		t.Fatalf("row key %q outside table range", k)
	}
	// "users_extra" is a different table that sorts after "users".
	if k := TablePrefix(DefaultDB, "users_extra"); k >= lower && k < upper {
		t.Fatalf("sibling table %q fell inside users' range", k)
	}
}

func TestOrderableIntSortsLikeTheIntegers(t *testing.T) {
	vals := []int64{math.MinInt64, -1 << 40, -1000, -1, 0, 1, 7, 1000, 1 << 40, math.MaxInt64}
	enc := make([][]byte, len(vals))
	for i, v := range vals {
		enc[i] = EncodeOrderableInt(v)
	}
	order := make([]int, len(vals))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return bytes.Compare(enc[order[a]], enc[order[b]]) < 0
	})
	for i, idx := range order {
		if vals[idx] != vals[i] {
			t.Fatalf("encoded order %v does not match numeric order %v", vals, order)
		}
	}
	// Same width for every value, which is what makes the order meaningful.
	for _, e := range enc {
		if len(e) != OrderableIntLen {
			t.Fatalf("encoded int is %d bytes, want %d", len(e), OrderableIntLen)
		}
	}
}

func TestOrderableFloatSortsLikeTheFloats(t *testing.T) {
	vals := []float64{
		math.Inf(-1), -math.MaxFloat64, -1e300, -1.5, -1, -0.5,
		math.Copysign(0, -1), 0, 0.5, 1, 1.5, 1e300, math.MaxFloat64, math.Inf(1),
	}
	enc := make([][]byte, len(vals))
	for i, v := range vals {
		enc[i] = EncodeOrderableFloat(v)
	}
	order := make([]int, len(vals))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return bytes.Compare(enc[order[a]], enc[order[b]]) < 0
	})
	for i, idx := range order {
		if vals[idx] != vals[i] {
			t.Fatalf("encoded float order %v does not match numeric order %v", vals, order)
		}
	}
}

func TestOrderableFloatRoundTrips(t *testing.T) {
	for _, v := range []float64{0, -0, 1, -1, 1e-300, -1e-300, math.MaxFloat64, -math.MaxFloat64} {
		got, err := DecodeOrderableFloat(EncodeOrderableFloat(v))
		if err != nil {
			t.Fatalf("decoding %v: %v", v, err)
		}
		if got != v {
			t.Fatalf("round trip: %v -> %v", v, got)
		}
	}
}

func TestOrderableIntRoundTrips(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
		got, err := DecodeOrderableInt(EncodeOrderableInt(v))
		if err != nil {
			t.Fatalf("decoding %v: %v", v, err)
		}
		if got != v {
			t.Fatalf("round trip: %v -> %v", v, got)
		}
	}
}

func TestOrderableTimestampRoundTrips(t *testing.T) {
	ts := time.Date(2024, 3, 17, 12, 30, 45, 123456789, time.UTC)
	got, err := DecodeOrderableTimestamp(EncodeOrderableTimestamp(ts))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(ts) {
		t.Fatalf("round trip: %v -> %v", ts, got)
	}
	// Chronological order has to survive too, or ORDER BY on a timestamp
	// column silently returns the wrong sequence.
	earlier := EncodeOrderableTimestamp(ts.Add(-time.Hour))
	later := EncodeOrderableTimestamp(ts.Add(time.Hour))
	if bytes.Compare(earlier, later) >= 0 {
		t.Fatal("timestamps do not encode in chronological order")
	}
}

func TestOrderableBoolSorts(t *testing.T) {
	if bytes.Compare(EncodeOrderableBool(false), EncodeOrderableBool(true)) >= 0 {
		t.Fatal("false must sort before true")
	}
}

func TestEncodeValueRejectsMismatch(t *testing.T) {
	if _, err := EncodeValue(TypeInt, "not an int"); err == nil {
		t.Fatal("encoding a string as INT should fail")
	}
	if _, err := EncodeValue(TypeInvalid, 1); err == nil {
		t.Fatal("encoding under the invalid type should fail")
	}
	// NULL is an empty value, never an error.
	b, err := EncodeValue(TypeVarchar, nil)
	if err != nil || b != nil {
		t.Fatalf("NULL: got (%v, %v), want (nil, nil)", b, err)
	}
}

func TestColumnTypeRoundTrip(t *testing.T) {
	for name := range typeNames {
		tp, err := ParseColumnType(name)
		if err != nil {
			t.Fatalf("ParseColumnType(%q): %v", name, err)
		}
		if _, err := ParseColumnType(strings.ToUpper(name)); err != nil {
			t.Fatalf("ParseColumnType should be case insensitive: %v", err)
		}
		if tp.String() == "INVALID" {
			t.Fatalf("type %q has no display name", name)
		}
	}
	if _, err := ParseColumnType("geometry"); err == nil {
		t.Fatal("unsupported type should be rejected")
	}
}

func TestMetaKeysAreExcludedFromScans(t *testing.T) {
	// Schema must never surface as table data, and a user table named the same
	// as a reserved segment must be impossible to create in the first place.
	meta := MetaTableKey(DefaultDB, "users")
	if !IsMetaKey(meta) {
		t.Fatalf("%q should be a meta key", meta)
	}
	if IsMetaKey(ColumnKey(DefaultDB, "users", "1", "name")) {
		t.Fatal("a data column key was reported as meta")
	}
	if IsMetaKey(TablePrefix(DefaultDB, "users")) {
		t.Fatal("a table prefix was reported as meta")
	}
	// The meta subtree must sit under the database subtree so schema travels
	// with the data it describes.
	if !bytes.HasPrefix([]byte(meta), []byte(DefaultDB+"/")) {
		t.Fatalf("meta key %q escaped the database subtree", meta)
	}
}

func TestGeneratedRowIDSortsChronologically(t *testing.T) {
	// A fixed width is what makes generated ids sort as they were issued.
	ids := []string{
		FormatGeneratedRowID(1),
		FormatGeneratedRowID(999999999),
		FormatGeneratedRowID(1000000000),
		FormatGeneratedRowID(math.MaxUint64),
	}
	for _, id := range ids {
		if len(id) != GeneratedRowIDWidth {
			t.Fatalf("generated id %q is %d chars, want %d", id, len(id), GeneratedRowIDWidth)
		}
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("generated ids do not sort in issue order: %v", ids)
	}
}
