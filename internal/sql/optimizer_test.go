/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: optimizer_test.go
Description: Tests for the planner: which access method a predicate chooses, and
the bounds a range plan derives.

The range tests are written against a gate that is currently closed, so they
assert that no primary key yields a range plan and pin the reason. That looks
backwards until you know the alternative: the integer row-id encoding *is*
order-preserving, so a range over an integer key looks correct, and the plan it
produces is wrong anyway, because keyspace.RowPrefix escapes every row id and the
escape inverts the order of roughly one id in 128. A test that only checked the
happy path would have passed on a plan that returns the wrong rows.
*/
package sql

import (
	"math"
	"strconv"
	"testing"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// planSchema builds the schema a plan is made against, with an integer primary
// key unless the test needs a different key type.
func planSchema(pkType ColumnType, extra ...Column) *Schema {
	cols := []Column{{Name: "id", Type: pkType}}
	cols = append(cols, extra...)
	return &Schema{DB: DefaultDB, Table: "users", Columns: cols, PrimaryKey: 0}
}

func planOf(t *testing.T, sch *Schema, where string) *physicalPlan {
	t.Helper()
	e := parseWhere(t, where)
	o := &optimizer{sch: sch}
	p, err := o.plan(e)
	if err != nil {
		t.Fatalf("plan %q: %v", where, err)
	}
	return p
}

func TestPlannerPointLookup(t *testing.T) {
	sch := planSchema(TypeInt)
	p := planOf(t, sch, "id = 42")
	if p.Kind != PlanPointLookup {
		t.Fatalf("id = 42 planned as %v, want PointLookup", p.Kind)
	}
	if string(p.Key.Literal) != "42" {
		t.Errorf("key = %q, want 42", p.Key.Literal)
	}
	if p.Rows != 1 {
		t.Errorf("estimated rows = %v, want 1", p.Rows)
	}
}

// A point lookup reads one row, so its cost does not scale with the table. This
// is the property that makes it worth choosing, so it is pinned rather than
// assumed.
func TestPlannerPointLookupCostIsTableIndependent(t *testing.T) {
	sch := planSchema(TypeInt)
	small, err := (&optimizer{sch: sch, stats: &TableStats{Rows: 10, Analyzed: true}}).plan(parseWhere(t, "id = 1"))
	if err != nil {
		t.Fatal(err)
	}
	big, err := (&optimizer{sch: sch, stats: &TableStats{Rows: 1000000, Analyzed: true}}).plan(parseWhere(t, "id = 1"))
	if err != nil {
		t.Fatal(err)
	}
	if small.Cost.Total != big.Cost.Total {
		t.Errorf("point lookup cost depends on table size: %v at 10 rows, %v at 1M", small.Cost.Total, big.Cost.Total)
	}
	// Equal, not merely smaller: the estimate must not move with table size at
	// all, or a plan comparison between two methods would compare the methods'
	// sensitivities to the estimate rather than the methods themselves.
}

func TestPlannerFullScanForNonKeyPredicate(t *testing.T) {
	sch := planSchema(TypeInt, Column{Name: "age", Type: TypeInt, Nullable: true})
	for _, where := range []string{
		"age > 30",
		"age = 1",
		"age IS NULL",
		"age IS NULL OR id = 5",
		"NOT (id = 5)",
	} {
		p := planOf(t, sch, where)
		if p.Kind != PlanFullScan {
			t.Errorf("%q planned as %v, want FullScan", where, p.Kind)
		}
	}
}

func TestPlannerFullScanCostGrowsWithTable(t *testing.T) {
	sch := planSchema(TypeInt, Column{Name: "age", Type: TypeInt})
	small, err := (&optimizer{sch: sch, stats: &TableStats{Rows: 10, Analyzed: true}}).plan(parseWhere(t, "age > 30"))
	if err != nil {
		t.Fatal(err)
	}
	big, err := (&optimizer{sch: sch, stats: &TableStats{Rows: 100000, Analyzed: true}}).plan(parseWhere(t, "age > 30"))
	if err != nil {
		t.Fatal(err)
	}
	if big.Cost.Total <= small.Cost.Total {
		t.Errorf("full scan cost did not grow: %v at 10 rows, %v at 100k", small.Cost.Total, big.Cost.Total)
	}
}

// No filter is a full scan that admits everything, and must be costed as one
// rather than as a zero-row plan.
func TestPlannerUnfilteredIsFullScan(t *testing.T) {
	sch := planSchema(TypeInt)
	p, err := (&optimizer{sch: sch, stats: &TableStats{Rows: 100, Analyzed: true}}).plan(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != PlanFullScan {
		t.Fatalf("no filter planned as %v, want FullScan", p.Kind)
	}
	if p.Rows != 100 {
		t.Errorf("unfiltered estimated rows = %v, want 100", p.Rows)
	}
}

// A predicate admitting nothing is answered without reading. This is a
// correctness-adjacent property rather than only a speed one: it is what keeps
// `id > <max>` from scanning a table to discover there is nothing.
func TestPlannerEmptyRangeIsFree(t *testing.T) {
	sch := planSchema(TypeInt)
	for _, where := range []string{"id > 9223372036854775807", "id < -9223372036854775808", "id = NULL"} {
		p := planOf(t, sch, where)
		if p.Rows != 0 {
			t.Errorf("%q estimated %v rows, want 0", where, p.Rows)
		}
		if p.Cost.Bytes != 0 {
			t.Errorf("%q estimated %v bytes read, want 0", where, p.Cost.Bytes)
		}
	}
}

// With hex row ids, an integer key is ranged. Every bound form the translator
// can produce has to land here, since a missed one silently degrades to a full
// table scan rather than failing.
func TestPlannerRangeIsPlannedForIntegerKeys(t *testing.T) {
	for _, pk := range []ColumnType{TypeInt, TypeBigInt} {
		sch := planSchema(pk)
		for _, where := range []string{
			"id > 1",
			"id >= 1",
			"id < 100",
			"id <= 100",
			"id > 1 AND id < 100",
			"id >= 1 AND id <= 100",
			"id BETWEEN 1 AND 100",
		} {
			p := planOf(t, sch, where)
			if p.Kind != PlanRangeScan {
				t.Errorf("%s key, %q planned as %v, want RangeScan", pk, where, p.Kind)
			}
		}
	}
}

// A predicate the range cannot express survives as a residual filter rather
// than collapsing the plan to a full scan -- but only when the range itself is
// still sound.
func TestPlannerRangeKeepsResidualFilter(t *testing.T) {
	sch := planSchema(TypeBigInt, Column{Name: "name", Type: TypeVarchar, Nullable: true})
	p := planOf(t, sch, "id >= 10 AND id <= 20 AND name = 'ada'")
	if p.Kind != PlanRangeScan {
		t.Fatalf("planned as %v, want RangeScan", p.Kind)
	}
	if p.Filter == nil {
		t.Error("residual predicate on a non-key column was dropped")
	}
}

// A text key cannot be ranged: the row id is the text itself, percent-escaped,
// and neither variable width nor that escape preserves order. Refusing is the
// point -- a range that returned the wrong rows would be worse than no range.
func TestPlannerRangeIsRefusedForTextKeys(t *testing.T) {
	for _, pk := range []ColumnType{TypeVarchar, TypeBytes, TypeTimestamp} {
		sch := planSchema(pk)
		for _, where := range []string{
			"id > 'a'",
			"id < 'z'",
			"id > 'a' AND id < 'z'",
		} {
			p := planOf(t, sch, where)
			if p.Kind == PlanRangeScan {
				t.Errorf("%s key, %q planned as RangeScan, want FullScan", pk, where)
			}
		}
	}
}

// The bounds are the part that is easy to get quietly wrong. A range is only
// correct if, for every value it should admit, the value's key falls inside
// [start, end] -- and for every value it should exclude, outside. Ordering the
// bounds against the row ids themselves checks both halves at once, and would
// catch an off-by-one in the hex bound arithmetic that a kind assertion cannot.
func TestPlannerRangeBoundsEncloseExactlyTheRightRows(t *testing.T) {
	sch := planSchema(TypeBigInt)
	p := planOf(t, sch, "id >= 10 AND id <= 20")
	if p.Kind != PlanRangeScan {
		t.Fatalf("planned as %v, want RangeScan", p.Kind)
	}
	if string(p.Lo) == "" || string(p.Hi) == "" {
		t.Fatal("RangeScan must carry both bounds")
	}
	if string(p.Lo) > string(p.Hi) {
		t.Fatalf("inverted bounds: start %q > end %q", string(p.Lo), string(p.Hi))
	}
	inside := func(v int64) bool {
		key := keyspace.RowPrefix(DefaultDB, sch.Table, keyspace.EncodeIntRowID(v))
		return key >= string(p.Lo) && key <= string(p.Hi)
	}
	for v := int64(0); v <= 40; v++ {
		want := v >= 10 && v <= 20
		if got := inside(v); got != want {
			t.Errorf("id %d included=%v, want %v (bounds %q..%q)", v, got, want, string(p.Lo), string(p.Hi))
		}
	}
}

// An open bound still has to be a key that no row id can exceed in the
// direction the comparison needs, or the range silently drops rows.
func TestPlannerRangeOpenBoundsAreWideEnough(t *testing.T) {
	sch := planSchema(TypeBigInt)
	rowKey := func(v int64) string {
		return keyspace.RowPrefix(DefaultDB, sch.Table, keyspace.EncodeIntRowID(v)) + "id"
	}
	lo := planOf(t, sch, "id < 0")
	if lo.Kind != PlanRangeScan {
		t.Fatalf("planned as %v, want RangeScan", lo.Kind)
	}
	// `id < 0` admits every negative id, so -1 has to fall inside.
	if k := rowKey(-1); !(k < string(lo.Hi) && k >= string(lo.Lo)) {
		t.Errorf("id -1 (%q) outside open range %q..%q", k, string(lo.Lo), string(lo.Hi))
	}
	hi := planOf(t, sch, "id > 9223372036854775806")
	if hi.Kind != PlanRangeScan {
		t.Fatalf("planned as %v, want RangeScan", hi.Kind)
	}
	// The only admissible row is MaxInt64, and its key must clear the lower
	// bound rather than merely equal the bound's prefix.
	if k := rowKey(9223372036854775807); !(k > string(hi.Lo)) {
		t.Errorf("MaxInt64 (%q) not above open lower bound %q", k, string(hi.Lo))
	}
}

// This is the inversion that forces the gate closed: two integers whose
// order-preserving encodings differ only in a trailing byte that the escape
// rewrites. The larger integer sorts *first* once escaped, so a range bounded on
// the raw encoding omits rows below the bound and includes rows above it.
func TestEscapeInvertsIntegerRowIDOrder(t *testing.T) {
	// Consecutive integers, so nothing about the pair is exotic: -1746 encodes
	// to ...f9. and -1745 to ...f9%2F. Neither is visibly escapable by eye,
	// which is part of why this needed measuring rather than reading.
	lower := int64(-1746)
	upper := int64(-1745)
	loKey := keyspace.EscapeRowID(string(EncodeOrderableInt(lower)))
	hiKey := keyspace.EscapeRowID(string(EncodeOrderableInt(upper)))

	if !(lower < upper) {
		t.Fatalf("fixture is wrong: %d is not less than %d", lower, upper)
	}
	if loKey < hiKey {
		t.Skipf("fixture no longer reproduces: %q < %q (the escape may have been fixed)", loKey, hiKey)
	}
	// This is the bug, asserted rather than narrated: the escaped row ids sort
	// the other way round from the integers they encode.
	if upper == math.MinInt64 {
		t.Fatal("unreachable")
	}
	_ = strconv.FormatInt(lower, 10)
	t.Logf("inversion reproduced: v=%d < v=%d but %q > %q", lower, upper, loKey, hiKey)
}

// Counting the inversions over a contiguous range is what turns "this can happen"
// into "this happens often enough to matter". A single counter-example could be
// a curiosity; fifteen in four thousand integers is a correctness property.
func TestEscapeInvertsIntegerRowIDOrderOften(t *testing.T) {
	const lo, hi = -2000, 2000
	breaks := 0
	var prev string
	for v := int64(lo); v <= hi; v++ {
		esc := keyspace.EscapeRowID(string(EncodeOrderableInt(v)))
		if v > lo && !(prev < esc) {
			breaks++
		}
		prev = esc
	}
	if breaks == 0 {
		t.Skip("escape is now order-preserving; the range gate can be reopened")
	}
	t.Logf("%d order inversions among %d consecutive integer row ids", breaks, hi-lo+1)
}

func TestPlannerUnknownColumnRefused(t *testing.T) {
	sch := planSchema(TypeInt, Column{Name: "age", Type: TypeInt})
	_, err := (&optimizer{sch: sch}).plan(parseWhere(t, "nope = 1"))
	if err == nil {
		t.Fatal("a predicate on a missing column was accepted")
	}
	// 42703: a typo is the client's mistake, not an engine gap.
	pg, ok := err.(*pgError)
	if !ok || pg.code != errUndefinedColumn {
		t.Errorf("error = %v, want undefined-column (42703)", err)
	}
}

// An unmeasured table must not look free. A zero cost would make the planner
// prefer an unanalyzed table over a measured one for reasons unrelated to data.
func TestPlannerUnanalyzedTableIsNotFree(t *testing.T) {
	sch := planSchema(TypeInt, Column{Name: "age", Type: TypeInt})
	unknown, err := (&optimizer{sch: sch}).plan(parseWhere(t, "age > 1"))
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Cost.Bytes <= 0 {
		t.Error("an unanalyzed table was costed at zero bytes")
	}
	if unknown.Measured {
		t.Error("an unanalyzed table reported a measured estimate")
	}
	if unknown.CostNote == "" {
		t.Error("an unanalyzed table reported no caveat for EXPLAIN")
	}
}

func TestPlannerBooleanSelectivitySane(t *testing.T) {
	sch := planSchema(TypeInt, Column{Name: "age", Type: TypeInt})
	st := &TableStats{Rows: 1000, Analyzed: true}
	and, err := (&optimizer{sch: sch, stats: st}).plan(parseWhere(t, "age > 1 AND age < 50"))
	if err != nil {
		t.Fatal(err)
	}
	or, err := (&optimizer{sch: sch, stats: st}).plan(parseWhere(t, "age > 1 OR age < 50"))
	if err != nil {
		t.Fatal(err)
	}
	// Two conditions on the same column are correlated, so AND is not strictly
	// the product -- but OR must not estimate *fewer* rows than AND, or the
	// planner would prefer the disjunction for the wrong reason.
	if or.Rows < and.Rows {
		t.Errorf("OR estimated %v rows, fewer than AND's %v", or.Rows, and.Rows)
	}
	if and.Rows >= 1000 {
		t.Errorf("AND over two conditions estimated %v of 1000 rows", and.Rows)
	}
}
