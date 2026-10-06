/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: expr_test.go
Description: Tests for the WHERE-clause expression tree. The cases that matter
are the ones where a plausible-looking tree returns the wrong rows: an inverted
operand, an inclusive endpoint, a NULL that is not a value, and a LIKE pattern
that is not a prefix. Each is pinned here rather than left to the planner's
tests, because a planner test that passes for the wrong reason is worse than no
planner test.
*/
package sql

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// parseWhere runs the real PG parser over a predicate, so the tests exercise the
// same shapes the server sees rather than hand-built parse trees.
func parseWhere(t *testing.T, where string) Expr {
	t.Helper()
	q := "SELECT * FROM t"
	if where != "" {
		q += " WHERE " + where
	}
	r, err := pg_query.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	e, err := translateExpr(r.GetStmts()[0].GetStmt().GetSelectStmt().GetWhereClause())
	if err != nil {
		t.Fatalf("translate %q: %v", where, err)
	}
	return e
}

func TestExprAbsentFilterIsNil(t *testing.T) {
	if e := parseWhere(t, ""); e != nil {
		t.Fatalf("no WHERE clause = %T, want nil (an unfiltered scan)", e)
	}
}

func TestExprComparisons(t *testing.T) {
	for _, tc := range []struct {
		where string
		op    CmpOp
	}{
		{"a = 1", OpEq},
		{"a < 1", OpLt},
		{"a <= 1", OpLe},
		{"a > 1", OpGt},
		{"a >= 1", OpGe},
	} {
		e := parseWhere(t, tc.where)
		c, ok := e.(*CmpExpr)
		if !ok {
			t.Errorf("%q = %T, want *CmpExpr", tc.where, e)
			continue
		}
		if c.Col != "a" || c.Op != tc.op {
			t.Errorf("%q = col %q op %v, want a %v", tc.where, c.Col, c.Op, tc.op)
		}
		if string(c.Val.Literal) != "1" {
			t.Errorf("%q literal = %q, want \"1\"", tc.where, c.Val.Literal)
		}
	}
}

func TestExprBooleanStructure(t *testing.T) {
	e := parseWhere(t, "a = 1 AND b = 2")
	b, ok := e.(*BoolExpr)
	if !ok || b.Op != BoolAnd || len(b.Args) != 2 {
		t.Fatalf("a=1 AND b=2 = %#v, want AND of 2", e)
	}
	if e := parseWhere(t, "a = 1 OR b = 2"); func() bool {
		b, ok := e.(*BoolExpr)
		return !ok || b.Op != BoolOr
	}() {
		t.Errorf("a=1 OR b=2 = %#v, want OR", e)
	}
	e = parseWhere(t, "NOT (a = 1)")
	b, ok = e.(*BoolExpr)
	if !ok || b.Op != BoolNot || len(b.Args) != 1 {
		t.Fatalf("NOT (a=1) = %#v, want NOT of 1", e)
	}
}

// The parser flattens a chain of conjunctions into one node. The tree has to
// come out the same shape whether the client wrote one AND or three, because the
// planner walks Args and a mixed shape would make it special-case.
func TestExprAndChainIsUniform(t *testing.T) {
	b, ok := parseWhere(t, "a = 1 AND b = 2 AND c = 3").(*BoolExpr)
	if !ok || b.Op != BoolAnd || len(b.Args) != 3 {
		t.Fatalf("three-way AND = %#v, want AND of 3", b)
	}
}

// `5 = a` asks the same question as `a = 5`. Reading only the left operand
// would reject it; reading it without inverting would compare the wrong way.
func TestExprInvertedOperands(t *testing.T) {
	e := parseWhere(t, "5 = a")
	c, ok := e.(*CmpExpr)
	if !ok || c.Col != "a" || c.Op != OpEq {
		t.Fatalf("5 = a = %#v, want a = <eq>", e)
	}
	e = parseWhere(t, "1 < a")
	c, ok = e.(*CmpExpr)
	if !ok || c.Col != "a" || c.Op != OpGt {
		t.Fatalf("1 < a = %#v, want a > ... (inverted)", e)
	}
}

func TestExprNullTests(t *testing.T) {
	e, ok := parseWhere(t, "a IS NULL").(*NullTestExpr)
	if !ok || e.Col != "a" || e.Not {
		t.Fatalf("a IS NULL = %#v, want col a, Not=false", e)
	}
	e, ok = parseWhere(t, "a IS NOT NULL").(*NullTestExpr)
	if !ok || e.Col != "a" || !e.Not {
		t.Fatalf("a IS NOT NULL = %#v, want col a, Not=true", e)
	}
}

// NULL is the absence of a key, so `= NULL` is a comparison with no value and
// filters every row out. Dropping the term instead would return the row, which
// is the failure this pins.
func TestExprNullComparisonIsFalse(t *testing.T) {
	for _, where := range []string{"a = NULL", "a > NULL", "a IS NULL AND b = NULL"} {
		if _, ok := parseWhere(t, where).(*FalseExpr); !ok {
			t.Errorf("%q = %T, want *FalseExpr (no row qualifies)", where, parseWhere(t, where))
		}
	}
}

// `a IS NULL OR b IS NULL` must not collapse into an equality on either
// column: it is a disjunction of two existence questions.
func TestExprOrOfNullTestsSurvives(t *testing.T) {
	e := parseWhere(t, "a IS NULL OR b IS NULL")
	b, ok := e.(*BoolExpr)
	if !ok || b.Op != BoolOr || len(b.Args) != 2 {
		t.Fatalf("got %#v, want OR of 2 null tests", e)
	}
	for _, arg := range b.Args {
		if _, ok := arg.(*NullTestExpr); !ok {
			t.Errorf("OR operand = %T, want *NullTestExpr", arg)
		}
	}
}

func TestExprLikePrefix(t *testing.T) {
	e, ok := parseWhere(t, "a LIKE 'ab%'").(*LikeExpr)
	if !ok || e.Col != "a" || string(e.Val.Literal) != "ab%" {
		t.Fatalf("a LIKE 'ab%%' = %#v, want LikeExpr on a with pattern ab%%", e)
	}
	// A pattern with no wildcard is a prefix match too (it matches one exact
	// value), and must not be refused.
	if err := checkLikePrefix([]byte("abc")); err != nil {
		t.Errorf("wildcard-free pattern refused: %v", err)
	}
}

func TestExprLikeNonPrefixRefused(t *testing.T) {
	for _, pat := range []string{"%ab", "a%b", "a_b%", "a__", "_a"} {
		q := "a LIKE '" + pat + "'"
		if _, err := translateExpr(mustWhere(t, q)); err == nil {
			t.Errorf("%q was accepted, want refused as a non-prefix pattern", q)
		} else if !strings.Contains(err.Error(), "prefix match") {
			t.Errorf("%q = %v, want a prefix-match refusal", q, err)
		}
	}
}

// A parameterized pattern cannot be checked at plan time, so it is carried
// rather than refused. The planner refuses it as a range instead.
func TestExprLikeParameterCarried(t *testing.T) {
	e, ok := parseWhere(t, "a LIKE $1").(*LikeExpr)
	if !ok || e.Val.Param != 1 {
		t.Fatalf("a LIKE $1 = %#v, want LikeExpr carrying param 1", e)
	}
	if exprHasLiteralOnly(e) {
		t.Error("a LIKE $1 reported literal-only; the pattern is a parameter")
	}
}

func TestExprBetweenDesugarsInclusively(t *testing.T) {
	e := parseWhere(t, "a BETWEEN 1 AND 5")
	b, ok := e.(*BoolExpr)
	if !ok || b.Op != BoolAnd || len(b.Args) != 2 {
		t.Fatalf("BETWEEN = %#v, want AND of 2 comparisons", e)
	}
	lo, ok1 := b.Args[0].(*CmpExpr)
	hi, ok2 := b.Args[1].(*CmpExpr)
	if !ok1 || !ok2 {
		t.Fatalf("BETWEEN args are %T and %T, want comparisons", b.Args[0], b.Args[1])
	}
	// BETWEEN includes both endpoints. Flipping either to a strict comparison
	// would drop rows the client asked for.
	if lo.Op != OpGe || string(lo.Val.Literal) != "1" {
		t.Errorf("BETWEEN low bound = %v %q, want >= 1", lo.Op, lo.Val.Literal)
	}
	if hi.Op != OpLe || string(hi.Val.Literal) != "5" {
		t.Errorf("BETWEEN high bound = %v %q, want <= 5", hi.Op, hi.Val.Literal)
	}
}

// `x AND TRUE` must fold away, so `WHERE id = 1 AND TRUE` stays a point lookup
// rather than becoming an unplannable conjunction.
func TestExprTrueFolds(t *testing.T) {
	e := parseWhere(t, "a = 1 AND TRUE")
	if _, ok := e.(*CmpExpr); !ok {
		t.Fatalf("a=1 AND TRUE = %T, want *CmpExpr", e)
	}
}

// `x OR TRUE` is TRUE. Simplifying it the same way as AND would turn
// `a = 1 OR TRUE` into `a = 1`, which returns too few rows while looking like a
// correct filter -- the quietest failure this file has to prevent.
func TestExprOrTrueAbsorbs(t *testing.T) {
	for _, q := range []string{"a = 1 OR TRUE", "TRUE OR a = 1"} {
		if _, ok := parseWhere(t, q).(*TrueExpr); !ok {
			t.Errorf("%q = %T, want *TrueExpr (every row qualifies)", q, parseWhere(t, q))
		}
	}
	if _, ok := parseWhere(t, "a = 1 OR FALSE").(*CmpExpr); !ok {
		t.Error("a = 1 OR FALSE should fold to the comparison")
	}
	if _, ok := parseWhere(t, "a = 1 AND FALSE").(*FalseExpr); !ok {
		t.Error("a = 1 AND FALSE should be *FalseExpr")
	}
	if _, ok := parseWhere(t, "NOT TRUE").(*FalseExpr); !ok {
		t.Error("NOT TRUE should be *FalseExpr")
	}
	if _, ok := parseWhere(t, "NOT (a = 1)").(*BoolExpr); !ok {
		t.Error("NOT (a = 1) should stay a NOT")
	}
}

func TestExprUnsupportedRefused(t *testing.T) {
	for _, q := range []string{
		"a + 1 = 2", // arithmetic is not a comparison
		"a = ANY(ARRAY[1])",
		"(SELECT 1) = 1", // subquery
		"a = b",          // two columns
	} {
		if _, err := translateExpr(mustWhere(t, q)); err == nil {
			t.Errorf("%q was accepted, want refused", q)
		}
	}
}

func TestExprColumnsAndParams(t *testing.T) {
	e := parseWhere(t, "a = 1 AND b = $1 AND NOT (c IS NULL)")
	cols := exprColumns(e)
	want := map[string]bool{"a": true, "b": true, "c": true}
	if len(cols) != 3 {
		t.Fatalf("columns = %v, want a, b, c", cols)
	}
	for _, c := range cols {
		if !want[c] {
			t.Errorf("unexpected column %q", c)
		}
	}
	// $1 is a parameter, so the tree is not literal-only and cannot become a
	// range before Bind.
	if exprHasLiteralOnly(e) {
		t.Error("tree with $1 reported literal-only")
	}
	if !exprHasLiteralOnly(parseWhere(t, "a = 1")) {
		t.Error("a = 1 reported parameterized")
	}
}

// walkExpr must visit parents before children and stop when told to, so a
// consumer can accumulate state without recursing itself.
func TestExprWalkOrderAndPruning(t *testing.T) {
	e := parseWhere(t, "a = 1 AND (b = 2 OR c = 3)")
	var order []string
	walkExpr(e, func(n Expr) bool {
		switch t := n.(type) {
		case *BoolExpr:
			order = append(order, "bool")
		case *CmpExpr:
			order = append(order, t.Col)
		}
		return true
	})
	if len(order) == 0 || order[0] != "bool" {
		t.Fatalf("walk order = %v, want the root boolean first", order)
	}
	if len(order) != 5 {
		t.Fatalf("walk visited %v, want 5 nodes (root, a, or, b, c)", order)
	}

	var seen int
	walkExpr(e, func(Expr) bool {
		seen++
		return false
	})
	if seen != 1 {
		t.Errorf("returning false visited %d nodes, want 1 (subtree pruned)", seen)
	}
}

func mustWhere(t *testing.T, q string) *pg_query.Node {
	t.Helper()
	r, err := pg_query.Parse("SELECT * FROM t WHERE " + q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	return r.GetStmts()[0].GetStmt().GetSelectStmt().GetWhereClause()
}
