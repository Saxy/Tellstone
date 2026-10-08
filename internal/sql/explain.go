/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: explain.go
Description: The EXPLAIN text tree (ADR-014 decision 7).

EXPLAIN never executes the statement it wraps. That is a decision, not a
limitation: the execution pipeline in this phase can return one row, so an
EXPLAIN ANALYZE would have to either run the query and truncate the answer, or
run it and report a plan for a query whose actual behaviour it did not observe.
Both are worse than refusing, so ANALYZE inside EXPLAIN is rejected rather than
silently degraded.

The tree is text rather than JSON or XML because JSON and XML both need to be
added to the wire type list, which is a compatibility surface. A text column is
a text column, and a client that cannot parse it can still print it.
*/
package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// Explain renders a plan as the rows of a one-column result set.
//
// One row per line is deliberate: it is what PostgreSQL does, and it means a
// client that does not understand the format still shows a readable plan rather
// than one opaque cell.
//
// statsOf supplies the statistics line for a node that has statistics. It is
// passed as a function rather than called once up front because a join node has
// no schema of its own, and its children do; resolving the caller's statistics
// eagerly against the root schema would panic when the root is a join.
func Explain(p *physicalPlan, statsOf func(*Schema) *TableStats, now time.Time) [][]byte {
	t := &explainTree{now: now, statsOf: statsOf}
	t.render(p, 0)
	out := make([][]byte, len(t.lines))
	for i, line := range t.lines {
		out[i] = []byte(line)
	}
	return out
}

// explainTree accumulates rendered lines. Rows come back from EXPLAIN already
// escaped as text cells by the caller; nothing here needs to know the wire.
type explainTree struct {
	lines   []string
	now     time.Time
	statsOf func(*Schema) *TableStats
}

func (t *explainTree) emit(indent int, format string, args ...any) {
	t.lines = append(t.lines, strings.Repeat("  ", indent)+fmt.Sprintf(format, args...))
}

func (t *explainTree) render(p *physicalPlan, depth int) {
	if p == nil {
		return
	}
	name := p.Kind.explainName()
	ind := ""
	if p.Schema != nil {
		ind = fmt.Sprintf(" on %s", p.Schema.Table)
	}
	c := p.Cost
	// Startup..Total, and rows and width. Width is the same for every method on
	// one table, so it is printed once per line rather than repeated as if it
	// were a per-method number. A join has no schema, so its width is the
	// estimate the cost model carried on the node.
	line := fmt.Sprintf("%s%s  (cost=%.2f..%.2f rows=%d width=%d)",
		name, ind, c.Startup, c.Total, int64(c.Rows), int(rowWidth(p)))
	if p.EmptyRange {
		line += "  [empty range]"
	}
	if p.CostNote != "" {
		line += "  (" + p.CostNote + ")"
	}
	t.emit(depth, "%s", line)

	if p.Join != nil {
		// A join node's equality names both tables, so it sits at the node's
		// already-indented depth, above the two scans the cost is spent on.
		t.emit(depth+1, "Hash Cond: (%s)", p.Join.CondText)
	}
	switch p.Kind {
	case PlanPointLookup:
		t.emit(depth+1, "Index Cond: (%s = %s)", p.Schema.PrimaryKeyName(), valRefText(p.Key))
	case PlanRangeScan:
		t.emit(depth+1, "Index Cond: %s", rangeCondText(p))
	}
	if p.Filter != nil {
		t.emit(depth+1, "Filter: (%s)", exprText(p.Filter))
	}
	// The statistics line names a table's row count, so it exists only where a
	// node has a schema to carry one and actual statistics to report: a join's
	// estimate is derived from its children, not read from a table.
	if p.Schema != nil && p.Measured && t.statsOf != nil {
		stats := t.statsOf(p.Schema)
		t.emit(depth+1, "(statistics: %s, %s)", statsAge(stats, t.now), pluralRows(stats.Rows))
	}
	if p.Join != nil {
		t.render(p.Join.Build, depth+1)
		t.render(p.Join.Probe, depth+1)
	}
}

// rangeCondText renders a range's bounds in SQL-ish terms.
//
// The bounds are key bytes, and printing them would tell the reader nothing
// about the query they wrote. What is informative is which side is open, because
// an unbounded side is the difference between a range and a full scan wearing a
// range's name.
func rangeCondText(p *physicalPlan) string {
	pk := p.Schema.PrimaryKeyName()
	return fmt.Sprintf("%s >= %s AND %s <= %s", pk, boundLabel(p, p.Lo, true), pk, boundLabel(p, p.Hi, false))
}

// boundLabel names what a bound actually admits.
//
// A bound is key bytes, and bytes tell the reader nothing about their query.
// Three cases are worth naming: the row id a bound pins, the table-wide edge
// that stands for "no bound on this side", and anything else -- which is the
// exclusive end of one row and is reported as such rather than guessed at.
func boundLabel(p *physicalPlan, bound []byte, isLo bool) string {
	switch string(bound) {
	case "":
		return "(unbounded)"
	case TablePrefix(p.Schema.DB, p.Schema.Table):
		if isLo {
			return "(unbounded)"
		}
	case TablePrefixEnd(p.Schema.DB, p.Schema.Table):
		if !isLo {
			return "(unbounded)"
		}
	}
	if id, ok := rowIDFromKey(TablePrefix(p.Schema.DB, p.Schema.Table), trimExclusiveEnd(bound)); ok {
		if n, err := keyspace.DecodeIntRowID(id); err == nil {
			return strconv.FormatInt(n, 10)
		}
		return id
	}
	return "row prefix"
}

// trimExclusiveEnd removes the 0xFF that an inclusive upper bound carries, so
// the remainder is a row prefix and can be read as a row id.
func trimExclusiveEnd(bound []byte) []byte {
	if n := len(bound); n > 0 && bound[n-1] == 0xff {
		return bound[:n-1]
	}
	return bound
}

// rowWidth is the estimated bytes per output row, which the estimator has
// already computed for the whole row.
func rowWidth(p *physicalPlan) float64 {
	if p.Join != nil {
		// A join projects from both sides and carries no schema of its own, so
		// the estimator's projected-row width lives on the join node.
		return p.Join.OutWidth
	}
	if p.Schema == nil {
		return 0
	}
	if p.Cost.Rows == 0 {
		return rowBytes(p.Schema)
	}
	return p.Cost.Bytes / p.Cost.Rows
}

func pluralRows(n uint64) string {
	if n == 1 {
		return "1 row"
	}
	return fmt.Sprintf("%d rows", n)
}

// explainName is the node name. Nesting is emitted separately by emit, so the
// name carries no leading whitespace of its own.
func (k PlanKind) explainName() string {
	switch k {
	case PlanPointLookup, PlanIndexScan:
		return "Index Scan"
	case PlanRangeScan:
		return "Range Scan"
	case PlanFullScan:
		return "Seq Scan"
	case PlanHashJoin:
		return "Hash Join"
	}
	return "Node"
}

// exprText renders a predicate in SQL. It is used only for display, so an
// unrenderable node prints as a placeholder rather than failing the EXPLAIN --
// a plan whose text cannot be produced is still a plan, and refusing to show it
// would hide the very thing the client asked about.
func exprText(e Expr) string {
	switch t := e.(type) {
	case *TrueExpr:
		return "true"
	case *FalseExpr:
		return "false"
	case *CmpExpr:
		return fmt.Sprintf("%s %s %s", t.Col, t.Op.SQL(), valRefText(t.Val))
	case *NullTestExpr:
		if t.Not {
			return t.Col + " IS NOT NULL"
		}
		return t.Col + " IS NULL"
	case *LikeExpr:
		return fmt.Sprintf("%s LIKE %s", t.Col, valRefText(t.Val))
	case *BoolExpr:
		var inner []string
		for _, a := range t.Args {
			inner = append(inner, exprText(a))
		}
		switch t.Op {
		case BoolNot:
			return "NOT " + inner[0]
		case BoolAnd:
			return strings.Join(inner, " AND ")
		case BoolOr:
			return strings.Join(inner, " OR ")
		}
	}
	return "?"
}

// valRefText renders a value or placeholder for display. A parameter is shown
// as $n: EXPLAIN cannot know its value at plan time, and printing an empty
// string would read as a real, empty constant.
func valRefText(v ValRef) string {
	if v.Param != 0 {
		return fmt.Sprintf("$%d", v.Param)
	}
	return string(v.Literal)
}
