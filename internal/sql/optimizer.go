/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: optimizer.go
Description: The Phase 10 planner: a WHERE-clause expression tree becomes a
physical access method with a cost (ADR-014 decision 3). The planner is
topology-agnostic -- it knows the schema and the table's statistics, and nothing
about regions or hops -- because a plan that depended on where data lived would
not be reproducible under EXPLAIN and would have to be rewritten when a region
moved.

With no secondary index there are at most two legal plans for a query, so this
is a comparison and not a search. A predicate that addresses the primary key
becomes a point lookup or a key range; everything else is a full scan with the
predicate evaluated as a residual filter. There is no plan to choose between an
index and a table yet, and building a search over two alternatives would be the
shape of a later phase rather than this one.

The planner is where a predicate stops being a question and becomes an access
method, so it is also where a query that cannot be served honestly has to be
refused. A text primary key cannot be range-scanned (keyspace.EscapeRowID does
not preserve order) and a non-key column cannot be range-scanned at all (the row
id precedes the column in the key), and both are planned as a full scan rather
than as a range that would return the wrong rows.
*/
package sql

import (
	"fmt"
	"math"
	"strconv"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// PlanKind is the access method a query uses.
type PlanKind uint8

// SQL renders a comparison operator the way the client wrote it.
func (o CmpOp) SQL() string {
	switch o {
	case OpEq:
		return "="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	}
	return "?"
}

const (
	// PlanPointLookup addresses exactly one row by its primary key. It is the
	// only method that can be executed in this phase: it yields at most one row,
	// and the wire path emits at most one DataRow.
	PlanPointLookup PlanKind = iota
	// PlanRangeScan walks a contiguous primary-key range. It can match many
	// rows, so it is planned and costed but refused at execution until the
	// result path can deliver more than one row (ADR-014 guardrail 4).
	PlanRangeScan
	// PlanFullScan walks the table's key range and evaluates Filter per row.
	// Also multi-row, and refused for the same reason.
	PlanFullScan
	// PlanIndexScan is declared but unimplemented. It exists so that Phase 13
	// adds an access method rather than widening this planner's switch, and so
	// that the plan space is visible before anything fills it.
	PlanIndexScan
)

func (k PlanKind) String() string {
	switch k {
	case PlanPointLookup:
		return "PointLookup"
	case PlanRangeScan:
		return "RangeScan"
	case PlanFullScan:
		return "FullScan"
	case PlanIndexScan:
		return "IndexScan"
	}
	return "Unknown"
}

// physicalPlan is the planner's output: an access method over one table.
//
// It is a flat structure rather than a tree because no plan has more than one
// node yet. EXPLAIN renders an indented tree, and a tree-shaped renderer over a
// single node would be a lie about the shape that the first multi-node plan -- a
// join, or an index scan with a residual filter -- then has to be retrofitted
// into.
type physicalPlan struct {
	Kind   PlanKind
	Schema *Schema

	// Key is the point lookup's primary key value. It stays a ValRef rather
	// than bytes because a parameterized equality is only known at Bind, and
	// refusing parameters here would make every extended-protocol query a
	// full scan.
	Key ValRef

	// Lo and Hi bound a RangeScan over the primary key, inclusive. A nil
	// pointer is that side unbounded. They are the *key* bounds, resolved
	// through the column's order-preserving encoding, not the SQL values.
	Lo, Hi []byte
	// EmptyRange marks a range whose bound overflowed its column's domain --
	// `id > <max>` is the reachable case. It is recorded rather than computed
	// from Lo>Hi, because a byte-encoded bound would wrap to a *valid-looking*
	// range over the whole column, which is how "greater than the largest
	// integer" turns into "every row".
	EmptyRange bool

	// Filter is the residual predicate, evaluated per row after the scan. It is
	// nil when the access method already expresses the whole predicate, and it
	// is also nil for a plan that admits every row.
	Filter Expr

	// Cost and Rows are the estimate, not a measurement. See cost.go.
	Cost Cost
	Rows float64
	// Measured reports whether Rows came from an ANALYZE reading rather than
	// from the planner's assumption. EXPLAIN prints the two differently,
	// because an estimate presented as a measurement is not correctable by
	// whoever reads it.
	Measured bool
	// CostNote is a short caveat for EXPLAIN, empty when there is nothing to
	// qualify.
	CostNote string
}

// optimizer is the planner's input beyond the statement itself: the resolved
// schema and the table's statistics. Statistics may be absent, in which case
// costs fall back to a stated assumption rather than to zero -- a zero cost
// would make an unmeasured table look free, and the planner would prefer it.
type optimizer struct {
	sch   *Schema
	stats *TableStats
}

// plan resolves an expression tree into an access method.
//
// The schema must be the one for the table being planned; a predicate naming a
// column the table does not have is refused here rather than evaluated against
// nothing, which would otherwise read as "no rows qualify" and look like an
// answer.
func (o *optimizer) plan(filter Expr) (*physicalPlan, error) {
	if err := o.checkColumns(filter); err != nil {
		return nil, err
	}
	conj := conjuncts(filter)

	// A predicate that admits nothing is answered without touching the table.
	// This is not an optimization: it is the difference between reporting zero
	// rows and scanning a table to discover there are none.
	if _, ok := filter.(*FalseExpr); ok {
		p := &physicalPlan{Kind: PlanFullScan, Schema: o.sch, Filter: filter}
		p.Cost = Cost{Startup: 0, Total: 0, Rows: 0}
		p.Rows = 0
		return p, nil
	}
	// A predicate that cannot match any value of the primary key's domain is
	// also free. This is a property of the comparison, not of the layout, so it
	// is checked before the access method is chosen: `id > <the largest
	// int64>` is unsatisfiable whatever a range plan could have done with it,
	// and answering it without reading is correct even on a layout where no
	// range is derivable at all.
	if o.domainIsEmpty(conj) {
		p := &physicalPlan{Kind: PlanFullScan, Schema: o.sch, Filter: filter}
		p.Cost = Cost{Startup: 0, Total: 0, Rows: 0, Bytes: 0}
		p.Rows = 0
		return p, nil
	}

	if p := o.pointLookup(conj); p != nil {
		// Only produce a point lookup when there is exactly one conjunct and it is primary-key equality.
		// Otherwise return nil so planning falls through to the existing execution path.
		if len(conj) != 1 {
			return nil
		}
		if !isPrimaryKeyEquality(conj[0]) {
			return nil
		}
		return o.finish(p)
	}
	if p := o.rangeScan(conj); p != nil {
		return o.finish(p)
	}

	p := &physicalPlan{Kind: PlanFullScan, Schema: o.sch, Filter: filter}
	return o.finish(p)
}

// checkColumns refuses a predicate naming a column the table does not have.
//
// The error is 42703 rather than an unsupported-feature error because a typo is
// the client's mistake, not a gap in the engine, and PostgreSQL reports it that
// way.
func (o *optimizer) checkColumns(e Expr) error {
	for _, col := range exprColumns(e) {
		if _, ok := o.sch.Column(col); !ok {
			return &pgError{
				code: errUndefinedColumn,
				msg:  fmt.Sprintf("column %q does not exist", col),
			}
		}
	}
	return nil
}

// domainIsEmpty reports whether a conjunct set cannot be satisfied by any value
// in the primary key's domain.
//
// It is deliberately narrow -- only the two boundaries -- because a wider
// version would need to know the value distribution, and that is the histogram
// this phase does not have (ADR-014 decision 6). Two cases are provable from
// the type alone, and both are ones a client writes by accident: `id >
// 9223372036854775807` and `id < -9223372036854775808`.
func (o *optimizer) domainIsEmpty(conj []Expr) bool {
	pk := o.sch.PrimaryKeyName()
	for _, c := range conj {
		cmp, ok := c.(*CmpExpr)
		if !ok || cmp.Col != pk || cmp.Val.Param != 0 {
			continue
		}
		if cmp.Op != OpGt && cmp.Op != OpLt {
			continue
		}
		pkCol, ok := o.sch.Column(pk)
		if !ok || (pkCol.Type != TypeInt && pkCol.Type != TypeBigInt) {
			continue
		}
		n, err := strconv.ParseInt(string(cmp.Val.Literal), 10, 64)
		if err != nil {
			continue
		}
		if (cmp.Op == OpGt && n == math.MaxInt64) || (cmp.Op == OpLt && n == math.MinInt64) {
			return true
		}
	}
	return false
}

// conjuncts flattens top-level ANDs into a flat list, so
// `id > 1 AND id < 5 AND name = 'x'` offers three independent constraints to
// the planner rather than one opaque nesting. Anything under an OR stays whole,
// because a disjunction cannot narrow a range -- the union of two ranges is not
// one range, and pretending otherwise would drop the rows outside the first.
func conjuncts(e Expr) []Expr {
	if e == nil {
		return nil
	}
	if b, ok := e.(*BoolExpr); ok && b.Op == BoolAnd {
		var out []Expr
		for _, arg := range b.Args {
			out = append(out, conjuncts(arg)...)
		}
		return out
	}
	return []Expr{e}
}

// pointLookup recognises an equality on the primary key.
//
// A point lookup wins over a range even when both are available, because it
// reads one row rather than a range and needs no bound arithmetic.
func (o *optimizer) pointLookup(conj []Expr) *physicalPlan {
	pk := o.sch.PrimaryKeyName()
	for _, c := range conj {
		cmp, ok := c.(*CmpExpr)
		if !ok || cmp.Col != pk || cmp.Op != OpEq {
			continue
		}
		return &physicalPlan{Kind: PlanPointLookup, Schema: o.sch, Key: cmp.Val}
	}
	return nil
}

// rowIDIsOrderPreserving reports whether a table's primary key row ids sort in
// the same order as the values they encode.
//
// Integer primary keys can, because rowID renders them with
// keyspace.EncodeIntRowID: 16 fixed-width hex digits of the sign-flipped value.
// Fixed width is what carries the order (a variable-width decimal does not
// sort numerically -- "10" lands before "9"), and hex is what makes the
// unconditional EscapeRowID a no-op, since hex digits are 0x30-0x39 and
// 0x61-0x66 and so contain neither the escape byte nor the separator.
//
// That second property is the one that used to rule every table out. Before the
// hex encoding, an integer row id was a decimal string and EscapeRowID still ran
// on it; for the raw fixed-width encoding the escape was observably inverting
// order, because '%' escapes to "%2F" and 0x25 sorts below the bytes it
// replaces. An escaped row id is not the row id.
//
// Text primary keys still cannot be ranged. Their row ids are the text itself,
// percent-escaped, so both problems remain: variable width and an escape that
// reorders. Refusing them here is the whole point of the gate -- a range that
// silently returned the wrong rows would be worse than no range plan at all.
func rowIDIsOrderPreserving(c *Column) bool {
	return c.Type == TypeInt || c.Type == TypeBigInt
}

// rangeScan recognises a conjunction of comparisons that bounds the primary key.
//
// It returns nil for schemas whose row ids do not sort with their values, per
// rowIDIsOrderPreserving above: text primary keys are refused, integer ones are
// planned.
func (o *optimizer) rangeScan(conj []Expr) *physicalPlan {
	pkCol, ok := o.sch.Column(o.sch.PrimaryKeyName())
	if !ok {
		return nil
	}
	// A parameterized bound is not known at plan time, so a range built from one
	// would be a guess. It stays a residual filter so the query still plans, as
	// a full scan.
	if !rowIDIsOrderPreserving(pkCol) {
		return nil
	}
	// The gate above already admits only integers, and for them the row id is
	// fixed-width hex, so a byte range over the row id is exactly a numeric
	// range. A text value's order under any collation is not the order of its
	// UTF-8 bytes in general, which is why it stays excluded.

	var lo, hi *int64
	empty := false
	found := false
	var residual []Expr

	for _, c := range conj {
		cmp, ok := c.(*CmpExpr)
		if !ok || cmp.Col != o.sch.PrimaryKeyName() {
			residual = append(residual, c)
			continue
		}
		if cmp.Val.Param != 0 {
			// A parameterized bound is not a range. It stays a residual filter
			// so the query still plans, as a full scan.
			residual = append(residual, c)
			continue
		}
		n, err := strconv.ParseInt(string(cmp.Val.Literal), 10, 64)
		if err != nil {
			// A bound the column's own type cannot parse is a value error, and
			// reporting it here would be premature: the column type is checked
			// at execution, where the client gets a syntax error naming it. It
			// is treated as a residual filter so the error surfaces once, there.
			residual = append(residual, c)
			continue
		}
		switch cmp.Op {
		case OpGt:
			if n == math.MaxInt64 {
				// Nothing is greater than the largest integer. Recorded rather
				// than encoded: the 8-byte order-preserving encoding wraps, so
				// encode(MaxInt64)+1 is encode(MinInt64) and the range would
				// come back covering the whole column.
				empty = true
				found = true
				continue
			}
			lo = tighter(lo, n+1, func(a, b int64) bool { return a > b })
			found = true
		case OpGe:
			lo = tighter(lo, n, func(a, b int64) bool { return a > b })
			found = true
		case OpLt:
			if n == math.MinInt64 {
				empty = true
				found = true
				continue
			}
			hi = tighter(hi, n-1, func(a, b int64) bool { return a < b })
			found = true
		case OpLe:
			hi = tighter(hi, n, func(a, b int64) bool { return a < b })
			found = true
		default:
			// OpEq would have been a point lookup.
			residual = append(residual, c)
		}
	}
	if !found || empty {
		// With an overflowed bound the answer is the empty set. Planning it as
		// a full scan would be wrong in the other direction -- it would return
		// rows the client excluded.
		if empty {
			return &physicalPlan{
				Kind: PlanRangeScan, Schema: o.sch,
				EmptyRange: true,
				Filter:     orAll(residual),
			}
		}
		return nil
	}

	p := &physicalPlan{Kind: PlanRangeScan, Schema: o.sch, Filter: orAll(residual)}
	// Bounds are row prefixes, not bare key bytes: a range has to cover every
	// column key of every row inside it, and those all share the row prefix.
	//
	// A bound goes through exactly the rendering rowID uses. Anything else --
	// the column's value encoding, say -- produces bytes that no row id can
	// ever have, and the range comes back empty while looking well formed.
	if lo != nil {
		p.Lo = []byte(RowPrefix(o.sch.DB, o.sch.Table, keyspace.EncodeIntRowID(*lo)))
	} else {
		// Unbounded below, so the bound is the table prefix itself. Leaving Lo
		// nil instead would hand the caller an absent bound it has no contract
		// for, and the natural reading of nil -- scan from the table start --
		// happens to be right by luck rather than by statement.
		p.Lo = []byte(keyspace.TablePrefix(o.sch.DB, o.sch.Table))
	}
	if hi != nil {
		// The key bound is inclusive, but a row's range extends past its last
		// column key, so the exclusive upper bound appends 0xFF -- the same
		// successor RowIDPrefixEnd uses. Appending the successor row id instead
		// would exclude rows whose id encodes to exactly that value.
		p.Hi = []byte(RowIDPrefixEnd(o.sch.DB, o.sch.Table, keyspace.EncodeIntRowID(*hi)))
	} else {
		p.Hi = []byte(TablePrefixEnd(o.sch.DB, o.sch.Table))
	}
	return p
}

// tighter keeps whichever bound is more restrictive. A conjunct can only ever
// narrow a range, so a bound that would widen it is ignored -- `id > 1 AND id >
// 100` starts at 100, and taking the last one seen would start at 1.
func tighter(cur *int64, v int64, better func(a, b int64) bool) *int64 {
	if cur == nil || better(v, *cur) {
		out := v
		return &out
	}
	return cur
}

// orAll rebuilds a residual filter from the conjuncts the access method could
// not absorb. More than one is recombined with AND, which is what they were
// joined by; a single one is returned alone so the plan reads as the predicate
// the client wrote.
func orAll(residual []Expr) Expr {
	switch len(residual) {
	case 0:
		return nil
	case 1:
		return residual[0]
	}
	return &BoolExpr{Op: BoolAnd, Args: residual}
}

// planSelectStatement translates and plans a SELECT's filter.
//
// Translation stays pure and planning does not: this is the seam where a plan
// starts to depend on the live catalog.
func planSelectStatement(ss *pg_query.SelectStmt, sch *Schema, stats *TableStats) (*physicalPlan, error) {
	filter, err := translateExpr(ss.GetWhereClause())
	if err != nil {
		return nil, err
	}
	o := &optimizer{sch: sch, stats: stats}
	return o.plan(filter)
}

// walkPlan is the counterpart to walkExpr for the planned form. The planner
// hands back one node today, so this exists to make the shape of the future
// explicit rather than so a caller can recurse over it.
func walkPlan(p *physicalPlan, fn func(*physicalPlan) bool) bool {
	if p == nil {
		return true
	}
	return fn(p)
}
