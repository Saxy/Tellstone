/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: agg.go
Description: Aggregates, GROUP BY and HAVING (ADR-016 Stage B).

A grouped statement folds its input rows into one output row per group before
anything the client sees is decided, so the fold runs on the same materialized
result the scan, join and point lookup already produce, and the ordering,
DISTinct and bounding in order.go apply to the folded rows. Nothing is folded
inside an access method: a group key may name a column the projection does not
return, which is carried as a hidden trailing cell exactly like a hidden ORDER
BY column, and the grouping itself only ever reads cells already materialized.

The aggregates are count, sum, min, max and avg over a plain column argument,
the five whose result this engine can compute from wire cells exactly: count
counts non-NULL cells, sum and avg add them through their declared numeric
type, and min and max keep the winning cell. Each returns the wire type
PostgreSQL gives it -- count and integer sum are bigint, avg is double
precision, and min/max keep the argument's type -- so a client that declared
its result columns from RowDescription decodes them correctly.

HAVING is evaluated per group after the aggregates are computed, as one
aggregate compared against one literal, which is the only HAVING shape whose
result this engine can promise; a NULL aggregate value passes no comparison,
matching PostgreSQL's three-valued logic.
*/
package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// aggSpec is one aggregate function the statement computes. Pos is the output
// position the value is returned at, or -1 when the aggregate exists only to
// evaluate HAVING and is not projected. Arg is the argument column as written,
// empty for count(*).
type aggSpec struct {
	Pos  int
	Func string
	Arg  string
}

// groupSpec is one GROUP BY item: a column reference as written, or an ordinal
// position into the select list. Exactly one of Col and Pos is set; Pos is
// zero-based and -1 when Col carries the item.
type groupSpec struct {
	Col string
	Pos int
}

// havingSpec is a HAVING predicate comparing one aggregate against one
// literal. Idx is the position of the aggregate in Plan.Aggs (which is where a
// HAVING-only aggregate is appended), Op is the comparison, and Val/Type
// describe the literal as a wire cell of a comparable type.
type havingSpec struct {
	Idx  int
	Op   string
	Val  []byte
	Type ColumnType
}

// aggregateTarget reads one aggregate call from a target list. Only count,
// sum, min, max and avg over a plain column (or count's star) are accepted;
// anything else -- another function, an expression argument, DISTINCT or
// FILTER within the call -- is refused rather than approximated, because
// approximating an aggregate answers a different question from the one asked.
func aggregateTarget(fc *pg_query.FuncCall) (aggSpec, error) {
	fn := ""
	if len(fc.GetFuncname()) == 1 {
		fn = fc.GetFuncname()[0].GetString_().GetSval()
	}
	switch strings.ToLower(fn) {
	case "count", "sum", "min", "max", "avg":
	default:
		return aggSpec{}, fmt.Errorf("%w: function %q is not supported", errUnsupported, fn)
	}
	if fc.GetAggDistinct() || fc.GetAggFilter() != nil || fc.GetOver() != nil {
		return aggSpec{}, fmt.Errorf("%w: DISTINCT, FILTER or OVER within an aggregate is not supported", errUnsupported)
	}
	spec := aggSpec{Func: strings.ToLower(fn)}
	if spec.Func == "count" && fc.GetAggStar() {
		return spec, nil
	}
	if fc.GetAggStar() {
		return aggSpec{}, fmt.Errorf("%w: only count(*) is supported", errUnsupported)
	}
	if len(fc.GetArgs()) != 1 {
		return aggSpec{}, fmt.Errorf("%w: %s takes one argument", errUnsupported, spec.Func)
	}
	name, ok := columnRef(fc.GetArgs()[0])
	if !ok || name == "*" {
		return aggSpec{}, fmt.Errorf("%w: aggregate arguments must be plain columns", errUnsupported)
	}
	spec.Arg = name
	return spec, nil
}

// havingOf reads a HAVING clause as one aggregate compared against one
// literal. An aggregate the select list already projects is reused by identity,
// so `SELECT count(*) … HAVING count(*) > 1` computes the count once; an
// aggregate that appears only in HAVING is appended to the plan's aggregates
// with a sentinel Pos so it is computed but not projected.
func havingOf(h *pg_query.Node, plan *Plan) (havingSpec, error) {
	ae := h.GetAExpr()
	if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP || len(ae.GetName()) != 1 {
		return havingSpec{}, fmt.Errorf("%w: HAVING must compare one aggregate against a value", errUnsupported)
	}
	op := ae.GetName()[0].GetString_().GetSval()
	flip := false
	fc, konst := ae.GetLexpr().GetFuncCall(), ae.GetRexpr().GetAConst()
	if fc == nil || konst == nil {
		fc, konst = ae.GetRexpr().GetFuncCall(), ae.GetLexpr().GetAConst()
		flip = true
	}
	if fc == nil || konst == nil {
		return havingSpec{}, fmt.Errorf("%w: HAVING must compare one aggregate against a value", errUnsupported)
	}
	switch op {
	case "=", "<>", "!=":
	case "<", ">", "<=", ">=":
	default:
		return havingSpec{}, fmt.Errorf("%w: HAVING operator %q is not supported", errUnsupported, op)
	}
	if flip {
		switch op {
		case "<":
			op = ">"
		case ">":
			op = "<"
		case "<=":
			op = ">="
		case ">=":
			op = "<="
		}
	}
	spec, err := aggregateTarget(fc)
	if err != nil {
		return havingSpec{}, err
	}
	var val []byte
	var typ ColumnType
	switch {
	case konst.GetIsnull() || konst.GetIval() == nil && konst.GetSval() == nil && konst.GetFval() == nil:
		return havingSpec{}, fmt.Errorf("%w: HAVING cannot compare against NULL", errUnsupported)
	case konst.GetIval() != nil:
		val = []byte(strconv.FormatInt(int64(konst.GetIval().GetIval()), 10))
		typ = TypeBigInt
	case konst.GetFval() != nil:
		fv, ferr := strconv.ParseFloat(konst.GetFval().GetFval(), 64)
		if ferr != nil {
			return havingSpec{}, fmt.Errorf("%w: HAVING literal must be a number", errUnsupported)
		}
		val = []byte(strconv.FormatFloat(fv, 'g', -1, 64))
		typ = TypeFloat
	case konst.GetSval() != nil:
		val = []byte(konst.GetSval().GetSval())
		typ = TypeVarchar
	default:
		return havingSpec{}, fmt.Errorf("%w: HAVING literal must be a number or string", errUnsupported)
	}
	// Reuse the aggregate the select list already projects when the function
	// and its argument match; otherwise append a HAVING-only aggregate.
	for i, a := range plan.Aggs {
		if a.Func == spec.Func && a.Arg == spec.Arg {
			return havingSpec{Idx: i, Op: op, Val: val, Type: typ}, nil
		}
	}
	spec.Pos = -1
	plan.Aggs = append(plan.Aggs, spec)
	return havingSpec{Idx: len(plan.Aggs) - 1, Op: op, Val: val, Type: typ}, nil
}

// aggResultType is the wire type one aggregate returns. count is bigint, sum
// keeps the argument's type when it is a floating point and widens integers to
// bigint, avg is always double precision, and min/max keep the argument's type
// -- which is what PostgreSQL declares for each of them.
func aggResultType(fn string, arg ColumnType) (ColumnType, error) {
	switch fn {
	case "count":
		return TypeBigInt, nil
	case "avg":
		if arg != TypeInt && arg != TypeBigInt && arg != TypeFloat {
			return 0, &pgError{code: errFeatureNotSupported, msg: "argument of avg must be numeric"}
		}
		return TypeFloat, nil
	case "sum":
		switch arg {
		case TypeInt, TypeBigInt:
			return TypeBigInt, nil
		case TypeFloat:
			return TypeFloat, nil
		}
		return 0, &pgError{code: errFeatureNotSupported, msg: "argument of sum must be numeric"}
	case "min", "max":
		return arg, nil
	}
	return 0, fmt.Errorf("%w: function %q is not supported", errUnsupported, fn)
}

// aggResultOID is the OID sendRowDescription announces for one aggregate, the
// same mapping oidOf does for a column.
func aggResultOID(fn string, arg ColumnType) (int32, error) {
	t, err := aggResultType(fn, arg)
	if err != nil {
		return 0, err
	}
	return oidOf(t), nil
}

// aggState is one aggregate's running value over one group's rows. n counts
// non-NULL argument cells for count (and the rows avg divides by); sumI and
// sumF accumulate the two numeric families; min/max keep the winning cell and
// the type it compares as.
type aggState struct {
	n      int64
	sumI   int64
	sumF   float64
	min    []byte
	max    []byte
	minMax ColumnType
	hasMM  bool
}

// aggRuntime is a statement's aggregates resolved against one materialized
// row shape: argIdx is the cell each argument rides at, -1 for count(*), and
// typ is the argument's declared type, used to sum and compare.
type aggRuntime struct {
	spec   aggSpec
	argIdx int
	typ    ColumnType
	out    ColumnType
}

// groupWork is a grouped statement resolved against one materialized row
// shape: fetch maps each output position to the cell its plain column rides
// at (-1 where the position is an aggregate), aggs holds every aggregate's
// runtime, and groupIdx are the cells the grouping key reads.
type groupWork struct {
	fetch     []int
	nPlain    int
	aggs      []aggRuntime
	groupIdx  []int
	having    *havingSpec
	outTypes  []Column
	groupKeys bool
}

// resolveGroupWork binds a grouped plan's outputs, aggregates and grouping
// columns to cells of the materialized row, which is the shape the executor
// built: the visible projection in output order (skipping aggregate positions,
// which no input column fills) followed by the hidden cells its aggregates and
// grouping keys read. refCell resolves a column reference to a hidden cell
// position, appending it when the projection does not already carry it; the
// three paths supply their own because a join resolves against the merged row
// and a point lookup against the schema directly.
// resolveGroupWork binds a grouped plan's outputs, aggregates and grouping
// columns to cells of the materialized row the executor builds: the visible
// plain projection in output order (skipping aggregate positions, which no
// input column fills) followed by hidden cells appended for the columns the
// aggregates and grouping keys read. refCell resolves a column reference to
// its fetch position, appending a hidden cell when the projection does not
// already carry the column; the three paths supply their own because a join
// resolves against the merged row and a point lookup against the schema.
func resolveGroupWork(plan *Plan, refCell func(qual, col string) (int, Column, error)) (*groupWork, error) {
	gw := &groupWork{having: plan.Having, groupKeys: len(plan.GroupCols) > 0}
	// Plain output positions are fetched in output order at 0..nPlain-1.
	plain := 0
	gw.fetch = make([]int, len(plan.Cols))
	for i := range plan.Cols {
		if plan.isAggPos(i) {
			gw.fetch[i] = -1
			continue
		}
		gw.fetch[i] = plain
		plain++
	}
	gw.nPlain = plain
	resolveRef := func(ref string) (int, Column, error) {
		qual, col := splitRef(ref)
		return refCell(qual, col)
	}
	gw.outTypes = make([]Column, len(plan.Cols))
	for _, a := range plan.Aggs {
		rt := aggRuntime{spec: a, argIdx: -1, typ: 0}
		if a.Arg != "" {
			p, col, err := resolveRef(a.Arg)
			if err != nil {
				return nil, err
			}
			rt.argIdx, rt.typ = p, col.Type
		}
		out, err := aggResultType(a.Func, rt.typ)
		if err != nil {
			return nil, err
		}
		rt.out = out
		gw.aggs = append(gw.aggs, rt)
		if a.Pos >= 0 {
			gw.outTypes[a.Pos] = Column{Name: a.Func, Type: out}
		}
	}
	// Grouping keys: an ordinal names an output position and so a column the
	// projection returns; a reference resolves like an aggregate argument.
	for _, g := range plan.GroupCols {
		if g.Col == "" {
			p := gw.fetch[g.Pos]
			if p < 0 {
				return nil, &pgError{code: errFeatureNotSupported, msg: "GROUP BY position names an aggregate"}
			}
			gw.groupIdx = append(gw.groupIdx, p)
			continue
		}
		p, _, err := resolveRef(g.Col)
		if err != nil {
			return nil, err
		}
		gw.groupIdx = append(gw.groupIdx, p)
	}
	// Every projected plain column must be a grouping key when the statement
	// groups, which is the rule PostgreSQL enforces; the star is exempt here
	// because resolvePlan expands it into the schema's columns before the
	// executor runs, and the expanded names are checked here like any other.
	if len(plan.Cols) == 0 || plan.Cols[0] != "*" {
		for i, name := range plan.Cols {
			if plan.isAggPos(i) {
				continue
			}
			_, col := splitRef(name)
			found := false
			for _, p := range gw.groupIdx {
				if p == gw.fetch[i] {
					found = true
					break
				}
			}
			if !found {
				return nil, &pgError{code: errGroupColumnRequired, msg: fmt.Sprintf("column %q must appear in the GROUP BY clause or be used in an aggregate function", col)}
			}
		}
	}
	return gw, nil
}

// resolveAggOIDs fills plan.ColOIDs for every output position of a grouped
// catalog statement so RowDescription can announce each result column's type
// before any row exists. Aggregate positions take their function's result
// type; plain positions take the input column's, because the per-name lookup
// in sendRowDescription cannot type an aggregate and, on a join, cannot tell
// which side a bare name came from once the aggregates shift the positions.
func resolveAggOIDs(plan *Plan) error {
	if !plan.grouped() || plan.Schema == nil {
		return nil
	}
	oids := make([]int32, len(plan.Cols))
	argType := func(ref string) (ColumnType, error) {
		qual, col := splitRef(ref)
		if plan.Phys != nil && plan.Phys.Join != nil {
			mi, err := mergedOrderColumn(plan.Phys.Join, qual, col)
			if err != nil {
				return 0, err
			}
			return plan.Phys.Join.Merged.Columns[mi].Type, nil
		}
		si, err := tableOrderColumn(plan.Schema, plan, qual, col)
		if err != nil {
			return 0, err
		}
		return plan.Schema.Columns[si].Type, nil
	}
	plain := 0
	for i := range plan.Cols {
		if plan.isAggPos(i) {
			continue
		}
		switch {
		case plan.Phys != nil && plan.Phys.Join != nil:
			if plain < len(plan.Phys.Join.Proj) {
				oids[i] = oidOf(plan.Phys.Join.Proj[plain].Col.Type)
			} else {
				oids[i] = int32(oidText)
			}
		default:
			if idx, ok := plan.Schema.columnIndex(plan.Cols[i]); ok {
				oids[i] = oidOf(plan.Schema.Columns[idx].Type)
			} else {
				oids[i] = int32(oidText)
			}
		}
		plain++
	}
	for _, a := range plan.Aggs {
		if a.Pos < 0 {
			continue
		}
		var t ColumnType
		if a.Arg != "" {
			at, err := argType(a.Arg)
			if err != nil {
				return err
			}
			t = at
		}
		oid, err := aggResultOID(a.Func, t)
		if err != nil {
			return err
		}
		oids[a.Pos] = oid
	}
	plan.ColOIDs = oids
	return nil
}

// isAggPos reports whether output position i is filled by an aggregate.
func (p *Plan) isAggPos(i int) bool {
	for _, a := range p.Aggs {
		if a.Pos == i {
			return true
		}
	}
	return false
}

// apply folds materialized rows into the grouped result and replaces the
// outcome's rows with it. rows are the executor's materialized rows: the
// visible plain projection followed by the hidden cells the aggregates and
// grouping keys read. Each output row is built from the group's first input
// row for plain columns and the aggregate's computed value for aggregates; a
// group with no rows (an ungrouped aggregate over an empty input) yields one
// row of counts and NULLs.
func (gw *groupWork) apply(plan *Plan, out *execOutcome, rows [][][]byte) error {
	type group struct {
		cells [][]byte
		st    []aggState
	}
	var groups []*group
	index := map[string]int{}
	lookup := func(key string) *group {
		if i, ok := index[key]; ok {
			return groups[i]
		}
		g := &group{st: make([]aggState, len(gw.aggs))}
		for i := range g.st {
			g.st[i].minMax = gw.aggs[i].typ
		}
		groups = append(groups, g)
		index[key] = len(groups) - 1
		return g
	}
	for _, r := range rows {
		g := lookup(groupKeyOf(r, gw.groupIdx))
		if g.cells == nil {
			g.cells = make([][]byte, gw.nPlain)
			for i := 0; i < gw.nPlain; i++ {
				g.cells[i] = r[i]
			}
		}
		for i, a := range gw.aggs {
			var cell []byte
			if a.argIdx >= 0 {
				cell = r[a.argIdx]
			}
			if err := g.st[i].add(a, cell); err != nil {
				return err
			}
		}
	}
	if len(groups) == 0 && !gw.groupKeys {
		// An ungrouped aggregate over an empty input is one row: count is 0
		// and every other aggregate is NULL, which is what PostgreSQL returns.
		g := &group{cells: make([][]byte, gw.nPlain), st: make([]aggState, len(gw.aggs))}
		for i := range g.st {
			g.st[i].minMax = gw.aggs[i].typ
		}
		groups = append(groups, g)
	}
	res := make([][][]byte, 0, len(groups))
	cols := gw.outTypes
	for _, g := range groups {
		cells := make([][]byte, len(gw.aggs))
		for i, a := range gw.aggs {
			cell, err := g.st[i].value(a)
			if err != nil {
				return err
			}
			cells[i] = cell
		}
		if h := gw.having; h != nil && !havingPass(cells[h.Idx], gw.aggs[h.Idx].out, h) {
			continue
		}
		row := make([][]byte, len(plan.Cols))
		for i := range plan.Cols {
			if p := gw.fetch[i]; p >= 0 {
				row[i] = g.cells[p]
			}
		}
		for i, a := range gw.aggs {
			if a.spec.Pos >= 0 {
				row[a.spec.Pos] = cells[i]
			}
		}
		res = append(res, row)
	}
	out.rows = res
	out.tag = fmt.Sprintf("SELECT %d", len(res))
	keys, err := outputSortKeys(plan, cols)
	if err != nil {
		return err
	}
	return finalizeResult(plan, out, cols, keys, len(plan.Cols))
}

// havingPass reports whether one aggregate's value satisfies the HAVING
// comparison. A NULL value passes nothing, which is PostgreSQL's three-valued
// logic applied to the group.
func havingPass(cell []byte, typ ColumnType, h *havingSpec) bool {
	if cell == nil {
		return false
	}
	c := compareCell(typ, cell, h.Val)
	switch h.Op {
	case "=":
		return c == 0
	case "<>", "!=":
		return c != 0
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	case ">=":
		return c >= 0
	}
	return false
}

// add folds one input row's argument cell into the aggregate's running state.
func (st *aggState) add(a aggRuntime, cell []byte) error {
	if cell == nil && a.argIdx >= 0 {
		// NULL arguments are not counted by count(column) and not added by
		// sum/avg; min and max ignore them too.
		return nil
	}
	switch a.spec.Func {
	case "count":
		st.n++
		return nil
	case "sum", "avg":
		if a.typ == TypeFloat {
			f, err := strconv.ParseFloat(string(cell), 64)
			if err != nil {
				return &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", a.spec.Arg, err)}
			}
			st.sumF += f
			st.n++
			return nil
		}
		v, err := strconv.ParseInt(string(cell), 10, 64)
		if err != nil {
			return &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", a.spec.Arg, err)}
		}
		if a.spec.Func == "avg" {
			st.sumF += float64(v)
			st.n++
			return nil
		}
		if (v > 0 && st.sumI > math.MaxInt64-v) || (v < 0 && st.sumI < math.MinInt64-v) {
			return &pgError{code: errFeatureNotSupported, msg: "sum overflows bigint"}
		}
		st.sumI += v
		st.n++
		return nil
	case "min", "max":
		if st.hasMM {
			c := compareCell(st.minMax, cell, st.min)
			if a.spec.Func == "min" && c >= 0 {
				return nil
			}
			if a.spec.Func == "max" && c <= 0 {
				return nil
			}
		}
		st.min, st.max = cell, cell
		st.hasMM = true
		return nil
	}
	return fmt.Errorf("%w: function %q is not supported", errUnsupported, a.spec.Func)
}

// value renders the aggregate's final wire cell. An aggregate with nothing to
// aggregate (count over no rows) is 0; every other empty aggregate is NULL.
func (st *aggState) value(a aggRuntime) ([]byte, error) {
	switch a.spec.Func {
	case "count":
		return []byte(strconv.FormatInt(st.n, 10)), nil
	case "sum":
		if st.n == 0 {
			return nil, nil
		}
		if a.out == TypeFloat {
			return []byte(strconv.FormatFloat(st.sumF, 'g', -1, 64)), nil
		}
		return []byte(strconv.FormatInt(st.sumI, 10)), nil
	case "avg":
		if st.n == 0 {
			return nil, nil
		}
		return []byte(strconv.FormatFloat(st.sumF/float64(st.n), 'g', -1, 64)), nil
	case "min":
		if !st.hasMM {
			return nil, nil
		}
		return st.min, nil
	case "max":
		if !st.hasMM {
			return nil, nil
		}
		return st.max, nil
	}
	return nil, fmt.Errorf("%w: function %q is not supported", errUnsupported, a.spec.Func)
}

// groupKeyOf encodes one input row's grouping cells as a map key, length
// prefixed so two rows differing only in a cell boundary cannot collide, and
// NULL-marked so a NULL key groups with NULLs and not with empty strings.
func groupKeyOf(row [][]byte, keys []int) string {
	var b strings.Builder
	for _, k := range keys {
		c := row[k]
		if c == nil {
			b.WriteString("\x00")
			continue
		}
		b.WriteString(strconv.Itoa(len(c)))
		b.WriteString(":")
		b.Write(c)
	}
	return b.String()
}

// outputSortKeys resolves ORDER BY against a grouped result's output columns
// only: grouping has already erased every input column, so a name the select
// list does not return is the error PostgreSQL reports rather than a hidden
// cell the sort could read. A qualified reference answers to its bare name,
// because the output columns carry no table qualifiers.
func outputSortKeys(plan *Plan, cols []Column) ([]sortKey, error) {
	names := plan.OutNames
	if names == nil {
		names = plan.Cols
	}
	var keys []sortKey
	for _, k := range plan.OrderBy {
		key := sortKey{idx: -1, desc: k.Desc, nullsFirst: k.NullsFirst}
		if k.Out >= 0 {
			key.idx, key.typ = k.Out, cols[k.Out].Type
		} else {
			_, col := splitRef(k.Src)
			pos := indexOfName(names, col)
			if pos < 0 {
				pos = indexOfName(plan.Cols, col)
			}
			if pos < 0 {
				return nil, &pgError{code: errGroupColumnRequired, msg: fmt.Sprintf("column %q must appear in the GROUP BY clause or be used in an aggregate function", col)}
			}
			key.idx, key.typ = pos, cols[pos].Type
		}
		keys = append(keys, key)
	}
	return keys, nil
}
