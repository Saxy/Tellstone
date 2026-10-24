/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: join.go
Description: The Phase 11 hash join: the planner that resolves a translated join
against both tables' schemas, and the executor built on the scan engine and the
arena. The join is the last Phase 11 operator (MULTI-CLUSTER-PLAN.md deliverable
3), and it is deliberately the smallest supported shape: one equality on two
plain tables -- INNER, LEFT, RIGHT or FULL -- both sides full scans, the WHERE
clause as a post-join filter, and a projection across the merged row. Every
other shape is refused at translation (translateJoinSelect), so the planner
below cannot be asked for one.
*/
package sql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math"
	"strings"
)

// planJoin produces the physical form of a translated join statement.
//
// Translation settled the join's shape (two plain tables, one equality of a
// kind the hash join preserves correctly);
// what it could not settle is which side a name belongs to. A name's side is a
// property of the schemas, and translation is a pure function over a parse tree
// with no catalog. This is the join's resolvePlan time: it reads the right
// table's catalog entry, resolves the ON operands, the WHERE clause and the
// projection against both sides' column sets, and builds the merged row the
// executor reads them out of.
//
// It is idempotent like resolvePlan itself, because Describe and Execute both
// plan the same statement.
func (s *Server) planJoin(plan *Plan) error {
	if plan.Phys != nil && plan.Phys.Kind == PlanHashJoin {
		return nil
	}
	js := plan.Join
	// The left side is either one table or a join of its own: a statement
	// that joins three tables is a chain of two joins, and the first of them
	// is planned here, before the join that reads its result.
	var left *Schema
	var buildPlan *Plan
	if js.Inner != nil {
		if err := s.resolvePlan(js.Inner); err != nil {
			return err
		}
		inner := js.Inner.Phys
		if inner == nil || inner.Kind != PlanHashJoin {
			return &pgError{code: errInternal, msg: "the left side of the join has no plan"}
		}
		left = inner.Join.Merged
		buildPlan = js.Inner
	} else {
		left = plan.Schema
	}
	right, err := s.resolveRelation(js.RightSchema, js.Right)
	if err != nil {
		return err
	}

	// The merged row's namespace is "qualifier.column": the two schemas may
	// both declare a "name", and a bare "name" is then ambiguous. A merged row
	// whose cells all answer to distinct names is what lets the WHERE clause
	// be resolved once, here, and evaluated against bytes later.
	nb := len(left.Columns)
	mergedCols := make([]Column, 0, nb+len(right.Columns))
	if js.Inner != nil {
		// The left side is a join of its own: its columns already carry the
		// qualifier of the table they came from, and they keep it.
		mergedCols = append(mergedCols, left.Columns...)
	} else {
		for _, c := range left.Columns {
			mergedCols = append(mergedCols, Column{Name: js.LeftAlias + "." + c.Name, Type: c.Type, Nullable: c.Nullable, Default: c.Default})
		}
	}
	for _, c := range right.Columns {
		mergedCols = append(mergedCols, Column{Name: js.RightAlias + "." + c.Name, Type: c.Type, Nullable: c.Nullable, Default: c.Default})
	}
	merged := &Schema{
		DB:         DefaultDB,
		Table:      mergedName(js),
		Columns:    mergedCols,
		PrimaryKey: 0,
	}

	// leftLookup finds a column of the left side by name. The left side is
	// either one table, whose columns are named plainly, or a join, whose
	// columns carry the qualifier of the table they came from ("a.x"): a bare
	// name then names the joined table that has the column, and names it
	// ambiguously when more than one of them has it.
	leftLookup := func(name string) (int, bool) {
		if idx, ok := left.columnIndex(name); ok {
			return idx, true
		}
		found := -1
		for i, c := range left.Columns {
			if !strings.HasSuffix(c.Name, "."+name) {
				continue
			}
			if found >= 0 {
				return 0, false
			}
			found = i
		}
		if found < 0 {
			return 0, false
		}
		return found, true
	}

	// mergedNameOf names a resolved column the way the merged row names it:
	// the left side's columns carry the qualifier of the table they came
	// from, and the right side's carry the right table's. The left side is
	// itself a join when the statement joins three tables, and then its
	// columns already carry their qualifier.
	mergedNameOf := func(op *joinOperand) string {
		if op.side == 0 {
			if js.Inner != nil {
				return op.col.Name
			}
			return js.LeftAlias + "." + op.col.Name
		}
		return js.RightAlias + "." + op.col.Name
	}

	// resolve names one reference: an ON operand, a WHERE column or a
	// projection. The reference shapes are the translator's: qualified
	// ("u.name") or bare ("name"). A qualified reference finds its side by the
	// statement's alias, which is the only qualifier PostgreSQL accepts once a
	// table is aliased -- the underlying table name is hidden, so a qualifier
	// naming it reports the missing FROM-clause entry rather than resolving to
	// the side an alias shadows. A bare one holds iff exactly one side has the
	// column, and is the one reference PostgreSQL reports as ambiguous.
	resolve := func(name string) (*joinOperand, error) {
		if i := strings.IndexByte(name, '.'); i > 0 {
			q, rest := name[:i], name[i+1:]
			// A joined left side keeps the name its own merged row gave the
			// column, so the whole name may name one of its columns.
			if idx, ok := left.columnIndex(name); ok {
				return &joinOperand{side: 0, idx: idx, col: left.Columns[idx]}, nil
			}
			switch {
			case strings.EqualFold(q, js.LeftAlias):
				idx, ok := left.columnIndex(rest)
				if !ok {
					return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", rest)}
				}
				return &joinOperand{side: 0, idx: idx, col: left.Columns[idx]}, nil
			case strings.EqualFold(q, js.RightAlias):
				idx, ok := right.columnIndex(rest)
				if !ok {
					return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", rest)}
				}
				return &joinOperand{side: 1, idx: idx, col: right.Columns[idx]}, nil
			default:
				return nil, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("missing FROM-clause entry for table %q", q)}
			}
		}
		li, lok := leftLookup(name)
		ri, rok := right.columnIndex(name)
		switch {
		case lok && rok:
			return nil, &pgError{code: errAmbiguousColumn, msg: fmt.Sprintf("column reference %q is ambiguous", name)}
		case lok:
			return &joinOperand{side: 0, idx: li, col: left.Columns[li]}, nil
		case rok:
			return &joinOperand{side: 1, idx: ri, col: right.Columns[ri]}, nil
		default:
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
	}

	lo, err := resolve(js.LKey)
	if err != nil {
		return err
	}
	ro, err := resolve(js.RKey)
	if err != nil {
		return err
	}
	if lo.side == ro.side {
		return &pgError{code: errFeatureNotSupported, msg: "join equality must name one column from each table"}
	}
	if lo.col.Type != ro.col.Type {
		return &pgError{code: errFeatureNotSupported, msg: fmt.Sprintf("join keys have types %s and %s", lo.col.Type, ro.col.Type)}
	}
	// The build side is fixed at the statement's left table (JoinPlan.Build),
	// so which ON operand supplies each key depends on which side it resolved
	// to. The equality reads both operands, so the join itself is symmetric;
	// only the executor's roles come from this assignment.
	var buildKey, probeKey int
	if lo.side == 0 {
		buildKey, probeKey = lo.idx, ro.idx
	} else {
		buildKey, probeKey = ro.idx, lo.idx
	}
	lRef, rRef := lo, ro
	if ro.side == 0 {
		lRef, rRef = ro, lo
	}
	cond := fmt.Sprintf("%s = %s", mergedNameOf(lRef), mergedNameOf(rRef))

	filter, err := plan.predicate()
	if err != nil {
		return err
	}
	filter, err = mapExprCols(filter, func(col string) (string, error) {
		op, err := resolve(col)
		if err != nil {
			return "", err
		}
		return mergedNameOf(op), nil
	})
	if err != nil {
		return err
	}
	// The ON clause's remainder rides along as the join's match filter: the
	// conditions that decide whether a matched pair is joined at all. They
	// are resolved against the merged row like the WHERE clause is, and they
	// are tested against matched pairs only.
	matchFilter, err := s.joinMatchFilter(js, resolve, mergedNameOf)
	if err != nil {
		return err
	}

	// The CASE targets are computed from the merged row, so their conditions
	// are resolved against it: the merged row names its columns the way the
	// statement named them, and a condition that names one of the joined
	// tables' columns is decided here.
	caseAt := map[int]int{}
	for k := range plan.Cases {
		ct := &plan.Cases[k]
		if err := plan.translateCaseConditions(ct, func(col string) (string, error) {
			op, err := resolve(col)
			if err != nil {
				return "", err
			}
			return mergedNameOf(op), nil
		}); err != nil {
			return err
		}
		caseAt[ct.Pos] = k
	}

	proj := make([]joinProj, 0, len(plan.Cols))
	disp := make([]string, 0, len(plan.Cols))
	for i, c := range plan.Cols {
		if k, isCase := caseAt[i]; isCase {
			// The CASE fills its own position: it is computed from the
			// merged row rather than read from it.
			ct := &plan.Cases[k]
			proj = append(proj, joinProj{Idx: -1, Col: Column{Name: ct.Name, Type: ct.Type}, Case: k})
			disp = append(disp, ct.Name)
			continue
		}
		if plan.isAggPos(i) {
			// An aggregate is computed by the fold, not projected from either
			// input side: its display name is the function name the target
			// list used, and no joinProj cell fills its position. Keeping the
			// name in disp preserves the one-to-one alignment between plan.Cols
			// and the output positions the executor and RowDescription walk.
			disp = append(disp, c)
			continue
		}
		switch {
		case c == "*":
			// The whole merged row, left side first, in declaration order. The
			// two sides may repeat a name, which is what PostgreSQL does, and
			// the wire keeps the two cells distinguishable by position.
			for i, col := range left.Columns {
				proj = append(proj, joinProj{Idx: i, Col: col, Case: -1})
				disp = append(disp, col.Name)
			}
			for i, col := range right.Columns {
				proj = append(proj, joinProj{Idx: nb + i, Col: col, Case: -1})
				disp = append(disp, col.Name)
			}
		case strings.HasSuffix(c, ".*"):
			q := c[:len(c)-2]
			switch {
			case strings.EqualFold(q, js.LeftAlias):
				for i, col := range left.Columns {
					proj = append(proj, joinProj{Idx: i, Col: col, Case: -1})
					disp = append(disp, col.Name)
				}
			case strings.EqualFold(q, js.RightAlias):
				for i, col := range right.Columns {
					proj = append(proj, joinProj{Idx: nb + i, Col: col, Case: -1})
					disp = append(disp, col.Name)
				}
			default:
				return &pgError{code: errUndefinedTable, msg: fmt.Sprintf("missing FROM-clause entry for table %q", q)}
			}
		default:
			op, err := resolve(c)
			if err != nil {
				return err
			}
			idx := op.idx
			if op.side == 1 {
				idx = nb + op.idx
			}
			proj = append(proj, joinProj{Idx: idx, Col: op.col, Case: -1})
			disp = append(disp, op.col.Name)
		}
	}
	// Both sides are full scans. Nothing narrower is possible: the join
	// equality is between two columns rather than against a constant, so no
	// bound can be derived from the statement before the join runs. Planning
	// them with the single-table optimizer keeps the join's estimate the sum
	// of the same model its children use.
	build, err := (&optimizer{sch: left, stats: s.statsFor(left)}).plan(nil)
	if err != nil {
		return err
	}
	if buildPlan != nil {
		// The build side is a join of its own: it is run by the executor
		// rather than scanned, and the estimate it carries is the estimate
		// of the plan that produced it.
		build.Rows = buildPlan.Phys.Rows
		build.Cost = buildPlan.Phys.Cost
	}
	probe, err := (&optimizer{sch: right, stats: s.statsFor(right)}).plan(nil)
	if err != nil {
		return err
	}
	outW := 0.0
	for _, p := range proj {
		outW += float64(len(p.Col.Name)) + 1 + valueWidthGuess(p.Col.Type)
	}
	// An inner join emits matched pairs, of which the estimate is the smaller
	// side's rows at most. An outer join emits at least every row of the side
	// it preserves -- plus their matches -- so its estimate is that side's
	// count instead, and a full join's is both counts together. The figures
	// are crude in the same direction the rest of the model is crude: an
	// estimate that can only over-count is better than one that silently drops
	// a preserved row.
	rows := clampRows(math.Min(build.Rows, probe.Rows))
	switch js.Preserve {
	case PreserveLeft:
		rows = clampRows(build.Rows)
	case PreserveRight:
		rows = clampRows(probe.Rows)
	case PreserveBoth:
		rows = clampRows(build.Rows + probe.Rows)
	}
	meas := build.Measured && probe.Measured
	rowCost := rows * outW
	phys := &physicalPlan{
		Kind: PlanHashJoin,
		Rows: rows,
		Cost: Cost{
			Startup: build.Cost.Total,
			Total:   build.Cost.Total + probe.Cost.Total + rowCost,
			Rows:    rows,
			Bytes:   build.Cost.Bytes + probe.Cost.Bytes + rowCost,
		},
		Measured: meas,
		Join: &JoinPlan{
			Build:       build,
			Probe:       probe,
			BuildPlan:   buildPlan,
			BuildKey:    buildKey,
			ProbeKey:    probeKey,
			BuildName:   js.LeftAlias,
			ProbeName:   js.RightAlias,
			CondText:    cond,
			Merged:      merged,
			Proj:        proj,
			OutWidth:    outW,
			Outer:       js.Preserve,
			MatchFilter: matchFilter,
		},
	}
	// The WHERE clause, qualified against the merged row, rides the node's own
	// Filter exactly like every other access method's residual predicate.
	phys.Filter = filter
	if !meas {
		phys.CostNote = "rows estimated, table never analyzed"
	}
	// The display names land on the plan only now, when both child optimizers
	// have succeeded and the physical plan is complete. planJoin re-runs after
	// a failure (its idempotence test is plan.Phys), and a plan.Cols already
	// rewritten to display names would be re-resolved as if the statement had
	// written them -- losing the "*" expansion it was meant to produce.
	plan.Cols = disp
	plan.Phys = phys
	return nil
}

// mergedName is the name a join's merged row is given: the tables it joins,
// with the inner join's own name standing for the tables it joins.
func mergedName(js *joinSpec) string {
	left := js.Left
	if js.Inner != nil {
		left = js.Inner.Phys.Join.Merged.Table
	}
	if left == "" {
		return js.Right
	}
	return left + " join " + js.Right
}

// joinMatchFilter translates the ON clause's remainder into the expression
// the executor tests against a matched pair.
//
// The conditions are resolved against the merged row the same way the WHERE
// clause is: a qualified name names its side through the statement's alias, a
// bare one is the side that has the column. The result is nil when the ON
// clause was nothing but the equality, which is the common case.
func (s *Server) joinMatchFilter(js *joinSpec, resolve func(string) (*joinOperand, error), nameOf func(*joinOperand) string) (Expr, error) {
	if js.OnRest == nil {
		return nil, nil
	}
	e, err := translateExpr(js.OnRest)
	if err != nil {
		return nil, errOrSyntax(err)
	}
	return mapExprCols(e, func(col string) (string, error) {
		op, err := resolve(col)
		if err != nil {
			return "", err
		}
		return nameOf(op), nil
	})
}

// execNestedJoin runs a join whose result is another join's input, and hands
// each row of the result to the callback.
//
// The rows are the join's projection in the storage encoding of the joined
// schema, not the wire encoding: the join that reads them hashes their bytes
// and compares them, which is a question about the stored values rather than
// about their rendered form.
func (s *Server) execNestedJoin(st Store, plan *Plan, params []paramVal, cb func(cells []rowValue) error) error {
	phys := plan.Phys
	if phys == nil || phys.Kind != PlanHashJoin {
		return &pgError{code: errInternal, msg: "the left side of the join has no plan"}
	}
	jp := phys.Join
	filt, err := s.compileFilter(phys.Filter, jp.Merged, params)
	if err != nil {
		return err
	}
	outCols := make([]Column, len(jp.Proj))
	for i, p := range jp.Proj {
		outCols[i] = p.Col
	}
	// The rows are read out of the join's own chunk, so the callback sees
	// them in the order the join produced them.
	cells := make([]rowValue, len(jp.Proj))
	return s.runHashJoin(st, plan, jp, filt, jp.Proj, outCols, &execOutcome{selectRows: true}, params, nil, func(c *Chunk) error {
		return forEachChunkRow(c, cells, func(_ uint32, row []rowValue) error {
			return cb(row)
		})
	})
}

// joinOperand is one resolved column reference: which side of the join it names
// and where that side keeps it.
type joinOperand struct {
	side int // 0 is the build (left) side, 1 the probe (right) side
	idx  int
	col  Column
}

// execJoin runs a hash join end to end.
//
// The build side (the statement's left table; see JoinPlan.Build) is scanned
// first, and every row's full cell set is copied into the arena region, chained
// under its join key's hash. The probe side then streams through its own scan:
// each row's key finds every matching build row through the bucket chain, the
// merged row is checked against the WHERE filter, and survivors fill the output
// chunk. The store offers no resume point on either side, so materializing the
// build side once and streaming the probe once is the whole cost.
func (s *Server) execJoin(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.grouped() {
		return s.execJoinGrouped(st, plan, params)
	}
	jp := plan.Phys.Join

	filt, err := s.compileFilter(plan.Phys.Filter, jp.Merged, params)
	if err != nil {
		return nil, err
	}
	if filt.alwaysFalse() {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	out := &execOutcome{selectRows: true}
	outCols := make([]Column, len(jp.Proj))
	for i, p := range jp.Proj {
		outCols[i] = p.Col
	}
	// The CASE targets are computed from the merged row, so they are compiled
	// against it.
	ccases, _, err := s.compileCases(plan, jp.Merged, params)
	if err != nil {
		return nil, err
	}
	// ORDER BY is resolved against the merged row while both schemas are at
	// hand: a key the projection returns indexes outCols as built above, and a
	// key it does not becomes a hidden trailing column, carried by the same
	// projection the visible cells are and dropped before the client sees them.
	keys, hidden, err := joinSortKeys(plan, jp, outCols)
	if err != nil {
		return nil, err
	}
	projAll := jp.Proj
	if len(hidden) > 0 {
		projAll = make([]joinProj, 0, len(jp.Proj)+len(hidden))
		projAll = append(projAll, jp.Proj...)
		projAll = append(projAll, hidden...)
		for _, h := range hidden {
			outCols = append(outCols, h.Col)
		}
	}
	vis := len(jp.Proj)

	if err := s.runHashJoin(st, plan, jp, filt, projAll, outCols, out, params, ccases, func(c *Chunk) error {
		return drainChunk(c, outCols, out)
	}); err != nil {
		return nil, err
	}
	if err := finalizeResult(plan, out, outCols, keys, vis); err != nil {
		return nil, err
	}
	return out, nil
}

// execJoinGrouped folds a join's matched rows into one output row per group.
// The visible join projection is joined by the columns the aggregates and
// grouping keys read, appended as hidden trailing cells while both schemas are
// at hand; the probe then materializes those rows and the fold turns them into
// the grouped result, exactly as the scan path does.
func (s *Server) execJoinGrouped(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	jp := plan.Phys.Join
	filt, err := s.compileFilter(plan.Phys.Filter, jp.Merged, params)
	if err != nil {
		return nil, err
	}
	out := &execOutcome{selectRows: true}
	outCols := make([]Column, 0, len(jp.Proj))
	projAll := make([]joinProj, 0, len(jp.Proj))
	for _, p := range jp.Proj {
		projAll = append(projAll, p)
		outCols = append(outCols, p.Col)
	}
	refCell := func(qual, col string) (int, Column, error) {
		mi, err := mergedOrderColumn(jp, qual, col)
		if err != nil {
			return 0, Column{}, err
		}
		for i, p := range projAll {
			if p.Idx == mi {
				return i, p.Col, nil
			}
		}
		projAll = append(projAll, joinProj{Idx: mi, Col: jp.Merged.Columns[mi], Case: -1})
		outCols = append(outCols, jp.Merged.Columns[mi])
		return len(projAll) - 1, jp.Merged.Columns[mi], nil
	}
	gw, err := resolveGroupWork(plan, refCell)
	if err != nil {
		return nil, err
	}
	if !filt.alwaysFalse() {
		if err := s.runHashJoin(st, plan, jp, filt, projAll, outCols, out, params, nil, func(c *Chunk) error {
			return drainChunk(c, outCols, out)
		}); err != nil {
			return nil, err
		}
	}
	if err := gw.apply(plan, out, out.rows); err != nil {
		return nil, err
	}
	return out, nil
}

// runHashJoin probes the right table against the built left table, draining
// every row the join owes the output -- matched pairs, and for an outer join
// the unmatched rows of the side it preserves -- as projected cells (projAll,
// declared by outCols) into the outcome's result set. Both the plain join and
// the grouped fold share it; the fold differs only in what the projection
// carries and what happens to the rows afterward.
func (s *Server) runHashJoin(st Store, plan *Plan, jp *JoinPlan, filt *compiledNode, projAll []joinProj, outCols []Column, out *execOutcome, params []paramVal, ccases []*compiledCase, drain func(*Chunk) error) error {
	ctx := &ExecCtx{arena: newArena(int(jp.Build.Rows * rowBytes(jp.Build.Schema)))}
	defer ctx.arena.reset()
	hj := newHashJoin(jp)

	// The ON clause's remainder, when the clause had one, is tested against a
	// matched pair before the pair is emitted -- and before the pair counts
	// as a match, because a pair that fails the ON clause is a pair that was
	// never joined: the outer join still owes its left row.
	matchFilt, err := s.compileFilter(jp.MatchFilter, jp.Merged, params)
	if err != nil {
		return err
	}

	// An outer join keeps the rows its equality found nothing for: LEFT keeps
	// the build side's, RIGHT the probe side's, FULL both. The match that
	// counts is the ON equality alone. The WHERE clause is applied to every
	// row the join emits, matched or not, so `LEFT JOIN ... WHERE r.x IS NULL`
	// reads as "the left rows nothing matched" -- which is what PostgreSQL
	// makes it mean, and what a filter applied only to matched pairs would
	// get exactly backwards.
	preserveLeft := jp.Outer == PreserveLeft || jp.Outer == PreserveBoth
	preserveRight := jp.Outer == PreserveRight || jp.Outer == PreserveBoth

	// Build: the left table, every row into the hash table. The scan's cells
	// alias its reused chunk, and insertRow copies each row into the region
	// before the chunk is reused for the next batch. A NULL join key can never
	// match: it is skipped outright for an inner join and kept off to the side
	// for one that preserves the build side, where it still owes a row.
	var nullBuild []uint32
	buildCells := make([]rowValue, len(jp.Build.Schema.Columns))
	// The build side is either a table, which is scanned, or a join of its
	// own, which is run: its rows are the build side's rows, in the same
	// storage encoding a scan would have delivered.
	buildRows := func(cells []rowValue) error {
		if !cells[hj.bKey].set {
			if preserveLeft {
				off, _ := hj.copyRow(ctx.arena, cells)
				nullBuild = append(nullBuild, uint32(off))
			}
			return nil
		}
		hj.insertRow(ctx.arena, cells)
		return nil
	}
	if jp.BuildPlan != nil {
		if err := s.execNestedJoin(st, jp.BuildPlan, params, buildRows); err != nil {
			return err
		}
	} else if err := newScan(st, jp.Build, jp.Build.Schema, identityProj(jp.Build.Schema), nil, nil).run(func(c *Chunk) error {
		return forEachChunkRow(c, buildCells, func(_ uint32, cells []rowValue) error {
			return buildRows(cells)
		})
	}); err != nil {
		return &pgError{code: errDataCorrupt, msg: err.Error()}
	}

	// The region is fully built, so the probe holds this slice through its own
	// run; growth is over, and the slice is stable. The probe's cells alias its
	// scan chunk batch by batch, so the merged row only borrows them for the
	// duration of one match, which is exactly how long the filter and the
	// projection below need them.
	region := ctx.arena.region
	nb := len(jp.Build.Schema.Columns)
	merged := make([]rowValue, nb+len(jp.Probe.Schema.Columns))
	probeCells := make([]rowValue, len(jp.Probe.Schema.Columns))
	jc := newChunk(len(projAll))

	// emitMerged drains one joined row over the merged row as it stands: the
	// WHERE filter, the projection, then a batch flush. Matched pairs,
	// unmatched probe rows and unmatched build rows all go through it, which
	// is what makes the filter and the projection identical for all of them --
	// the only difference between an outer join's rows and an inner join's is
	// which half of the merged row was filled in.
	emitMerged := func() error {
		if filt != nil && !filt.eval(merged) {
			return nil
		}
		r := jc.Rows
		for j, pr := range projAll {
			if pr.Case >= 0 {
				// A CASE is computed from the merged row: the first branch
				// whose condition holds gives the value, and a CASE that
				// matches no branch is NULL.
				v := ccases[pr.Case].eval(merged)
				if v == nil {
					jc.setNull(r, uint32(j))
					continue
				}
				jc.appendVal(v)
				continue
			}
			if !merged[pr.Idx].set {
				jc.setNull(r, uint32(j))
				continue
			}
			jc.appendVal(merged[pr.Idx].value)
		}
		jc.Rows++
		if jc.Rows >= BatchRows {
			if err := drain(jc); err != nil {
				return err
			}
			jc.reset(uint16(len(projAll)))
		}
		return nil
	}
	// The two halves are reset to NULL independently: whichever side the next
	// emission does not overwrite is the side that row supplies nothing for,
	// and a cell left over from the previous emission would answer the filter
	// and the projection as a value the row never had.
	clearBuild := func() {
		for i := range merged[:nb] {
			merged[i] = rowValue{}
		}
	}
	clearProbe := func() {
		for i := nb; i < len(merged); i++ {
			merged[i] = rowValue{}
		}
	}

	err = newScan(st, jp.Probe, jp.Probe.Schema, identityProj(jp.Probe.Schema), nil, nil).run(func(c *Chunk) error {
		return forEachChunkRow(c, probeCells, func(_ uint32, pcells []rowValue) error {
			key := pcells[hj.pKey]
			if !key.set {
				// A NULL join key matches nothing, on either side. RIGHT and
				// FULL still owe a row for it, with the other side NULLed.
				if preserveRight {
					clearBuild()
					copy(merged[nb:], pcells)
					return emitMerged()
				}
				return nil
			}
			h := maphash.Bytes(hj.seed, key.value)
			b := h & uint64(len(hj.buckets)-1)
			matched := false
			for n := hj.buckets[b]; n != -1; n = hj.nodes[n].next {
				node := &hj.nodes[n]
				if !bytes.Equal(region[node.koff:node.koff+node.klen], key.value) {
					continue
				}
				decodeRegionRow(region, node.off, merged[:nb])
				copy(merged[nb:], pcells)
				if matchFilt != nil && !matchFilt.eval(merged) {
					// The pair fails the ON clause's remaining conditions, so
					// the two rows are not joined after all. The build row
					// stays unmatched, and the outer join owes it a row with
					// the other side NULL.
					continue
				}
				matched = true
				node.matched = true
				if err := emitMerged(); err != nil {
					return err
				}
			}
			// The probe row no build row claimed: RIGHT and FULL keep it.
			if !matched && preserveRight {
				clearBuild()
				copy(merged[nb:], pcells)
				return emitMerged()
			}
			return nil
		})
	})
	if err != nil {
		return &pgError{code: errDataCorrupt, msg: err.Error()}
	}

	// The probe has run, so every node it claimed carries matched=true and the
	// build side's unpaid rows are exactly the rest: the unclaimed hash rows
	// and the NULL-key rows set aside during the build. LEFT and FULL owe each
	// of them a row with the probe half NULLed.
	if preserveLeft {
		for i := range hj.nodes {
			if hj.nodes[i].matched {
				continue
			}
			decodeRegionRow(region, hj.nodes[i].off, merged[:nb])
			clearProbe()
			if err := emitMerged(); err != nil {
				return &pgError{code: errDataCorrupt, msg: err.Error()}
			}
		}
		for _, off := range nullBuild {
			decodeRegionRow(region, off, merged[:nb])
			clearProbe()
			if err := emitMerged(); err != nil {
				return &pgError{code: errDataCorrupt, msg: err.Error()}
			}
		}
	}
	if jc.Rows > 0 {
		if err := drain(jc); err != nil {
			return &pgError{code: errDataCorrupt, msg: err.Error()}
		}
	}
	return nil
}

// identityProj is the ordinal list of every column, which is what a join's
// child scans must project: a row's whole cell set may contribute cells to the
// filter or the output, so neither side may drop a column.
func identityProj(sch *Schema) []int {
	proj := make([]int, len(sch.Columns))
	for i := range proj {
		proj[i] = i
	}
	return proj
}

// newHashJoin sizes the bucket array from the build side's row estimate, bound
// between 64 and a million entries and rounded up to a power of two.
//
// The buckets are deliberately never rehashed: a chain degrades to linear
// rather than to a wrong answer, and rehashing mid-scan would move rows the
// probe is walking.
func newHashJoin(jp *JoinPlan) *hashJoin {
	est := int(jp.Build.Rows)
	const minBuckets, maxBuckets = 64, 1 << 20
	if est < minBuckets {
		est = minBuckets
	}
	if est > maxBuckets {
		est = maxBuckets
	}
	size := 1
	for size < est {
		size <<= 1
	}
	buckets := make([]int32, size)
	for i := range buckets {
		buckets[i] = -1
	}
	return &hashJoin{
		seed:    maphash.MakeSeed(),
		bKey:    jp.BuildKey,
		pKey:    jp.ProbeKey,
		buckets: buckets,
		nodes:   make([]hashNode, 0, est),
	}
}

// insertRow copies one build-side row into the arena region and chains it into
// its key's bucket. The row's join key must be non-NULL; the caller checks,
// because what a NULL key still owes the output -- nothing, or one unpaid row
// for an outer join -- is the caller's decision to make.
//
// The key is read from cells before the copy: the cells alias the scan's
// reused chunk, and the region's backing array may move while the row lands.
// The key is hashed up front, and both the key frame offset and the row offset
// are recorded as region offsets, which survive any growth.
func (hj *hashJoin) insertRow(a *arena, cells []rowValue) {
	key := cells[hj.bKey].value
	h := maphash.Bytes(hj.seed, key)
	off, keyOff := hj.copyRow(a, cells)
	if keyOff < 0 {
		// A NULL key the caller vouched for: a row with no key bytes cannot be
		// chained, and chaining one would answer a hash probe with a key that
		// was never there.
		return
	}
	b := h & uint64(len(hj.buckets)-1)
	node := int32(len(hj.nodes))
	hj.nodes = append(hj.nodes, hashNode{
		next: hj.buckets[b],
		off:  uint32(off),
		koff: uint32(off + keyOff),
		klen: uint32(len(key)),
	})
	hj.buckets[b] = node
}

// copyRow copies one row's cells into the arena region and returns the row's
// start offset and, within that row, the offset of the join key's bytes -- or
// -1 when the key cell is NULL and has no bytes. Offsets, not pointers: the
// region grows while the build side is read, and growth copies the backing
// array.
func (hj *hashJoin) copyRow(a *arena, cells []rowValue) (off int, keyOff int) {
	off = len(a.region)
	n := 0
	for _, c := range cells {
		n += 4
		if c.set {
			n += len(c.value)
		}
	}
	region := a.growRegion(n)
	at := off
	keyOff = -1
	for j := range cells {
		if !cells[j].set {
			binary.BigEndian.PutUint32(region[at:], 0)
			at += 4
			continue
		}
		binary.BigEndian.PutUint32(region[at:], uint32(len(cells[j].value))+1)
		at += 4
		copy(region[at:], cells[j].value)
		if j == hj.bKey {
			keyOff = at - off
		}
		at += len(cells[j].value)
	}
	return off, keyOff
}

// decodeRegionRow reads a serialized row out of the arena region into cells.
// The frame is the one insertRow wrote: a zero length is a NULL cell, and n+1
// is the length of a present n-byte value, so an empty string (n = 0, frame 1)
// stays distinct from NULL.
func decodeRegionRow(region []byte, off uint32, cells []rowValue) {
	for j := range cells {
		v := binary.BigEndian.Uint32(region[off:])
		off += 4
		if v == 0 {
			cells[j].value = nil
			cells[j].set = false
			continue
		}
		n := int(v - 1)
		cells[j].value = region[off : off+uint32(n)]
		cells[j].set = true
		off += uint32(n)
	}
}
