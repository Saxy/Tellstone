/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: join.go
Description: The Phase 11 hash join: the planner that resolves a translated join
against both tables' schemas, and the executor built on the scan engine and the
arena. The join is the last Phase 11 operator (MULTI-CLUSTER-PLAN.md deliverable
3), and it is deliberately the smallest supported shape: one INNER equality on
two plain tables, both sides full scans, the WHERE clause as a post-join filter,
and a projection across the merged row. Every other shape is refused at
translation (translateJoinSelect), so the planner below cannot be asked for one.
*/
package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"strings"
)

// planJoin produces the physical form of a translated join statement.
//
// Translation settled the join's shape (two plain tables, one INNER equality);
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
	left := plan.Schema
	right, err := s.catalog().Get(js.Right)
	if err != nil {
		if errors.Is(err, ErrNoSuchTable) {
			return &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", js.Right)}
		}
		return s.schemaError(err)
	}

	// The merged row's namespace is "qualifier.column": the two schemas may
	// both declare a "name", and a bare "name" is then ambiguous. A merged row
	// whose cells all answer to distinct names is what lets the WHERE clause
	// be resolved once, here, and evaluated against bytes later.
	nb := len(left.Columns)
	mergedCols := make([]Column, 0, nb+len(right.Columns))
	for _, c := range left.Columns {
		mergedCols = append(mergedCols, Column{Name: js.LeftAlias + "." + c.Name, Type: c.Type, Nullable: c.Nullable, Default: c.Default})
	}
	for _, c := range right.Columns {
		mergedCols = append(mergedCols, Column{Name: js.RightAlias + "." + c.Name, Type: c.Type, Nullable: c.Nullable, Default: c.Default})
	}
	merged := &Schema{
		DB:         DefaultDB,
		Table:      js.Left + " join " + js.Right,
		Columns:    mergedCols,
		PrimaryKey: 0,
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
		li, lok := left.columnIndex(name)
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
	cond := fmt.Sprintf("%s.%s = %s.%s", js.LeftAlias, lRef.col.Name, js.RightAlias, rRef.col.Name)

	filter, err := plan.predicate()
	if err != nil {
		return err
	}
	filter, err = mapExprCols(filter, func(col string) (string, error) {
		op, err := resolve(col)
		if err != nil {
			return "", err
		}
		if op.side == 0 {
			return js.LeftAlias + "." + op.col.Name, nil
		}
		return js.RightAlias + "." + op.col.Name, nil
	})
	if err != nil {
		return err
	}

	proj := make([]joinProj, 0, len(plan.Cols))
	disp := make([]string, 0, len(plan.Cols))
	for _, c := range plan.Cols {
		switch {
		case c == "*":
			// The whole merged row, left side first, in declaration order. The
			// two sides may repeat a name, which is what PostgreSQL does, and
			// the wire keeps the two cells distinguishable by position.
			for i, col := range left.Columns {
				proj = append(proj, joinProj{Idx: i, Col: col})
				disp = append(disp, col.Name)
			}
			for i, col := range right.Columns {
				proj = append(proj, joinProj{Idx: nb + i, Col: col})
				disp = append(disp, col.Name)
			}
		case strings.HasSuffix(c, ".*"):
			q := c[:len(c)-2]
			switch {
			case strings.EqualFold(q, js.LeftAlias):
				for i, col := range left.Columns {
					proj = append(proj, joinProj{Idx: i, Col: col})
					disp = append(disp, col.Name)
				}
			case strings.EqualFold(q, js.RightAlias):
				for i, col := range right.Columns {
					proj = append(proj, joinProj{Idx: nb + i, Col: col})
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
			proj = append(proj, joinProj{Idx: idx, Col: op.col})
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
	probe, err := (&optimizer{sch: right, stats: s.statsFor(right)}).plan(nil)
	if err != nil {
		return err
	}

	outW := 0.0
	for _, p := range proj {
		outW += float64(len(p.Col.Name)) + 1 + valueWidthGuess(p.Col.Type)
	}
	rows := clampRows(math.Min(build.Rows, probe.Rows))
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
			Build:     build,
			Probe:     probe,
			BuildKey:  buildKey,
			ProbeKey:  probeKey,
			BuildName: js.LeftAlias,
			ProbeName: js.RightAlias,
			CondText:  cond,
			Merged:    merged,
			Proj:      proj,
			OutWidth:  outW,
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
func (s *Server) execJoin(plan *Plan, params []paramVal) (*execOutcome, error) {
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

	ctx := &ExecCtx{arena: newArena(int(jp.Build.Rows * rowBytes(jp.Build.Schema)))}
	defer ctx.arena.reset()
	hj := newHashJoin(jp)

	// Build: the left table, every row into the hash table. The scan's cells
	// alias its reused chunk, and insertRow copies each row into the region
	// before the chunk is reused for the next batch.
	buildCells := make([]rowValue, len(jp.Build.Schema.Columns))
	if err := s.newScan(jp.Build, jp.Build.Schema, identityProj(jp.Build.Schema), nil).run(func(c *Chunk) error {
		return forEachChunkRow(c, buildCells, func(_ uint32, cells []rowValue) error {
			hj.insertRow(ctx.arena, cells)
			return nil
		})
	}); err != nil {
		return nil, &pgError{code: errDataCorrupt, msg: err.Error()}
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
	jc := newChunk(len(jp.Proj))

	err = s.newScan(jp.Probe, jp.Probe.Schema, identityProj(jp.Probe.Schema), nil).run(func(c *Chunk) error {
		if err := forEachChunkRow(c, probeCells, func(_ uint32, pcells []rowValue) error {
			key := pcells[hj.pKey]
			if !key.set {
				// A NULL join key matches nothing, on either side.
				return nil
			}
			h := maphash.Bytes(hj.seed, key.value)
			b := h & uint64(len(hj.buckets)-1)
			for n := hj.buckets[b]; n != -1; n = hj.nodes[n].next {
				node := &hj.nodes[n]
				if !bytes.Equal(region[node.koff:node.koff+node.klen], key.value) {
					continue
				}
				decodeRegionRow(region, node.off, merged[:nb])
				copy(merged[nb:], pcells)
				if filt != nil && !filt.eval(merged) {
					continue
				}
				r := jc.Rows
				for j, pr := range jp.Proj {
					if !merged[pr.Idx].set {
						jc.setNull(r, uint32(j))
						continue
					}
					jc.appendVal(merged[pr.Idx].value)
				}
				jc.Rows++
				if jc.Rows >= BatchRows {
					if err := drainChunk(jc, outCols, out); err != nil {
						return err
					}
					jc.reset(uint16(len(jp.Proj)))
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, &pgError{code: errDataCorrupt, msg: err.Error()}
	}
	if jc.Rows > 0 {
		if err := drainChunk(jc, outCols, out); err != nil {
			return nil, &pgError{code: errDataCorrupt, msg: err.Error()}
		}
	}
	out.tag = fmt.Sprintf("SELECT %d", len(out.rows))
	return out, nil
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
// its key's bucket. A row whose join key is NULL joins nothing and is skipped.
//
// The key is read from cells before the copy: the cells alias the scan's
// reused chunk, and the region's backing array may move while the row lands.
// The key is hashed up front, and both the key frame offset and the row offset
// are recorded as region offsets, which survive any growth.
func (hj *hashJoin) insertRow(a *arena, cells []rowValue) {
	if !cells[hj.bKey].set {
		return
	}
	key := cells[hj.bKey].value
	h := maphash.Bytes(hj.seed, key)
	off := len(a.region)
	n := 0
	for _, c := range cells {
		n += 4
		if c.set {
			n += len(c.value)
		}
	}
	region := a.growRegion(n)
	at := off
	keyOff := 0
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
