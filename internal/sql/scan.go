/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: scan.go
Description: The Phase 11 scan engine: a full-table or primary-key-range scan
built on Store.ScanPrefix's callback, reconstructing rows from the key stream,
rejecting them with the D5 residual filter before any copy, and batching the
survivors into a reused Chunk for the wire drain.

The engine's one hard constraint is the store's seam. ScanPrefix walks a prefix
to its end and offers no resume point, so a scan cannot be paused and resumed
across batches without re-walking everything below the current row -- quadratic
in the table size. The scan therefore runs the whole statement in one pass,
filling a fixed Chunk with each batch of rows and handing the batch to a sink;
it stops early only at the range's hi bound or when the sink returns false. The
Phase 11 join retrofit kept this shape deliberately: a join's build and probe
sides are the same push scans with a different sink, because the store's seam
does not care how many tables the statement names.
*/
package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
)

// ---- Chunk framing ----

// reset clears a chunk for a fresh batch of ncols-wide rows. The data buffer is
// kept, so a batch that leaves the capacity in place never reallocates.
func (c *Chunk) reset(ncols uint16) {
	c.Rows = 0
	c.NCols = ncols
	c.data = c.data[:0]
	for i := range c.nulls {
		c.nulls[i] = 0
	}
}

// ensureNulls sizes the presence bitmap to hold Rows of NCols columns. It is
// called once by the scan, which knows its batch width up front.
func (c *Chunk) ensureNulls(ncols uint16) {
	words := (uint64(ncols)*BatchRows + 63) / 64
	if uint64(len(c.nulls)) >= words {
		return
	}
	c.nulls = make([]uint64, words)
}

// nullIndex is the bitmap bit for one column of one row.
func (c *Chunk) nullIndex(row, col uint32) uint64 {
	return uint64(row)*uint64(c.NCols) + uint64(col)
}

// setNull marks one cell NULL and appends nothing to the payload: a NULL
// column has no bytes, and the bitmap is what distinguishes it from an empty
// string, which does have a zero-length frame (ADR-013: NULL is the absence of
// a key, not the presence of an empty value).
func (c *Chunk) setNull(row, col uint32) {
	idx := c.nullIndex(row, col)
	c.nulls[idx>>6] |= 1 << (idx & 63)
}

// isNull reports whether one cell is NULL.
func (c *Chunk) isNull(row, col uint32) bool {
	idx := c.nullIndex(row, col)
	return c.nulls[idx>>6]&(1<<(idx&63)) != 0
}

// appendVal appends a present cell as a 4-byte big-endian length frame plus
// its bytes, the same framing the wire's DataRow uses. The length encodes an
// empty string as zero, distinct from NULL, which appends nothing.
func (c *Chunk) appendVal(value []byte) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(value)))
	c.data = append(c.data, hdr[:]...)
	c.data = append(c.data, value...)
}

// forEachChunkRow walks a filled chunk's cells, handing each row's cells to fn
// as a slice into the chunk's buffers. NULL cells report an unset value; a
// present empty string still has its zero-length frame distinguished by set.
// The cells alias the chunk, so a callback that needs a value past the batch
// must copy it: the join's build side copies into its arena, and the probe side
// consumes on the spot.
func forEachChunkRow(c *Chunk, cells []rowValue, fn func(row uint32, cells []rowValue) error) error {
	off := 0
	for row := uint32(0); row < c.Rows; row++ {
		for j := range cells {
			if c.isNull(row, uint32(j)) {
				cells[j].value = nil
				cells[j].set = false
				continue
			}
			l := binary.BigEndian.Uint32(c.data[off:])
			off += 4
			cells[j].value = c.data[off : off+int(l)]
			cells[j].set = true
			off += int(l)
		}
		if err := fn(row, cells); err != nil {
			return err
		}
	}
	return nil
}

// commonPrefix returns the longest string that is a prefix of both bounds. The
// scan seeks it so a narrow range does not walk the whole table, and it is at
// least the table prefix whenever the two bounds are row or table bounds. A nil
// or empty result means the bounds share nothing, which the caller must not use
// as a seek key.
func commonPrefix(a, b []byte) []byte {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return a[:i]
}

// ---- row assembly ----

// run executes the scan, calling emit with each filled batch. The chunk is
// reused across batches, so only emit's own output allocates. It returns the
// first store or emit error. The scan never pauses mid-table: ScanPrefix has
// no seek point, so a pass runs to the hi bound or the end of the prefix, and
// the emit callback is what notices the batch boundary.
func (ts *tableScan) run(emit func(*Chunk) error) error {
	as := ts.as
	as.dst = ts.chunk
	ts.chunk.reset(ts.projLen())
	_, err := ts.store.ScanPrefix(string(ts.seek), func(key, value []byte) bool {
		// The seek prefix is a common prefix of the range's bounds, not the
		// lower bound itself, so the store may hand over keys before the range
		// starts. The bound filters are what make the yielded set exact.
		if ts.beg != nil && bytes.Compare(key, ts.beg) < 0 {
			return true
		}
		if ts.hi != nil && bytes.Compare(key, ts.hi) >= 0 {
			return false
		}
		if err := as.add(key, value); err != nil {
			as.err = err
			return false
		}
		if as.dst.Rows >= BatchRows {
			if err := emit(as.dst); err != nil {
				as.err = err
				return false
			}
			as.dst.reset(ts.projLen())
		}
		return true
	})
	if err != nil {
		return err
	}
	if as.err != nil {
		return as.err
	}
	// The stream ended, so the row being assembled saw no next-row boundary.
	// Flushing it here is what delivers the last row of a table or range.
	if as.cur != nil {
		if err := as.flush(); err != nil {
			return err
		}
	}
	if as.dst.Rows > 0 {
		return emit(as.dst)
	}
	return nil
}

// projLen is the chunk's column count: the projected schema columns plus the
// trailing CASE cells the flush appends after them.
func (ts *tableScan) projLen() uint16 { return uint16(len(ts.proj) + len(ts.cases)) }

// matchNameBytes reports whether a column name equals a key suffix, without
// materializing either side. It is the zero-allocation lookup the assembler
// uses instead of a per-key string conversion.
func matchNameBytes(name string, b []byte) bool {
	if len(name) != len(b) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if name[i] != b[i] {
			return false
		}
	}
	return true
}

// add absorbs one key of the scan stream. It opens a new row when the key's
// row id differs from the row being assembled (flushing the completed one), and
// places the key's value into the cell for its column.
func (as *rowAssembler) add(key, value []byte) error {
	rest := key[len(as.prefix):]
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		// A key inside the table range with no row/column separator is not a
		// row this scan can account for; the catalog counts ignore it too.
		return nil
	}
	id := rest[:i]
	if !bytes.Equal(id, as.cur) {
		if as.cur != nil {
			if err := as.flush(); err != nil {
				return err
			}
		}
		as.cur = append(as.cur[:0], id...)
		for k := range as.cells {
			as.cells[k].set = false
		}
	}
	// The column name is the key suffix after the row id's separator. Column
	// names cannot contain a separator, so the remainder is exactly the name.
	for ord := range as.sch.Columns {
		if matchNameBytes(as.sch.Columns[ord].Name, rest[i+1:]) {
			// The value is the store's callback buffer, which the next key may
			// overwrite and which the scan may recycle when it returns, while
			// this cell is read at the next row boundary (or after the scan for
			// the last row). It is copied into the column's own buffer, which
			// is reused across rows and so stops allocating once it has seen
			// the column's widest value.
			v := append(as.owned[ord][:0], value...)
			as.owned[ord] = v
			as.cells[ord] = rowValue{value: v, set: true}
			break
		}
	}
	return nil
}

// flush applies the residual filter to the completed row and, if it survives,
// copies only the projected cells into the chunk. A rejected row copies
// nothing. The cells are owned by the callback, so the projected values are
// copied rather than borrowed.
func (as *rowAssembler) flush() error {
	if as.filt != nil && !as.filt.eval(as.cells) {
		as.cur = as.cur[:0]
		return nil
	}
	row := as.dst.Rows
	for j, ord := range as.proj {
		cell := as.cells[ord]
		if !cell.set {
			as.dst.setNull(row, uint32(j))
			continue
		}
		as.dst.appendVal(cell.value)
	}
	// A CASE cell is appended after the projection, in the chunk's trailing
	// columns, so the projected layout and the join's stay untouched. A nil
	// result is NULL -- the absence of a key, not an empty value (ADR-013).
	for k, cc := range as.cases {
		v := cc.eval(as.cells)
		if v == nil {
			as.dst.setNull(row, uint32(len(as.proj)+k))
			continue
		}
		as.dst.appendVal(v)
	}
	as.dst.Rows++
	as.cur = as.cur[:0]
	return nil
}

// compiledCase is one CASE target lowered for execution: its conditions as
// compiled filters over schema-width row cells and its branch results in the
// result type's storage encoding, so evaluation is a byte-level walk with no
// decode -- the same discipline the residual filter follows (D5).
type compiledCase struct {
	typ     ColumnType
	whens   []compiledCaseWhen
	elseVal []byte
	// elseSub is the ELSE branch's own CASE, when the branch is one.
	elseSub *compiledCase
}

type compiledCaseWhen struct {
	node *compiledNode
	val  []byte
	// sub is the branch's own CASE, when the branch is one rather than a
	// literal.
	sub *compiledCase
}

// compileCase binds a CASE's conditions and encodes its branch literals.
// A nil result encodes as nil, which every consumer reads as NULL; an empty
// string encodes as a zero-length value, which is a present cell (ADR-013).
func (s *Server) compileCase(ct *caseTarget, sch *Schema, params []paramVal) (*compiledCase, error) {
	cc := &compiledCase{typ: ct.Type}
	for i := range ct.Whens {
		if ct.Whens[i].expr == nil {
			// planAccess translates conditions before anything executes; a
			// nil one means that step was skipped, which is a bug to report
			// rather than a query to mis-answer.
			return nil, &pgError{code: errInternal, msg: "CASE condition was not translated"}
		}
		n, err := s.compileFilter(ct.Whens[i].expr, sch, params)
		if err != nil {
			return nil, err
		}
		if ct.Whens[i].sub != nil {
			// The branch is a CASE of its own, so its value is computed
			// from the same row.
			sub, err := s.compileCase(ct.Whens[i].sub, sch, params)
			if err != nil {
				return nil, err
			}
			cc.whens = append(cc.whens, compiledCaseWhen{node: n, sub: sub})
			continue
		}
		v, err := ct.encodeCaseResult(ct.Whens[i].lit)
		if err != nil {
			return nil, err
		}
		cc.whens = append(cc.whens, compiledCaseWhen{node: n, val: v})
	}
	if ct.ElseSub != nil {
		sub, err := s.compileCase(ct.ElseSub, sch, params)
		if err != nil {
			return nil, err
		}
		cc.elseSub = sub
	}
	if ct.Else != nil {
		v, err := ct.encodeCaseResult(*ct.Else)
		if err != nil {
			return nil, err
		}
		cc.elseVal = v
	}
	return cc, nil
}

// encodeCaseResult renders one branch literal in the result type's storage
// encoding: raw bytes for text, the orderable 8-byte form for integers (both
// int4- and int8-typed results store the same way, as every integer column
// does), one byte for a boolean, nil for NULL.
func (ct *caseTarget) encodeCaseResult(l caseLit) ([]byte, error) {
	if l.null {
		return nil, nil
	}
	switch ct.Type {
	case TypeVarchar:
		return []byte(l.s), nil
	case TypeInt, TypeBigInt:
		return EncodeValue(ct.Type, l.i)
	case TypeBool:
		return EncodeValue(TypeBool, l.b)
	}
	return nil, &pgError{code: errInternal, msg: "unreachable CASE result type"}
}

// eval runs one row through the CASE: the first condition that evaluates true
// yields its branch's bytes, else the ELSE branch, else NULL.
func (cc *compiledCase) eval(cells rowCells) []byte {
	for _, w := range cc.whens {
		if w.node.eval(cells) {
			if w.sub != nil {
				return w.sub.eval(cells)
			}
			return w.val
		}
	}
	if cc.elseSub != nil {
		return cc.elseSub.eval(cells)
	}
	return cc.elseVal
}

// compileCases lowers every CASE target of a plan and reports the output
// positions each one fills. Both the scan and the point-lookup path compile
// the same way, so a condition's errors (a missing column, an unbindable
// parameter) surface identically however the row is reached.
func (s *Server) compileCases(plan *Plan, sch *Schema, params []paramVal) ([]*compiledCase, map[int]int, error) {
	if len(plan.Cases) == 0 {
		return nil, nil, nil
	}
	ccases := make([]*compiledCase, 0, len(plan.Cases))
	at := make(map[int]int, len(plan.Cases))
	for k := range plan.Cases {
		cc, err := s.compileCase(&plan.Cases[k], sch, params)
		if err != nil {
			return nil, nil, err
		}
		ccases = append(ccases, cc)
		at[plan.Cases[k].Pos] = len(ccases) - 1
	}
	return ccases, at, nil
}

// ---- residual filter ----

// compileFilter resolves a statement's residual predicate into the compiled
// form, binding parameters and encoding literals in the column storage
// encodings. It returns nil for a nil filter (admit every row).
func (s *Server) compileFilter(e Expr, sch *Schema, params []paramVal) (*compiledNode, error) {
	switch t := e.(type) {
	case nil:
		return nil, nil
	case *TrueExpr:
		return &compiledNode{lit: true, val: true}, nil
	case *FalseExpr:
		return &compiledNode{lit: true, val: false}, nil
	case *CmpExpr:
		idx, ok := sch.columnIndex(t.Col)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Col)}
		}
		if paramBoundNull(t.Val, params) {
			// A comparison against a bound NULL is UNKNOWN, which filters every
			// row out. The translator folds a literal NULL this way; a NULL
			// bound parameter folds here, before the column's nullability can
			// turn a filter into an error.
			return &compiledNode{lit: true, val: false}, nil
		}
		enc, isNull, err := resolveColumnRef(&sch.Columns[idx], t.Val, params)
		if err != nil {
			return nil, err
		}
		if isNull {
			// A NULL literal comparison reached the planner folded; a NULL
			// bound on a nullable column reaches here.
			return &compiledNode{lit: true, val: false}, nil
		}
		return &compiledNode{f: &compiledFilter{col: idx, op: t.Op, enc: enc}}, nil
	case *NullTestExpr:
		idx, ok := sch.columnIndex(t.Col)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Col)}
		}
		return &compiledNode{f: &compiledFilter{col: idx, null: true, not: t.Not}}, nil
	case *LikeExpr:
		idx, ok := sch.columnIndex(t.Col)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Col)}
		}
		if paramBoundNull(t.Val, params) {
			// `col LIKE NULL` is UNKNOWN: no row qualifies.
			return &compiledNode{lit: true, val: false}, nil
		}
		pat, err := s.likePattern(&sch.Columns[idx], t.Val, params)
		if errors.Is(err, errLikePatternNull) {
			return &compiledNode{lit: true, val: false}, nil
		}
		if err != nil {
			return nil, err
		}
		// A trailing-wildcard-only pattern answers by HasPrefix; anything wider
		// answers by the matcher. Both are decided here, where the bytes are
		// known, so a bound parameter takes the same path a literal does.
		if pref, ok := likePrefixPattern(pat); ok {
			return &compiledNode{f: &compiledFilter{col: idx, like: pref, not: t.Not}}, nil
		}
		return &compiledNode{f: &compiledFilter{col: idx, pat: pat, not: t.Not}}, nil
	case *NeExpr:
		idx, ok := sch.columnIndex(t.Col)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Col)}
		}
		if paramBoundNull(t.Val, params) {
			// `col <> NULL` is UNKNOWN: no row qualifies.
			return &compiledNode{lit: true, val: false}, nil
		}
		enc, isNull, err := resolveColumnRef(&sch.Columns[idx], t.Val, params)
		if err != nil {
			return nil, err
		}
		if isNull {
			return &compiledNode{lit: true, val: false}, nil
		}
		return &compiledNode{f: &compiledFilter{col: idx, enc: enc, ne: true}}, nil
	case *ColCmpExpr:
		// A comparison between two columns of the same row. The bytes are
		// only comparable when both columns share a type, because the
		// comparison is a byte compare over the storage encoding.
		li, ok := sch.columnIndex(t.Left)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Left)}
		}
		ri, ok := sch.columnIndex(t.Right)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Right)}
		}
		if sch.Columns[li].Type != sch.Columns[ri].Type {
			return nil, &pgError{code: errFeatureNotSupported, msg: fmt.Sprintf("cannot compare columns of types %s and %s", sch.Columns[li].Type.String(), sch.Columns[ri].Type.String())}
		}
		return &compiledNode{f: &compiledFilter{col: li, col2: ri, twoCol: true, ne: t.Not, op: t.Op}}, nil
	case *RegexExpr:
		idx, ok := sch.columnIndex(t.Col)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Col)}
		}
		if paramBoundNull(t.Val, params) {
			// `col ~ NULL` is UNKNOWN: no row qualifies.
			return &compiledNode{lit: true, val: false}, nil
		}
		pat, isNull, err := resolveColumnRef(&sch.Columns[idx], t.Val, params)
		if err != nil {
			return nil, err
		}
		if isNull {
			return &compiledNode{lit: true, val: false}, nil
		}
		if sch.Columns[idx].Type != TypeVarchar {
			return nil, &pgError{code: errDatatypeMismatch, msg: fmt.Sprintf("a regular expression match needs a text column, not type %s", sch.Columns[idx].Type.String())}
		}
		re, err := regexp.Compile(string(pat))
		if err != nil {
			return nil, &pgError{code: errInvalidTextRepresentation, msg: fmt.Sprintf("invalid regular expression: %v", err)}
		}
		return &compiledNode{f: &compiledFilter{col: idx, re: re, not: t.Not}}, nil
	case *BoolColExpr:
		idx, ok := sch.columnIndex(t.Col)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", t.Col)}
		}
		if sch.Columns[idx].Type != TypeBool {
			return nil, &pgError{code: errDatatypeMismatch, msg: fmt.Sprintf("argument of a condition must be type boolean, not type %s", sch.Columns[idx].Type.String())}
		}
		return &compiledNode{f: &compiledFilter{col: idx, boolCol: true}}, nil
	case *BoolExpr:
		args := make([]*compiledNode, 0, len(t.Args))
		for _, a := range t.Args {
			c, err := s.compileFilter(a, sch, params)
			if err != nil {
				return nil, err
			}
			args = append(args, c)
		}
		return &compiledNode{op: t.Op, args: args}, nil
	default:
		return nil, fmt.Errorf("%w: residual filter node %T", errUnsupported, e)
	}
}

// paramBoundNull reports whether a value reference is a parameter bound to
// SQL NULL. It is the one NULL state that cannot be folded at translation,
// because the binding happens at execution.
func paramBoundNull(v ValRef, params []paramVal) bool {
	return v.Param > 0 && v.Param <= len(params) && params[v.Param-1].null
}

// errLikePatternNull reports a LIKE pattern that resolved to NULL, which makes
// the whole term UNKNOWN: no row qualifies. compileFilter folds it into an
// always-false node rather than surfacing it as a statement error, because the
// NULL was the value's state, not a mistake.
var errLikePatternNull = fmt.Errorf("sql: LIKE pattern is NULL")

// likePattern resolves a LIKE pattern to the bytes the matcher reads. The shape
// (prefix or general) is decided by the caller with likePrefixPattern, at plan
// time for a literal and here for a parameter, where the bound bytes are known
// for both alike.
func (s *Server) likePattern(col *Column, v ValRef, params []paramVal) ([]byte, error) {
	pat, isNull, err := resolveColumnRef(col, v, params)
	if err != nil {
		return nil, err
	}
	if isNull {
		// `col LIKE NULL` is UNKNOWN, so no row qualifies. An always-false node
		// is invented rather than a zero-length pattern, which would match every
		// value.
		return nil, errLikePatternNull
	}
	return pat, nil
}

// alwaysFalse reports whether a compiled filter matches no row at all. It lets
// a statement whose filter provably rejects everything answer without touching
// the store.
func (n *compiledNode) alwaysFalse() bool {
	return n != nil && n.lit && !n.val
}

// eval evaluates the filter against an assembled row.
func (n *compiledNode) eval(cells rowCells) bool {
	if n.f != nil {
		return n.f.eval(cells)
	}
	if n.lit {
		return n.val
	}
	switch n.op {
	case BoolAnd:
		for _, a := range n.args {
			if !a.eval(cells) {
				return false
			}
		}
		return true
	case BoolOr:
		for _, a := range n.args {
			if a.eval(cells) {
				return true
			}
		}
		return false
	default: // BoolNot
		return !n.args[0].eval(cells)
	}
}

// eval evaluates one leaf against a row. A comparison reads only the cell's
// raw storage bytes; a null test reads whether the cell's key is present.
func (f *compiledFilter) eval(cells rowCells) bool {
	if f.null {
		present := cells[f.col].set
		if f.not {
			return present
		}
		return !present
	}
	if !cells[f.col].set {
		// A comparison against an absent column (NULL) is UNKNOWN, so the row
		// does not qualify -- for an inequality as much as for an equality.
		return false
	}
	if f.re != nil {
		matched := f.re.Match(cells[f.col].value)
		if f.not {
			return !matched
		}
		return matched
	}
	if f.boolCol {
		// The cell is the condition. A boolean is stored as one byte, and a
		// NULL cell has already been rejected above: it is UNKNOWN, which
		// drops the row.
		return len(cells[f.col].value) > 0 && cells[f.col].value[0] != 0
	}
	if f.twoCol {
		// Two columns of the same row. A NULL on either side is UNKNOWN, so
		// the row does not qualify -- the same answer `=` gives when one
		// side is NULL.
		if !cells[f.col2].set {
			return false
		}
		if f.ne {
			return !bytes.Equal(cells[f.col].value, cells[f.col2].value)
		}
		switch f.op {
		case OpEq:
			return bytes.Equal(cells[f.col].value, cells[f.col2].value)
		case OpLt:
			return bytes.Compare(cells[f.col].value, cells[f.col2].value) < 0
		case OpLe:
			return bytes.Compare(cells[f.col].value, cells[f.col2].value) <= 0
		case OpGt:
			return bytes.Compare(cells[f.col].value, cells[f.col2].value) > 0
		case OpGe:
			return bytes.Compare(cells[f.col].value, cells[f.col2].value) >= 0
		}
		return false
	}
	if f.ne {
		return !bytes.Equal(cells[f.col].value, f.enc)
	}
	if f.like != nil || f.pat != nil {
		var matched bool
		if f.pat != nil {
			matched = likeMatch(cells[f.col].value, f.pat)
		} else {
			matched = bytes.HasPrefix(cells[f.col].value, f.like)
		}
		if f.not {
			return !matched
		}
		return matched
	}
	switch f.op {
	case OpEq:
		return bytes.Equal(cells[f.col].value, f.enc)
	case OpLt:
		return bytes.Compare(cells[f.col].value, f.enc) < 0
	case OpLe:
		return bytes.Compare(cells[f.col].value, f.enc) <= 0
	case OpGt:
		return bytes.Compare(cells[f.col].value, f.enc) > 0
	case OpGe:
		return bytes.Compare(cells[f.col].value, f.enc) >= 0
	}
	return true
}
