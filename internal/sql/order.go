/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: order.go
Description: ORDER BY, DISTINCT and LIMIT/OFFSET over a materialized result.

A scan fills a result set batch by batch and a join probes the same way, so the
three clauses ADR-016 Stage B adds are applied once, to the finished result,
rather than inside either access method: sorting needs every row before it can
order any of them, and folding and bounding are row counts over the whole set.
That keeps the scan and the join unchanged where they are hottest and puts the
clauses in one place both paths shares.

The rows held by execOutcome are the wire text of each cell. Ordering therefore
compares that text through the column's declared type -- numbers by their value,
everything else by their bytes -- so `10` sorts after `9` even though the text
`"10"` sorts before `"9"`. Text collation is byte order, as it is under a C
locale; PostgreSQL's locale collation is not reproduced.
*/
package sql

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// sortKey is one ORDER BY item resolved against a concrete row shape: idx is
// the cell the comparison reads, in the row as the executor materialized it,
// and typ is that cell's declared type. A column the projection does not return
// is appended to every row as a hidden trailing cell, so idx may sit past the
// width the client is told about.
type sortKey struct {
	idx        int
	typ        ColumnType
	desc       bool
	nullsFirst bool
}

// finalizeResult applies the plan's ordering, folding and bounding to a
// materialized SELECT result and recomputes its command tag.
//
// vis is the number of cells per row the client receives; rows wider than that
// carry hidden sort cells the sort reads first and the truncation drops. It is
// a no-op for a statement with no such clauses, which is every statement today
// that does not opt in at translation.
func finalizeResult(plan *Plan, out *execOutcome, cols []Column, keys []sortKey, vis int) error {
	if len(keys) > 0 {
		if err := checkDistinctOrder(plan, keys, vis); err != nil {
			return err
		}
		sort.SliceStable(out.rows, func(i, j int) bool {
			return compareRows(out.rows[i], out.rows[j], keys, cols) < 0
		})
	}
	// Truncate to the visible width, dropping any hidden sort cells the sort
	// needed and the client must not see.
	if len(out.rows) > 0 && len(out.rows[0]) > vis {
		for i, r := range out.rows {
			out.rows[i] = r[:vis]
		}
	}
	if plan.Distinct {
		out.rows = distinctRows(out.rows)
	}
	if plan.HasOffset && plan.Offset > 0 {
		if int(plan.Offset) >= len(out.rows) {
			out.rows = nil
		} else {
			out.rows = out.rows[plan.Offset:]
		}
	}
	if plan.HasLimit && int(plan.Limit) < len(out.rows) {
		out.rows = out.rows[:plan.Limit]
	}
	out.tag = fmt.Sprintf("SELECT %d", len(out.rows))
	return nil
}

// checkDistinctOrder rejects the shape PostgreSQL rejects: with SELECT
// DISTINCT, an ORDER BY item must appear in the select list, and here that
// means it cannot be one of the hidden cells the sort appended. Refusing after
// planning rather than silently sorting on a column the fold would then erase
// keeps the error at the point where the conflict is known; it is a feature
// limitation rather than a syntax problem, so it carries 0A000.
func checkDistinctOrder(plan *Plan, keys []sortKey, vis int) error {
	if !plan.Distinct {
		return nil
	}
	for _, k := range keys {
		if k.idx >= vis {
			return &pgError{code: errFeatureNotSupported, msg: "for SELECT DISTINCT, ORDER BY expressions must appear in select list"}
		}
	}
	return nil
}

// compareRows orders two rows by keys, each item flipping the result when it
// asks for descending order, and NULLs going first or last as the item says.
// Two rows equal on every key compare equal however wide they are, which is
// what keeps a sort stable across hidden cells.
func compareRows(a, b [][]byte, keys []sortKey, cols []Column) int {
	for _, k := range keys {
		x, y := a[k.idx], b[k.idx]
		switch {
		case x == nil && y == nil:
			continue
		case x == nil:
			if k.nullsFirst {
				return -1
			}
			return 1
		case y == nil:
			if k.nullsFirst {
				return 1
			}
			return -1
		}
		c := compareCell(k.typ, x, y)
		if c == 0 {
			continue
		}
		if k.desc {
			return -c
		}
		return c
	}
	return 0
}

// compareCell orders two non-NULL wire cells of one declared type. Numeric
// types compare by value, which is what makes "10" greater than "9"; every
// other type compares by its wire bytes, and for the text-like types here that
// is a total order (bytea, bool, and the fixed-layout timestamp encoding).
func compareCell(t ColumnType, a, b []byte) int {
	switch t {
	case TypeInt, TypeBigInt:
		x, ea := strconv.ParseInt(string(a), 10, 64)
		y, eb := strconv.ParseInt(string(b), 10, 64)
		if ea == nil && eb == nil {
			return cmpInt(x, y)
		}
	case TypeFloat:
		x, ea := strconv.ParseFloat(string(a), 64)
		y, eb := strconv.ParseFloat(string(b), 64)
		if ea == nil && eb == nil {
			return cmpFloat(x, y)
		}
	}
	return bytes.Compare(a, b)
}

func cmpInt(x, y int64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

func cmpFloat(x, y float64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

// distinctRows drops later rows equal to one already kept, comparing whole
// cells including NULLs. The comparison is keyed on a length-prefixed encoding
// so two rows differing only in a cell boundary cannot collide.
func distinctRows(rows [][][]byte) [][][]byte {
	seen := make(map[string]struct{}, len(rows))
	out := rows[:0]
	for _, r := range rows {
		var b strings.Builder
		for _, c := range r {
			if c == nil {
				b.WriteString("\x00")
				continue
			}
			b.WriteString(strconv.Itoa(len(c)))
			b.WriteString(":")
			b.Write(c)
		}
		if _, dup := seen[b.String()]; dup {
			continue
		}
		seen[b.String()] = struct{}{}
		out = append(out, r)
	}
	return out
}

// tableSortKeys resolves one statement's ORDER BY against a catalog table's
// schema. cols are the projection's declarations, so a key the projection
// carries indexes them directly, and a key it does not becomes a hidden schema
// column: hidden receives them in row order, and the returned keys index as if
// they had already been appended to the projection.
func tableSortKeys(plan *Plan, sch *Schema, cols []Column) (keys []sortKey, hidden []int, err error) {
	names := plan.OutNames
	if names == nil {
		names = plan.Cols
	}
	for _, k := range plan.OrderBy {
		key := sortKey{idx: -1, desc: k.Desc, nullsFirst: k.NullsFirst}
		if k.Out >= 0 {
			key.idx, key.typ = k.Out, cols[k.Out].Type
		} else {
			qual, col := splitRef(k.Src)
			// A bare name answers first to the select list, which is what
			// PostgreSQL resolves it against; a qualified one names an input
			// column outright and so skips that step.
			if qual == "" {
				if pos := indexOfName(names, col); pos >= 0 && pos < len(cols) {
					key.idx, key.typ = pos, cols[pos].Type
				}
			}
			if key.idx < 0 {
				si, err := tableOrderColumn(sch, plan, qual, col)
				if err != nil {
					return nil, nil, err
				}
				if pos := indexOfName(plan.Cols, sch.Columns[si].Name); pos >= 0 {
					key.idx, key.typ = pos, cols[pos].Type
				} else {
					hidden = append(hidden, si)
					key.idx = len(cols) + len(hidden) - 1
					key.typ = sch.Columns[si].Type
				}
			}
		}
		keys = append(keys, key)
	}
	return keys, hidden, nil
}

// tableOrderColumn finds the schema column an ORDER BY reference names. A
// qualifier must be the statement's alias or the table itself -- an alias hides
// the table name, so a qualifier naming anything else reports the missing
// FROM-clause entry PostgreSQL reports rather than resolving past it.
func tableOrderColumn(sch *Schema, plan *Plan, qual, col string) (int, error) {
	if qual != "" && !strings.EqualFold(qual, plan.TableAlias) && !strings.EqualFold(qual, sch.Table) {
		return 0, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("missing FROM-clause entry for table %q", qual)}
	}
	idx, ok := sch.columnIndex(col)
	if !ok {
		return 0, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", col)}
	}
	return idx, nil
}

// joinSortKeys is tableSortKeys for a join: the input row is the merged row of
// both sides, so an unprojected key becomes a hidden merged column appended to
// the join's projection. cols are the join projection's declarations.
func joinSortKeys(plan *Plan, jp *JoinPlan, cols []Column) (keys []sortKey, hidden []joinProj, err error) {
	names := plan.OutNames
	if names == nil {
		names = plan.Cols
	}
	for _, k := range plan.OrderBy {
		key := sortKey{idx: -1, desc: k.Desc, nullsFirst: k.NullsFirst}
		if k.Out >= 0 {
			key.idx, key.typ = k.Out, cols[k.Out].Type
		} else {
			qual, col := splitRef(k.Src)
			if qual == "" {
				if pos := indexOfName(names, col); pos >= 0 && pos < len(cols) {
					key.idx, key.typ = pos, cols[pos].Type
				}
			}
			if key.idx < 0 {
				mi, err := mergedOrderColumn(jp, qual, col)
				if err != nil {
					return nil, nil, err
				}
				projPos := projIndexOf(jp.Proj, mi)
				if projPos >= 0 {
					key.idx, key.typ = projPos, cols[projPos].Type
				} else {
					hidden = append(hidden, joinProj{Idx: mi, Col: jp.Merged.Columns[mi], Case: -1})
					key.idx = len(cols) + len(hidden) - 1
					key.typ = jp.Merged.Columns[mi].Type
				}
			}
		}
		keys = append(keys, key)
	}
	return keys, hidden, nil
}

// mergedOrderColumn finds the merged-row column an ORDER BY reference names:
// a qualified one answers to its side's alias, and a bare one must name exactly
// one side, which is the ambiguity PostgreSQL reports.
func mergedOrderColumn(jp *JoinPlan, qual, col string) (int, error) {
	if qual != "" {
		// A joined left side keeps the name its own merged row gave the
		// column, so the whole name may name one of its columns.
		if idx, ok := jp.Merged.columnIndex(qual + "." + col); ok {
			return idx, nil
		}
		if !strings.EqualFold(qual, jp.BuildName) && !strings.EqualFold(qual, jp.ProbeName) {
			return 0, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("missing FROM-clause entry for table %q", qual)}
		}
		idx, ok := jp.Merged.columnIndex(qual + "." + col)
		if !ok {
			return 0, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", col)}
		}
		return idx, nil
	}
	found := -1
	for i, c := range jp.Merged.Columns {
		if _, name := splitRef(c.Name); name == col {
			if found >= 0 {
				return 0, &pgError{code: errAmbiguousColumn, msg: fmt.Sprintf("column reference %q is ambiguous", col)}
			}
			found = i
		}
	}
	if found < 0 {
		return 0, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", col)}
	}
	return found, nil
}

// projIndexOf reports the projection position carrying merged column mi, or -1
// when the projection does not return it.
func projIndexOf(proj []joinProj, mi int) int {
	for i, p := range proj {
		if p.Idx == mi {
			return i
		}
	}
	return -1
}

// implicitSortKeys resolves ORDER BY for the Phase 8 two-column table, whose
// projection is "key" and/or "value" and whose row is assembled from those two
// values. A key the projection does not return rides as one of those names for
// the caller to append.
func implicitSortKeys(plan *Plan, cols []Column) (keys []sortKey, hidden []string, err error) {
	names := plan.OutNames
	if names == nil {
		names = plan.Cols
	}
	for _, k := range plan.OrderBy {
		key := sortKey{idx: -1, desc: k.Desc, nullsFirst: k.NullsFirst}
		if k.Out >= 0 {
			key.idx, key.typ = k.Out, cols[k.Out].Type
		} else {
			if qual, _ := splitRef(k.Src); qual != "" {
				return nil, nil, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("missing FROM-clause entry for table %q", qual)}
			}
			if pos := indexOfName(names, k.Src); pos >= 0 && pos < len(cols) {
				key.idx, key.typ = pos, cols[pos].Type
			} else if k.Src == colKey || k.Src == colValue {
				hidden = append(hidden, k.Src)
				key.idx = len(cols) + len(hidden) - 1
				key.typ = implicitColumnType(k.Src)
			} else {
				return nil, nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", k.Src)}
			}
		}
		keys = append(keys, key)
	}
	return keys, hidden, nil
}

// implicitColumnType is the type the two-column table's cells are compared as:
// its key is text and its value is bytea, which is how the wire declares them.
func implicitColumnType(name string) ColumnType {
	if name == colValue {
		return TypeBytes
	}
	return TypeVarchar
}

// splitRef splits a column reference into the qualifier a statement wrote and
// the column name it qualifies: "u.id" to ("u", "id") and "id" to ("", "id").
func splitRef(ref string) (qual, col string) {
	if i := strings.LastIndexByte(ref, '.'); i > 0 {
		return ref[:i], ref[i+1:]
	}
	return "", ref
}

// indexOfName returns the position of name in names, or -1 when absent.
func indexOfName(names []string, name string) int {
	for i, n := range names {
		if n == name {
			return i
		}
	}
	return -1
}
