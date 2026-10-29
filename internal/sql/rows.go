/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: rows.go
Description: Reading and writing one multi-column row against a catalog schema.

A row is a single key holding a packed value (ADR-013, Phase 5 D1-A): the key is
the row's prefix (`db/table/<row-id>/`), and the value is every column's encoded
bytes in schema order with a per-column presence flag. This is the KV fast-path
shape the Phase 5 plan chose over the earlier one-key-per-column layout: a point
read is one Get, a write is one Set/SetIfAbsent, and the row is written and read
atomically rather than as several keys that a crash could split.
*/
package sql

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// ErrDuplicateRow reports an INSERT whose primary key is already present.
var ErrDuplicateRow = fmt.Errorf("%w: duplicate key", ErrBadSchema)

// rowValue is one column of a row being written.
type rowValue struct {
	// value is the encoded value. A nil value is SQL NULL, which is written as
	// the absence of the column's key. A null column is not a zero-length value,
	// which is what lets an empty string stay an empty string instead of
	// becoming indistinguishable from NULL.
	value []byte
	// set reports whether this statement named the column at all. It is
	// separate from value because "set to NULL" and "not mentioned" are
	// different operations: an UPDATE that names two of a row's five columns
	// must clear a named column to NULL and leave the other three alone, and a
	// nil value cannot express both.
	set bool
}

// rowCells are indexed by schema column ordinal.
type rowCells []rowValue

// rowVersion is the leading byte of a packed row value. It exists so a future
// change to the encoding can be told apart from an old value.
const rowVersion = 0x01

// packRow serializes a full row's cells into one value. The schema fixes the
// column order, so the encoding is a per-column presence flag followed by the
// value bytes: a present column is [0x01][u32 len][bytes], an absent (NULL)
// column is [0x00]. A present empty string is 0x01 with length zero, distinct
// from NULL. A cell that is set but nil (an explicit NULL) encodes as absent.
func packRow(cells rowCells) []byte {
	buf := make([]byte, 0, 16+len(cells))
	buf = append(buf, rowVersion)
	var hdr [4]byte
	for i := range cells {
		cell := cells[i]
		if cell.set && cell.value != nil {
			buf = append(buf, 0x01)
			binary.BigEndian.PutUint32(hdr[:], uint32(len(cell.value)))
			buf = append(buf, hdr[:]...)
			buf = append(buf, cell.value...)
		} else {
			buf = append(buf, 0x00)
		}
	}
	return buf
}

// unpackRowInto decodes a packed value into cells, copying each present value
// into the owned buffers so the cells outlive the store's value buffer. cells
// and owned must both be pre-sized to the schema width (owned[i] backs cells[i]
// and is reused across rows). It returns the cells for convenience.
func unpackRowInto(buf []byte, cells rowCells, owned [][]byte) (rowCells, error) {
	if len(buf) == 0 || buf[0] != rowVersion {
		return nil, fmt.Errorf("%w: row value is not a packed row", ErrBadSchema)
	}
	p := 1
	for i := range cells {
		if p >= len(buf) {
			return nil, fmt.Errorf("%w: truncated packed row", ErrBadSchema)
		}
		switch buf[p] {
		case 0x00:
			cells[i] = rowValue{}
			p++
		case 0x01:
			p++
			if p+4 > len(buf) {
				return nil, fmt.Errorf("%w: truncated length in packed row", ErrBadSchema)
			}
			l := int(binary.BigEndian.Uint32(buf[p:]))
			p += 4
			if p+l > len(buf) {
				return nil, fmt.Errorf("%w: truncated value in packed row", ErrBadSchema)
			}
			v := append(owned[i][:0], buf[p:p+l]...)
			owned[i] = v
			cells[i] = rowValue{value: v, set: true}
			p += l
		default:
			return nil, fmt.Errorf("%w: corrupt presence flag in packed row", ErrBadSchema)
		}
	}
	return cells, nil
}

// unpackRow decodes a packed value into a fresh, schema-width cell set.
func unpackRow(buf []byte, ncols int) (rowCells, error) {
	cells := make(rowCells, ncols)
	return unpackRowInto(buf, cells, make([][]byte, ncols))
}

// rowKey is the single key a catalog row lives under: the row prefix with no
// column suffix. Its empty final segment can never collide with a real column
// name (which may not be empty), so the key is unambiguous.
func rowKey(s *Schema, rowID string) string {
	return RowPrefix(s.DB, s.Table, rowID)
}

// rowExists reports whether a row is present, which is a single read of the
// row's key.
func rowExists(st Store, sch *Schema, rowID string) (bool, error) {
	_, ok, err := st.GetErr(rowKey(sch, rowID))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// readRow assembles one row, reporting whether it exists. Cells come back in
// schema column order with a nil value for a column that is NULL. If scratch is
// non-nil, the row cells and owned buffers are reused from it, eliminating
// allocations on the hot path.
func readRow(st Store, sch *Schema, rowID string, scratch *readScratch) (rowCells, bool, error) {
	value, ok, err := st.GetErr(rowKey(sch, rowID))
	if err != nil || !ok {
		return nil, false, err
	}
	if scratch != nil {
		// Reuse the pre-allocated buffers.
		cells, err := unpackRowInto(value, scratch.cells, scratch.owned)
		if err != nil {
			return nil, false, err
		}
		// Reset owned buffers for next use: clear the backing arrays so the
		// next unpackRowInto can append into them without growing.
		for i := range scratch.owned {
			scratch.owned[i] = scratch.owned[i][:0]
		}
		return cells, true, nil
	}
	cells, err := unpackRow(value, len(sch.Columns))
	if err != nil {
		return nil, false, err
	}
	return cells, true, nil
}

// insertRow writes a row that must not already exist, and reports a duplicate
// rather than overwriting one. The whole row is written as one conditional
// write, so the insert is atomic in the strongest sense the store offers: two
// concurrent inserts of the same row id cannot both report success, and there
// is no window in which a partially written row is visible.
func insertRow(st Store, sch *Schema, rowID string, cells rowCells) error {
	if err := validateRowCells(sch, cells, true); err != nil {
		return err
	}
	applied, err := st.SetIfAbsent(rowKey(sch, rowID), packRow(cells), 0)
	if err != nil {
		return err
	}
	if !applied {
		return ErrDuplicateRow
	}
	return nil
}

// updateRow rewrites the columns the statement named on an existing row and
// reports whether the row existed. A cell that is set with a nil value clears
// the column, since a null column is the absence of its key; a cell that is not
// set is left untouched. The primary key column is not updatable: it is the
// row's identity, and changing it would make this a different row. The caller
// needs the existence result because PostgreSQL reports it as the affected-row
// count, so an UPDATE that matches nothing must say UPDATE 0.
func updateRow(st Store, sch *Schema, rowID string, cells rowCells) (bool, error) {
	if err := validateRowCells(sch, cells, false); err != nil {
		return false, err
	}
	key := rowKey(sch, rowID)
	value, ok, err := st.GetErr(key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	cur, err := unpackRow(value, len(sch.Columns))
	if err != nil {
		return false, err
	}
	for i, cell := range cells {
		// A column the statement did not name keeps whatever it has.
		if i == sch.PrimaryKey || !cell.set {
			continue
		}
		cur[i] = cell
	}
	if err := st.Set(key, packRow(cur), 0); err != nil {
		return false, err
	}
	return true, nil
}

// deleteRow removes a row, reporting whether it existed.
func deleteRow(st Store, sch *Schema, rowID string) (bool, error) {
	return st.Delete(rowKey(sch, rowID))
}

// validateRowCells rejects a cell set that could not be stored as a row: the
// wrong number of columns, a NULL in a NOT NULL column, or a value that does not
// decode as its column's type. The last check is what stops a row being written
// that no reader could later interpret.
func validateRowCells(sch *Schema, cells rowCells, insert bool) error {
	if len(cells) != len(sch.Columns) {
		return fmt.Errorf("%w: %d values for %d columns", ErrBadSchema, len(cells), len(sch.Columns))
	}
	for i, cell := range cells {
		// The primary key is the row's identity, not a value: an update never
		// touches it and an insert fills it from the row id, so it is never
		// this function's business.
		if i == sch.PrimaryKey && !insert {
			continue
		}
		if !cell.set {
			// An insert decides every column, so an undecided one becomes NULL
			// and the schema still has to be asked whether that is allowed. An
			// update may name a subset, and an unmentioned column is left alone.
			if !insert {
				continue
			}
			if !sch.Columns[i].Nullable {
				return fmt.Errorf("%w: null value in column %q", ErrNotNull, sch.Columns[i].Name)
			}
			continue
		}
		if cell.value == nil {
			if !sch.Columns[i].Nullable {
				return fmt.Errorf("%w: null value in column %q", ErrNotNull, sch.Columns[i].Name)
			}
			continue
		}
		if _, err := DecodeValue(sch.Columns[i].Type, cell.value); err != nil {
			return fmt.Errorf("%w: column %q: %w", ErrBadSchema, sch.Columns[i].Name, err)
		}
	}
	return nil
}

// ErrNotNull reports a NULL in a column the schema declares NOT NULL.
var ErrNotNull = fmt.Errorf("%w: not-null violation", ErrBadSchema)

// rowIDFromKey recovers the row id from a key under a table prefix, undoing the
// escaping a row id may have needed. It is the inverse used by scans that walk a
// whole table rather than one row. The packed row key's empty column suffix is
// ignored, so the row id is still recovered from `db/table/<row-id>/`.
func rowIDFromKey(tablePrefix string, key []byte) (string, bool) {
	rest := string(key[len(tablePrefix):])
	i := strings.IndexByte(rest, '/')
	if i < 0 {
		return "", false
	}
	id, err := UnescapeRowID(rest[:i])
	if err != nil {
		return "", false
	}
	return id, true
}
