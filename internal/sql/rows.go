/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: rows.go
Description: Reading and writing one multi-column row against a catalog schema.
A row is a contiguous key range, so a read is a single range scan and a write is
one store operation per column. That asymmetry is the whole difficulty here:
the store offers atomic single-key conditional writes and no transaction
spanning several keys, so this file decides which key represents the row and in
what order the rest are written.

The decision is that a row is identified by its primary key column alone. The
primary key key is written first, conditionally, and that single operation is the
row's commit: a duplicate insert loses, and a delete takes effect before its
remaining columns are removed. It also makes existence a one-key read, so the
common SELECT and UPDATE paths never scan a row that is not there.

The cost of that choice is stated plainly rather than hidden: a row is several
keys, and a crash between two column writes can leave a row that exists without
all of its columns. There is no rollback to lean on here, so the write path
compensates on a *reported* failure and the crash case is left to a future
transaction, which is the one thing the store does not have.
*/
package sql

import (
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

// rowKey is the key of one column of one row.
func rowKey(s *Schema, rowID string, col int) string {
	return ColumnKey(s.DB, s.Table, rowID, s.Columns[col].Name)
}

// primaryKeyKey is the key that represents the row. Its absence means the row
// does not exist.
func primaryKeyKey(s *Schema, rowID string) string { return rowKey(s, rowID, s.PrimaryKey) }

// rowExists reports whether a row is present, which is a single read of the
// primary key rather than a scan of the row's range.
func (s *Server) rowExists(sch *Schema, rowID string) (bool, error) {
	_, ok, err := s.store.GetErr(primaryKeyKey(sch, rowID))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// readRow assembles one row, reporting whether it exists. Cells come back in
// schema column order with nil for a column that has no key.
//
// The cells are placed by looking each scanned column name up in the schema, not
// by position. Key order is lexicographic, so the range scan returns columns
// sorted by name and in no particular relation to declaration order: a table
// declared (id, age, name) scans back age, id, name.
func (s *Server) readRow(sch *Schema, rowID string) (rowCells, bool, error) {
	ok, err := s.rowExists(sch, rowID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	prefix := RowPrefix(sch.DB, sch.Table, rowID)
	cells := make(rowCells, len(sch.Columns))
	_, err = s.store.ScanPrefix(prefix, func(key, value []byte) bool {
		name := columnNameFromKey(prefix, key)
		if col, found := sch.columnIndex(name); found {
			// The value is copied because the scan's buffers are only valid for
			// the duration of the callback, and these outlive it.
			cells[col] = rowValue{value: append([]byte(nil), value...), set: true}
		}
		// A key that names no declared column is not part of the row. The
		// schema defines the row's shape, and this phase has no ALTER TABLE, so
		// such a key can only be a leftover; refusing to read the row because
		// one is present would make a table unreadable by an unrelated write.
		return true
	})
	if err != nil {
		return nil, false, err
	}
	return cells, true, nil
}

// columnNameFromKey strips a row prefix off a key, yielding the column name.
// The prefix ends at the row id's separator, and a column name cannot contain a
// separator, so the remainder is exactly the name.
func columnNameFromKey(prefix string, key []byte) string {
	return string(key[len(prefix):])
}

// insertRow writes a row that must not already exist, and reports a duplicate
// rather than overwriting one.
//
// The primary key is written first and conditionally. That is what makes the
// insert atomic in the only sense the store can offer: two concurrent inserts of
// the same row id cannot both report success. Writing the other columns first
// would be worse than non-atomic -- a losing insert would have already
// overwritten the winner's columns before discovering the conflict.
func (s *Server) insertRow(sch *Schema, rowID string, cells rowCells) error {
	if err := validateRowCells(sch, cells, true); err != nil {
		return err
	}
	pk := primaryKeyKey(sch, rowID)
	applied, err := s.store.SetIfAbsent(pk, cells[sch.PrimaryKey].value, 0)
	if err != nil {
		return err
	}
	if !applied {
		return ErrDuplicateRow
	}
	// The row is now committed. A failure below leaves it committed but
	// incomplete, so the written columns are removed again before the error is
	// reported. This covers a reported store failure, which is the case a
	// client can retry; it does not cover a crash, which no compensation in
	// this process could survive.
	for i, cell := range cells {
		if i == sch.PrimaryKey || !cell.set || cell.value == nil {
			continue
		}
		// A plain Set, not SetIfPresent. The primary key has already been
		// claimed, so this row is ours and these keys are being created rather
		// than updated; a conditional update would simply decline to write keys
		// that do not exist yet, leaving a row that existed but had none of its
		// non-key columns.
		if err := s.store.Set(rowKey(sch, rowID, i), cell.value, 0); err != nil {
			s.compensateRow(sch, rowID)
			return err
		}
	}
	return nil
}

// updateRow rewrites the columns the statement named on an existing row. A
// cell that is set with a nil value clears the column, since a null column is
// the absence of its key; a cell that is not set is left untouched. The primary
// key column is not updatable: it is the row's identity, and changing it would
// make this a different row.
func (s *Server) updateRow(sch *Schema, rowID string, cells rowCells) error {
	if err := validateRowCells(sch, cells, false); err != nil {
		return err
	}
	ok, err := s.rowExists(sch, rowID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	for i, cell := range cells {
		// A column the statement did not name keeps whatever it has, so its
		// key is neither written nor removed.
		if i == sch.PrimaryKey || !cell.set {
			continue
		}
		key := rowKey(sch, rowID, i)
		if cell.value == nil {
			if _, err := s.store.Delete(key); err != nil {
				return err
			}
			continue
		}
		// A plain Set, not SetIfPresent. The row is known to exist by now --
		// rowExists checked the primary key above -- but the column being named
		// may have no key yet, because a nullable column the INSERT omitted was
		// never written. A write conditional on that column's own existence would
		// decline, and this method has no way to report the affected-row count, so
		// the UPDATE would silently do nothing and still succeed.
		if err := s.store.Set(key, cell.value, 0); err != nil {
			return err
		}
	}
	return nil
}

// deleteRow removes a row, reporting whether it existed.
//
// The primary key is deleted first so the row stops existing immediately, before
// its remaining columns are swept. Doing it the other way round would leave a
// window in which the row is still visible but has lost columns, which is the
// worse failure: a reader would see a truncated row rather than none.
func (s *Server) deleteRow(sch *Schema, rowID string) (bool, error) {
	pk := primaryKeyKey(sch, rowID)
	ok, err := s.store.Delete(pk)
	if err != nil || !ok {
		return false, err
	}
	if _, err := s.sweepRow(sch, rowID); err != nil {
		return true, err
	}
	return true, nil
}

// sweepRow removes every key of a row except the primary key, which the caller
// has already dealt with.
//
// The keys are collected first and deleted afterwards, rather than deleting from
// inside the scan callback. That is not a style preference: a scan callback runs
// under the storage engine's read lock, and a write issued from inside it would
// need the write lock that the read lock is holding. The cluster and router
// stores happen to materialise their runs before invoking the callback, so the
// callback there is already outside the lock and this code was safe -- but
// nothing in the Store contract promises that, and a direct engine-backed store
// would have deadlocked on the first row delete. Materialising makes the
// requirement local instead of relying on every implementation getting it right.
func (s *Server) sweepRow(sch *Schema, rowID string) (int, error) {
	prefix := RowPrefix(sch.DB, sch.Table, rowID)
	pk := primaryKeyKey(sch, rowID)
	keys := make([]string, 0, 8)
	_, err := s.store.ScanPrefix(prefix, func(key, _ []byte) bool {
		// The key is copied because the scan's buffer is only valid for the
		// duration of the callback, and these are deleted after it returns.
		if string(key) != pk {
			keys = append(keys, string(key))
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	deleted := 0
	var firstErr error
	for _, key := range keys {
		if _, err := s.store.Delete(key); err != nil {
			// A delete failure mid-sweep leaves orphan column keys. They are
			// invisible because the primary key is gone, so this is reported
			// but not compensated: there is no row left to protect. The rest of
			// the sweep still runs, because stopping would leave more orphans
			// for no benefit.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		deleted++
	}
	return deleted, firstErr
}

// compensateRow removes a partially inserted row, best effort. It is called when
// insertRow has already committed the primary key and then failed, so the row
// must not be left behind.
func (s *Server) compensateRow(sch *Schema, rowID string) {
	if _, err := s.store.Delete(primaryKeyKey(sch, rowID)); err != nil {
		return
	}
	_, _ = s.sweepRow(sch, rowID)
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
// whole table rather than one row.
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
