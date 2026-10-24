/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: pgcat_store.go
Description: The store that serves the virtual catalog relations. A client's
introspection reads the engine's real schema rather than a copy of it, so the
catalog relations are projected at query time instead of being stored: this
store answers a read of a catalog key from the projection and passes every
other key down to the store below it.
*/
package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// catalogStore serves the virtual catalog relations and delegates every other
// key to the store below it.
//
// The catalog relations are read-only. A write to one is refused rather than
// silently dropped, because a catalog that accepts a row it never reports is
// worse than one that refuses it.
type catalogStore struct {
	base Store
	ctx  *catalogContext
	// built caches the projected rows per relation, so a scan that reads many
	// keys of one relation projects it once.
	built map[string]*catalogRows
}

// catalogRows is one relation's projected rows: the row identifiers and the
// cells, in the order the rows are returned.
type catalogRows struct {
	ids   []string
	cells [][]rowValue
}

// newCatalogStore wraps a store so that the catalog relations are served from
// the projection.
func newCatalogStore(base Store, cat *Catalog, db, user string) *catalogStore {
	return &catalogStore{
		base:  base,
		ctx:   &catalogContext{cat: cat, db: db, user: user},
		built: map[string]*catalogRows{},
	}
}

// rows projects a relation, caching the result.
func (s *catalogStore) rows(rel *catalogRelation) (*catalogRows, error) {
	if r, ok := s.built[rel.name]; ok {
		return r, nil
	}
	if rel.rows == nil {
		// A relation with no row builder is one the clients in scope do not
		// read. It is present so that naming it is not an error, and it is
		// empty.
		out := &catalogRows{}
		s.built[rel.name] = out
		return out, nil
	}
	cells, err := rel.rows(s.ctx)
	if err != nil {
		return nil, err
	}
	out := &catalogRows{ids: make([]string, 0, len(cells)), cells: cells}
	for _, row := range cells {
		out.ids = append(out.ids, catalogRowID(rel, row))
	}
	s.built[rel.name] = out
	return out, nil
}

// catalogRowID derives a row's key segment from the columns that identify it.
//
// A single-column key yields the identifier the engine's own key layout
// expects -- the sign-flipped hex form for an integer, the text itself for a
// string -- so that a point lookup on the key addresses the projected row. A
// composite key yields the parts joined, which identifies the row but is not
// derivable from one column, so such a relation is read by a scan.
func catalogRowID(rel *catalogRelation, cells []rowValue) string {
	parts := make([]string, 0, len(rel.key))
	for _, name := range rel.key {
		idx := rel.columnIndex(name)
		if idx < 0 || idx >= len(cells) {
			return ""
		}
		cell := cells[idx]
		if !cell.set {
			return ""
		}
		switch rel.columns[idx].typ {
		case TypeInt, TypeBigInt:
			v, err := DecodeValue(rel.columns[idx].typ, cell.value)
			if err != nil {
				return ""
			}
			n, _ := v.(int64)
			parts = append(parts, keyspace.EncodeIntRowID(n))
		default:
			parts = append(parts, string(cell.value))
		}
	}
	return strings.Join(parts, ":")
}

// parseKey splits a key into the relation it names, the row and the column.
func (s *catalogStore) parseKey(key string) (*catalogRelation, string, string, bool) {
	schema, rest, ok := cut(key, '/')
	if !ok || !isCatalogSchema(schema) {
		return nil, "", "", false
	}
	table, rest, ok := cut(rest, '/')
	if !ok {
		return nil, "", "", false
	}
	rel := lookupCatalogRelation(schema, table)
	if rel == nil {
		return nil, "", "", false
	}
	rowID, col, ok := cut(rest, '/')
	if !ok {
		return nil, "", "", false
	}
	return rel, rowID, col, true
}

// cut splits a string at its first separator.
func cut(s string, sep byte) (before, after string, ok bool) {
	i := strings.IndexByte(s, sep)
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// GetErr reads one key. A catalog key is answered from the projection; every
// other key is read from the store below.
func (s *catalogStore) GetErr(key string) ([]byte, bool, error) {
	rel, rowID, col, ok := s.parseKey(key)
	if !ok {
		return s.base.GetErr(key)
	}
	rows, err := s.rows(rel)
	if err != nil {
		return nil, false, err
	}
	idx := rel.columnIndex(col)
	if idx < 0 {
		return nil, false, nil
	}
	for i, id := range rows.ids {
		if keyspace.EscapeRowID(id) != rowID {
			continue
		}
		cell := rows.cells[i][idx]
		if !cell.set {
			// A null column is the absence of its key, so a read of it finds
			// nothing, which is what the layout says a null is.
			return nil, false, nil
		}
		return cell.value, true, nil
	}
	return nil, false, nil
}

// ScanPrefix walks a key range. A range inside a catalog relation is answered
// from the projection; every other range is walked by the store below.
func (s *catalogStore) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	rel, ok := s.relationForPrefix(prefix)
	if !ok {
		return s.base.ScanPrefix(prefix, fn)
	}
	rows, err := s.rows(rel)
	if err != nil {
		return 0, err
	}
	n := 0
	for i, id := range rows.ids {
		rowPrefix := keyspace.RowPrefix(rel.schema, rel.name, id)
		if !strings.HasPrefix(rowPrefix, prefix) {
			continue
		}
		for j, col := range rel.columns {
			cell := rows.cells[i][j]
			if !cell.set {
				// A null column has no key, so it is not written.
				continue
			}
			key := keyspace.ColumnKey(rel.schema, rel.name, id, col.name)
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if !fn([]byte(key), cell.value) {
				return n, nil
			}
			n++
		}
	}
	return n, nil
}

// relationForPrefix finds the relation a key range belongs to.
func (s *catalogStore) relationForPrefix(prefix string) (*catalogRelation, bool) {
	for _, rel := range catalogRelations {
		if strings.HasPrefix(prefix, keyspace.TablePrefix(rel.schema, rel.name)) {
			return rel, true
		}
	}
	return nil, false
}

// Set refuses a write to a catalog relation.
func (s *catalogStore) Set(key string, value []byte, ttl time.Duration) error {
	return s.readOnly(key)
}

// SetIfAbsent refuses a write to a catalog relation.
func (s *catalogStore) SetIfAbsent(key string, value []byte, ttl time.Duration) (bool, error) {
	return false, s.readOnly(key)
}

// SetIfPresent refuses a write to a catalog relation.
func (s *catalogStore) SetIfPresent(key string, value []byte, ttl time.Duration) (bool, error) {
	return false, s.readOnly(key)
}

// Delete refuses a write to a catalog relation.
func (s *catalogStore) Delete(key string) (bool, error) {
	return false, s.readOnly(key)
}

// readOnly reports that a catalog relation cannot be written.
func (s *catalogStore) readOnly(key string) error {
	return &pgError{
		code: errFeatureNotSupported,
		msg:  fmt.Sprintf("catalog relation %q is read-only", key),
	}
}
