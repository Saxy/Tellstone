/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: rows_test.go
Description: Tests for multi-column row storage and the DDL path that publishes
schemas. The properties that matter here are about failure, not the happy path:
what a row looks like after a partial write, whether one row's columns can be
confused with another's, and whether a row id can reach keys it should not.
*/
package sql

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

// newRowServer builds a server over an in-memory store with one catalog table.
func newRowServer(t *testing.T, sch *Schema) (*Server, *testStore) {
	t.Helper()
	store := newTestStore()
	srv := &Server{store: store}
	if err := NewCatalog(store, DefaultDB).Create(sch); err != nil {
		t.Fatal(err)
	}
	return srv, store
}

func rowTestSchema() *Schema {
	return &Schema{
		DB:    DefaultDB,
		Table: "users",
		Columns: []Column{
			{Name: "id", Type: TypeBigInt},
			{Name: "age", Type: TypeInt, Nullable: true},
			{Name: "name", Type: TypeVarchar, Nullable: true},
		},
		PrimaryKey: 0,
	}
}

func cellsFor(t *testing.T, sch *Schema, values map[string]any) rowCells {
	t.Helper()
	cells := make(rowCells, len(sch.Columns))
	for i, col := range sch.Columns {
		v, ok := values[col.Name]
		if !ok {
			continue
		}
		enc, err := EncodeValue(col.Type, v)
		if err != nil {
			t.Fatal(err)
		}
		cells[i] = rowValue{value: enc, set: true}
	}
	return cells
}

// TestRowRoundTrip is the basic contract: a written row reads back identical,
// in schema column order, with the columns placed by name rather than by the
// order the scan happened to return them.
func TestRowRoundTrip(t *testing.T) {
	sch := rowTestSchema()
	srv, _ := newRowServer(t, sch)
	// Declaration order is id, age, name. Key order is age, id, name, so a
	// reconstruction that trusted scan position would swap age and name.
	if err := srv.insertRow(sch, "7", cellsFor(t, sch, map[string]any{
		"id": int64(7), "age": int64(42), "name": "ada",
	})); err != nil {
		t.Fatal(err)
	}
	cells, found, err := srv.readRow(sch, "7")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("row was not found after insert")
	}
	if len(cells) != 3 {
		t.Fatalf("got %d cells", len(cells))
	}
	for i, want := range []any{int64(7), int64(42), "ada"} {
		got, err := DecodeValue(sch.Columns[i].Type, cells[i].value)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("column %d is %#v, want %#v", i, got, want)
		}
	}
}

// TestRowExistenceIsThePrimaryKey pins the decision that makes inserts atomic:
// a row is present exactly when its primary key column key is present. If this
// ever changed to "any column present", a partial write would be visible as a
// phantom row and INSERT would lose its single atomic commit point.
func TestRowExistenceIsThePrimaryKey(t *testing.T) {
	sch := rowTestSchema()
	srv, store := newRowServer(t, sch)
	// A stray column key with no primary key must not make a row exist.
	if err := store.Set(ColumnKey(DefaultDB, "users", "7", "name"), []byte("orphan"), 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := srv.rowExists(sch, "7"); err != nil || ok {
		t.Fatalf("a row with only an orphan column reported present: %v, %v", ok, err)
	}
	if _, found, err := srv.readRow(sch, "7"); err != nil || found {
		t.Fatalf("readRow returned a row built from an orphan column: %v, %v", found, err)
	}
	// Once the primary key lands, the same row is present.
	if err := store.Set(ColumnKey(DefaultDB, "users", "7", "id"), []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := srv.rowExists(sch, "7"); err != nil || !ok {
		t.Fatalf("row with a primary key reported absent: %v, %v", ok, err)
	}
}

func TestRowInsertRejectsDuplicate(t *testing.T) {
	sch := rowTestSchema()
	srv, _ := newRowServer(t, sch)
	first := cellsFor(t, sch, map[string]any{"id": int64(1), "name": "first"})
	if err := srv.insertRow(sch, "1", first); err != nil {
		t.Fatal(err)
	}
	// A second insert must not overwrite the first: two concurrent inserts of
	// the same row id cannot both report success.
	err := srv.insertRow(sch, "1", cellsFor(t, sch, map[string]any{"id": int64(1), "name": "second"}))
	if err == nil {
		t.Fatal("duplicate insert was accepted")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate insert reported %v", err)
	}
	cells, _, err := srv.readRow(sch, "1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := DecodeValue(TypeVarchar, cells[2].value)
	if got != "first" {
		t.Fatalf("the losing insert overwrote name with %q", got)
	}
}

func TestRowNullAndNotNull(t *testing.T) {
	sch := rowTestSchema()
	sch.Columns[1].Nullable = false // age becomes NOT NULL
	srv, _ := newRowServer(t, sch)

	// age omitted entirely on an insert: no default, NOT NULL, so refused.
	err := srv.insertRow(sch, "1", cellsFor(t, sch, map[string]any{"id": int64(1)}))
	if err == nil {
		t.Fatal("insert with an unset NOT NULL column was accepted")
	}
	if !strings.Contains(err.Error(), "not-null") {
		t.Fatalf("unset NOT NULL reported %v", err)
	}
	// An explicit NULL is the same violation.
	cells := cellsFor(t, sch, map[string]any{"id": int64(1)})
	cells[1] = rowValue{value: nil, set: true}
	if err := srv.insertRow(sch, "1", cells); err == nil {
		t.Fatal("insert with a NULL NOT NULL column was accepted")
	}
	// The nullable column may be absent, and reads back as NULL.
	if err := srv.insertRow(sch, "2", cellsFor(t, sch, map[string]any{"id": int64(2), "age": int64(1)})); err != nil {
		t.Fatal(err)
	}
	got, found, err := srv.readRow(sch, "2")
	if err != nil || !found {
		t.Fatalf("read %v, %v", found, err)
	}
	if got[2].set {
		t.Fatal("an absent column read back as present")
	}
}

func TestRowUpdateTouchesOnlyNamedColumns(t *testing.T) {
	sch := rowTestSchema()
	srv, _ := newRowServer(t, sch)
	if err := srv.insertRow(sch, "1", cellsFor(t, sch, map[string]any{
		"id": int64(1), "age": int64(30), "name": "ada",
	})); err != nil {
		t.Fatal(err)
	}
	// Naming one column must leave the others alone, and must be able to clear
	// a named column to NULL. A plain nil-slice cell set could not express
	// both operations.
	cells := make(rowCells, len(sch.Columns))
	cells[2] = rowValue{value: nil, set: true} // name -> NULL
	if err := srv.updateRow(sch, "1", cells); err != nil {
		t.Fatal(err)
	}
	got, _, err := srv.readRow(sch, "1")
	if err != nil {
		t.Fatal(err)
	}
	age, _ := DecodeValue(TypeInt, got[1].value)
	if age != int64(30) {
		t.Fatalf("unnamed column age was disturbed: %v", age)
	}
	if got[2].set {
		t.Fatal("named column was not cleared to NULL")
	}
	// An update to a row that is not there reports no affected row rather than
	// creating one.
	if err := srv.updateRow(sch, "missing", cells); err != nil {
		t.Fatal(err)
	}
	if ok, _ := srv.rowExists(sch, "missing"); ok {
		t.Fatal("update created a row that did not exist")
	}
}

// TestRowDeleteRemovesEveryKey checks the sweep, not just the primary key. A
// delete that left column keys behind would let a later CREATE TABLE of the
// same name inherit them, and would make a table's row count wrong.
func TestRowDeleteRemovesEveryKey(t *testing.T) {
	sch := rowTestSchema()
	srv, store := newRowServer(t, sch)
	if err := srv.insertRow(sch, "1", cellsFor(t, sch, map[string]any{
		"id": int64(1), "age": int64(30), "name": "ada",
	})); err != nil {
		t.Fatal(err)
	}
	ok, err := srv.deleteRow(sch, "1")
	if err != nil || !ok {
		t.Fatalf("delete = %v, %v", ok, err)
	}
	remaining := 0
	store.ScanPrefix(TablePrefix(DefaultDB, "users"), func(_, _ []byte) bool {
		remaining++
		return true
	})
	if remaining != 0 {
		t.Fatalf("delete left %d keys behind", remaining)
	}
	ok, err = srv.deleteRow(sch, "1")
	if err != nil || ok {
		t.Fatalf("second delete = %v, %v; want no row and no error", ok, err)
	}
}

// TestRowIDContainingTheSeparator is the case the escaping exists for. A row id
// holding a raw '/' would otherwise forge a column boundary and make one row
// look like several.
func TestRowIDContainingTheSeparator(t *testing.T) {
	sch := rowTestSchema()
	srv, _ := newRowServer(t, sch)
	for _, id := range []string{"a/b", "a%b", "%2F", "plain"} {
		if err := srv.insertRow(sch, id, cellsFor(t, sch, map[string]any{
			"id": int64(1), "name": "row-" + id,
		})); err != nil {
			t.Fatalf("insert %q: %v", id, err)
		}
	}
	// Each id must read back as its own row, not as a neighbour's.
	for _, id := range []string{"a/b", "a%b", "%2F", "plain"} {
		cells, found, err := srv.readRow(sch, id)
		if err != nil {
			t.Fatalf("read %q: %v", id, err)
		}
		if !found {
			t.Fatalf("row %q not found", id)
		}
		got, _ := DecodeValue(TypeVarchar, cells[2].value)
		if got != "row-"+id {
			t.Fatalf("row %q read back name %q", id, got)
		}
	}
	n, err := NewCatalog(srv.store, DefaultDB).RowCount("users")
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("RowCount = %d, want 4", n)
	}
}

// TestRowIDIsNotConfusedWithALongerID is why the row prefix ends in a separator:
// without it, the range scan for row "1" would match the keys of row "10".
func TestRowIDIsNotConfusedWithALongerID(t *testing.T) {
	sch := rowTestSchema()
	srv, _ := newRowServer(t, sch)
	if err := srv.insertRow(sch, "1", cellsFor(t, sch, map[string]any{"id": int64(1), "name": "one"})); err != nil {
		t.Fatal(err)
	}
	if err := srv.insertRow(sch, "10", cellsFor(t, sch, map[string]any{"id": int64(10), "name": "ten"})); err != nil {
		t.Fatal(err)
	}
	one, _, err := srv.readRow(sch, "1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := DecodeValue(TypeVarchar, one[2].value)
	if got != "one" {
		t.Fatalf("row \"1\" read %q; the scan leaked row \"10\" into it", got)
	}
}

// TestRowInsertCompensatesOnFailure checks the row does not survive a write
// that reported an error. The primary key is committed first, so a failure on a
// later column has to remove what was already written.
func TestRowInsertCompensatesOnFailure(t *testing.T) {
	sch := rowTestSchema()
	store := newTestStore()
	srv := &Server{store: store}
	if err := NewCatalog(store, DefaultDB).Create(sch); err != nil {
		t.Fatal(err)
	}
	// Fail every write after the first, which is the primary key claim. The
	// claim uses SetIfAbsent and the remaining columns use Set, so both have to
	// be intercepted for the failure to land where the compensation path runs.
	fail := &failingStore{testStore: store, failAfter: 1}
	srv.store = fail

	cells := cellsFor(t, sch, map[string]any{"id": int64(1), "age": int64(30), "name": "ada"})
	if err := srv.insertRow(sch, "1", cells); err == nil {
		t.Fatal("insert reported success despite a failing store")
	}
	// Nothing may be left behind, or the next insert of the same row id would
	// report a duplicate against a row that does not exist.
	if ok, _ := srv.rowExists(sch, "1"); ok {
		t.Fatal("a failed insert left the primary key behind")
	}
	remaining := 0
	store.ScanPrefix(TablePrefix(DefaultDB, "users"), func(_, _ []byte) bool {
		remaining++
		return true
	})
	if remaining != 0 {
		t.Fatalf("a failed insert left %d keys behind", remaining)
	}
}

// failingStore fails every write after the first n succeed.
type failingStore struct {
	*testStore
	failAfter int
	writes    int
}

func (f *failingStore) SetIfAbsent(key string, v []byte, ttl time.Duration) (bool, error) {
	f.writes++
	if f.writes > f.failAfter {
		return false, errors.New("injected write failure")
	}
	return f.testStore.SetIfAbsent(key, v, ttl)
}

func (f *failingStore) Set(key string, v []byte, ttl time.Duration) error {
	f.writes++
	if f.writes > f.failAfter {
		return errors.New("injected write failure")
	}
	return f.testStore.Set(key, v, ttl)
}

func (f *failingStore) SetIfPresent(key string, v []byte, ttl time.Duration) (bool, error) {
	f.writes++
	if f.writes > f.failAfter {
		return false, errors.New("injected write failure")
	}
	return f.testStore.SetIfPresent(key, v, ttl)
}

func TestRowRejectsUndecodableValues(t *testing.T) {
	sch := rowTestSchema()
	srv, _ := newRowServer(t, sch)
	// A value that does not decode as its column's type must be refused at
	// write time, not stored and left to fail on the next read.
	cells := cellsFor(t, sch, map[string]any{"id": int64(1), "name": "ada"})
	cells[1] = rowValue{value: []byte("not an int"), set: true}
	if err := srv.insertRow(sch, "1", cells); err == nil {
		t.Fatal("insert stored a value that does not decode as int")
	}
}

func TestWireCellAndEncodeTextAs(t *testing.T) {
	t.Run("round trip through text", func(t *testing.T) {
		cases := []struct {
			typ  ColumnType
			text string
			want any
		}{
			{TypeInt, "42", int64(42)},
			{TypeInt, "-7", int64(-7)},
			{TypeBigInt, "9007199254740993", int64(9007199254740993)},
			{TypeFloat, "1.5", 1.5},
			{TypeFloat, "-0.25", -0.25},
			{TypeBool, "true", "t"},
			{TypeBool, "f", "f"},
			{TypeVarchar, "hello", "hello"},
			{TypeVarchar, "", ""},
			{TypeJSONB, `{"a":1}`, `{"a":1}`},
			{TypeBytes, `\xdeadbeef`, []byte{0xde, 0xad, 0xbe, 0xef}},
		}
		for _, tc := range cases {
			enc, err := encodeTextAs(tc.typ, []byte(tc.text))
			if err != nil {
				t.Fatalf("encodeTextAs(%s, %q): %v", tc.typ, tc.text, err)
			}
			cell, err := wireCell(tc.typ, enc)
			if err != nil {
				t.Fatalf("wireCell(%s): %v", tc.typ, err)
			}
			// A bool is re-rendered in PostgreSQL's own text form, so its cell
			// is not the text that was sent; the rest round trip verbatim.
			if tc.typ == TypeBool {
				if string(cell) != tc.want {
					t.Fatalf("bool cell is %q, want %q", cell, tc.want)
				}
				decoded, err := DecodeValue(tc.typ, enc)
				if err != nil {
					t.Fatal(err)
				}
				wantBool := strings.HasPrefix(strings.ToLower(tc.text), "t")
				if decoded != wantBool {
					t.Fatalf("bool decoded to %#v from %q, want %v", decoded, tc.text, wantBool)
				}
				continue
			}
			if string(cell) != tc.text {
				t.Fatalf("%s cell is %q, want %q", tc.typ, cell, tc.text)
			}
			decoded, err := DecodeValue(tc.typ, enc)
			if err != nil {
				t.Fatal(err)
			}
			switch want := tc.want.(type) {
			case []byte:
				got, ok := decoded.([]byte)
				if !ok || !bytes.Equal(got, want) {
					t.Fatalf("%s decoded to %#v, want %#v", tc.typ, decoded, want)
				}
			default:
				if decoded != want {
					t.Fatalf("%s decoded to %#v, want %#v", tc.typ, decoded, want)
				}
			}
		}
	})

	t.Run("rejects text that is not the type", func(t *testing.T) {
		for _, tc := range []struct {
			typ  ColumnType
			text string
		}{
			{TypeInt, "abc"},
			{TypeInt, ""},
			{TypeFloat, "1.2.3"},
			{TypeBool, "maybe"},
			{TypeTimestamp, "not a time"},
			{TypeJSONB, "{oops"},
		} {
			if _, err := encodeTextAs(tc.typ, []byte(tc.text)); err == nil {
				t.Fatalf("encodeTextAs(%s, %q) was accepted", tc.typ, tc.text)
			}
		}
	})

	t.Run("oid per type", func(t *testing.T) {
		for typ, want := range map[ColumnType]int32{
			TypeInt: 23, TypeBigInt: 20, TypeFloat: 701, TypeVarchar: 25,
			TypeBytes: 17, TypeBool: 16, TypeJSONB: 3802, TypeTimestamp: 1184,
		} {
			if got := oidOf(typ); got != want {
				t.Errorf("oidOf(%s) = %d, want %d", typ, got, want)
			}
		}
	})
}
