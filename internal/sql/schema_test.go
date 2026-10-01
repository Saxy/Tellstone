/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: schema_test.go
Description: Tests for the table catalog and the DDL that writes it. The
properties checked here are the ones a client's next statement depends on: a
schema survives a round trip byte for byte, a rejected CREATE TABLE leaves the
catalog untouched, and a schema read back from the store is the one that was
written rather than something the decoder could have substituted.
*/
package sql

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func testSchema() *Schema {
	return &Schema{
		DB:    DefaultDB,
		Table: "users",
		Columns: []Column{
			{Name: "id", Type: TypeBigInt},
			{Name: "name", Type: TypeVarchar, Nullable: true},
			{Name: "age", Type: TypeInt, Nullable: true},
		},
		PrimaryKey: 0,
	}
}

func TestSchemaValidateRejectsUnusableDefinitions(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Schema)
		want error
	}{
		{"no columns", func(s *Schema) { s.Columns = nil }, ErrBadSchema},
		{"pk out of range", func(s *Schema) { s.PrimaryKey = 9 }, ErrBadSchema},
		{"pk unset", func(s *Schema) { s.PrimaryKey = -1 }, ErrBadSchema},
		{"column without a type", func(s *Schema) { s.Columns[1].Type = TypeInvalid }, ErrBadSchema},
		{"duplicate column", func(s *Schema) { s.Columns[2].Name = "name" }, ErrDuplicateColumn},
		{"duplicate column differing case", func(s *Schema) { s.Columns[2].Name = "Name" }, ErrDuplicateColumn},
		{"table name with a separator", func(s *Schema) { s.Table = "a/b" }, ErrInvalidSegment},
		{"table name with a reserved marker", func(s *Schema) { s.Table = "~meta" }, ErrInvalidSegment},
		{"column name with a separator", func(s *Schema) { s.Columns[1].Name = "x/y" }, ErrInvalidSegment},
		{"default on the primary key", func(s *Schema) { s.Columns[0].Default = []byte("x") }, ErrBadSchema},
		{"default of the wrong type", func(s *Schema) { s.Columns[2].Default = []byte("not an int") }, ErrBadSchema},
		{"jsonb default that is not json", func(s *Schema) {
			s.Columns[1].Type = TypeJSONB
			s.Columns[1].Default = []byte("{oops")
		}, ErrBadSchema},
		{"too many columns", func(s *Schema) {
			cols := make([]Column, MaxColumns+1)
			for i := range cols {
				cols[i] = Column{Name: string(rune('a'+i%26)) + strings.Repeat("z", i/26), Type: TypeVarchar, Nullable: true}
			}
			s.Columns = cols
		}, ErrBadSchema},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testSchema()
			tc.mut(s)
			err := s.Validate()
			if err == nil {
				t.Fatalf("Validate accepted the schema, want %v", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want.Error()) {
				t.Fatalf("Validate reported %v, want it to mention %v", err, tc.want)
			}
			// A schema that fails validation must never reach the store, so
			// encoding it has to fail too.
			if _, err := EncodeSchema(s); err == nil {
				t.Fatal("EncodeSchema accepted an invalid schema")
			}
		})
	}
}

func TestSchemaRoundTrip(t *testing.T) {
	cases := map[string]*Schema{
		"minimal": testSchema(),
		"defaults and types": {
			DB:    DefaultDB,
			Table: "mixed",
			Columns: []Column{
				{Name: "id", Type: TypeBigInt},
				{Name: "score", Type: TypeFloat, Nullable: true, Default: mustEncode(t, TypeFloat, 1.5)},
				{Name: "active", Type: TypeBool, Nullable: true, Default: mustEncode(t, TypeBool, true)},
				{Name: "at", Type: TypeTimestamp, Nullable: true, Default: mustEncode(t, TypeTimestamp, time.Unix(1700000000, 123).UTC())},
				{Name: "blob", Type: TypeBytes, Nullable: true, Default: []byte{0x00, 0xff, 0x7f}},
				{Name: "doc", Type: TypeJSONB, Nullable: true, Default: []byte(`{"a":1}`)},
				{Name: "note", Type: TypeVarchar, Nullable: true, Default: []byte("")},
			},
			PrimaryKey: 0,
		},
		"single column": {
			DB: DefaultDB, Table: "t", Columns: []Column{{Name: "id", Type: TypeInt}}, PrimaryKey: 0,
		},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			blob, err := EncodeSchema(in)
			if err != nil {
				t.Fatal(err)
			}
			out, err := DecodeSchema(in.DB, in.Table, blob)
			if err != nil {
				t.Fatal(err)
			}
			again, err := EncodeSchema(out)
			if err != nil {
				t.Fatal(err)
			}
			// Byte equality, not just structural equality: the encoding has to
			// be stable, because the blob is replicated and a re-encode that
			// differs would look like divergence during replay.
			if !bytes.Equal(blob, again) {
				t.Fatalf("re-encode differs:\n%x\n%x", blob, again)
			}
			if out.PrimaryKey != in.PrimaryKey || len(out.Columns) != len(in.Columns) {
				t.Fatalf("shape changed: %+v", out)
			}
			for i := range in.Columns {
				if out.Columns[i].Name != in.Columns[i].Name ||
					out.Columns[i].Type != in.Columns[i].Type ||
					out.Columns[i].Nullable != in.Columns[i].Nullable ||
					!bytes.Equal(out.Columns[i].Default, in.Columns[i].Default) {
					t.Fatalf("column %d changed: %+v vs %+v", i, out.Columns[i], in.Columns[i])
				}
			}
		})
	}
}

// TestSchemaEncodeIsDeterministic guards the claim the catalog's replication
// depends on: the same schema must encode identically every time, with nothing
// drawn from a map or a clock.
func TestSchemaEncodeIsDeterministic(t *testing.T) {
	first, err := EncodeSchema(testSchema())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		again, err := EncodeSchema(testSchema())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("encoding varied on run %d", i)
		}
	}
}

func TestDecodeSchemaRejectsDamagedBlobs(t *testing.T) {
	good, err := EncodeSchema(testSchema())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func([]byte) []byte{
		"empty":         func(b []byte) []byte { return nil },
		"too short":     func(b []byte) []byte { return b[:3] },
		"bad magic":     func(b []byte) []byte { c := clone(b); c[0] = 'X'; return c },
		"bad version":   func(b []byte) []byte { c := clone(b); c[4] = 99; return c },
		"truncated":     func(b []byte) []byte { return b[:len(b)-4] },
		"trailing junk": func(b []byte) []byte { return append(clone(b), 0xde, 0xad) },
		"count lies":    func(b []byte) []byte { c := clone(b); c[5] = 0xff; return c },
		"unknown column flags": func(b []byte) []byte {
			c := clone(b)
			c[len(schemaMagic)+1+4+len(DefaultDB)+len("users")+1] |= 0x80
			return c
		},
		"default length beyond the sentinel": func(b []byte) []byte {
			c := clone(b)
			// The default length of the first column, which is the byte pair
			// just past the name.
			at := len(schemaMagic) + 1 + 4 + len(DefaultDB) + len("users") + 1
			c = append(c[:at], appendU16(append([]byte(nil), c[at:]...), 0xfffe)...)
			return c
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeSchema(DefaultDB, "users", damage(good))
			if err == nil {
				t.Fatal("DecodeSchema accepted a damaged blob")
			}
			// Every failure has to be reported as corruption, never as a
			// missing table, so a caller cannot mistake it for "drop then
			// recreate" and overwrite a schema it failed to read.
			if !strings.Contains(err.Error(), ErrSchemaCorrupt.Error()) {
				t.Fatalf("error %v is not reported as corruption", err)
			}
		})
	}
}

// TestDecodeSchemaChecksTheKeyItCameFrom matters because the catalog shares its
// keyspace with user data: a value written under one table's key and read back
// under another must not be adopted as that table's schema.
func TestDecodeSchemaChecksTheKeyItCameFrom(t *testing.T) {
	blob, err := EncodeSchema(testSchema())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSchema(DefaultDB, "other", blob); err == nil {
		t.Fatal("DecodeSchema accepted a blob whose embedded names differ from the key")
	}
}

func TestCatalogCreateGetDrop(t *testing.T) {
	store := newTestStore()
	cat := NewCatalog(store, DefaultDB)

	if _, err := cat.Get("users"); !strings.Contains(err.Error(), ErrNoSuchTable.Error()) {
		t.Fatalf("Get on an empty catalog returned %v, want no such table", err)
	}
	if err := cat.Create(testSchema()); err != nil {
		t.Fatal(err)
	}
	got, err := cat.Get("users")
	if err != nil {
		t.Fatal(err)
	}
	if got.PrimaryKey != 0 || got.Columns[1].Name != "name" {
		t.Fatalf("read back %+v", got)
	}
	// The catalog entry must sit in the meta subtree, never in the user range.
	if !IsMetaKey(MetaTableKey(DefaultDB, "users")) {
		t.Fatal("catalog entry is not under the meta prefix")
	}

	// A second CREATE must not clobber the first, because rows may already have
	// been written against the definition the second one replaces.
	if err := cat.Create(testSchema()); !strings.Contains(err.Error(), ErrTableExists.Error()) {
		t.Fatalf("duplicate Create returned %v, want table exists", err)
	}

	if err := cat.Drop("users"); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Get("users"); !strings.Contains(err.Error(), ErrNoSuchTable.Error()) {
		t.Fatalf("Get after Drop returned %v, want no such table", err)
	}
}

func TestCatalogTablesAndRowCount(t *testing.T) {
	store := newTestStore()
	cat := NewCatalog(store, DefaultDB)
	for _, name := range []string{"users", "orders", "events"} {
		s := testSchema()
		s.Table = name
		if err := cat.Create(s); err != nil {
			t.Fatal(err)
		}
	}
	names, err := cat.Tables()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"events", "orders", "users"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("Tables returned %v, want %v", names, want)
	}

	// Row count distinguishes rows from columns, which is the whole point of
	// the <row-id>/<column> layout.
	if n, err := cat.RowCount("users"); err != nil || n != 0 {
		t.Fatalf("RowCount on an empty table = %d, %v", n, err)
	}
	for _, k := range []string{
		ColumnKey(DefaultDB, "users", "1", "id"),
		ColumnKey(DefaultDB, "users", "1", "name"),
		ColumnKey(DefaultDB, "users", "1", "age"),
		ColumnKey(DefaultDB, "users", "2", "id"),
		// A row id holding the separator, escaped, must still count once.
		ColumnKey(DefaultDB, "users", EscapeRowID("a/b"), "id"),
	} {
		if err := store.Set(k, []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := cat.RowCount("users"); err != nil || n != 3 {
		t.Fatalf("RowCount = %d, %v; want 3", n, err)
	}
	// A different table's rows must not leak into the count.
	if n, err := cat.RowCount("orders"); err != nil || n != 0 {
		t.Fatalf("RowCount for an untouched table = %d, %v", n, err)
	}
}

// TestCatalogDropRefusesToOrphanRows is the check that keeps DROP TABLE honest:
// a catalog entry removed while its rows remain would let a later CREATE TABLE
// of the same name inherit rows written against a different definition.
func TestCatalogDropRefusesToOrphanRows(t *testing.T) {
	store := newTestStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(testSchema()); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ColumnKey(DefaultDB, "users", "1", "id"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := cat.Drop("users"); err == nil {
		t.Fatal("Drop removed a catalog entry that still has rows")
	}
	if _, err := cat.Get("users"); err != nil {
		t.Fatalf("the refused Drop also removed the entry: %v", err)
	}
}

func TestTranslateCreateTable(t *testing.T) {
	t.Run("inline primary key", func(t *testing.T) {
		p, err := Translate(`CREATE TABLE users (id bigint PRIMARY KEY, name text, age int)`)
		if err != nil {
			t.Fatal(err)
		}
		if p.Kind != StmtCreateTable {
			t.Fatalf("kind is %v, want StmtCreateTable", p.Kind)
		}
		s := p.Schema
		if s.Table != "users" || s.PrimaryKey != 0 {
			t.Fatalf("schema %+v", s)
		}
		if s.Columns[0].Nullable {
			t.Fatal("primary key column is nullable")
		}
		if !s.Columns[1].Nullable {
			t.Fatal("unconstrained column should be nullable")
		}
		if s.Columns[1].Type != TypeVarchar || s.Columns[2].Type != TypeInt {
			t.Fatalf("types resolved to %s, %s", s.Columns[1].Type, s.Columns[2].Type)
		}
	})

	t.Run("table level primary key", func(t *testing.T) {
		p, err := Translate(`CREATE TABLE users (name text, id bigint, PRIMARY KEY (id))`)
		if err != nil {
			t.Fatal(err)
		}
		// The key is the second declared column, and the index has to say so
		// rather than assuming declaration order.
		if p.Schema.PrimaryKey != 1 || p.Schema.PrimaryKeyName() != "id" {
			t.Fatalf("primary key index %d", p.Schema.PrimaryKey)
		}
		if p.Schema.Columns[1].Nullable {
			t.Fatal("primary key column is nullable")
		}
	})

	t.Run("not null and defaults", func(t *testing.T) {
		p, err := Translate(`CREATE TABLE t (id int PRIMARY KEY, n int NOT NULL DEFAULT 7, f float DEFAULT 1.5, b bool DEFAULT true, s text DEFAULT 'hi', j jsonb DEFAULT '{"a":1}')`)
		if err != nil {
			t.Fatal(err)
		}
		s := p.Schema
		if s.Columns[1].Nullable {
			t.Fatal("NOT NULL column is nullable")
		}
		checks := map[string]any{
			"n": int64(7), "f": 1.5, "b": true, "s": "hi", "j": `{"a":1}`,
		}
		for name, want := range checks {
			col, ok := s.Column(name)
			if !ok {
				t.Fatalf("no column %q", name)
			}
			got, err := DecodeValue(col.Type, col.Default)
			if err != nil {
				t.Fatalf("column %q default does not decode: %v", name, err)
			}
			if got != want {
				t.Fatalf("column %q default is %#v, want %#v", name, got, want)
			}
		}
	})

	t.Run("if not exists is carried", func(t *testing.T) {
		p, err := Translate(`CREATE TABLE IF NOT EXISTS t (id int PRIMARY KEY)`)
		if err != nil {
			t.Fatal(err)
		}
		if !p.IfNotExists {
			t.Fatal("IF NOT EXISTS was dropped")
		}
	})

	t.Run("qualified type names resolve", func(t *testing.T) {
		// The parser spells built-ins as pg_catalog.int4.
		p, err := Translate(`CREATE TABLE t (id pg_catalog.int4 PRIMARY KEY)`)
		if err != nil {
			t.Fatal(err)
		}
		if p.Schema.Columns[0].Type != TypeInt {
			t.Fatalf("type is %s", p.Schema.Columns[0].Type)
		}
	})

	t.Run("varchar length is accepted and dropped", func(t *testing.T) {
		p, err := Translate(`CREATE TABLE t (id int PRIMARY KEY, s varchar(10))`)
		if err != nil {
			t.Fatal(err)
		}
		if p.Schema.Columns[1].Type != TypeVarchar {
			t.Fatalf("type is %s", p.Schema.Columns[1].Type)
		}
	})
}

// TestTranslateRejectsUnusableDDL is the table of things a client is most
// likely to try. Each case names what is missing so the message is actionable,
// and each one must be refused before it reaches the store.
func TestTranslateRejectsUnusableDDL(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{`CREATE TABLE t (a int)`, ErrNoPrimaryKey.Error()},
		{`CREATE TABLE t (a int, b int, PRIMARY KEY (a, b))`, ErrCompositePrimaryKey.Error()},
		{`CREATE TABLE t (a int PRIMARY KEY, b int PRIMARY KEY)`, "more than one PRIMARY KEY"},
		{`CREATE TABLE t (a int, PRIMARY KEY (nosuch))`, "undeclared column"},
		{`CREATE TABLE t ()`, "no columns"},
		{`CREATE TABLE t (a int, a int, PRIMARY KEY (a))`, ErrDuplicateColumn.Error()},
		{`CREATE TABLE t (id int PRIMARY KEY, UNIQUE (id))`, "UNIQUE constraints are not supported"},
		{`CREATE TABLE t (id int PRIMARY KEY, CHECK (id > 0))`, "CHECK constraints are not supported"},
		{`CREATE TABLE t (id int PRIMARY KEY, x int REFERENCES other(id))`, "FOREIGN KEY constraints are not supported"},
		{`CREATE TABLE t (id serial PRIMARY KEY)`, "serial is not supported"},
		{`CREATE TABLE t (id int PRIMARY KEY, s int[])`, "array types are not supported"},
		{`CREATE TABLE t (id int PRIMARY KEY, s numeric(10,2))`, "not supported in phase 9"},
		{`CREATE TABLE t (id int PRIMARY KEY, s timestamp(3))`, "type modifier"},
		{`CREATE TABLE t (id int PRIMARY KEY, s text DEFAULT now())`, "only literal defaults"},
		{`CREATE TABLE t (id int PRIMARY KEY, s text DEFAULT NULL)`, "NULL defaults"},
		{`CREATE TABLE t (id int PRIMARY KEY DEFAULT 5)`, "cannot have a default"},
		{`CREATE TABLE tellstone (id int PRIMARY KEY)`, "reserved table"},
		{`CREATE TABLE "~meta" (id int PRIMARY KEY)`, "reserved table"},
		{`CREATE TABLE t (id int PRIMARY KEY) INHERITS (other)`, "inheritance is not supported"},
		{`CREATE TABLE t (id int PRIMARY KEY) WITH (fillfactor=70)`, "options"},
		{`CREATE TABLE t (id int PRIMARY KEY) USING heapless`, "USING heapless"},
		{`CREATE TEMP TABLE t (id int PRIMARY KEY)`, "TEMP tables"},
		{`CREATE TABLE other.t (id int PRIMARY KEY)`, "not available"},
		{`CREATE INDEX i ON t (id)`, "CREATE INDEX is not supported"},
		{`CREATE VIEW v AS SELECT 1`, "CREATE VIEW is not supported"},
		{`ALTER TABLE t ADD COLUMN b int`, "ALTER TABLE is not supported"},
		{`DROP INDEX i`, "only DROP TABLE is supported"},
		{`DROP TABLE a, b`, "expected one table name"},
		{`DROP TABLE tellstone`, "reserved table"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := Translate(tc.sql)
			if err == nil {
				t.Fatalf("Translate accepted %q", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Translate(%q) reported %v, want it to mention %q", tc.sql, err, tc.want)
			}
		})
	}
}

func TestTranslateDropTable(t *testing.T) {
	p, err := Translate(`DROP TABLE users`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != StmtDropTable || p.Table != "users" || p.IfExists {
		t.Fatalf("plan is %+v", p)
	}
	p, err = Translate(`DROP TABLE IF EXISTS users`)
	if err != nil {
		t.Fatal(err)
	}
	if !p.IfExists {
		t.Fatal("IF EXISTS was dropped")
	}
	// A quoted name keeps its capitalisation rather than being folded away.
	p, err = Translate(`DROP TABLE "Users"`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Table != "Users" {
		t.Fatalf("quoted name became %q", p.Table)
	}
}

// TestDecodeValueRoundTrips is the property the row path depends on: whatever
// a column was written as must come back equal, including the NULL that is
// stored as an absent key and the boundary values of each integer encoding.
func TestDecodeValueRoundTrips(t *testing.T) {
	cases := []struct {
		typ ColumnType
		val any
	}{
		{TypeInt, int64(0)},
		{TypeInt, int64(-1)},
		{TypeInt, int64(1 << 31)},
		{TypeBigInt, int64(-(1 << 62))},
		{TypeFloat, 0.0},
		{TypeFloat, -0.5},
		{TypeFloat, 1e300},
		{TypeBool, true},
		{TypeBool, false},
		{TypeVarchar, "hello"},
		{TypeVarchar, ""},
		{TypeBytes, []byte{0, 1, 2, 255}},
		{TypeJSONB, `{"nested":[1,2]}`},
		{TypeTimestamp, time.Unix(0, 0).UTC()},
		{TypeTimestamp, time.Unix(1700000000, 999).UTC()},
	}
	for _, tc := range cases {
		enc, err := EncodeValue(tc.typ, tc.val)
		if err != nil {
			t.Fatalf("EncodeValue(%s, %#v): %v", tc.typ, tc.val, err)
		}
		got, err := DecodeValue(tc.typ, enc)
		if err != nil {
			t.Fatalf("DecodeValue(%s, %x): %v", tc.typ, enc, err)
		}
		if !valuesEqual(tc.typ, got, tc.val) {
			t.Fatalf("round trip of %#v as %s gave %#v", tc.val, tc.typ, got)
		}
	}
	// An empty string is a value, not a NULL: a null column is the absence of
	// its key, so the bytes alone never have to mean NULL. This is the case
	// that a zero-length-is-NULL rule would silently corrupt.
	if got, err := DecodeValue(TypeVarchar, []byte{}); err != nil || got != "" {
		t.Fatalf("DecodeValue(varchar, empty) = %#v, %v; want an empty string", got, err)
	}
	if got, err := DecodeValue(TypeBytes, []byte{}); err != nil {
		t.Fatalf("DecodeValue(bytes, empty) = %#v, %v", got, err)
	}
	// A corrupt value must be reported, not decoded into a wrong one.
	if _, err := DecodeValue(TypeInt, []byte{0x00}); err == nil {
		t.Fatal("DecodeValue accepted a one byte int")
	}
	if _, err := DecodeValue(TypeBool, []byte{0x00, 0x00}); err == nil {
		t.Fatal("DecodeValue accepted a two byte bool")
	}
}

func valuesEqual(typ ColumnType, got, want any) bool {
	switch want := want.(type) {
	case []byte:
		g, ok := got.([]byte)
		return ok && bytes.Equal(g, want)
	case time.Time:
		g, ok := got.(time.Time)
		return ok && g.Equal(want)
	case float64:
		g, ok := got.(float64)
		return ok && g == want
	default:
		return got == want
	}
}

func mustEncode(t *testing.T, typ ColumnType, v any) []byte {
	t.Helper()
	b, err := EncodeValue(typ, v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }

// TestCatalogIfExistsForms covers the two clauses whose whole point is to turn
// an error into a success, including the case where they must not paper over a
// real problem.
func TestCatalogIfExistsForms(t *testing.T) {
	store := newTestStore()
	cat := NewCatalog(store, DefaultDB)

	created, err := cat.CreateIfNotExists(testSchema())
	if err != nil || !created {
		t.Fatalf("first CreateIfNotExists = %v, %v", created, err)
	}
	// The second call must not replace the first definition.
	other := testSchema()
	other.Columns[1].Type = TypeBytes
	created, err = cat.CreateIfNotExists(other)
	if err != nil || created {
		t.Fatalf("second CreateIfNotExists = %v, %v; want the name to be left alone", created, err)
	}
	got, err := cat.Get("users")
	if err != nil {
		t.Fatal(err)
	}
	if got.Columns[1].Type != TypeVarchar {
		t.Fatalf("the existing schema was overwritten: %s", got.Columns[1].Type)
	}

	dropped, err := cat.DropIfExists("users")
	if err != nil || !dropped {
		t.Fatalf("DropIfExists = %v, %v", dropped, err)
	}
	dropped, err = cat.DropIfExists("users")
	if err != nil || dropped {
		t.Fatalf("second DropIfExists = %v, %v; want no error and no drop", dropped, err)
	}

	// IF EXISTS waives a missing table, not a table with rows in it.
	if err := cat.Create(testSchema()); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ColumnKey(DefaultDB, "users", "1", "id"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.DropIfExists("users"); err == nil {
		t.Fatal("DropIfExists removed an entry that still had rows")
	}
}
