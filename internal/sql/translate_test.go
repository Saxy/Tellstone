/*
Package sql
Tellstone PostgreSQL Wire Frontend Tests
File: translate_test.go
Description: Table-driven tests for the SQL-to-plan translation: the statement
shapes that must be accepted, and the near-miss shapes that must be rejected
rather than approximated.
*/
package sql

import (
	"strings"
	"testing"
)

func TestTranslateSupported(t *testing.T) {
	cases := []struct {
		sql      string
		kind     StmtKind
		keyParam int
		valParam int
		cols     []string
		conflict ConflictAction
	}{
		{"SELECT key, value FROM tellstone WHERE key = 'foo'", StmtSelect, 0, 0, []string{"key", "value"}, ConflictRaise},
		{"SELECT value FROM tellstone WHERE key = $1", StmtSelect, 1, 0, []string{"value"}, ConflictRaise},
		{"SELECT * FROM tellstone WHERE key = 'foo'", StmtSelect, 0, 0, []string{"key", "value"}, ConflictRaise},
		{"SELECT key FROM tellstone WHERE key = $1", StmtSelect, 1, 0, []string{"key"}, ConflictRaise},
		{"INSERT INTO tellstone (key, value) VALUES ($1, $2)", StmtInsert, 1, 2, nil, ConflictRaise},
		{"INSERT INTO tellstone (value, key) VALUES ('b', 'a')", StmtInsert, 0, 0, nil, ConflictRaise},
		{"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT DO NOTHING", StmtInsert, 0, 0, nil, ConflictDoNothing},
		{"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (key) DO NOTHING", StmtInsert, 0, 0, nil, ConflictDoNothing},
		{"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (key) DO UPDATE SET value = excluded.value", StmtInsert, 0, 0, nil, ConflictDoUpdate},
		{"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT DO UPDATE SET value = excluded.value", StmtInsert, 0, 0, nil, ConflictDoUpdate},
		{"UPDATE tellstone SET value = $2 WHERE key = $1", StmtUpdate, 1, 2, nil, ConflictRaise},
		{"DELETE FROM tellstone WHERE key = $1", StmtDelete, 1, 0, nil, ConflictRaise},
		{"BEGIN", StmtBegin, 0, 0, nil, ConflictRaise},
		{"COMMIT", StmtCommit, 0, 0, nil, ConflictRaise},
		{"ROLLBACK", StmtRollback, 0, 0, nil, ConflictRaise},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			p, err := Translate(c.sql)
			if err != nil {
				t.Fatalf("Translate(%q): %v", c.sql, err)
			}
			if p.Kind != c.kind {
				t.Fatalf("kind = %d, want %d", p.Kind, c.kind)
			}
			if p.Conflict != c.conflict {
				t.Errorf("conflict = %d, want %d", p.Conflict, c.conflict)
			}
			if p.Key.Param != c.keyParam {
				t.Errorf("key param = %d, want %d", p.Key.Param, c.keyParam)
			}
			if p.Val.Param != c.valParam {
				t.Errorf("value param = %d, want %d", p.Val.Param, c.valParam)
			}
			if len(p.Cols) != len(c.cols) {
				t.Fatalf("cols = %v, want %v", p.Cols, c.cols)
			}
			for i := range c.cols {
				if p.Cols[i] != c.cols[i] {
					t.Errorf("cols[%d] = %q, want %q", i, p.Cols[i], c.cols[i])
				}
			}
		})
	}
}

// A statement naming a table other than the implicit one is no longer rejected
// by name at translation time. Translation cannot know whether that table exists
// -- it may have been created a moment ago by another client -- so a hardcoded
// check could only ever be right by accident, and was wrong whenever the catalog
// was right. Existence is settled against the catalog at execution instead.
func TestTranslateAcceptsUnknownTable(t *testing.T) {
	for _, c := range []string{
		"SELECT key FROM other WHERE key = 'a'",
		"SELECT * FROM users WHERE id = 1",
		"INSERT INTO users (id, name) VALUES (1, 'a')",
		"UPDATE users SET name = 'b' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
	} {
		t.Run(c, func(t *testing.T) {
			p, err := Translate(c)
			if err != nil {
				t.Fatalf("Translate(%q) = %v, want a plan", c, err)
			}
			// The plan must carry the table so execution can resolve it.
			if p.Table == "" {
				t.Fatalf("Translate(%q) produced no table name", c)
			}
			if p.Schema != nil {
				t.Fatalf("Translate(%q) resolved a schema; translation has no catalog", c)
			}
		})
	}
}

// Translation must still refuse the table names it cannot represent, because
// those are decidable without the catalog: a schema naming a database that does
// not exist would point the statement at a different table than the client
// named, and a catalog keyspace name is not a table.
func TestTranslateRejectsForeignAndReservedTables(t *testing.T) {
	for _, c := range []string{
		"SELECT * FROM otherdb.users WHERE id = 1",
		"SELECT * FROM ~meta WHERE id = 1",
		"INSERT INTO ~meta (id) VALUES (1)",
	} {
		t.Run(c, func(t *testing.T) {
			if p, err := Translate(c); err == nil {
				t.Fatalf("Translate(%q) succeeded with plan %+v, want error", c, p)
			}
		})
	}
}

// The names a client is most likely to send must keep working: the implicit
// table by its own name, and a catalog table qualified by "public", which is the
// schema unqualified clients resolve against by default.
func TestTranslateAcceptsExpectedTableSpellings(t *testing.T) {
	for _, c := range []string{
		`SELECT * FROM tellstone WHERE key = 'a'`,
		"SELECT * FROM public.users WHERE id = 1",
		"SELECT * FROM tellstone.users WHERE id = 1",
		"SELECT * FROM users WHERE id = 1",
	} {
		t.Run(c, func(t *testing.T) {
			p, err := Translate(c)
			if err != nil {
				t.Fatalf("Translate(%q) = %v, want a plan", c, err)
			}
			if p.Table == "" {
				t.Fatalf("Translate(%q) produced no table name", c)
			}
		})
	}
}

// INSERT ... VALUES with no column list is the ordinary positional form and must
// translate. Its values line up with the schema's columns by position, and the
// arity is checked once the schema is known. The implicit table is a different
// case: it has exactly two columns whose names matter, so it still requires the
// list to be written out.
func TestTranslateInsertWithoutAColumnList(t *testing.T) {
	p, err := Translate(`INSERT INTO users VALUES (1, 'ada', 36)`)
	if err != nil {
		t.Fatalf("positional INSERT rejected: %v", err)
	}
	if p.Columns != nil {
		t.Fatalf("Columns = %v, want nil for a positional INSERT", p.Columns)
	}
	if len(p.Values) != 3 {
		t.Fatalf("Values has %d entries, want 3", len(p.Values))
	}
	if _, err := Translate(`INSERT INTO tellstone VALUES ('k', 'v')`); err == nil {
		t.Error("implicit-table INSERT without a column list should still be refused")
	}
}

func TestTranslateRejected(t *testing.T) {
	cases := []string{
		"",
		"SELECT 1",
		"SELECT key FROM tellstone",
		"SELECT key FROM tellstone WHERE key > 'a'",
		"SELECT key INTO x FROM tellstone WHERE key = 'a'",
		"SELECT count(*) FROM tellstone WHERE key = 'x'",
		"INSERT INTO tellstone VALUES ('a')",
		"INSERT INTO tellstone (k, v) VALUES ('a', 'b')",
		"UPDATE tellstone SET key = 'x' WHERE key = 'a'",
		"UPDATE tellstone SET value = 'b'",
		"DELETE FROM tellstone",
		// Only the canonical upsert assignment is accepted; anything else would
		// have to be silently downgraded to an overwrite.
		"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (key) DO UPDATE SET value = 'b'",
		"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE value <> 'b'",
		"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (value) DO UPDATE SET value = excluded.value",
		"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT ON CONSTRAINT tellstone_pkey DO NOTHING",
		"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (key) DO UPDATE SET key = excluded.value",
		"SELECT a, b FROM tellstone; SELECT a FROM tellstone;",
		"DROP TABLE tellstone",
		"GRANT ALL ON tellstone TO admin",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if p, err := Translate(c); err == nil {
				t.Fatalf("Translate(%q) succeeded with plan %+v, want error", c, p)
			} else if strings.Contains(err.Error(), "GRANT") {
				// sanity: the switch must reject non-data statements
				t.Logf("rejected: %v", err)
			}
		})
	}
}
