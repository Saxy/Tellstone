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
	}{
		{"SELECT key, value FROM tellstone WHERE key = 'foo'", StmtSelect, 0, 0, []string{"key", "value"}},
		{"SELECT value FROM tellstone WHERE key = $1", StmtSelect, 1, 0, []string{"value"}},
		{"SELECT * FROM tellstone WHERE key = 'foo'", StmtSelect, 0, 0, []string{"key", "value"}},
		{"SELECT key FROM tellstone WHERE key = $1", StmtSelect, 1, 0, []string{"key"}},
		{"INSERT INTO tellstone (key, value) VALUES ($1, $2)", StmtInsert, 1, 2, nil},
		{"INSERT INTO tellstone (value, key) VALUES ('b', 'a')", StmtInsert, 0, 0, nil},
		{"INSERT INTO tellstone (key, value) VALUES ('a', 'b') ON CONFLICT (key) DO UPDATE SET value = 'b'", StmtInsert, 0, 0, nil},
		{"UPDATE tellstone SET value = $2 WHERE key = $1", StmtUpdate, 1, 2, nil},
		{"DELETE FROM tellstone WHERE key = $1", StmtDelete, 1, 0, nil},
		{"BEGIN", StmtBegin, 0, 0, nil},
		{"COMMIT", StmtCommit, 0, 0, nil},
		{"ROLLBACK", StmtRollback, 0, 0, nil},
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

func TestTranslateRejected(t *testing.T) {
	cases := []string{
		"",
		"SELECT 1",
		"SELECT key FROM tellstone",
		"SELECT key FROM tellstone WHERE key > 'a'",
		"SELECT key INTO x FROM tellstone WHERE key = 'a'",
		"SELECT key FROM other WHERE key = 'a'",
		"SELECT count(*) FROM tellstone WHERE key = 'x'",
		"INSERT INTO tellstone VALUES ('a')",
		"INSERT INTO tellstone (k, v) VALUES ('a', 'b')",
		"UPDATE tellstone SET key = 'x' WHERE key = 'a'",
		"UPDATE tellstone SET value = 'b'",
		"DELETE FROM tellstone",
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
