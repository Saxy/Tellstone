package keyspace

import (
	"bytes"
	"strings"
	"testing"
)

func TestRowPrefixOf(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want string
		ok   bool
	}{
		{"first column of a row", "db/tbl/7/id", "db/tbl/7/", true},
		{"later column", "db/tbl/7/name", "db/tbl/7/", true},
		{"multi segment column name", "db/tbl/7/a/b", "db/tbl/7/", true},
		{"row id containing a slash", "db/tbl/a%2Fb/id", "db/tbl/a%2Fb/", true},
		{"row id that looks like segments", "db/tbl/a%2Fb%2Fc/id", "db/tbl/a%2Fb%2Fc/", true},
		{"row id beginning with an escape", "db/tbl/%25x/id", "db/tbl/%25x/", true},

		// Not a row: nothing to cut, so the split point may stay put.
		{"table prefix only", "db/tbl/", "", false},
		{"table prefix with row id, no column", "db/tbl/7", "", false},
		{"catalog key", "db/~meta/tables/db/tbl", "", false},
		{"catalog tables prefix", "db/~meta/tables/db/", "", false},
		{"reserved marker table", "db/*index/marker", "", false},
		{"single segment", "plainkey", "", false},
		{"two segments", "db/tbl", "", false},
		{"empty", "", "", false},
		{"leading separator", "/tbl/7/id", "", false},
		{"empty database", "/tbl/7/id/", "", false},
		{"empty table", "db//7/id", "", false},
		{"empty row id", "db/tbl//id", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := RowPrefixOf([]byte(tc.key))
			if ok != tc.ok {
				t.Fatalf("RowPrefixOf(%q) ok = %v, want %v", tc.key, ok, tc.ok)
			}
			if !ok {
				return
			}
			if string(got) != tc.want {
				t.Fatalf("RowPrefixOf(%q) = %q, want %q", tc.key, got, tc.want)
			}
			// The returned slice must be a genuine prefix of the key, since that
			// is the property the boundary proof rests on.
			if !bytes.HasPrefix([]byte(tc.key), got) {
				t.Fatalf("RowPrefixOf(%q) = %q, which is not a prefix of the key", tc.key, got)
			}
		})
	}
}

// A row prefix must cover every column key of its row, and no key of any other
// row. If this fails, a split snapped to that prefix would cut a row.
func TestRowPrefixCoversExactlyItsOwnRow(t *testing.T) {
	rowIDs := []string{"1", "7", "42", "a/b", "a%b", "%2F", "~meta", "x y", strings.Repeat("z", 40)}
	columns := []string{"a", "b", "id", "name", "zzz"}
	for _, db := range []string{"db", "tellstone"} {
		for _, table := range []string{"t", "users", "a_b"} {
			for _, rowID := range rowIDs {
				prefix := []byte(RowPrefix(db, table, rowID))
				for _, col := range columns {
					key := []byte(ColumnKey(db, table, rowID, col))
					if !bytes.HasPrefix(key, prefix) {
						t.Fatalf("column key %q is not covered by its row prefix %q", key, prefix)
					}
				}
				// Every key of a *different* row must sort outside this prefix,
				// entirely before or entirely after.
				for _, other := range rowIDs {
					if other == rowID {
						continue
					}
					for _, col := range columns {
						otherKey := []byte(ColumnKey(db, table, other, col))
						if bytes.HasPrefix(otherKey, prefix) {
							t.Fatalf("row %q key %q is covered by row %q prefix %q",
								other, otherKey, rowID, prefix)
						}
					}
				}
			}
		}
	}
}

// The escape encoding has to be injective, or one primary key could forge
// another's column keys. This is the property that makes a row boundary a
// boundary at all.
func TestEscapeRowIDIsInjective(t *testing.T) {
	ids := []string{"", "/", "%", "%/", "/%", "%25", "%2F", "a", "a/b", "a%b", "%%", "//",
		"%2F%25", "%25%2F", "~meta", "a/b/c", "a%2Fb", strings.Repeat("%", 10)}
	seen := make(map[string]string, len(ids))
	for _, id := range ids {
		esc := EscapeRowID(id)
		if prev, dup := seen[esc]; dup {
			t.Fatalf("EscapeRowID(%q) == EscapeRowID(%q) == %q", id, prev, esc)
		}
		seen[esc] = id
		// An escaped id must be usable as one segment.
		if strings.Contains(esc, "/") {
			t.Fatalf("EscapeRowID(%q) = %q still contains a separator", id, esc)
		}
		back, err := UnescapeRowID(esc)
		if err != nil {
			t.Fatalf("UnescapeRowID(%q): %v", esc, err)
		}
		if back != id {
			t.Fatalf("UnescapeRowID(EscapeRowID(%q)) = %q", id, back)
		}
	}
}

func TestUnescapeRowIDRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"%2", "%", "%zz", "abc%", "%2G"} {
		if _, err := UnescapeRowID(bad); err == nil {
			t.Fatalf("UnescapeRowID(%q) succeeded, want error", bad)
		}
	}
}

func TestValidateNames(t *testing.T) {
	if err := ValidateTableName("users"); err != nil {
		t.Fatalf("ValidateTableName(users): %v", err)
	}
	for _, bad := range []string{"", "a/b", "~meta", "*x", "~", "*"} {
		if err := ValidateTableName(bad); err == nil {
			t.Fatalf("ValidateTableName(%q) succeeded, want error", bad)
		}
		if err := ValidateColumnName(bad); err == nil {
			t.Fatalf("ValidateColumnName(%q) succeeded, want error", bad)
		}
	}
}

// An empty row id is unreachable through SQL -- the row layer rejects it with
// 23502 before a key is written -- so RowPrefix and RowPrefixOf cannot be
// expected to agree on it. RowPrefixOf stays conservative there: it reports "no
// row", which leaves a split point where it is rather than snapping it to a
// prefix that no written key actually starts with. This test pins that
// behaviour so the two functions' disagreement is deliberate and recorded
// rather than a latent surprise.
func TestRowPrefixOfRejectsEmptyRowID(t *testing.T) {
	key := []byte(ColumnKey("db", "t", "", "a"))
	if got, ok := RowPrefixOf(key); ok {
		t.Fatalf("RowPrefixOf(%q) = %q, want no row for an empty row id", key, got)
	}
}

// RowPrefixOf must agree with RowPrefix for keys the layout actually writes.
// A disagreement between the two would mean the splitter and the writer have
// different ideas of where a row ends.
func TestRowPrefixOfMatchesRowPrefix(t *testing.T) {
	ids := []string{"1", "abc", "a/b", "a%b", "%", "~meta"}
	cols := []string{"a", "id", "name"}
	for _, db := range []string{"db", "tellstone"} {
		for _, table := range []string{"t", "users"} {
			for _, id := range ids {
				for _, col := range cols {
					key := []byte(ColumnKey(db, table, id, col))
					got, ok := RowPrefixOf(key)
					if !ok {
						t.Fatalf("RowPrefixOf(%q) found no row", key)
					}
					want := RowPrefix(db, table, id)
					if string(got) != want {
						t.Fatalf("RowPrefixOf(%q) = %q, RowPrefix = %q", key, got, want)
					}
				}
			}
		}
	}
}
