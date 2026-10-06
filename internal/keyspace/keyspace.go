/*
Package keyspace defines the byte grammar that Tellstone keys are written in.

It exists so that exactly one implementation decides where a row begins and
ends. That matters beyond tidiness: a region split that lands inside a row
separates the row's columns across two regions, which turns every later read and
write of that row into a cross-region operation. The consensus layer has to
choose split points, so it needs the row boundary rule, but it must not import
internal/sql to get it -- SQL depends on the cluster, not the other way round.

The grammar is:

	<database>/<table>/<row-id>/<column>

Databases, tables, and columns are schema-level names and are validated rather
than escaped: silently rewriting a user's identifier is worse than refusing it.
Row ids are user *data*, so they are escaped instead, which keeps the encoding
total and injective. See EscapeRowID for why that matters.

This package deliberately knows nothing about SQL types, values, or the catalog.
It owns the key shape only.
*/
package keyspace

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Separator divides the segments of a key. It is a byte that a validated
// database, table, or column name can never contain, which is what makes the
// segment structure unambiguous.
const Separator = "/"

// SeparatorByte is Separator as a byte, for scanning a raw key.
const SeparatorByte byte = '/'

// MetaPrefix introduces the reserved subtree that holds the schema catalog. It
// is excluded from table scans, and a table name may not begin with it, which
// is what keeps user data out of the subtree the engine reads control
// information from.
const MetaPrefix = "~meta"

// MetaMarker is the leading byte of MetaPrefix. RowPrefixOf rejects it so that a
// split never snaps a catalog key onto a row boundary.
const MetaMarker = "~"

// ErrInvalidSegment reports a name that cannot appear as one path segment.
var ErrInvalidSegment = errors.New("keyspace: invalid key segment")

// reservedPrefixes are rejected at the start of a table name. MetaMarker
// introduces the catalog subtree and "*" is held back for future layout hints
// such as index markers; allowing either as a table name would put user data
// inside a subtree the engine scans for control information.
var reservedPrefixes = []string{MetaMarker, "*"}

// validateSegment rejects a name that cannot appear as one key segment. A
// separator inside a name is refused rather than escaped, because these are
// schema-level names where rewriting the user's identifier would be worse than
// refusing it. Row ids come from user data and are escaped instead.
func validateSegment(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty %s name", ErrInvalidSegment, kind)
	}
	if strings.Contains(name, Separator) {
		return fmt.Errorf("%w: %s name %q contains a separator", ErrInvalidSegment, kind, name)
	}
	for _, r := range reservedPrefixes {
		if strings.HasPrefix(name, r) {
			return fmt.Errorf("%w: %s name %q starts with a reserved marker", ErrInvalidSegment, kind, name)
		}
	}
	return nil
}

// ValidateTableName rejects a table name that cannot be a key segment.
func ValidateTableName(name string) error { return validateSegment("table", name) }

// ValidateColumnName rejects a column name that cannot be a key segment.
func ValidateColumnName(name string) error { return validateSegment("column", name) }

// ValidateDatabaseName rejects a database name that cannot be a key segment.
// A database is written by the server rather than the user, but it is checked
// with the same rule as tables and columns so the grammar has one entry point.
func ValidateDatabaseName(name string) error { return validateSegment("database", name) }

// validTableSegment reports whether a key segment names a real table, i.e. it
// is non-empty and carries no reserved marker. It is the cheap form used on the
// split path, where a malformed segment simply means "not a row key" rather than
// an error to report.
func validTableSegment(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range reservedPrefixes {
		if strings.HasPrefix(s, r) {
			return false
		}
	}
	return true
}

// TablePrefix returns the prefix covering every row of a table.
func TablePrefix(db, table string) string {
	return db + Separator + table + Separator
}

// RowPrefix returns the prefix covering every column of one row. A row is one
// contiguous range, which is the point of the layout: reading a row is a single
// range read rather than one lookup per column.
func RowPrefix(db, table, rowID string) string {
	return TablePrefix(db, table) + EscapeRowID(rowID) + Separator
}

// ColumnKey returns the key holding one column of one row.
func ColumnKey(db, table, rowID, column string) string {
	return RowPrefix(db, table, rowID) + column
}

// escapeChar is the escape byte for a row id. It has to be a byte that cannot
// otherwise start an escape sequence, or the encoding would be ambiguous: '%'
// satisfies that because the two escapes are spelled "%25" and "%2F", and a
// literal '%' is itself escaped to "%25".
const escapeChar byte = 0x25 // '%'

// escapeChars are the bytes EscapeRowID rewrites.
const escapeChars = "%" + Separator

// RowIDPrefixEnd is the exclusive upper bound of the key range covering one
// row, used to turn a row prefix into a bounded range read.
//
// It appends 0xFF, which sorts above every byte that can appear in a column
// name while staying a prefix of no real key. Appending 0x00 would also work
// but is a weaker bound: a key equal to the prefix itself is legitimate, and
// the exclusive upper bound has to exclude it either way.
func RowIDPrefixEnd(db, table, rowID string) string {
	return RowPrefix(db, table, rowID) + "\xff"
}

// TablePrefixEnd returns the exclusive upper bound of a table's key range.
func TablePrefixEnd(db, table string) string {
	return TablePrefix(db, table) + "\xff"
}

// EscapeRowID makes a row id safe to use as a single key segment.
//
// A row id is user data, unlike a table name, so it is escaped rather than
// rejected. The encoding escapes '%' and '/' and nothing else, which makes it
// total and injective: two distinct row ids always produce distinct row
// prefixes, so one primary key can never forge another's column boundary. That
// is the property the layout depends on -- an unescaped '/' would let a row id
// claim column keys belonging to a different row.
//
// The encoding is not order preserving. Escaping only these two bytes keeps
// '/' and '%' correctly ordered relative to each other but not relative to
// every other byte: '%' escapes to "%2F", whose leading 0x25 sorts below any
// byte it replaces, so a row id that needed escaping can land on the wrong side
// of a bound. A textual primary key is therefore never range scanned.
//
// An integer primary key is escaped by nothing, because EncodeIntRowID emits
// only hex digits -- which are order preserving as well, so it is safe to range
// on. Note that this is a property of the row id's own alphabet, not of the
// column's value encoding: an integer value rendered as raw fixed-width bytes
// is full of escapable bytes, and an integer value rendered as a decimal string
// is order preserving but not escape-safe in general and not fixed width. Only
// the hex rendering is both at once.
func EscapeRowID(id string) string {
	if !strings.ContainsAny(id, escapeChars) {
		return id
	}
	var b strings.Builder
	b.Grow(len(id) + 8)
	for i := 0; i < len(id); i++ {
		switch id[i] {
		case '%':
			b.WriteString("%25")
		case '/':
			b.WriteString("%2F")
		default:
			b.WriteByte(id[i])
		}
	}
	return b.String()
}

// UnescapeRowID is the inverse of EscapeRowID. It exists so a scan can recover
// the original row id from a key without storing it separately, which is what
// lets a full-table scan report real primary keys.
func UnescapeRowID(escaped string) (string, error) {
	if strings.IndexByte(escaped, escapeChar) < 0 {
		return escaped, nil
	}
	var b strings.Builder
	b.Grow(len(escaped))
	for i := 0; i < len(escaped); i++ {
		if escaped[i] != escapeChar {
			b.WriteByte(escaped[i])
			continue
		}
		if i+3 > len(escaped) {
			return "", fmt.Errorf("%w: truncated escape in row id %q", ErrInvalidSegment, escaped)
		}
		switch escaped[i+1 : i+3] {
		case "25":
			b.WriteByte('%')
			i += 2
		case "2F":
			b.WriteByte('/')
			i += 2
		default:
			return "", fmt.Errorf("%w: unknown escape %q in row id", ErrInvalidSegment, escaped[i:i+3])
		}
	}
	return b.String(), nil
}

// RowPrefixOf returns the row prefix that owns key, and whether key lies inside
// a row at all.
//
// This is the single definition of a row boundary, and it is what keeps a region
// split from landing in the middle of a row. A key is a row key when it has at
// least four segments whose first three name a real database, a real table, and
// a non-empty row id. The returned prefix runs up to and including the third
// separator.
//
// Snapping a split point to the returned prefix is always safe, and only in
// that direction. The proof is that a row prefix is a prefix of every key in
// the row and of no key outside it, so a boundary placed on it leaves the whole
// row on the right of the boundary and every other row entirely on one side.
//
// Snapping the other way is *not* derivable from a key alone, and this is the
// reason rather than an oversight. The start of the next row depends on the
// lexicographic successor of the longest column name in the row, which is not
// computable without reading the row. Appending a byte above every legal column
// name (0xFF) is a tempting shortcut and is wrong: that byte sorts above all
// real column names but still lands *inside* the row, because the row's range
// extends past its last column key. Any such construction splits the row it was
// meant to protect. A region that cannot be split by snapping backward must
// therefore refuse the split rather than cut a row.
//
// ok is false when key is not inside a row, in which case a split landing there
// may stay where it is: there is no row to cut in half. That covers catalog keys
// (the table segment is the reserved ~meta marker, which validTableSegment
// rejects), the legacy flat table's arbitrary user keys, and any key with fewer
// than four segments.
func RowPrefixOf(key []byte) (prefix []byte, ok bool) {
	first := bytes.IndexByte(key, SeparatorByte)
	if first <= 0 {
		// No separator, or a leading one: the database segment is missing.
		return nil, false
	}
	rest := key[first+1:]
	second := bytes.IndexByte(rest, SeparatorByte)
	if second <= 0 {
		return nil, false
	}
	// The table segment is between the first and second separator, and has to
	// be checked before rest is advanced past it -- reading the segment after
	// it would validate the row id's *successor*, which for a catalog key is a
	// perfectly ordinary table name.
	if !validTableSegment(string(rest[:second])) {
		return nil, false
	}
	rest = rest[second+1:]
	third := bytes.IndexByte(rest, SeparatorByte)
	if third <= 0 {
		// The row id is empty, so this is a table-level key rather than a row.
		return nil, false
	}
	return key[:first+1+second+1+third+1], true
}

// EncodeIntRowID renders an integer row id as 16 lowercase hex digits of the
// sign-flipped 64-bit value.
//
// This is the only row-id encoding that is both order-preserving and escape-free,
// and a range scan needs both properties at once.
//
// **Order-preserving.** The sign flip puts negative values below positive ones
// without disturbing magnitude order, and fixed-width hex preserves that order
// as text: the digits run 0-9 then a-f, which is exactly ascending value order,
// so comparing two hex strings character by character gives the same answer as
// comparing the two numbers. The width must be fixed -- a variable-width decimal
// string does not sort numerically ("10" sorts before "9"), which is the reason
// EncodeOrderableInt is a fixed 8 bytes rather than a decimal rendering.
//
// **Escape-free.** Hex digits are 0x30-0x39 and 0x61-0x66, so a hex row id can
// contain neither the escape byte '%' (0x25) nor the separator '/' (0x2F), and
// EscapeRowID is a no-op on it. That matters because RowPrefix escapes every row
// id unconditionally, and the escape is not order-preserving: '%' escapes to the
// three bytes "%2F", which sort *below* the byte it replaced when that byte was
// in (0x25, 0x2F) -- so '/' would land before '&'. A row id made only of hex
// digits avoids the question rather than answering it.
//
// Together these are what let ADR-014's RangeScan exist. Without them a range
// bounded on the encoding returns the wrong rows, which is worse than not
// offering the plan at all.
func EncodeIntRowID(v int64) string {
	return fmt.Sprintf("%016x", uint64(v)^(1<<63))
}

// DecodeIntRowID is the inverse of EncodeIntRowID. It reports an error rather
// than a zero value for input that is not a hex row id, because a silently zero
// row id would address a real -- and unrelated -- row.
func DecodeIntRowID(s string) (int64, error) {
	u, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q is not a hex row id", ErrInvalidSegment, s)
	}
	if len(s) != intRowIDHexLen {
		return 0, fmt.Errorf("%w: hex row id must be %d characters, got %d",
			ErrInvalidSegment, intRowIDHexLen, len(s))
	}
	return int64(u ^ (1 << 63)), nil
}

// IsIntRowID reports whether s has the shape EncodeIntRowID produces, without
// decoding it. Scans use it to tell an integer row id from a text one, so a text
// primary key whose value happens to be 16 hex characters is not mistaken for an
// integer and silently reformatted.
func IsIntRowID(s string) bool {
	if len(s) != intRowIDHexLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// intRowIDHexLen is the width of a hex row id: two digits per byte, eight bytes.
const intRowIDHexLen = 16
