/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: layout.go
Description: The ADR-013 key layout: how a SQL table becomes KV keys. A table
maps to <db>/<table>/<row-id>/<column>, so a whole row is one contiguous range.
This file owns that layout in one place -- key construction, row-id escaping, and
the order-preserving value encodings -- because the separator is what makes the
layout work, and a row id carrying a raw separator would forge a column
boundary.

A row is identified by its primary key column alone, not by any of its columns;
rows.go owns that rule and the reason for it. A NULL column is likewise not a
value but the absence of its key, which is what lets an empty string stay an
empty string.

Nothing here talks to the store. It is the mapping the executor, the schema
catalog and the region splitter all share, which is deliberate: the splitter
needs to recognise a row boundary, and it can only do that if exactly one
function defines where boundaries are.
*/
package sql

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// DefaultDB is the database subtree every SQL-visible byte lives under
// (ADR-013 decision 1). It is a constant rather than a session setting in
// Phase 9; multiple databases only add a sibling.
const DefaultDB = "tellstone"

// metaPrefix introduces the reserved subtree holding the schema catalog. It
// sits inside the database subtree so schema travels with the data it
// describes, and is excluded from table scans. A leading '~' is rejected in
// table names (decision 5), which is what keeps the two from colliding.
const metaPrefix = keyspace.MetaPrefix

// metaRoot is the full reserved prefix, e.g. "tellstone/~meta".
var metaRoot = DefaultDB + "/" + metaPrefix

// MetaTableKey is the catalog key for one table. The whole catalog for a table
// is a single key rather than a key per column: DDL is rare relative to DML,
// the blob keeps the catalog consistent by construction, and a single key needs
// no cross-key atomicity that the engine does not offer.
func MetaTableKey(db, table string) string {
	return metaRoot + "/tables/" + db + "/" + table
}

// MetaTablePrefix returns the prefix covering every table's catalog entry, for
// listing tables.
func MetaTablePrefix(db string) string {
	return metaRoot + "/tables/" + db + "/"
}

// IsMetaKey reports whether a key belongs to the reserved catalog subtree. Such
// keys must never appear in a table scan result.
func IsMetaKey(key string) bool {
	return strings.HasPrefix(key, metaRoot+"/")
}

// ColumnType is a Phase 9 column type. The set is deliberately small: each one
// needs an order-preserving encoding (decision 4) so that Phase 10's optimizer
// can range-scan a column key without rewriting encodings later.
type ColumnType uint8

const (
	// TypeInvalid is the zero value, so a decoded type is never silently valid.
	TypeInvalid ColumnType = iota
	TypeInt
	TypeBigInt
	TypeFloat
	TypeVarchar
	TypeBytes
	TypeTimestamp
	TypeJSONB
	TypeBool
)

// typeNames maps a SQL type name onto a ColumnType. Lookup is case-insensitive
// because the parser hands back whatever the client typed.
var typeNames = map[string]ColumnType{
	"int":         TypeInt,
	"integer":     TypeInt,
	"int4":        TypeInt,
	"bigint":      TypeBigInt,
	"int8":        TypeBigInt,
	"float":       TypeFloat,
	"float8":      TypeFloat,
	"double":      TypeFloat,
	"real":        TypeFloat,
	"varchar":     TypeVarchar,
	"text":        TypeVarchar,
	"bytea":       TypeBytes,
	"bytes":       TypeBytes,
	"timestamp":   TypeTimestamp,
	"timestamptz": TypeTimestamp,
	"jsonb":       TypeJSONB,
	"json":        TypeJSONB,
	"bool":        TypeBool,
	"boolean":     TypeBool,
}

// ParseColumnType resolves a SQL type name, reporting an error for a type Phase
// 9 does not encode.
func ParseColumnType(name string) (ColumnType, error) {
	t, ok := typeNames[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return TypeInvalid, fmt.Errorf("unsupported column type %q", name)
	}
	return t, nil
}

// String names the type as it would be written in a CREATE TABLE.
func (t ColumnType) String() string {
	switch t {
	case TypeInt:
		return "INTEGER"
	case TypeBigInt:
		return "BIGINT"
	case TypeFloat:
		return "FLOAT"
	case TypeVarchar:
		return "VARCHAR"
	case TypeBytes:
		return "BYTES"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeJSONB:
		return "JSONB"
	case TypeBool:
		return "BOOL"
	default:
		return "INVALID"
	}
}

// Errors returned when a key cannot be built. They are distinct so the executor
// can map them onto the right SQLSTATE rather than collapsing every layout
// problem into one opaque failure.
var (
	// ErrInvalidSegment reports a path segment that would break the layout: an
	// empty name, a reserved marker, or a name holding a separator.
	ErrInvalidSegment = keyspace.ErrInvalidSegment
	// ErrTypeMismatch reports a value encoded under the wrong column type.
	ErrTypeMismatch = errors.New("sql: value does not match column type")
	// ErrRowIDReserved reports a row id that collides with the catalog subtree.
	ErrRowIDReserved = errors.New("sql: row id uses a reserved prefix")
)

// reservedPrefixes are rejected at the start of a table name (decision 5). '~'
// introduces the meta subtree and '*' is reserved for future layout hints such
// as index markers; allowing either as a table name would put user data inside a
// subtree the engine scans for control information.

// ValidateTableName rejects a table name that cannot be a key segment. The rule
// lives in internal/keyspace so the cluster layer splits regions on the same
// boundary this package writes rows against.
func ValidateTableName(name string) error { return keyspace.ValidateTableName(name) }

// ValidateColumnName rejects a column name that cannot be a key segment.
func ValidateColumnName(name string) error { return keyspace.ValidateColumnName(name) }

// TablePrefix returns the prefix covering every row of a table.
func TablePrefix(db, table string) string { return keyspace.TablePrefix(db, table) }

// RowPrefix returns the prefix covering every column of one row. A row is one
// contiguous range, which is the whole point of the layout: a row read is a
// single range read rather than one lookup per column.
func RowPrefix(db, table, rowID string) string { return keyspace.RowPrefix(db, table, rowID) }

// ColumnKey returns the key holding one column of one row.
func ColumnKey(db, table, rowID, column string) string {
	return keyspace.ColumnKey(db, table, rowID, column)
}

func RowIDPrefixEnd(db, table, rowID string) string {
	return keyspace.RowIDPrefixEnd(db, table, rowID)
}

// TablePrefixEnd returns the exclusive upper bound of a table's key range.
func TablePrefixEnd(db, table string) string { return keyspace.TablePrefixEnd(db, table) }

// EscapeRowID makes a row id safe to use as a single key segment.
//
// A row id is user data, unlike a table name, so it is escaped rather than
// rejected. The encoding escapes '%' and '/' and nothing else, which makes it
// total and injective: two distinct row ids always produce distinct key
// prefixes, so two distinct primary keys can never share a row. That is the
// property guardrail 5 requires -- an unescaped '/' would let one primary key
// forge another's column boundary.
//
// The encoding is not order preserving. Escaping only these two bytes keeps
// '/' and '%' correctly ordered relative to each other but does not preserve
// order against every other byte, and Phase 9 has no range scan over a textual
// primary key (decision 6). An integer primary key uses EncodeOrderableInt
// instead, which is order preserving and needs no escaping.
func EscapeRowID(id string) string { return keyspace.EscapeRowID(id) }

// UnescapeRowID is the inverse of EscapeRowID. It exists so a scan can recover
// the original row id from a key without storing it separately, which is what
// lets a full-table scan report real primary keys.
func UnescapeRowID(escaped string) (string, error) { return keyspace.UnescapeRowID(escaped) }

// OrderableIntLen is the width of an order-preserving integer encoding: an 8
// byte big-endian value with its sign bit flipped. Fixed width is what makes it
// sortable, since a variable-width decimal string sorts wrongly ("10" < "9").
const OrderableIntLen = 8

// EncodeOrderableInt encodes a signed integer so that byte order matches
// numeric order (decision 4). Big-endian two's complement orders the magnitude
// correctly already; flipping the sign bit of the raw bits is what moves
// negatives below positives.
func EncodeOrderableInt(v int64) []byte {
	var buf [OrderableIntLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v)^(1<<63))
	return buf[:]
}

// DecodeOrderableInt is the inverse of EncodeOrderableInt.
func DecodeOrderableInt(b []byte) (int64, error) {
	if len(b) != OrderableIntLen {
		return 0, fmt.Errorf("%w: orderable int is %d bytes, want %d", ErrTypeMismatch, len(b), OrderableIntLen)
	}
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63)), nil
}

// EncodeOrderableFloat encodes a float so byte order matches numeric order.
// IEEE-754 already orders non-negative values, so the transform is confined to
// making the sign bit sort negatives below positives, with -0.0 kept adjacent to
// +0.0 rather than jumping across the whole range.
func EncodeOrderableFloat(f float64) []byte {
	bits := math.Float64bits(f)
	if f >= 0 || math.IsNaN(f) {
		bits |= 1 << 63
	} else {
		bits = ^bits
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], bits)
	return buf[:]
}

// DecodeOrderableFloat is the inverse of EncodeOrderableFloat.
func DecodeOrderableFloat(b []byte) (float64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("%w: orderable float is %d bytes, want 8", ErrTypeMismatch, len(b))
	}
	bits := binary.BigEndian.Uint64(b)
	if bits&(1<<63) != 0 {
		bits &^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits), nil
}

// EncodeOrderableBool encodes a bool as a single byte that sorts false before
// true.
func EncodeOrderableBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

// EncodeOrderableTimestamp encodes a time so byte order matches chronological
// order. The wall clock is used rather than the monotonic reading, because the
// value has to be meaningful on another replica.
func EncodeOrderableTimestamp(t time.Time) []byte {
	return EncodeOrderableInt(t.UnixNano())
}

// DecodeOrderableTimestamp is the inverse of EncodeOrderableTimestamp.
func DecodeOrderableTimestamp(b []byte) (time.Time, error) {
	ns, err := DecodeOrderableInt(b)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ns), nil
}

// EncodeValue encodes a value for a column, using the column's declared type so
// the encoding is order preserving. A NULL is stored as an empty value, which
// is unambiguous because every encoding above is at least one byte long.
func EncodeValue(t ColumnType, v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case TypeInt, TypeBigInt:
		switch n := v.(type) {
		case int64:
			return EncodeOrderableInt(n), nil
		case int:
			return EncodeOrderableInt(int64(n)), nil
		case int32:
			return EncodeOrderableInt(int64(n)), nil
		}
	case TypeFloat:
		switch n := v.(type) {
		case float64:
			return EncodeOrderableFloat(n), nil
		case float32:
			return EncodeOrderableFloat(float64(n)), nil
		}
	case TypeTimestamp:
		switch ts := v.(type) {
		case time.Time:
			return EncodeOrderableTimestamp(ts), nil
		case int64:
			return EncodeOrderableTimestamp(time.Unix(0, ts)), nil
		}
	case TypeBool:
		if b, ok := v.(bool); ok {
			return EncodeOrderableBool(b), nil
		}
	case TypeVarchar:
		switch s := v.(type) {
		case string:
			return []byte(s), nil
		case []byte:
			return s, nil
		}
	case TypeBytes, TypeJSONB:
		switch b := v.(type) {
		case []byte:
			return b, nil
		case string:
			return []byte(b), nil
		}
	default:
		return nil, fmt.Errorf("%w: type %s has no encoding", ErrTypeMismatch, t)
	}
	return nil, fmt.Errorf("%w: %T is not a %s", ErrTypeMismatch, v, t)
}

// truncateForError bounds a value echoed into an error message, so a corrupt
// large value cannot flood the client or the log with its own contents.
func truncateForError(b []byte) []byte {
	const limit = 64
	if len(b) <= limit {
		return b
	}
	return append(append([]byte(nil), b[:limit]...), []byte("...")...)
}

// DecodeValue is the inverse of EncodeValue: it turns a stored column value
// back into a Go value of the shape the PG frontend will serialise.
//
// NULL is not a value in this layer. A null column is represented by the
// absence of its key, so DecodeValue never has to invent a NULL from the bytes
// it is handed, and an empty string stays an empty string rather than
// collapsing into NULL. Fixed-width types still reject a wrong length, because
// there is no reading of a one byte int.
//
// The decoded types are the ones encoding/json and the wire layer already
// handle, and int64 rather than int32 for the integer types so a bigint does not
// silently lose its width on the way back out.
func DecodeValue(t ColumnType, b []byte) (any, error) {
	switch t {
	case TypeInt, TypeBigInt:
		v, err := DecodeOrderableInt(b)
		if err != nil {
			return nil, err
		}
		return v, nil
	case TypeFloat:
		return DecodeOrderableFloat(b)
	case TypeTimestamp:
		return DecodeOrderableTimestamp(b)
	case TypeBool:
		if len(b) != 1 {
			return nil, fmt.Errorf("%w: bool is %d bytes, want 1", ErrTypeMismatch, len(b))
		}
		return b[0] == 1, nil
	case TypeVarchar:
		return string(b), nil
	case TypeBytes:
		// A copy, because the caller may hold b past the scan callback that
		// produced it, and bytea is the one type a client can write back into
		// a value that is later compared.
		out := make([]byte, len(b))
		copy(out, b)
		return out, nil
	case TypeJSONB:
		if !json.Valid(b) {
			return nil, fmt.Errorf("%w: jsonb value is not valid JSON: %q", ErrTypeMismatch, truncateForError(b))
		}
		return string(b), nil
	default:
		return nil, fmt.Errorf("%w: type %s has no decoding", ErrTypeMismatch, t)
	}
}

// GeneratedRowIDWidth is the rendered width of a TSO-derived row id
// (decision 3). A fixed width is required for the ids to sort chronologically:
// a variable-width rendering would order 999 before 1000. The width covers the
// whole uint64 range, which is wider than the 60 bit timestamp component
// ADR-003 allocates, so a row id never outgrows its column and reorders.
const GeneratedRowIDWidth = 20

// FormatGeneratedRowID renders a TSO timestamp as a zero-padded row id. It is
// called on the proposer and the result travels inside the committed key, so
// replicas never generate an id themselves (guardrail 4).
func FormatGeneratedRowID(tso uint64) string {
	return fmt.Sprintf("%0*d", GeneratedRowIDWidth, tso)
}
