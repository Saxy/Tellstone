/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: wiretype.go
Description: The mapping between a catalog column type and the PostgreSQL type
OID and text representation the wire protocol needs. A column's RowDescription
entry and its DataRow bytes both come from here, so the type a client is told to
expect and the bytes it is sent cannot drift apart.
*/
package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL type OIDs for the catalog's column types. The catalog set is
// deliberately small (layout.go), so this is a direct mapping rather than a
// lookup that could silently miss a type.
// oidText and oidBytea are declared with the implicit table in translate.go and
// reused here so the implicit and catalog paths cannot disagree about them.
const (
	oidBool        = 16
	oidInt8        = 20
	oidInt4        = 23
	oidFloat8      = 701
	oidTimestamptz = 1184
	oidJSONB       = 3802
)

// oidOf maps a column type onto its wire OID.
func oidOf(t ColumnType) int32 {
	switch t {
	case TypeInt:
		return oidInt4
	case TypeBigInt:
		return oidInt8
	case TypeFloat:
		return oidFloat8
	case TypeVarchar:
		return oidText
	case TypeBytes:
		return oidBytea
	case TypeTimestamp:
		return oidTimestamptz
	case TypeJSONB:
		return oidJSONB
	case TypeBool:
		return oidBool
	default:
		// A type that reached the wire without an OID is a catalog value this
		// build does not understand; text is the only safe general fallback,
		// and the decoded value will still carry its bytes.
		return oidText
	}
}

// timestampText is the layout PostgreSQL uses for timestamptz in text format:
// a space between date and time, fractional seconds trimmed of trailing zeros,
// and a signed offset with minutes.
const timestampText = "2006-01-02 15:04:05.999999-07"

// wireCell renders a stored column value as the text-format cell for a DataRow.
// A nil value is NULL, which is transmitted as a length of -1 by the caller and
// is not this function's concern.
func wireCell(t ColumnType, value []byte) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := DecodeValue(t, value)
	if err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case int64:
		return []byte(strconv.FormatInt(x, 10)), nil
	case float64:
		// 'g' with precision -1 is the shortest representation that round
		// trips, which is what PostgreSQL emits.
		return []byte(strconv.FormatFloat(x, 'g', -1, 64)), nil
	case bool:
		if x {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case string:
		return []byte(x), nil
	case []byte:
		return encodeByteaText(x), nil
	case time.Time:
		return []byte(x.UTC().Format(timestampText)), nil
	default:
		return nil, fmt.Errorf("%w: type %s has no wire representation", ErrTypeMismatch, t)
	}
}

// encodeTextAs parses a value that arrived in PostgreSQL text format and
// re-encodes it in the column's order-preserving encoding. Parameters and string
// literals reach the frontend as text whatever the column's type is, so the
// column type is the only thing that can say how to read them.
//
// Doing this at the boundary rather than trusting the client is what keeps the
// stored bytes well formed: a value that could not be parsed is rejected here,
// instead of being stored and failing to decode on the next read.
func encodeTextAs(t ColumnType, text []byte) ([]byte, error) {
	if t == TypeBytes {
		// bytea arrives in the \x hex form, which is the same encoding the
		// implicit table's value column already uses.
		decoded, err := decodeByteaText(text)
		if err != nil {
			return nil, err
		}
		return EncodeValue(t, decoded)
	}
	switch t {
	case TypeInt, TypeBigInt:
		n, err := strconv.ParseInt(strings.TrimSpace(string(text)), 10, 64)
		if err != nil {
			return nil, &pgError{code: errSyntax, msg: fmt.Sprintf("invalid input syntax for type %s: %q", t, truncateForError(text))}
		}
		return EncodeValue(t, n)
	case TypeFloat:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(text)), 64)
		if err != nil {
			return nil, &pgError{code: errSyntax, msg: fmt.Sprintf("invalid input syntax for type %s: %q", t, truncateForError(text))}
		}
		return EncodeValue(t, f)
	case TypeBool:
		switch strings.ToLower(strings.TrimSpace(string(text))) {
		case "t", "true", "y", "yes", "on", "1":
			return EncodeValue(t, true)
		case "f", "false", "n", "no", "off", "0":
			return EncodeValue(t, false)
		}
		return nil, &pgError{code: errSyntax, msg: fmt.Sprintf("invalid input syntax for type %s: %q", t, truncateForError(text))}
	case TypeTimestamp:
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999-07:00",
			"2006-01-02 15:04:05.999999Z07:00",
			"2006-01-02 15:04:05.999999",
			"2006-01-02",
		} {
			if ts, err := time.Parse(layout, strings.TrimSpace(string(text))); err == nil {
				return EncodeValue(t, ts.UTC())
			}
		}
		return nil, &pgError{code: errSyntax, msg: fmt.Sprintf("invalid input syntax for type %s: %q", t, truncateForError(text))}
	case TypeJSONB:
		if !json.Valid(text) {
			return nil, &pgError{code: errSyntax, msg: fmt.Sprintf("invalid input syntax for type %s", t)}
		}
		return EncodeValue(t, string(text))
	default:
		// varchar and anything else textual passes through verbatim.
		return EncodeValue(t, string(text))
	}
}
