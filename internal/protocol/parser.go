/*
Package protocol
Tellstone Cloud-Native In-Memory Database
File: parser.go
Description: Statically inlined, zero-allocation byte-scanner for relational

	SQL text-queries. Eliminates heap allocations and AST-overhead
	by operating directly on live network transaction buffers.

"Speed is not about doing things faster, but about doing less things."

Authors:

	Maximilian Hagen
*/
package protocol

import (
	"errors"
	"time"
)

// ErrParse is returned when the input SQL cannot be parsed into a valid query.
var ErrParse = errors.New("invalid query")

// ErrTTLOverflow signals that a TTL value is unreasonably large (overflow).
var ErrTTLOverflow = errors.New("ttl overflow")

// trimSpaceInline removes leading and trailing ASCII whitespace without allocating.
func trimSpaceInline(b []byte) []byte {
	start := 0
	end := len(b)
	for start < end {
		c := b[start]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			start++
			continue
		}
		break
	}
	for end > start {
		c := b[end-1]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			end--
			continue
		}
		break
	}
	return b[start:end]
}

// indexByte returns the index of the first occurrence of c in b, or -1 if not found.
func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

// ParseQuery orchestrates the allocation-free scanning of a raw query byte-slice.
// It routes the relational SQL dialect intents into standard key-value primitives.
//
// DESIGN NOTE: To survive unthrottled CPU load, all child matchers work entirely
// inline on the 'raw' memory segment without duplicating slices or creating heap-escaped objects.
func ParseQuery(raw []byte) (ParsedQuery, error) {
	var q ParsedQuery
	trimmed := trimSpaceInline(raw)
	if len(trimmed) == 0 {
		return q, ErrParse
	}
	// Micro-Optimized Static Byte-Tokens to circumvent dynamic runtime generation
	kwSelect := []byte("select")
	kwInsert := []byte("insert")
	kwDelete := []byte("delete")
	// Cascade routing via case-insensitive prefix validation
	switch {
	case hasPrefixCase(trimmed, kwSelect):
		q.Type = CmdGet
		q.Key = extractSelectKeyInline(trimmed)
		if q.Key == nil {
			return q, ErrParse
		}
	case hasPrefixCase(trimmed, kwInsert):
		q.Type = CmdSet
		var overflow bool
		q.Key, q.Value, q.TTL, overflow = extractSetPayloadInline(trimmed)
		if overflow {
			return q, ErrTTLOverflow
		}
		if q.Key == nil || q.Value == nil {
			return q, ErrParse
		}
	case hasPrefixCase(trimmed, kwDelete):
		q.Type = CmdDelete
		q.Key = extractSelectKeyInline(trimmed)
		if q.Key == nil {
			return q, ErrParse
		}
	default:
		return q, ErrParse
	}

	return q, nil
}

// hasPrefixCase acts as a zero-alloc equivalent to bytes.HasPrefix while providing
// inline case-insensitivity. It reads characters on-the-fly without casting to strings.
func hasPrefixCase(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		c := b[i]
		// Inlined ASCII bit-shift conversion for lowercase matching
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if c != prefix[i] {
			return false
		}
	}
	return true
}

// indexCase implements a stack-local, case-insensitive substring index locator.
// It tracks execution offsets without generating heap garbage or intermediate copies.
func indexCase(b, substr []byte) int {
	if len(substr) == 0 {
		return 0
	}
	// Bound calculations prevent unnecessary inner out-of-range checks
	end := len(b) - len(substr) + 1
	for i := 0; i < end; i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			c := b[i+j]
			// Inlined ASCII conversion on raw indices
			if c >= 'A' && c <= 'Z' {
				c += 32
			}
			if c != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// extractSelectKeyInline isolates the key parameter from a "WHERE key = 'x'" schema statement.
// It slices directly out of the primary incoming window array, yielding sub-100ns execution depth.
func extractSelectKeyInline(b []byte) []byte {
	kwWhere := []byte("where")
	idx := indexCase(b, kwWhere)
	if idx == -1 {
		return nil
	}

	// Advance stream past the "where" token length (5 bytes)
	sub := b[idx+5:]
	eqIdx := indexByte(sub, '=')
	if eqIdx == -1 {
		return nil
	}

	// Forward past the '=' assignment character and strip wrapping quotes
	return extractQuotedValueInline(sub[eqIdx+1:])
}

// extractSetPayloadInline leverages manual index markers to split relational INSERT parameters.
// This serves as an allocation-immune replacement for bytes.Split or regex parsers, keeping
// the performance footprint bound to CPU L1/L2 cache speeds.
func extractSetPayloadInline(b []byte) ([]byte, []byte, time.Duration, bool) {
	kwValues := []byte("values")
	valIdx := indexCase(b, kwValues)
	if valIdx == -1 {
		return nil, nil, 0, false
	}
	sub := b[valIdx:]
	openParen := indexByte(sub, '(')
	if openParen == -1 {
		return nil, nil, 0, false
	}
	closeParen := indexByte(sub[openParen:], ')')
	if closeParen == -1 {
		return nil, nil, 0, false
	}
	// Isolate argument tokens wrapped inside the target parentheses block
	argsBlock := sub[openParen+1 : openParen+closeParen]
	var key, value []byte
	var ttl time.Duration
	// Token Step 1: Scan and isolate the 'Key' byte boundary
	comma1 := indexByte(argsBlock, ',')
	if comma1 == -1 {
		return extractQuotedValueInline(argsBlock), nil, 0, false
	}
	key = extractQuotedValueInline(argsBlock[:comma1])
	// Token Step 2: Scan and isolate the 'Value' byte boundary
	rem1 := argsBlock[comma1+1:]
	comma2 := indexByte(rem1, ',')
	if comma2 == -1 {
		return key, extractQuotedValueInline(rem1), 0, false
	}
	value = extractQuotedValueInline(rem1[:comma2])
	// Token Step 3: Parse optional 'TTL' parameter (in milliseconds)
	rem2 := trimSpaceInline(rem1[comma2+1:])
	if len(rem2) > 0 {
		var val int64
		for i := 0; i < len(rem2); i++ {
			if rem2[i] >= '0' && rem2[i] <= '9' {
				val = val*10 + int64(rem2[i]-'0')
			} else {
				break
			}
		}
		if val > int64((24*time.Hour)/time.Millisecond) {
			return nil, nil, 0, true
		}
		ttl = time.Duration(val) * time.Millisecond
	}

	return key, value, ttl, false
}

// extractQuotedValueInline evaluates boundaries for surrounding SQL string characters.
// Returns a clean sub-slice pointing directly into the network segment buffer.
func extractQuotedValueInline(b []byte) []byte {
	b = trimSpaceInline(b)
	if len(b) == 0 {
		return nil
	}
	// Strips singular or double-quotes interchangeably without creating data twins
	if (b[0] == '\'' && b[len(b)-1] == '\'') || (b[0] == '"' && b[len(b)-1] == '"') {
		return b[1 : len(b)-1]
	}
	return b
}
