/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: protocol.go
Description: Zero-allocation RESP2 (Redis Serialization Protocol) codec. Parses multibulk
command frames directly out of the network read buffer and provides append-style reply
encoders. Used by the optional RESP listener so Tellstone can be driven by standard tooling
(redis-benchmark, memtier_benchmark) for cross-system comparison.

Authors:

	Maximilian Hagen
*/
package resp

import (
	"errors"
	"strconv"
)

var (
	// errIncomplete signals that buf does not yet contain a full command; the caller should
	// wait for more bytes (mirrors errShortRead in the binary protocol).
	errIncomplete = errors.New("resp: incomplete command")
	// errProtocol signals a malformed frame; the connection should be closed.
	errProtocol = errors.New("resp: protocol error")
)

// maxBulkLen caps a single bulk-string length to guard against malformed or hostile framing.
const maxBulkLen = 512 * 1024 * 1024 // 512 MiB

// Pre-encoded constant replies for the hot path.
var (
	respPong     = []byte("+PONG\r\n")
	respOK       = []byte("+OK\r\n")
	respNullBulk = []byte("$-1\r\n")
)

// Parse decodes a single RESP2 multibulk command from buf, appending the argument slices
// (which point INTO buf) to dst[:0]. It returns the arguments, the number of bytes consumed,
// and an error. errIncomplete means buf needs more data; errProtocol means malformed framing.
//
// Only the multibulk form (*<n>\r\n $<len>\r\n<bytes>\r\n ...) is supported, which is what
// redis-benchmark and memtier_benchmark emit.
func Parse(buf []byte, dst [][]byte) (args [][]byte, consumed int, err error) {
	dst = dst[:0]
	if len(buf) == 0 {
		return nil, 0, errIncomplete
	}
	if buf[0] != '*' {
		return nil, 0, errProtocol
	}
	n, pos, err := parseLine(buf, 1)
	if err != nil {
		return nil, 0, err
	}
	if n < 0 || n > maxBulkLen {
		return nil, 0, errProtocol
	}
	for i := 0; i < n; i++ {
		if pos >= len(buf) {
			return nil, 0, errIncomplete
		}
		if buf[pos] != '$' {
			return nil, 0, errProtocol
		}
		blen, next, lerr := parseLine(buf, pos+1)
		if lerr != nil {
			return nil, 0, lerr
		}
		if blen < 0 || blen > maxBulkLen {
			return nil, 0, errProtocol
		}
		// Need blen bytes plus the trailing CRLF.
		if next+blen+2 > len(buf) {
			return nil, 0, errIncomplete
		}
		if buf[next+blen] != '\r' || buf[next+blen+1] != '\n' {
			return nil, 0, errProtocol
		}
		dst = append(dst, buf[next:next+blen])
		pos = next + blen + 2
	}
	return dst, pos, nil
}

// parseLine reads a decimal integer terminated by CRLF starting at offset start (just past a
// type byte). It returns the parsed value and the offset just after the CRLF.
func parseLine(buf []byte, start int) (val int, next int, err error) {
	i := start
	for i < len(buf) && buf[i] != '\r' {
		i++
	}
	if i+1 >= len(buf) {
		return 0, 0, errIncomplete // no room for CRLF yet
	}
	if buf[i+1] != '\n' {
		return 0, 0, errProtocol
	}
	v, perr := strconv.Atoi(string(buf[start:i]))
	if perr != nil {
		return 0, 0, errProtocol
	}
	return v, i + 2, nil
}

// AppendSimpleString appends a RESP simple string ("+<s>\r\n").
func AppendSimpleString(dst []byte, s string) []byte {
	dst = append(dst, '+')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

// AppendError appends a RESP error ("-<s>\r\n").
func AppendError(dst []byte, s string) []byte {
	dst = append(dst, '-')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

// AppendBulk appends a RESP bulk string ("$<len>\r\n<b>\r\n").
func AppendBulk(dst, b []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

// AppendNullBulk appends a RESP2 null bulk string ("$-1\r\n").
func AppendNullBulk(dst []byte) []byte {
	return append(dst, respNullBulk...)
}

// AppendInt appends a RESP integer (":<n>\r\n").
func AppendInt(dst []byte, n int64) []byte {
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, '\r', '\n')
}

// EqualFold reports whether the ASCII command token a equals the upper-case literal b
// case-insensitively. b must already be upper-case (e.g. "GET").
func EqualFold(a []byte, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		c := a[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != b[i] {
			return false
		}
	}
	return true
}
