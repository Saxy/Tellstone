/*
Package network
Tellstone Secure TCP Networking Package
File: role.go
Description: Wire codec for the ROLE OpCodes. Requests pack their argument list into the
message Value field as length-prefixed tokens; responses use the same primitive. All lengths
are big-endian uint16, so a single token is capped at 64 KiB. ROLE is an admin operation, so
these helpers allocate freely — they never touch the KV hot path.

Authors:

	Maximilian Hagen
*/
package network

import (
	"encoding/binary"
	"math"
)

// Request encoding: [2B argCount] then, for each argument, [2B len][bytes].
// The message Key field stays empty and the whole token list rides in Value.

// EncodeRoleArgs packs args into a request payload. Empty args yield a
// two-byte zero count. ok is false when the argument count or any single
// argument exceeds the 64 KiB length-prefix limit — encoding it would
// silently truncate the wire form.
func EncodeRoleArgs(args [][]byte) ([]byte, bool) {
	if len(args) > math.MaxUint16 {
		return nil, false
	}
	size := 2
	for _, a := range args {
		if len(a) > math.MaxUint16 {
			return nil, false
		}
		size += 2 + len(a)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[:2], uint16(len(args)))
	pos := 2
	for _, a := range args {
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(a)))
		pos += 2
		copy(buf[pos:], a)
		pos += len(a)
	}
	return buf, true
}

// DecodeRoleArgs unpacks a role request payload into dst[:0]. ok is false when
// the payload is truncated or has trailing garbage.
func DecodeRoleArgs(payload []byte, dst [][]byte) (args [][]byte, ok bool) {
	dst = dst[:0]
	if len(payload) < 2 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2
	for i := 0; i < count; i++ {
		if pos+2 > len(payload) {
			return nil, false
		}
		n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+n > len(payload) {
			return nil, false
		}
		dst = append(dst, payload[pos:pos+n])
		pos += n
	}
	return dst, pos == len(payload)
}

// RoleUser is the decoded ROLE GETUSER response.
type RoleUser struct {
	Role    string // empty when the user has no explicit role
	HasPass bool
}

// RoleListEntry is one role from a ROLE LIST response.
type RoleListEntry struct {
	Name       string
	Commands   []string
	Namespaces [][]byte
}

// GETUSER response: [2B roleLen][role][1B haspass].

// EncodeRoleGetUserResponse packs a ROLE GETUSER response.
func EncodeRoleGetUserResponse(u RoleUser) []byte {
	buf := make([]byte, 0, 2+len(u.Role)+1)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(u.Role)))
	buf = append(buf, u.Role...)
	if u.HasPass {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

// DecodeRoleGetUserResponse unpacks a ROLE GETUSER response.
func DecodeRoleGetUserResponse(payload []byte) (RoleUser, bool) {
	if len(payload) < 3 {
		return RoleUser{}, false
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	// The response must be exactly [2B roleLen][role][1B haspass]; trailing
	// bytes mean a mismatched or corrupt frame, so reject them like the other
	// decoders reject trailing garbage.
	if 2+n+1 != len(payload) {
		return RoleUser{}, false
	}
	return RoleUser{
		Role:    string(payload[2 : 2+n]),
		HasPass: payload[2+n] == 1,
	}, true
}

// encodeByteList appends a [2B count]{[2B len][bytes]} list to buf. ok is
// false when the count or any element exceeds the 64 KiB length-prefix limit.
func encodeList[T []byte | string](buf []byte, items []T) ([]byte, bool) {
	if len(items) > math.MaxUint16 {
		return nil, false
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(items)))
	for _, b := range items {
		if len(b) > math.MaxUint16 {
			return nil, false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(b)))
		buf = append(buf, b...)
	}
	return buf, true
}

// decodeU16String reads one [2B len][bytes] token from payload at pos and
// returns it as a string with the position after the token; ok is false when
// the length or the token overruns the buffer.
func decodeU16String(payload []byte, pos int) (string, int, bool) {
	if pos+2 > len(payload) {
		return "", 0, false
	}
	n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+n > len(payload) {
		return "", 0, false
	}
	return string(payload[pos : pos+n]), pos + n, true
}

// decodeStringList reads a [2B count]{[2B len][str]} list into dst; ok is
// false when any token overruns the buffer.
func decodeStringList(payload []byte, pos int, dst []string) ([]string, int, bool) {
	if pos+2 > len(payload) {
		return nil, 0, false
	}
	count := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	for i := 0; i < count; i++ {
		s, next, ok := decodeU16String(payload, pos)
		if !ok {
			return nil, 0, false
		}
		dst = append(dst, s)
		pos = next
	}
	return dst, pos, true
}

// decodeByteList reads a [2B count]{[2B len][bytes]} list into dst, deep-copying
// each element so the slices outlive the payload buffer they alias; ok is false
// when any token overruns the buffer.
func decodeByteList(payload []byte, pos int, dst [][]byte) ([][]byte, int, bool) {
	if pos+2 > len(payload) {
		return nil, 0, false
	}
	count := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	for i := 0; i < count; i++ {
		if pos+2 > len(payload) {
			return nil, 0, false
		}
		n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+n > len(payload) {
			return nil, 0, false
		}
		dst = append(dst, append([]byte(nil), payload[pos:pos+n]...))
		pos += n
	}
	return dst, pos, true
}

// LIST response: [2B roleCount] then per role
// [2B nameLen][name][2B cmdCount]{[2B len][cmd]}[2B nsCount]{[2B len][ns]}.

// EncodeRoleListResponse packs a ROLE LIST response. ok is false when the
// entry count or a name, command, or namespace exceeds the 64 KiB length-prefix
// limit.
func EncodeRoleListResponse(entries []RoleListEntry) ([]byte, bool) {
	if len(entries) > math.MaxUint16 {
		return nil, false
	}
	buf := make([]byte, 0, 2)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(entries)))
	for _, e := range entries {
		if len(e.Name) > math.MaxUint16 {
			return nil, false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.Name)))
		buf = append(buf, e.Name...)
		var ok bool
		if buf, ok = encodeList(buf, e.Commands); !ok {
			return nil, false
		}
		if buf, ok = encodeList(buf, e.Namespaces); !ok {
			return nil, false
		}
	}
	return buf, true
}

// DecodeRoleListResponse unpacks a ROLE LIST response. ok is false on a
// truncated payload.
func DecodeRoleListResponse(payload []byte) ([]RoleListEntry, bool) {
	if len(payload) < 2 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2
	entries := make([]RoleListEntry, 0, count)
	for i := 0; i < count; i++ {
		var e RoleListEntry
		var ok bool
		if e.Name, pos, ok = decodeU16String(payload, pos); !ok {
			return nil, false
		}
		if e.Commands, pos, ok = decodeStringList(payload, pos, e.Commands); !ok {
			return nil, false
		}
		if e.Namespaces, pos, ok = decodeByteList(payload, pos, e.Namespaces); !ok {
			return nil, false
		}
		entries = append(entries, e)
	}
	return entries, pos == len(payload)
}
