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

import "encoding/binary"

// Request encoding: [2B argCount] then, for each argument, [2B len][bytes].
// The message Key field stays empty and the whole token list rides in Value.

// EncodeRoleArgs packs args into a request payload. Empty args yield a
// two-byte zero count.
func EncodeRoleArgs(args [][]byte) []byte {
	size := 2
	for _, a := range args {
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
	return buf
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
	if 2+n+1 > len(payload) {
		return RoleUser{}, false
	}
	return RoleUser{
		Role:    string(payload[2 : 2+n]),
		HasPass: payload[2+n] == 1,
	}, true
}

// LIST response: [2B roleCount] then per role
// [2B nameLen][name][2B cmdCount]{[2B len][cmd]}[2B nsCount]{[2B len][ns]}.

// EncodeRoleListResponse packs a ROLE LIST response.
func EncodeRoleListResponse(entries []RoleListEntry) []byte {
	buf := make([]byte, 0, 2)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(entries)))
	for _, e := range entries {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.Name)))
		buf = append(buf, e.Name...)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.Commands)))
		for _, cmd := range e.Commands {
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(cmd)))
			buf = append(buf, cmd...)
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.Namespaces)))
		for _, ns := range e.Namespaces {
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(ns)))
			buf = append(buf, ns...)
		}
	}
	return buf
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
		if pos+2 > len(payload) {
			return nil, false
		}
		n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+n > len(payload) {
			return nil, false
		}
		e.Name = string(payload[pos : pos+n])
		pos += n
		if pos+2 > len(payload) {
			return nil, false
		}
		cmdCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		for j := 0; j < cmdCount; j++ {
			if pos+2 > len(payload) {
				return nil, false
			}
			clen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if pos+clen > len(payload) {
				return nil, false
			}
			e.Commands = append(e.Commands, string(payload[pos:pos+clen]))
			pos += clen
		}
		if pos+2 > len(payload) {
			return nil, false
		}
		nsCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		for j := 0; j < nsCount; j++ {
			if pos+2 > len(payload) {
				return nil, false
			}
			nsl := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if pos+nsl > len(payload) {
				return nil, false
			}
			e.Namespaces = append(e.Namespaces, payload[pos:pos+nsl])
			pos += nsl
		}
		entries = append(entries, e)
	}
	return entries, pos == len(payload)
}
