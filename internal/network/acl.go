/*
Package network
Tellstone Secure TCP Networking Package
File: acl.go
Description: Wire codec for the ACL OpCodes. Requests reuse the ROLE primitive — length-prefixed
tokens in the message Value field — and ACL LIST responses use the same primitive as ROLE LIST.
All lengths are big-endian uint16, so a single token is capped at 64 KiB. ACL is an admin
operation, so these helpers allocate freely — they never touch the KV hot path.

Authors:

	Maximilian Hagen
*/
package network

import (
	"encoding/binary"
	"math"
)

// ACLUser is one user from an ACL LIST response.
type ACLUser struct {
	Username   string
	Role       string // empty when the default role applies
	HasPass    bool
	Commands   []string
	Namespaces [][]byte
}

// LIST response: [2B userCount] then per user
// [2B nameLen][name][2B roleLen][role][1B hasPass][2B cmdCount]{[2B len][cmd]}[2B nsCount]{[2B len][ns]}.

// EncodeACLListResponse packs an ACL LIST response. ok is false when the user
// count or a name, command, or namespace exceeds the 64 KiB length-prefix
// limit.
func EncodeACLListResponse(users []ACLUser) ([]byte, bool) {
	if len(users) > math.MaxUint16 {
		return nil, false
	}
	buf := make([]byte, 0, 2)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(users)))
	for _, u := range users {
		if len(u.Username) > math.MaxUint16 {
			return nil, false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(u.Username)))
		buf = append(buf, u.Username...)
		if len(u.Role) > math.MaxUint16 {
			return nil, false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(u.Role)))
		buf = append(buf, u.Role...)
		if u.HasPass {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		if len(u.Commands) > math.MaxUint16 {
			return nil, false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(u.Commands)))
		for _, cmd := range u.Commands {
			if len(cmd) > math.MaxUint16 {
				return nil, false
			}
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(cmd)))
			buf = append(buf, cmd...)
		}
		if len(u.Namespaces) > math.MaxUint16 {
			return nil, false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(u.Namespaces)))
		for _, ns := range u.Namespaces {
			if len(ns) > math.MaxUint16 {
				return nil, false
			}
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(ns)))
			buf = append(buf, ns...)
		}
	}
	return buf, true
}

// DecodeACLListResponse unpacks an ACL LIST response. ok is false on a
// truncated payload.
func DecodeACLListResponse(payload []byte) ([]ACLUser, bool) {
	if len(payload) < 2 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2
	users := make([]ACLUser, 0, count)
	for i := 0; i < count; i++ {
		var u ACLUser
		if pos+2 > len(payload) {
			return nil, false
		}
		n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+n > len(payload) {
			return nil, false
		}
		u.Username = string(payload[pos : pos+n])
		pos += n
		if pos+2 > len(payload) {
			return nil, false
		}
		n = int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+n > len(payload) {
			return nil, false
		}
		u.Role = string(payload[pos : pos+n])
		pos += n
		if pos >= len(payload) {
			return nil, false
		}
		u.HasPass = payload[pos] == 1
		pos++
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
			u.Commands = append(u.Commands, string(payload[pos:pos+clen]))
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
			u.Namespaces = append(u.Namespaces, append([]byte(nil), payload[pos:pos+nsl]...))
			pos += nsl
		}
		users = append(users, u)
	}
	return users, pos == len(payload)
}

// AuthLogEntry is one ACL LOG record. Timestamp is an RFC3339 string, the same
// rendering the RESP ACL LOG handler emits, so both protocols expose identical
// log content.
type AuthLogEntry struct {
	Timestamp  string
	Username   string
	RemoteAddr string
	Reason     string
}

// LOG response: [2B entryCount] then per entry
// [2B tsLen][timestamp][2B userLen][username][2B addrLen][remoteAddr][2B reasonLen][reason].

// EncodeACLLogResponse packs an ACL LOG response. ok is false when the entry
// count or any field exceeds the 64 KiB length-prefix limit.
func EncodeACLLogResponse(entries []AuthLogEntry) ([]byte, bool) {
	if len(entries) > math.MaxUint16 {
		return nil, false
	}
	buf := make([]byte, 0, 2)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(entries)))
	for _, e := range entries {
		for _, field := range []string{e.Timestamp, e.Username, e.RemoteAddr, e.Reason} {
			if len(field) > math.MaxUint16 {
				return nil, false
			}
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(field)))
			buf = append(buf, field...)
		}
	}
	return buf, true
}

// DecodeACLLogResponse unpacks an ACL LOG response. ok is false on a truncated
// payload.
func DecodeACLLogResponse(payload []byte) ([]AuthLogEntry, bool) {
	if len(payload) < 2 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2
	entries := make([]AuthLogEntry, 0, count)
	for i := 0; i < count; i++ {
		var e AuthLogEntry
		fields := []*string{&e.Timestamp, &e.Username, &e.RemoteAddr, &e.Reason}
		for _, field := range fields {
			if pos+2 > len(payload) {
				return nil, false
			}
			n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if pos+n > len(payload) {
				return nil, false
			}
			*field = string(payload[pos : pos+n])
			pos += n
		}
		entries = append(entries, e)
	}
	return entries, pos == len(payload)
}
