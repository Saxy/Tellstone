/*
Package network
Tellstone Cloud-Native In-Memory Database
File: client_role.go
Description: Binary-protocol client methods for the ROLE command family. Requests carry
their arguments in the message Value as length-prefixed tokens (EncodeRoleArgs); the server
replies with ResponseOK in a MsgResponse frame on success, an encoded typed payload
(LIST/GETUSER), or the error detail in a MsgError frame that surfaces as the returned error.
Admin ops only — no allocation concerns on the hot path.

Authors:

	Maximilian Hagen
*/
package network

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// errRBACReply converts a non-success response into an error. The server
// signals ROLE failures with a MsgError frame carrying the error detail.
func errRBACReply(payload []byte) error {
	return fmt.Errorf("server: %s", payload)
}

// roleRequestPayload packs a ROLE op into the MsgRequest wire layout
// [1B op][2B keyLen][8B ttl][key][value]. ROLE ops carry no key, so the fixed
// header is zeroed and the encoded args ride in the value section.
func roleRequestPayload(op OpCode, args [][]byte) ([]byte, error) {
	enc, ok := EncodeRoleArgs(args)
	if !ok {
		return nil, fmt.Errorf("role request argument exceeds the 64 KiB wire limit")
	}
	buf := make([]byte, 11+len(enc))
	buf[0] = byte(op)
	copy(buf[11:], enc)
	return buf, nil
}

func (c *Client) roleMutate(op OpCode, args [][]byte, scratchBuf []byte) error {
	payload, err := roleRequestPayload(op, args)
	if err != nil {
		return err
	}
	var resp Message
	if err := c.Call(MsgRequest, payload, scratchBuf, &resp); err != nil {
		return err
	}
	// Success is exactly a MsgResponse carrying ResponseOK; every other frame
	// (MsgError, MsgAuthErr, ...) is a failure surfaced as an error.
	if resp.Type != MsgResponse || !bytes.Equal(resp.Value, ResponseOK) {
		return errRBACReply(resp.Value)
	}
	return nil
}

// roleListValue issues a typed-list admin request (ROLE LIST, ACL LIST, ACL LOG)
// and returns the raw MsgResponse value after validating the reply frame. The
// caller decodes the value into its concrete entry type.
func (c *Client) roleListValue(op OpCode, scratchBuf []byte) ([]byte, error) {
	payload, err := roleRequestPayload(op, nil)
	if err != nil {
		return nil, err
	}
	var resp Message
	if err := c.Call(MsgRequest, payload, scratchBuf, &resp); err != nil {
		return nil, err
	}
	if resp.Type != MsgResponse {
		return nil, errRBACReply(resp.Value)
	}
	return resp.Value, nil
}

// RoleCreate issues ROLE CREATE <name> <rule>... .
func (c *Client) RoleCreate(role string, rules []string, scratchBuf []byte) error {
	args := make([][]byte, 0, 1+len(rules))
	args = append(args, []byte(role))
	for _, r := range rules {
		args = append(args, []byte(r))
	}
	return c.roleMutate(OpRoleCreate, args, scratchBuf)
}

// RoleSetUser issues ROLE SETUSER <username> <role> [>password] [nopass].
func (c *Client) RoleSetUser(username, role string, passOptions [][]byte, scratchBuf []byte) error {
	args := make([][]byte, 0, 2+len(passOptions))
	args = append(args, []byte(username), []byte(role))
	args = append(args, passOptions...)
	return c.roleMutate(OpRoleSetUser, args, scratchBuf)
}

// RoleDelUser issues ROLE DELUSER <username>.
func (c *Client) RoleDelUser(username string, scratchBuf []byte) error {
	return c.roleMutate(OpRoleDelUser, [][]byte{[]byte(username)}, scratchBuf)
}

// RoleDelete issues ROLE DELETE <role>.
func (c *Client) RoleDelete(role string, scratchBuf []byte) error {
	return c.roleMutate(OpRoleDelete, [][]byte{[]byte(role)}, scratchBuf)
}

// RoleList issues ROLE LIST and decodes the typed response.
func (c *Client) RoleList(scratchBuf []byte) ([]RoleListEntry, error) {
	value, err := c.roleListValue(OpRoleList, scratchBuf)
	if err != nil {
		return nil, err
	}
	entries, ok := DecodeRoleListResponse(value)
	if !ok {
		return nil, fmt.Errorf("server: malformed ROLE LIST response")
	}
	return entries, nil
}

// RoleGetUser issues ROLE GETUSER <username> and decodes the typed response.
func (c *Client) RoleGetUser(username string, scratchBuf []byte) (RoleUser, error) {
	payload, err := roleRequestPayload(OpRoleGetUser, [][]byte{[]byte(username)})
	if err != nil {
		return RoleUser{}, err
	}
	var resp Message
	if err := c.Call(MsgRequest, payload, scratchBuf, &resp); err != nil {
		return RoleUser{}, err
	}
	if resp.Type != MsgResponse {
		return RoleUser{}, errRBACReply(resp.Value)
	}
	u, ok := DecodeRoleGetUserResponse(resp.Value)
	if !ok {
		return RoleUser{}, fmt.Errorf("server: malformed ROLE GETUSER response")
	}
	return u, nil
}

// AuthUser authenticates with a username/password pair (RBAC mode). Returns
// nil on success, an error on wrong credentials or a missing user. The
// password is transmitted in cleartext unless the connection was made via
// DialTLS — TLS is an operator opt-in and this payload rides the same
// transport as every other message.
func (c *Client) AuthUser(username, password string, scratchBuf []byte) error {
	// The auth frame's length prefixes are uint16 fields; a longer username or
	// password would wrap them on the wire, so reject it up front instead of
	// sending a corrupt frame.
	if len(username) > math.MaxUint16 || len(password) > math.MaxUint16 {
		return ErrAuthCredentialsTooLong
	}
	payloadLen := 2 + len(username) + 2 + len(password)

	// Short credentials serialize into the stack buffer, keeping connection
	// setup allocation-free. OIDC bearer tokens (id_tokens) routinely exceed
	// 512 bytes, so fall back to a one-time heap buffer when the payload
	// overflows the stack.
	var stackBuf [512]byte
	var payload []byte
	if payloadLen > len(stackBuf) {
		payload = make([]byte, payloadLen)
	} else {
		payload = stackBuf[:payloadLen]
	}
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(username)))
	copy(payload[2:2+len(username)], username)
	binary.BigEndian.PutUint16(payload[2+len(username):4+len(username)], uint16(len(password)))
	copy(payload[4+len(username):payloadLen], password)

	var resp Message
	if err := c.Call(MsgAuth, payload, scratchBuf, &resp); err != nil {
		return err
	}
	if resp.Type != MsgAuthOk {
		return fmt.Errorf("auth failed: %s", resp.Value)
	}
	return nil
}
