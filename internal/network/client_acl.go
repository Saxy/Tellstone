/*
Package network
Tellstone Cloud-Native In-Memory Database
File: client_acl.go
Description: Binary-protocol client methods for the ACL command family. Requests carry their
arguments in the message Value as length-prefixed tokens (EncodeRoleArgs); the server replies
with ResponseOK in a MsgResponse frame on success, the encoded typed ACL LIST payload, or the
error detail in a MsgError frame that surfaces as the returned error. Admin ops only — no
allocation concerns on the hot path.

Authors:

	Maximilian Hagen
*/
package network

import "fmt"

// AclSetUser issues ACL SETUSER <username> <role> [>password] [nopass], the
// ACL alias of ROLE SETUSER. The role must already exist.
func (c *Client) AclSetUser(username, role string, passOptions [][]byte, scratchBuf []byte) error {
	args := make([][]byte, 0, 2+len(passOptions))
	args = append(args, []byte(username), []byte(role))
	args = append(args, passOptions...)
	return c.roleMutate(OpACLSetUser, args, scratchBuf)
}

// AclDelUser issues ACL DELUSER <username>.
func (c *Client) AclDelUser(username string, scratchBuf []byte) error {
	return c.roleMutate(OpACLDelUser, [][]byte{[]byte(username)}, scratchBuf)
}

// AclList issues ACL LIST and decodes the typed response: one user per entry
// with username, bound role, password presence, and the role's granted
// commands and namespace whitelist.
func (c *Client) AclList(scratchBuf []byte) ([]ACLUser, error) {
	value, err := c.roleListValue(OpACLList, scratchBuf)
	if err != nil {
		return nil, err
	}
	users, ok := DecodeACLListResponse(value)
	if !ok {
		return nil, fmt.Errorf("server: malformed ACL LIST response")
	}
	return users, nil
}

// AclLog issues ACL LOG and decodes the typed response: the recent auth-failure
// buffer in chronological order, each entry carrying timestamp, username,
// remote address, and reason.
func (c *Client) AclLog(scratchBuf []byte) ([]AuthLogEntry, error) {
	value, err := c.roleListValue(OpACLLog, scratchBuf)
	if err != nil {
		return nil, err
	}
	entries, ok := DecodeACLLogResponse(value)
	if !ok {
		return nil, fmt.Errorf("server: malformed ACL LOG response")
	}
	return entries, nil
}
