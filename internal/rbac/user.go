/*
Package rbac
Tellstone Role-Based Access Control
File: user.go
Description: Defines the User type — a username bound to a role plus an optional bcrypt password
hash — and the SETUSER password-option parser shared by the RESP and binary protocol layers.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// User is a named principal bound to exactly one role. PasswordHash is an
// opaque bcrypt hash verified by the protocol layer; a nil or empty value
// marks a nopass user that accepts any password (Redis ACL semantics). The
// hash is never rendered by ROLE LIST or ROLE GETUSER.
type User struct {
	Role         string
	PasswordHash []byte
}

// PasswordFromOpts parses ROLE SETUSER password options. "nopass" clears the
// hash; ">password" bcrypt-hashes the plaintext. Multiple options are allowed
// and applied left-to-right, so ">a nopass >b" ends with b's hash. Returns nil
// for a final nopass, or an error on a malformed option.
func PasswordFromOpts(opts [][]byte) ([]byte, error) {
	var passHash []byte
	for _, opt := range opts {
		switch {
		case strings.EqualFold(string(opt), "nopass"):
			passHash = nil
		case len(opt) > 1 && opt[0] == '>':
			h, err := bcrypt.GenerateFromPassword(opt[1:], bcrypt.DefaultCost)
			if err != nil {
				return nil, err
			}
			passHash = h
		default:
			return nil, fmt.Errorf("rbac: malformed password option %q", opt)
		}
	}
	return passHash, nil
}
