/*
Package crypto
Tellstone Encryption Key Sourcing
File: keyprovider_base64.go
Description: KeyProvider implementation for a key supplied as a base64 string through a
CLI flag or environment variable.

Authors:

	Mohamad Radi
*/
package crypto

import (
	"encoding/base64"
	"fmt"
)

// Base64KeyProvider decodes a standard-encoding base64 string into raw key bytes.
//
// The transport has to be text: process arguments and environment variables are
// NUL-terminated, so a key containing 0x00 — about 1 in 8 random 32-byte keys — cannot
// pass through them intact. Binary keys therefore belong in a file; see FileKeyProvider.
type Base64KeyProvider struct {
	encoded string
}

// NewBase64KeyProvider wraps the value of --encryption-key / TSD_ENCRYPTION_KEY.
func NewBase64KeyProvider(encoded string) *Base64KeyProvider {
	return &Base64KeyProvider{encoded: encoded}
}

// Key decodes the wrapped string. An empty string yields a nil key, which leaves the
// engine in pass-through mode.
func (p *Base64KeyProvider) Key() ([]byte, error) {
	if p.encoded == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(p.encoded)
	if err != nil {
		// The key itself must never reach the logs, so report only the failure.
		return nil, fmt.Errorf("crypto: decode base64 encryption key: %w", err)
	}
	return key, nil
}
