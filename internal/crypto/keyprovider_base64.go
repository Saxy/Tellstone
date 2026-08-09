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
	"errors"

	"github.com/Saxy/Tellstone/internal/log"
)

// Base64KeyProvider decodes a base64 string into raw key bytes, accepting the padded
// and unpadded standard encodings.
//
// The transport has to be text: process arguments and environment variables are
// NUL-terminated, so a key containing 0x00 — about 1 in 8 random 32-byte keys — cannot
// pass through them intact. Binary keys therefore belong in a file; see FileKeyProvider.
type Base64KeyProvider struct {
	encoded string
	logger  log.Logger
}

// NewBase64KeyProvider wraps the value of --encryption-key / TSD_ENCRYPTION_KEY. The
// logger reports use of the deprecated raw form; a nil logger silences it.
func NewBase64KeyProvider(encoded string, logger log.Logger) *Base64KeyProvider {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	return &Base64KeyProvider{encoded: encoded, logger: logger}
}

// Key decodes the wrapped string. An empty string yields a nil key, which leaves the
// engine in pass-through mode.
func (p *Base64KeyProvider) Key() ([]byte, error) {
	if p.encoded == "" {
		return nil, nil
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding} {
		if key, err := encoding.DecodeString(p.encoded); err == nil && len(key) == keySize {
			return key, nil
		}
	}
	// DEPRECATED: the raw 32-byte value. This flag has always documented itself as
	// base64 but never decoded, so deployments predating that fix pass the key as plain
	// text. Accepting it keeps them starting within v1. Remove this branch, its test,
	// and the deprecation notes in README.md and internal/crypto/README.md in v2, which
	// leaves base64 as the only accepted form.
	if len(p.encoded) == keySize {
		if p.logger.Enabled(log.LevelWarn) {
			// Re-encoding preserves the existing key; generating a new one would leave
			// already-encrypted data unreadable.
			p.logger.Log(log.LevelWarn, "raw --encryption-key value is deprecated and insecure",
				log.String("use", `--encryption-key "$(printf %s "$KEY" | base64)", or --encryption-key-file`),
			)
		}
		return []byte(p.encoded), nil
	}
	// Deliberately describes the accepted shapes without echoing the key itself.
	return nil, errors.New("crypto: --encryption-key must be a base64-encoded 32-byte key (44 characters padded, 43 unpadded), or the deprecated raw 32-character value")
}
