/*
Package crypto
Tellstone Encryption Key Sourcing
File: keyprovider.go
Description: KeyProvider abstracts how the 32-byte ChaCha20-Poly1305 key is sourced, so
NewEngine and the hot-path Encrypt/Decrypt calls stay agnostic of whether the key came
from a CLI flag, a mounted file, or (via a separate opt-in module) an external secret
manager.

Authors:

	Mohamad Radi
*/
package crypto

import "golang.org/x/crypto/chacha20poly1305"

// keySize is the ChaCha20-Poly1305 key length that NewEngine enforces.
const keySize = chacha20poly1305.KeySize

// KeyProvider resolves the raw encryption key. Resolution happens once at server
// startup; implementations are not called from the encrypt/decrypt hot path and may
// perform I/O.
type KeyProvider interface {
	// Key returns the raw key bytes. A nil or empty key with a nil error means
	// encryption is disabled; NewEngine enforces the required 32-byte length.
	Key() ([]byte, error)
}
