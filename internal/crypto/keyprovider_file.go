/*
Package crypto
Tellstone Encryption Key Sourcing
File: keyprovider_file.go
Description: KeyProvider implementation that reads the encryption key from a file,
typically a mounted Kubernetes Secret or a vault-agent-sidecar-rendered path.

Authors:

	Mohamad Radi
*/
package crypto

import (
	"fmt"
	"io"
	"os"
)

// keyReadLimit is one byte more than a valid key. The extra byte is what lets an
// oversized file be rejected: capping the read at the key size would silently accept
// the first 32 bytes of an arbitrarily long file as a valid key.
const keyReadLimit = keySize + 1

// FileKeyProvider reads the encryption key from a file. The key is resolved once, at
// startup; picking up a rotated file requires a process restart.
type FileKeyProvider struct {
	path string
}

// NewFileKeyProvider returns a provider that reads the key from path when Key is called.
func NewFileKeyProvider(path string) *FileKeyProvider {
	return &FileKeyProvider{path: path}
}

// Key reads and returns the file's contents verbatim. The key is raw binary, so no
// byte is treated as formatting: trimming a trailing CR/LF would corrupt the ~1-in-128
// random keys that legitimately end in 0x0D or 0x0A. NewEngine enforces the exact
// 32-byte length, which also rejects a file carrying a stray trailing newline.
func (p *FileKeyProvider) Key() ([]byte, error) {
	file, err := os.Open(p.path)
	if err != nil {
		return nil, fmt.Errorf("crypto: open key file %q: %w", p.path, err)
	}
	defer file.Close()

	raw, err := io.ReadAll(io.LimitReader(file, keyReadLimit))
	if err != nil {
		return nil, fmt.Errorf("crypto: read key file %q: %w", p.path, err)
	}
	return raw, nil
}
