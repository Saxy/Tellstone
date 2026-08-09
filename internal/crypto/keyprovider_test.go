package crypto

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A 32-byte key is 44 base64 characters padded and 43 unpadded; both are accepted.
func TestBase64KeyProviderAcceptsPaddedAndUnpadded(t *testing.T) {
	key := make([]byte, keySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	padded := base64.StdEncoding.EncodeToString(key)
	unpadded := base64.RawStdEncoding.EncodeToString(key)

	if len(padded) != 44 || len(unpadded) != 43 {
		t.Fatalf("unexpected encoded lengths: padded %d, unpadded %d", len(padded), len(unpadded))
	}

	for name, encoded := range map[string]string{"padded": padded, "unpadded": unpadded} {
		t.Run(name, func(t *testing.T) {
			got, err := NewBase64KeyProvider(encoded, nil).Key()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, key) {
				t.Fatalf("key mismatch: got %d bytes want %d", len(got), len(key))
			}
		})
	}
}

// DEPRECATED PATH: remove alongside the raw fallback in v2.
//
// The raw form predates the base64 decoding the flag always documented, so v1 keeps
// accepting it. It cannot collide with base64: 32 characters decode to 24 bytes, never
// the 32 the base64 branch requires.
func TestBase64KeyProviderAcceptsDeprecatedRawValue(t *testing.T) {
	raw := "0123456789abcdef0123456789abcdef"
	if len(raw) != keySize {
		t.Fatalf("fixture must be %d characters, got %d", keySize, len(raw))
	}
	if decoded, err := base64.StdEncoding.DecodeString(raw); err != nil || len(decoded) == keySize {
		t.Fatalf("fixture should decode as valid base64 to a length other than %d", keySize)
	}

	got, err := NewBase64KeyProvider(raw, nil).Key()
	if err != nil {
		t.Fatalf("deprecated raw key must still be accepted: %v", err)
	}
	if !bytes.Equal(got, []byte(raw)) {
		t.Fatalf("raw key not passed through verbatim: got %q", got)
	}
	if _, err = NewEngine(got, nil); err != nil {
		t.Fatalf("NewEngine rejected the deprecated raw key: %v", err)
	}
}

// Base64 is what makes the flag/env path usable at all: roughly 1 in 8 random 32-byte
// keys contains a NUL, which cannot survive a process argument or environment variable.
func TestBase64KeyProviderCarriesNULBytes(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i) // key[0] is 0x00
	}
	encoded := base64.StdEncoding.EncodeToString(key)
	if strings.ContainsRune(encoded, 0) {
		t.Fatal("encoded form must be NUL-free to survive argv")
	}

	got, err := NewBase64KeyProvider(encoded, nil).Key()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatalf("key with NUL not round-tripped: got %d bytes want %d", len(got), len(key))
	}
	eng, err := NewEngine(got, nil)
	if err != nil {
		t.Fatalf("NewEngine rejected a valid key containing NUL: %v", err)
	}
	if !eng.Enabled() {
		t.Fatal("engine should be enabled")
	}
}

func TestBase64KeyProviderEmpty(t *testing.T) {
	got, err := NewBase64KeyProvider("", nil).Key()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty key, got %d bytes", len(got))
	}
}

func TestBase64KeyProviderRejectsInvalidEncoding(t *testing.T) {
	_, err := NewBase64KeyProvider("not!valid!base64!", nil).Key()
	if err == nil {
		t.Fatal("expected an error for a malformed base64 key")
	}
	// The key material must never leak into logs or error strings.
	if strings.Contains(err.Error(), "not!valid!base64!") {
		t.Fatalf("error must not echo the key value: %v", err)
	}
}

func TestFileKeyProvider(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	got, err := NewFileKeyProvider(path).Key()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatalf("key mismatch: got %v want %v", got, key)
	}
}

// A raw key is binary, so a terminal 0x0D/0x0A is key material rather than formatting.
// Roughly 1 in 128 randomly generated keys ends in one of those bytes; trimming them
// would silently corrupt the key and reject a valid file.
func TestFileKeyProviderPreservesTerminalLineEndingBytes(t *testing.T) {
	tests := map[string]byte{
		"lf": '\n',
		"cr": '\r',
	}
	for name, last := range tests {
		t.Run(name, func(t *testing.T) {
			key := make([]byte, 32)
			for i := range key {
				key[i] = byte(i)
			}
			key[31] = last

			path := filepath.Join(t.TempDir(), "key")
			if err := os.WriteFile(path, key, 0o600); err != nil {
				t.Fatalf("write key file: %v", err)
			}
			got, err := NewFileKeyProvider(path).Key()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, key) {
				t.Fatalf("key not preserved verbatim: got %d bytes want %d", len(got), len(key))
			}
			// The whole point: a 32-byte key ending in this byte must still drive the engine.
			eng, err := NewEngine(got, nil)
			if err != nil {
				t.Fatalf("NewEngine rejected a valid 32-byte key: %v", err)
			}
			if !eng.Enabled() {
				t.Fatal("engine should be enabled")
			}
		})
	}
}

// A file carrying a stray trailing newline is 33 bytes and must be rejected rather
// than silently trimmed back to a "valid" 32.
func TestFileKeyProviderRejectsOversizedFile(t *testing.T) {
	content := append(make([]byte, 32), '\n')
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	got, err := NewFileKeyProvider(path).Key()
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if len(got) != 33 {
		t.Fatalf("expected the file verbatim (33 bytes), got %d", len(got))
	}
	if _, err = NewEngine(got, nil); err == nil {
		t.Fatal("expected NewEngine to reject a 33-byte key")
	}
}

// An unbounded read of a large or endless file would stall startup or exhaust memory
// before NewEngine ever got to check the length.
func TestFileKeyProviderBoundsTheRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, make([]byte, 8<<20), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	got, err := NewFileKeyProvider(path).Key()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != keyReadLimit {
		t.Fatalf("read was not bounded: got %d bytes want %d", len(got), keyReadLimit)
	}
	// Bounded, and still rejected rather than passing off the first 32 bytes as a key.
	if _, err = NewEngine(got, nil); err == nil {
		t.Fatal("expected NewEngine to reject an oversized key")
	}
}

// /dev/zero never reaches EOF, so an unbounded read of it never returns at all.
func TestFileKeyProviderTerminatesOnEndlessFile(t *testing.T) {
	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("/dev/zero not available on this platform")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		got, err := NewFileKeyProvider("/dev/zero").Key()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if len(got) != keyReadLimit {
			t.Errorf("read was not bounded: got %d bytes", len(got))
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("reading /dev/zero did not terminate: the read is unbounded")
	}
}

func TestFileKeyProviderMissingFile(t *testing.T) {
	_, err := NewFileKeyProvider(filepath.Join(t.TempDir(), "does-not-exist")).Key()
	if err == nil {
		t.Fatal("expected error for missing key file")
	}
}

func TestFileKeyProviderFeedsEngine(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	resolved, err := NewFileKeyProvider(path).Key()
	if err != nil {
		t.Fatalf("resolve key: %v", err)
	}
	eng, err := NewEngine(resolved, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if !eng.Enabled() {
		t.Fatal("engine should be enabled with a file-sourced 32-byte key")
	}
}
