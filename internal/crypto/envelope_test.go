/*
Package crypto
Tellstone Envelope Encryption
File: envelope_test.go
Description: Tests for the per-shard key envelope: key-length validation,
pass-through mode, Store/Load round-trips, KEK-change detection, corrupt-envelope
rejection, file permissions, and per-shard DEK isolation.

Authors:

	Maximilian Hagen
*/
package crypto

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func envelopeFileName(shardID uint32) string {
	return fmt.Sprintf("shard-%d.env", shardID)
}

// wrappedDEKLen is the EncryptInPlace output for a 32-byte key: nonce(12) +
// ciphertext(32) + Poly1305 tag(16).
const wrappedDEKLen = 12 + keySize + 16

func testKEK(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, keySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate KEK: %v", err)
	}
	return key
}

// NewEnvelope requires exactly a 32-byte key; a nil key selects pass-through mode
// instead, mirroring NewEngine.
func TestNewEnvelopeRejectsWrongKeyLength(t *testing.T) {
	for _, n := range []int{1, 16, 31, 33, 64} {
		if _, err := NewEnvelope(make([]byte, n), nil); err == nil {
			t.Fatalf("expected error for %d-byte key", n)
		}
	}
	env, err := NewEnvelope(make([]byte, keySize), nil)
	if err != nil {
		t.Fatalf("32-byte key should initialize: %v", err)
	}
	if !env.Enabled() {
		t.Fatal("expected an enabled envelope")
	}
}

// Without a key the envelope must be inert: no files, no state, no errors.
func TestEnvelopePassThroughWhenDisabled(t *testing.T) {
	env, err := NewEnvelope(nil, nil)
	if err != nil {
		t.Fatalf("nil key should not error: %v", err)
	}
	if env.Enabled() {
		t.Fatal("expected pass-through mode")
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("GenerateDEK should be a no-op: %v", err)
	}
	if err = env.Store(t.TempDir(), envelopeFileName(0)); err != nil {
		t.Fatalf("Store should be a no-op: %v", err)
	}
	dek, err := env.Load(t.TempDir(), envelopeFileName(0))
	if err != nil {
		t.Fatalf("Load should be a no-op: %v", err)
	}
	if dek != nil {
		t.Fatalf("Load should return nil DEK in pass-through mode, got %d bytes", len(dek))
	}
	if env.DEK() != nil {
		t.Fatal("DEK should be nil in pass-through mode")
	}
}

func TestEnvelopeGenerateDEK(t *testing.T) {
	env, err := NewEnvelope(testKEK(t), nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err := env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if len(env.DEK()) != keySize {
		t.Fatalf("DEK length = %d, want %d", len(env.DEK()), keySize)
	}
	first := append([]byte(nil), env.DEK()...)
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if bytes.Equal(env.DEK(), first) {
		t.Fatal("GenerateDEK produced the same key twice")
	}
}

// A DEK stored under one envelope must survive a Store -> fresh Envelope -> Load
// round-trip unchanged, which is exactly what a restart needs.
func TestEnvelopeStoreLoadRoundTrip(t *testing.T) {
	kek := testKEK(t)
	dir := t.TempDir()

	env, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if err = env.Store(dir, envelopeFileName(7)); err != nil {
		t.Fatalf("store: %v", err)
	}

	reloaded, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("reload init: %v", err)
	}
	got, err := reloaded.Load(dir, envelopeFileName(7))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !bytes.Equal(got, env.DEK()) {
		t.Fatal("recovered DEK differs from the stored one")
	}
}

// The on-disk envelope must carry the version byte, the KEK fingerprint, and the
// wrapped DEK — and must never leak the plaintext DEK.
func TestEnvelopeFileLayout(t *testing.T) {
	kek := testKEK(t)
	dir := t.TempDir()
	env, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if err = env.Store(dir, envelopeFileName(1)); err != nil {
		t.Fatalf("store: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, envelopeFileName(1)))
	if err != nil {
		t.Fatalf("read envelope file: %v", err)
	}
	if len(raw) != 1+envFingerprintLen+wrappedDEKLen {
		t.Fatalf("envelope size = %d, want %d", len(raw), 1+envFingerprintLen+wrappedDEKLen)
	}
	if raw[0] != envVersion {
		t.Fatalf("version byte = %d, want %d", raw[0], envVersion)
	}
	fp := FingerprintBytes(kek)
	if !bytes.Equal(raw[1:1+envFingerprintLen], fp[:]) {
		t.Fatal("stored fingerprint does not match the KEK")
	}
	if bytes.Contains(raw, env.DEK()) {
		t.Fatal("plaintext DEK leaked into the envelope file")
	}
}

// Loading with a different KEK must fail on the fingerprint check so a restart
// never silently regenerates DEKs and orphans the existing dataset.
func TestEnvelopeLoadRejectsChangedKEK(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnvelope(testKEK(t), nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if err = env.Store(dir, envelopeFileName(0)); err != nil {
		t.Fatalf("store: %v", err)
	}

	other, err := NewEnvelope(testKEK(t), nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if _, err = other.Load(dir, envelopeFileName(0)); err == nil {
		t.Fatal("expected fingerprint mismatch error for a different KEK")
	}
}

// A truncated or garbage envelope must be rejected at format or authentication
// time, not silently accepted.
func TestEnvelopeLoadRejectsBadFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, envelopeFileName(0))
	if err := os.WriteFile(path, []byte{0x99, 0x01}, 0600); err != nil {
		t.Fatalf("write envelope: %v", err)
	}
	env, err := NewEnvelope(testKEK(t), nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if _, err = env.Load(dir, envelopeFileName(0)); err == nil {
		t.Fatal("expected format error for garbage envelope")
	}
}

func TestEnvelopeLoadRejectsCorruptEnvelope(t *testing.T) {
	kek := testKEK(t)
	dir := t.TempDir()
	env, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if err = env.Store(dir, envelopeFileName(0)); err != nil {
		t.Fatalf("store: %v", err)
	}

	// Flip the last tag byte: the Poly1305 check must fail during unwrap.
	path := filepath.Join(dir, envelopeFileName(0))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read envelope: %v", err)
	}
	raw[len(raw)-1] ^= 0xff
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatalf("rewrite envelope: %v", err)
	}

	reloaded, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if _, err = reloaded.Load(dir, envelopeFileName(0)); err == nil {
		t.Fatal("expected unwrap failure on corrupted envelope")
	}
}

// Load must surface a missing file as os.ErrNotExist — the shard wiring uses
// errors.Is against it to tell first boot (generate) from restart (load).
func TestEnvelopeLoadMissingFile(t *testing.T) {
	env, err := NewEnvelope(testKEK(t), nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if _, err = env.Load(t.TempDir(), envelopeFileName(42)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

// The per-shard guarantee: N shards under one KEK must each get a unique random
// DEK, and each must load back its own without cross-talk.
func TestEnvelopePerShardIsolation(t *testing.T) {
	const numShards = 4
	kek := testKEK(t)
	dir := t.TempDir()

	deks := make([][]byte, numShards)
	for i := 0; i < numShards; i++ {
		env, err := NewEnvelope(kek, nil)
		if err != nil {
			t.Fatalf("envelope %d init: %v", i, err)
		}
		if err = env.GenerateDEK(); err != nil {
			t.Fatalf("generate DEK %d: %v", i, err)
		}
		deks[i] = append([]byte(nil), env.DEK()...)
		if err = env.Store(dir, envelopeFileName(uint32(i))); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	for i := 1; i < numShards; i++ {
		if bytes.Equal(deks[i], deks[0]) {
			t.Fatalf("shard %d reused shard 0's DEK", i)
		}
	}
	for i := 0; i < numShards; i++ {
		reloaded, err := NewEnvelope(kek, nil)
		if err != nil {
			t.Fatalf("reload %d init: %v", i, err)
		}
		got, err := reloaded.Load(dir, envelopeFileName(uint32(i)))
		if err != nil {
			t.Fatalf("load %d: %v", i, err)
		}
		if !bytes.Equal(got, deks[i]) {
			t.Fatalf("shard %d DEK mismatch on reload", i)
		}
	}
}

// Envelope files are key material: the directory and file must be created with
// restrictive permissions.
func TestEnvelopeFilePermissions(t *testing.T) {
	// Use a path Store must create itself so MkdirAll's mode is exercised.
	dir := filepath.Join(t.TempDir(), "enc")
	env, err := NewEnvelope(testKEK(t), nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	if err = env.Store(dir, envelopeFileName(0)); err != nil {
		t.Fatalf("store: %v", err)
	}
	var info os.FileInfo
	if info, err = os.Stat(dir); err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("envelope dir mode = %o, want 700 (err=%v)", info.Mode().Perm(), err)
	}
	if info, err = os.Stat(filepath.Join(dir, envelopeFileName(0))); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("envelope file mode = %o, want 600 (err=%v)", info.Mode().Perm(), err)
	}
}

// The recovered DEK must power a working data engine, and the KEK engine must not
// be able to open DEK-sealed values — the core separation envelope encryption
// provides.
func TestEnvelopeDEKFeedsEngine(t *testing.T) {
	kek := testKEK(t)
	env, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}

	dekEngine, err := NewEngine(env.DEK(), nil)
	if err != nil {
		t.Fatalf("DEK engine init: %v", err)
	}
	plaintext := []byte("sensitive value")
	sealed, err := dekEngine.EncryptInPlace(nil, plaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	kekEngine, err := NewEngine(kek, nil)
	if err != nil {
		t.Fatalf("KEK engine init: %v", err)
	}
	if _, err = kekEngine.DecryptInPlace(sealed); err == nil {
		t.Fatal("KEK engine should not open DEK-sealed data")
	}

	open, err := dekEngine.DecryptInPlace(sealed)
	if err != nil {
		t.Fatalf("DEK engine decrypt: %v", err)
	}
	if !bytes.Equal(open, plaintext) {
		t.Fatalf("DEK round-trip mismatch: got %q want %q", open, plaintext)
	}
}
