package crypto

import (
    "bytes"
    "crypto/rand"
    "testing"
)

func TestEngine_EncryptDecryptEnabled(t *testing.T) {
    // Generate a random 32‑byte key for ChaCha20‑Poly1305.
    key := make([]byte, 32)
    if _, err := rand.Read(key); err != nil {
        t.Fatalf("failed to generate key: %v", err)
    }

    eng, err := NewEngine(key)
    if err != nil {
        t.Fatalf("engine init error: %v", err)
    }
    if !eng.Enabled() {
        t.Fatalf("engine should be enabled with a valid key")
    }

    plaintext := []byte("the quick brown fox jumps over the lazy dog")

    // Allocate a destination buffer with enough capacity for nonce + ciphertext + tag.
    // Use a zero‑length slice that has the required capacity – this mirrors the
    // in‑place usage pattern of the production code.
    dst := make([]byte, 0, len(plaintext)+eng.aead.NonceSize()+eng.aead.Overhead())

    encrypted, err := eng.EncryptInPlace(dst, plaintext)
    if err != nil {
        t.Fatalf("encryption error: %v", err)
    }

    // Verify the output length matches the expected format.
    expectedLen := eng.aead.NonceSize() + len(plaintext) + eng.aead.Overhead()
    if len(encrypted) != expectedLen {
        t.Fatalf("unexpected encrypted length: got %d, want %d", len(encrypted), expectedLen)
    }

    // Decrypt and compare with the original plaintext.
    decrypted, err := eng.DecryptInPlace(encrypted)
    if err != nil {
        t.Fatalf("decryption error: %v", err)
    }
    if !bytes.Equal(decrypted, plaintext) {
        t.Fatalf("decrypted payload mismatch\nwant: %s\n got: %s", plaintext, decrypted)
    }
}

func TestEngine_PassThroughWhenDisabled(t *testing.T) {
    eng, err := NewEngine(nil) // nil key => disabled mode
    if err != nil {
        t.Fatalf("engine init error: %v", err)
    }
    if eng.Enabled() {
        t.Fatalf("engine should be disabled when no key is supplied")
    }

    plaintext := []byte("no‑encryption payload")
    // In disabled mode EncryptInPlace should simply return the input slice.
    out, err := eng.EncryptInPlace(nil, plaintext)
    if err != nil {
        t.Fatalf("encrypt error in disabled mode: %v", err)
    }
    if !bytes.Equal(out, plaintext) {
        t.Fatalf("disabled encrypt altered data")
    }

    // DecryptInPlace should also be a no‑op.
    dec, err := eng.DecryptInPlace(out)
    if err != nil {
        t.Fatalf("decrypt error in disabled mode: %v", err)
    }
    if !bytes.Equal(dec, plaintext) {
        t.Fatalf("disabled decrypt altered data")
    }
}
