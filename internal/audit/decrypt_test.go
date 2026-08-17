/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: decrypt_test.go
Description: Tests for the CLI audit log decryption path. Covers every exit
criterion in issue #41: envelope-mode round-trip, simple-mode round-trip,
legacy headerless files, fingerprint mismatch, missing key, corrupted length
prefix, and plaintext passthrough.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

// writeEncryptedAuditFile writes audit records through a real engine and closes
// it, returning the path to the finished file.
func writeEncryptedAuditFile(t *testing.T, dir string, engine *crypto.Engine) string {
	t.Helper()
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, engine)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	e.Record(EventAuthSuccess, "user logged in",
		log.String("user", "alice"),
		log.String("remote_addr", "10.0.0.1:5000"),
	)
	e.Record(EventACLDeny, "command denied",
		log.String("user", "bob"),
		log.String("command", "set"),
		log.String("key", "secret"),
	)
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	return singleAuditFile(t, dir)
}

// writeEnvelopeAuditFile writes audit records through an envelope-mode engine
// and closes it, returning the path to the finished file.
func writeEnvelopeAuditFile(t *testing.T, dir string, kek []byte) string {
	t.Helper()
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), true, kek, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	e.Record(EventAuthSuccess, "user logged in",
		log.String("user", "alice"),
	)
	e.Record(EventCommand, "GET executed",
		log.String("user", "bob"),
		log.String("command", "get"),
	)
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	return singleAuditFile(t, dir)
}

// --- Issue #41 exit criteria tests ---

func TestDecryptFileEnvelopeMode(t *testing.T) {
	dir := t.TempDir()
	kek := bytes.Repeat([]byte{0x2a}, 32)

	path := writeEnvelopeAuditFile(t, dir, kek)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	kekEngine, err := crypto.NewEngine(kek, nil)
	if err != nil {
		t.Fatal(err)
	}

	out, err := DecryptFile(f, dir, kekEngine)
	if err != nil {
		t.Fatalf("DecryptFile: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 decrypted records, got %d: %s", len(lines), out)
	}
	// First record: auth_success with user=alice
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v", err)
	}
	if m["event"] != "auth_success" || m["user"] != "alice" {
		t.Fatalf("line 0 = %v, want auth_success/alice", m)
	}
	// Second record: command event with user=bob
	if err := json.Unmarshal([]byte(lines[1]), &m); err != nil {
		t.Fatalf("line 1 is not valid JSON: %v", err)
	}
	if m["event"] != "command" || m["user"] != "bob" {
		t.Fatalf("line 1 = %v, want command/bob", m)
	}
}

func TestDecryptFileSimpleMode(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)

	ce, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := writeEncryptedAuditFile(t, dir, ce)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	out, err := DecryptFile(f, dir, ce)
	if err != nil {
		t.Fatalf("DecryptFile: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 decrypted records, got %d: %s", len(lines), out)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v", err)
	}
	if m["event"] != "auth_success" || m["user"] != "alice" {
		t.Fatalf("line 0 = %v, want auth_success/alice", m)
	}
	if err := json.Unmarshal([]byte(lines[1]), &m); err != nil {
		t.Fatalf("line 1 is not valid JSON: %v", err)
	}
	if m["event"] != "acl_deny" || m["user"] != "bob" {
		t.Fatalf("line 1 = %v, want acl_deny/bob", m)
	}
}

func TestDecryptFileLegacyHeaderless(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x55}, 32)

	// Write a headerless encrypted file manually: two length-prefixed sealed blobs.
	ce, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	record1 := []byte(`{"time":"2026-08-12T10:00:00Z","level":"AUDIT","event":"auth_success","user":"charlie"}` + "\n")
	record2 := []byte(`{"time":"2026-08-12T10:01:00Z","level":"AUDIT","event":"command","user":"dave","command":"get"}` + "\n")

	var buf bytes.Buffer
	for _, rec := range [][]byte{record1, record2} {
		sealed, err := ce.EncryptInPlace(nil, rec)
		if err != nil {
			t.Fatal(err)
		}
		var prefix [4]byte
		prefix[0] = byte(len(sealed) >> 24)
		prefix[1] = byte(len(sealed) >> 16)
		prefix[2] = byte(len(sealed) >> 8)
		prefix[3] = byte(len(sealed))
		buf.Write(prefix[:])
		buf.Write(sealed)
	}

	// Write without the TSDA header — legacy format.
	path := filepath.Join(dir, "legacy_0000000000_00000000_0_tsd.log")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	out, err := DecryptFile(f, dir, ce)
	if err != nil {
		t.Fatalf("DecryptFile: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 records from legacy file, got %d: %s", len(lines), out)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v", err)
	}
	if m["user"] != "charlie" {
		t.Fatalf("line 0 user = %v, want charlie", m["user"])
	}
}

func TestDecryptFileWrongKey(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)

	ce, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := writeEncryptedAuditFile(t, dir, ce)

	// Use a different key.
	wrongKey := bytes.Repeat([]byte{0x99}, 32)
	wrongEngine, err := crypto.NewEngine(wrongKey, nil)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	_, err = DecryptFile(f, dir, wrongEngine)
	if err == nil {
		t.Fatal("expected fingerprint mismatch error for wrong key")
	}
	if !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("error should mention fingerprint mismatch, got: %v", err)
	}
}

func TestDecryptFileMissingKey(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)

	ce, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := writeEncryptedAuditFile(t, dir, ce)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	_, err = DecryptFile(f, dir, nil)
	if err == nil {
		t.Fatal("expected error when no key is supplied")
	}
	if !strings.Contains(err.Error(), "no key") {
		t.Fatalf("error should mention missing key, got: %v", err)
	}
}

func TestDecryptFileCorruptedLengthPrefix(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)

	ce, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := writeEncryptedAuditFile(t, dir, ce)

	// Read the file and corrupt it: append garbage that looks like a truncated
	// length prefix + partial blob.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Append a length prefix promising 9999 bytes but only 2 bytes of payload.
	var tail [6]byte
	tail[0] = 0x00
	tail[1] = 0x00
	tail[2] = 0x27
	tail[3] = 0x07
	tail[4] = 0x01
	tail[5] = 0x02
	data = append(data, tail[:]...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// Should still decrypt the valid records and stop at the corruption.
	out, err := DecryptFile(f, dir, ce)
	if err != nil {
		t.Fatalf("DecryptFile should handle truncated tail: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 valid records despite corruption, got %d: %s", len(lines), out)
	}
}

func TestDecryptFilePlaintext(t *testing.T) {
	dir := t.TempDir()

	// Write a plaintext file with a KeyModeSimple header.
	key := bytes.Repeat([]byte{0xAA}, 32)
	fp := crypto.FingerprintBytes(key)
	path := filepath.Join(dir, "plain_0000000000_00000000_0_tsd.log")
	var buf bytes.Buffer
	// Header: TSDA + version 1 + KeyModeSimple + fingerprint
	buf.WriteString(auditFileMagic)
	buf.WriteByte(auditFileVersion)
	buf.WriteByte(KeyModeSimple)
	buf.Write(fp[:])
	buf.WriteString(`{"time":"2026-08-12T10:00:00Z","level":"AUDIT","event":"auth_success","user":"eve"}` + "\n")
	buf.WriteString(`{"time":"2026-08-12T10:01:00Z","level":"AUDIT","event":"command","user":"frank","command":"set"}` + "\n")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// Supply a matching key (even though it's plaintext, the header has a
	// fingerprint that must match).
	ce, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatal(err)
	}

	out, err := DecryptFile(f, dir, ce)
	if err != nil {
		t.Fatalf("DecryptFile: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 plaintext records, got %d: %s", len(lines), out)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v", err)
	}
	if m["user"] != "eve" {
		t.Fatalf("line 0 user = %v, want eve", m["user"])
	}
}

func TestDecryptFileEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty_tsd.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	_, err = DecryptFile(f, dir, nil)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error should mention empty, got: %v", err)
	}
}

func TestDecryptFileEnvelopeFingerprintMismatch(t *testing.T) {
	dir := t.TempDir()
	kek := bytes.Repeat([]byte{0x2a}, 32)

	path := writeEnvelopeAuditFile(t, dir, kek)

	// Use a different KEK for decryption.
	wrongKEK := bytes.Repeat([]byte{0x99}, 32)
	wrongEngine, err := crypto.NewEngine(wrongKEK, nil)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	_, err = DecryptFile(f, dir, wrongEngine)
	if err == nil {
		t.Fatal("expected KEK fingerprint mismatch error")
	}
	if !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("error should mention fingerprint mismatch, got: %v", err)
	}
}
