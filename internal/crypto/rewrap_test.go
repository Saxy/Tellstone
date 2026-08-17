/*
Package crypto
Tellstone Envelope Re-wrap
File: rewrap_test.go
Description: Tests for KEK rotation via RewrapEnvelopes: successful rewrap,
fingerprint-mismatch abort, idempotent retry, retain-old-keys backup,
same-key rejection, mixed-key rejection, and DEK preservation.

Authors:

	Maximilian Hagen
*/
package crypto

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// createEnvelope stores a random DEK under the given KEK in dir, returning
// the DEK for later comparison.
func createEnvelope(t *testing.T, dir string, kek []byte, name string) []byte {
	t.Helper()
	env, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	if err = env.GenerateDEK(); err != nil {
		t.Fatalf("generate DEK: %v", err)
	}
	dek := append([]byte(nil), env.DEK()...)
	if err = env.Store(dir, name); err != nil {
		t.Fatalf("store: %v", err)
	}
	return dek
}

// loadDEK reads an envelope file and returns the unwrapped DEK.
func loadDEK(t *testing.T, dir string, kek []byte, name string) []byte {
	t.Helper()
	env, err := NewEnvelope(kek, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	dek, err := env.Load(dir, name)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return dek
}

func TestRewrapSuccess(t *testing.T) {
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	// Create shard + audit envelopes.
	dek0 := createEnvelope(t, dir, oldKEK, "shard-0.env")
	dek1 := createEnvelope(t, dir, oldKEK, "shard-1.env")
	dekA := createEnvelope(t, dir, oldKEK, "audit.env")

	// Drop non-envelope fixtures alongside the envelopes. RewrapEnvelopes
	// must leave them byte-identical.
	walPath := filepath.Join(dir, "wal-0.data")
	walFixture := []byte("fake-wal-record-12345")
	if err := os.WriteFile(walPath, walFixture, 0600); err != nil {
		t.Fatalf("write WAL fixture: %v", err)
	}
	auditRecordPath := filepath.Join(dir, "audit-record.json")
	auditFixture := []byte(`{"event":"auth_success","level":"AUDIT","user":"admin"}`)
	if err := os.WriteFile(auditRecordPath, auditFixture, 0600); err != nil {
		t.Fatalf("write audit fixture: %v", err)
	}

	result, err := RewrapEnvelopes(dir, oldKEK, newKEK, false)
	if err != nil {
		t.Fatalf("rewrap: %v", err)
	}
	if result.Rewrapped != 3 {
		t.Fatalf("rewrapped = %d, want 3", result.Rewrapped)
	}
	if result.Skipped != 0 {
		t.Fatalf("skipped = %d, want 0", result.Skipped)
	}
	if result.Total != 3 {
		t.Fatalf("total = %d, want 3", result.Total)
	}

	// Verify DEKs survive the rewrap unchanged.
	got0 := loadDEK(t, dir, newKEK, "shard-0.env")
	got1 := loadDEK(t, dir, newKEK, "shard-1.env")
	gotA := loadDEK(t, dir, newKEK, "audit.env")

	if !bytes.Equal(got0, dek0) {
		t.Fatal("shard-0 DEK changed after rewrap")
	}
	if !bytes.Equal(got1, dek1) {
		t.Fatal("shard-1 DEK changed after rewrap")
	}
	if !bytes.Equal(gotA, dekA) {
		t.Fatal("audit DEK changed after rewrap")
	}

	// Verify old KEK can no longer load the envelopes.
	for _, name := range []string{"shard-0.env", "shard-1.env", "audit.env"} {
		if _, err := NewEnvelope(oldKEK, nil); err == nil {
			env, _ := NewEnvelope(oldKEK, nil)
			if _, err = env.Load(dir, name); err == nil {
				t.Fatalf("old KEK should fail to load %s after rewrap", name)
			}
		}
	}

	// Verify no .bak files exist when retainOld is false.
	for _, name := range []string{"shard-0.env.bak", "shard-1.env.bak", "audit.env.bak"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Fatalf("unexpected backup file %s", name)
		}
	}

	// Verify non-envelope files remain byte-identical.
	gotWal, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL after rewrap: %v", err)
	}
	if !bytes.Equal(gotWal, walFixture) {
		t.Fatal("WAL file changed after rewrap")
	}
	gotAudit, err := os.ReadFile(auditRecordPath)
	if err != nil {
		t.Fatalf("read audit record after rewrap: %v", err)
	}
	if !bytes.Equal(gotAudit, auditFixture) {
		t.Fatal("audit record file changed after rewrap")
	}
}

func TestRewrapIdempotent(t *testing.T) {
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	dek := createEnvelope(t, dir, oldKEK, "shard-0.env")

	// First rewrap.
	if _, err := RewrapEnvelopes(dir, oldKEK, newKEK, false); err != nil {
		t.Fatalf("first rewrap: %v", err)
	}

	// Second rewrap — should skip everything.
	result, err := RewrapEnvelopes(dir, oldKEK, newKEK, false)
	if err != nil {
		t.Fatalf("second rewrap: %v", err)
	}
	if result.Rewrapped != 0 {
		t.Fatalf("rewrapped = %d, want 0 (should skip)", result.Rewrapped)
	}
	if result.Skipped != 1 {
		t.Fatalf("skipped = %d, want 1", result.Skipped)
	}

	// DEK still intact.
	got := loadDEK(t, dir, newKEK, "shard-0.env")
	if !bytes.Equal(got, dek) {
		t.Fatal("DEK changed on idempotent rewrap")
	}
}

func TestRewrapMixedKeysAbort(t *testing.T) {
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	thirdKEK := testKEK(t)
	dir := t.TempDir()

	createEnvelope(t, dir, oldKEK, "shard-0.env")
	createEnvelope(t, dir, thirdKEK, "shard-1.env") // different key

	_, err := RewrapEnvelopes(dir, oldKEK, newKEK, false)
	if err == nil {
		t.Fatal("expected error for mixed-key dataset")
	}
	if !errors.Is(err, ErrMixedKeys) {
		t.Fatalf("expected ErrMixedKeys, got: %v", err)
	}

	// Verify nothing was rewritten — both files must still load with their original keys.
	env0, _ := NewEnvelope(oldKEK, nil)
	if _, err = env0.Load(dir, "shard-0.env"); err != nil {
		t.Fatalf("shard-0 should still load with old KEK: %v", err)
	}

	env1, _ := NewEnvelope(thirdKEK, nil)
	if _, err = env1.Load(dir, "shard-1.env"); err != nil {
		t.Fatalf("shard-1 should still load with third KEK: %v", err)
	}
}

func TestRewrapWrongOldKey(t *testing.T) {
	wrongKEK := testKEK(t)
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	createEnvelope(t, dir, oldKEK, "shard-0.env")

	_, err := RewrapEnvelopes(dir, wrongKEK, newKEK, false)
	if err == nil {
		t.Fatal("expected error when old KEK does not match")
	}
	if !errors.Is(err, ErrOldKeyMismatch) {
		t.Fatalf("expected ErrOldKeyMismatch, got: %v", err)
	}
}

func TestRewrapSameKey(t *testing.T) {
	kek := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	// Copy kek to newKEK so they're identical bytes.
	copy(newKEK, kek)

	_, err := RewrapEnvelopes(dir, kek, newKEK, false)
	if err == nil {
		t.Fatal("expected error for identical KEKs")
	}
}

func TestRewrapRetainOldKeys(t *testing.T) {
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	dek := createEnvelope(t, dir, oldKEK, "shard-0.env")

	_, err := RewrapEnvelopes(dir, oldKEK, newKEK, true)
	if err != nil {
		t.Fatalf("rewrap: %v", err)
	}

	// .bak file must exist and contain the original envelope.
	bakPath := filepath.Join(dir, "shard-0.env.bak")
	bakData, err := os.ReadFile(bakPath)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if len(bakData) == 0 {
		t.Fatal("backup file is empty")
	}

	// Original key must still load the backup.
	env, err := NewEnvelope(oldKEK, nil)
	if err != nil {
		t.Fatalf("envelope init: %v", err)
	}
	bakDEK, err := env.Load(dir, "shard-0.env.bak")
	if err != nil {
		t.Fatalf("load backup: %v", err)
	}
	if !bytes.Equal(bakDEK, dek) {
		t.Fatal("backup DEK differs from original")
	}

	// New key must load the rewritten file.
	got := loadDEK(t, dir, newKEK, "shard-0.env")
	if !bytes.Equal(got, dek) {
		t.Fatal("rewrapped DEK differs from original")
	}
}

func TestRewrapNoEnvelopeFiles(t *testing.T) {
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	_, err := RewrapEnvelopes(dir, oldKEK, newKEK, false)
	if err == nil {
		t.Fatal("expected error for empty directory")
	}
}

func TestRewrapRejectsBadFormat(t *testing.T) {
	oldKEK := testKEK(t)
	newKEK := testKEK(t)
	dir := t.TempDir()

	// Write a garbage envelope file.
	path := filepath.Join(dir, "shard-0.env")
	if err := os.WriteFile(path, []byte{0x99, 0x01}, 0600); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	_, err := RewrapEnvelopes(dir, oldKEK, newKEK, false)
	if err == nil {
		t.Fatal("expected error for bad envelope format")
	}
}
