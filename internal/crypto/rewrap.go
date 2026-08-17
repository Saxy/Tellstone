/*
Package crypto
Tellstone Envelope Re-wrap
File: rewrap.go
Description: Offline KEK rotation for envelope-encrypted files. RewrapEnvelopes
reads every shard-*.env and audit.env from a data directory, verifies that all
envelopes match the current KEK fingerprint, and re-wraps each DEK with a new
KEK. Individual file writes are atomic (tmp + fsync + rename). The operation is
idempotent: envelopes already carrying the new KEK fingerprint are skipped, so a
crash mid-rewrap can be recovered by re-running the same command.

Authors:

	Maximilian Hagen
*/
package crypto

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// findEnvelopeFiles returns shard-*.env and audit.env files in dir, sorted
// for deterministic processing order. Unrelated .env files are excluded.
func findEnvelopeFiles(dir string) ([]string, error) {
	shards, err := filepath.Glob(filepath.Join(dir, "shard-*.env"))
	if err != nil {
		return nil, err
	}
	var matches []string
	matches = append(matches, shards...)
	audit := filepath.Join(dir, "audit.env")
	if info, err := os.Stat(audit); err == nil && !info.IsDir() {
		matches = append(matches, audit)
	}
	sort.Strings(matches)
	return matches, nil
}

// ErrMixedKeys is returned when an envelope carries a KEK fingerprint that
// matches neither the old nor the new key, indicating a mixed-key dataset
// that must not be partially rewrapped.
var ErrMixedKeys = errors.New("rewrap: envelope carries an unrecognized KEK fingerprint; dataset has mixed keys")

// ErrOldKeyMismatch is returned when no envelope matches the old KEK, meaning
// the dataset was already rewrapped or was written with a different key.
var ErrOldKeyMismatch = errors.New("rewrap: no envelope matches the current KEK; already rewrapped or wrong key")

// RewrapResult reports what the rewrap operation did.
type RewrapResult struct {
	Rewrapped int
	Skipped   int
	Total     int
}

// RewrapEnvelopes re-wraps all envelope files in dir from oldKEK to newKEK.
//
// The operation is idempotent and crash-safe:
//   - Envelopes with oldKEK's fingerprint are rewrapped.
//   - Envelopes with newKEK's fingerprint are skipped (already done).
//   - Any envelope with a third fingerprint aborts the entire operation
//     before any file is rewritten (fail-closed).
//   - Each file write is atomic: tmp + fsync + rename.
//
// If retainOld is true, each original envelope is copied to <name>.bak
// before rewriting, providing a manual recovery path.
//
// The data directory must be quiesced — the server must not be running
// during a rewrap.
func RewrapEnvelopes(dir string, oldKEK, newKEK []byte, retainOld bool) (*RewrapResult, error) {
	if len(oldKEK) != keySize {
		return nil, fmt.Errorf("rewrap: old KEK must be exactly %d bytes, got %d", keySize, len(oldKEK))
	}
	if len(newKEK) != keySize {
		return nil, fmt.Errorf("rewrap: new KEK must be exactly %d bytes, got %d", keySize, len(newKEK))
	}
	if bytes.Equal(oldKEK, newKEK) {
		return nil, errors.New("rewrap: old and new KEK are identical")
	}
	oldFP := FingerprintBytes(oldKEK)
	newFP := FingerprintBytes(newKEK)
	matches, err := findEnvelopeFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("rewrap: glob envelope files: %w", err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("rewrap: no envelope files found in %s", dir)
	}
	type entry struct {
		path    string
		name    string
		raw     []byte
		status  byte
		wrapped []byte
	}
	entries := make([]entry, len(matches))
	var oldCount, newCount, unknownCount int
	for i, path := range matches {
		var raw []byte
		raw, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("rewrap: read %s: %w", filepath.Base(path), err)
		}
		e := entry{path: path, name: filepath.Base(path), raw: raw}
		headerLen := 1 + envFingerprintLen
		if len(raw) < headerLen || raw[0] != envVersion {
			return nil, fmt.Errorf("rewrap: %s: unsupported envelope format", e.name)
		}
		fp := raw[1 : 1+envFingerprintLen]
		e.wrapped = raw[headerLen:]
		switch {
		case bytes.Equal(fp, oldFP[:]):
			e.status = 'O'
			oldCount++
		case bytes.Equal(fp, newFP[:]):
			e.status = 'N'
			newCount++
		default:
			e.status = 'U'
			unknownCount++
		}
		entries[i] = e
	}
	if oldCount == 0 && newCount > 0 {
		return &RewrapResult{Skipped: newCount, Total: len(entries)}, nil
	}
	if oldCount == 0 {
		return nil, ErrOldKeyMismatch
	}
	if unknownCount > 0 {
		return nil, fmt.Errorf("%w: %d envelope(s) with unrecognized fingerprint", ErrMixedKeys, unknownCount)
	}
	newEnv, err := NewEnvelope(newKEK, nil)
	if err != nil {
		return nil, fmt.Errorf("rewrap: init new KEK envelope: %w", err)
	}

	// Phase 1: complete all cryptographic processing before touching the filesystem.
	type prepared struct {
		entry
		buf []byte
	}
	var toWrite []prepared
	result := &RewrapResult{Total: len(entries)}
	for _, e := range entries {
		if e.status == 'N' {
			result.Skipped++
			continue
		}
		var oldEnv *Envelope
		var dek, wrapped []byte
		oldEnv, err = NewEnvelope(oldKEK, nil)
		if err != nil {
			return nil, fmt.Errorf("rewrap: init old KEK envelope: %w", err)
		}
		dek, err = oldEnv.decrypt(e.wrapped)
		if err != nil {
			return nil, fmt.Errorf("rewrap: unwrap DEK %s: %w", e.name, err)
		}
		newEnv.dek = dek
		wrapped, err = newEnv.encrypt()
		if err != nil {
			return nil, fmt.Errorf("rewrap: wrap DEK %s: %w", e.name, err)
		}
		buf := make([]byte, 1+envFingerprintLen+len(wrapped))
		buf[0] = envVersion
		copy(buf[1:1+envFingerprintLen], newFP[:])
		copy(buf[1+envFingerprintLen:], wrapped)
		toWrite = append(toWrite, prepared{entry: e, buf: buf})
	}

	// Phase 2: retain-old-keys backups and atomic writes.
	for _, p := range toWrite {
		if retainOld {
			bak := p.path + ".bak"
			if err = copyFile(p.path, bak); err != nil {
				return nil, fmt.Errorf("rewrap: backup %s: %w", p.name, err)
			}
		}
		if err = atomicWrite(p.path, p.buf); err != nil {
			return nil, fmt.Errorf("rewrap: write %s: %w", p.name, err)
		}
		result.Rewrapped++
	}

	return result, nil
}

// atomicWrite writes data to path atomically via tmp + fsync + rename.
// The tmp file is created in the same directory as path to ensure the
// rename is on the same filesystem.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		if err = f.Close(); err != nil {
			return err
		}
		if err = os.Remove(tmp); err != nil {
			return err
		}
		return err
	}
	if err = f.Sync(); err != nil {
		if err = f.Close(); err != nil {
			return err
		}
		if err = os.Remove(tmp); err != nil {
			return err
		}
		return err
	}
	if err = f.Close(); err != nil {
		if err = os.Remove(tmp); err != nil {
			return err
		}
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		if err = os.Remove(tmp); err != nil {
			return err
		}
		return err
	}
	// Sync the parent directory to persist the rename.
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err = d.Sync(); err != nil {
		if closeErr := d.Close(); closeErr != nil {
			return fmt.Errorf("rewrap: sync directory: %v; close: %w", err, closeErr)
		}
		return err
	}
	return d.Close()
}

// copyFile copies src to dst, preserving the source file's content.
// Used for --retain-old-keys backups.
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0600)
}
