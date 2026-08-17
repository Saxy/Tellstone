/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: decrypt.go
Description: Standalone audit log decryption for the CLI tool. Reads a single
audit file, parses the self-describing header, resolves the correct decryption
engine from the supplied key and the header's keyMode/fingerprint, and writes
every decrypted JSON record to the caller's buffer. Used by
"tellstone audit decrypt" — not on the server hot path; allocations are
acceptable.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Saxy/Tellstone/internal/crypto"
)

// DecryptFile reads an audit file from r, parses the header, and decrypts every
// sealed record. dir is the file's parent directory, used to locate audit.env
// when the header indicates envelope mode.
//
// The returned byte slice contains one JSON line per record, separated by
// newlines — ready for stdout, file output, or piping to jq.
//
// Three header scenarios are handled:
//   - No magic (legacy): records are decrypted with engine directly.
//   - KeyModeSimple: records are decrypted with engine directly.
//   - KeyModeEnvelope: the DEK is unwrapped from audit.env using the KEK in
//     engine, and a fresh engine is built from the recovered DEK.
//
// Fail-closed: a fingerprint mismatch, missing envelope, or undecryptable
// record is returned as an error. A truncated trailing record (from a process
// killed mid-write) stops the walk cleanly and returns what was recovered.
func DecryptFile(r io.Reader, dir string, engine *crypto.Engine) ([]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("audit: read: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("audit: empty file")
	}

	keyMode, fingerprint, rest, ok, headerErr := parseHeader(data, nil, "")
	if headerErr != nil {
		return nil, headerErr
	}

	// Resolve the decryption engine from the header.
	decryptEngine, err := resolveEngine(ok, keyMode, fingerprint, dir, engine)
	if err != nil {
		return nil, err
	}

	// No header — legacy file. Decrypt with the supplied engine directly.
	if !ok {
		return decodeAllRecords(data, decryptEngine)
	}

	switch keyMode {
	case KeyModeSimple:
		return decodeAllRecords(rest, decryptEngine)
	case KeyModeEnvelope:
		return decodeAllRecords(rest, decryptEngine)
	default:
		return nil, fmt.Errorf("audit: unsupported keyMode %d", keyMode)
	}
}

// resolveEngine builds the correct decryption engine from the file header and
// the caller-supplied key engine.
//
// KeyModeSimple verifies fingerprint, returns nil (plaintext data).
// KeyModeEnvelope: if the engine fingerprint matches the header, records were
// sealed directly with that key (return engine as-is). Otherwise unwrap the DEK
// from audit.env using the KEK engine.
// Legacy (no header) returns engine as-is.
func resolveEngine(hasHeader bool, keyMode byte, fingerprint [16]byte, dir string, engine *crypto.Engine) (*crypto.Engine, error) {
	if !hasHeader {
		// Legacy file — the caller's engine is the authority.
		return engine, nil
	}
	switch keyMode {
	case KeyModeSimple:
		// KeyModeSimple means the file has a header but records are plaintext
		// (the key fingerprint identifies which key would seal future encrypted
		// files). Validate the fingerprint when a key is supplied, but return
		// nil engine so decodeAllRecords treats the data as plaintext.
		if engine != nil && engine.Enabled() {
			if engine.KeyFingerprint() != fingerprint {
				return nil, fmt.Errorf("audit: key fingerprint mismatch (file=%x, key=%x); the supplied key did not seal this file", fingerprint, engine.KeyFingerprint())
			}
		}
		return nil, nil
	case KeyModeEnvelope:
		if engine == nil || !engine.Enabled() {
			return nil, errors.New("audit: envelope-encrypted file but no key supplied; use --encryption-key or --encryption-key-file")
		}
		if engine.KeyFingerprint() == fingerprint {
			// The supplied engine's key matches the file's fingerprint. The
			// records were sealed directly with this key (non-envelope mode
			// where NewLogEngine still writes KeyModeEnvelope). No DEK
			// unwrapping needed.
			return engine, nil
		}
		// Fingerprint differs — this may be a true envelope file where the
		// header carries the DEK fingerprint and the user supplied the KEK.
		// Try to unwrap the DEK from audit.env.
		envPath := filepath.Join(dir, envelopeFileName)
		if _, statErr := os.Stat(envPath); errors.Is(statErr, os.ErrNotExist) {
			// No envelope file — the supplied key simply doesn't match.
			return nil, fmt.Errorf("audit: key fingerprint mismatch (file=%x, key=%x); the supplied key did not seal this file", fingerprint, engine.KeyFingerprint())
		}
		return unwrapDEK(dir, fingerprint, engine)
	default:
		return nil, fmt.Errorf("audit: unsupported keyMode %d", keyMode)
	}
}

// unwrapDEK reads audit.env from dir, verifies the KEK fingerprint, and returns
// an engine built from the recovered DEK. The envelope layout is
// [version:1][KEK fingerprint:16][wrapped DEK]; we verify the fingerprint
// against the caller-supplied KEK engine, then decrypt the DEK in-place.
func unwrapDEK(dir string, auditFingerprint [16]byte, kekEngine *crypto.Engine) (*crypto.Engine, error) {
	envPath := filepath.Join(dir, envelopeFileName)
	raw, err := os.ReadFile(envPath)
	if err != nil {
		return nil, fmt.Errorf("audit: read envelope %s: %w", envPath, err)
	}
	const headerLen = 1 + 16 // version + KEK fingerprint
	if len(raw) < headerLen {
		return nil, fmt.Errorf("audit: envelope file %s is truncated", envPath)
	}
	if raw[0] != 1 { // envVersion
		return nil, fmt.Errorf("audit: unsupported envelope format in %s", envPath)
	}
	kekFP := raw[1 : 1+16]
	if kekEngine.KeyFingerprint() != ([16]byte)(kekFP) {
		return nil, fmt.Errorf("audit: KEK fingerprint mismatch for envelope (file=%x, key=%x); the supplied key did not create this envelope", kekFP, kekEngine.KeyFingerprint())
	}
	wrappedDEK := raw[headerLen:]
	dek, err := kekEngine.DecryptInPlace(wrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("audit: unwrap DEK from %s: %w", envPath, err)
	}
	dekEngine, err := crypto.NewEngine(dek, nil)
	if err != nil {
		return nil, fmt.Errorf("audit: DEK engine init: %w", err)
	}
	if dekEngine.KeyFingerprint() != auditFingerprint {
		return nil, fmt.Errorf("audit: DEK fingerprint mismatch (file=%x, recovered DEK=%x); envelope DEK does not match the audit file", auditFingerprint, dekEngine.KeyFingerprint())
	}
	return dekEngine, nil
}

// decodeAllRecords walks the [4-byte big-endian length][sealed blob] framing
// and returns every decrypted record as a JSON line. Unlike decodeEncrypted
// (used by replay), this does not filter by event type — every record in the
// file is decrypted and emitted.
//
// Each encrypted blob already contains a trailing newline from the
// json.Encoder that produced it, so no separator is appended. The plaintext
// path returns data as-is since it is already newline-delimited.
//
// A truncated trailing record (process killed mid-write) stops the walk
// cleanly and returns what was recovered. Any other decryption failure or
// malformed record is returned as an error — this is a fail-closed design
// because partial output from a wrong key is meaningless.
func decodeAllRecords(data []byte, engine *crypto.Engine) ([]byte, error) {
	if engine == nil || !engine.Enabled() {
		// Plaintext — data is newline-delimited JSON.
		return data, nil
	}
	var buf bytes.Buffer
	for len(data) >= 4 {
		blobLen := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if blobLen == 0 {
			return nil, errors.New("audit: malformed frame: zero-length blob")
		}
		if uint64(blobLen) > uint64(len(data)) {
			// Truncated trailing record — process killed mid-write.
			// Return what we have so far.
			break
		}
		plain, err := engine.DecryptInPlace(data[:blobLen])
		data = data[blobLen:]
		if err != nil {
			return nil, fmt.Errorf("audit: decrypt record: %w", err)
		}
		if !json.Valid(plain) {
			return nil, errors.New("audit: decrypted record is not valid JSON; wrong key or corrupted data")
		}
		buf.Write(plain)
	}
	return buf.Bytes(), nil
}
