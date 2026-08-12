/*
Package crypto
Tellstone Envelope Encryption
File: envelope.go
Description: Envelope encryption for per-shard data keys. Each shard owns a random
32-byte Data Encryption Key (DEK) that is wrapped (encrypted) with a Key Encryption
Key (KEK) supplied by the operator. On disk every shard has exactly one envelope:

	[version:1][KEK fingerprint:16][wrapped DEK: nonce(12)+ct(32)+tag(16)]

The KEK never touches data: values are sealed by the shard's DEK engine, and the KEK
exists only to protect the DEK at rest. A wrapped DEK is worthless without its KEK, so
envelope files can live alongside the data they protect. A changed KEK is detected on
Load via the stored fingerprint and fails to close instead of silently generating fresh
DEKs and losing the dataset.

Lifecycle per shard:
  - first boot: NewEnvelope(kek) -> GenerateDEK -> Store
  - restart:    NewEnvelope(kek) -> Load (fingerprint check, unwrap, restore DEK)

Authors:

	Maximilian Hagen
*/
package crypto

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Saxy/Tellstone/internal/log"
)

const (
	// envVersion identifies the on-disk envelope layout. Bump it, not the fields
	// inside, when the format changes.
	envVersion byte = 1
	// envFingerprintLen is the truncated SHA-256 of the KEK carried by each
	// envelope, so Load can reject a changed KEK before attempting to unwrap.
	envFingerprintLen = 16
)

// Envelope holds a Key Encryption Key (KEK) and the shard's current Data Encryption
// Key (DEK). It wraps the DEK with the KEK for durable storage (Store) and unwraps
// it again on restart (Load). The DEK is what the shard builds its data Engine
// from; the KEK is never used for data.
type Envelope struct {
	logger       log.Logger
	kek          []byte
	dek          []byte
	cryptoEngine *Engine
	enabled      bool
}

// NewEnvelope validates the KEK and prepares the wrapping engine. A nil or empty
// key yields a pass-through envelope (enabled == false) whose lifecycle methods
// are no-ops, mirroring Engine's disabled mode.
func NewEnvelope(key []byte, logger log.Logger) (*Envelope, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	engine, err := NewEngine(key, logger)
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "envelope: initialized in pass-through mode (disabled)")
		}
		return &Envelope{
			enabled: false,
			logger:  logger,
			kek:     key,
		}, nil
	}
	if len(key) != 32 {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "envelope: failed to initialize key length mismatch",
				log.Int("provided_bytes", len(key)),
				log.Int("required_bytes", 32),
			)
		}
		return nil, errors.New("envelope: KEK must be exactly 32 bytes")
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "envelope: successfully initialized with KEK")
	}
	return &Envelope{
		logger:       logger,
		kek:          key,
		cryptoEngine: engine,
		enabled:      true,
	}, nil
}

// Enabled reports whether the envelope actively wraps DEKs (false in pass-through
// mode).
func (e *Envelope) Enabled() bool {
	return e.enabled
}

// GenerateDEK draws a fresh random DEK for this shard. It must not be called on a
// restart where an envelope already exists — that path is Load, so old data stays
// decryptable.
func (e *Envelope) GenerateDEK() error {
	if !e.enabled {
		return nil
	}
	data, err := rnd()
	if err != nil {
		if e.logger.Enabled(log.LevelError) {
			e.logger.Log(log.LevelError, "envelope: failed to create random 32 bytes", log.String("error", err.Error()))
		}
		return err
	}
	e.dek = data
	return nil
}

// DEK returns the shard's data key. In pass-through mode it returns the configured
// key so callers can build a pass-through engine without special-casing.
func (e *Envelope) DEK() []byte {
	if !e.enabled {
		return e.kek
	}
	return e.dek
}

func (e *Envelope) encrypt() ([]byte, error) {
	if !e.enabled {
		return e.kek, nil
	}
	var buf []byte
	return e.cryptoEngine.EncryptInPlace(buf, e.dek)
}
func (e *Envelope) decrypt(encryptedDEK []byte) ([]byte, error) {
	if !e.enabled {
		return e.kek, nil
	}
	return e.cryptoEngine.DecryptInPlace(encryptedDEK)
}

// Store wraps the DEK with the KEK and persists it to <dir>/shard-<n>.env.
// On-disk layout: [version:1][KEK fingerprint:16][wrapped DEK: nonce+ct+tag].
func (e *Envelope) Store(dir string, fileName string) error {
	if !e.enabled {
		return nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		if e.logger.Enabled(log.LevelError) {
			e.logger.Log(log.LevelError, "envelope: failed to create envelope directory",
				log.String("dir", dir), log.String("error", err.Error()))
		}
		return fmt.Errorf("envelope: create envelope directory %s: %w", dir, err)
	}
	wrapped, err := e.encrypt()
	if err != nil {
		return fmt.Errorf("envelope: wrap DEK with KEK: %w", err)
	}
	buf := make([]byte, 1+envFingerprintLen+len(wrapped))
	buf[0] = envVersion
	fp := fingerprintBytes(e.kek)
	copy(buf[1:1+envFingerprintLen], fp[:])
	copy(buf[1+envFingerprintLen:], wrapped)
	name := filepath.Join(dir, fileName)
	tmp := name + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("envelope: open envelope file: %w", err)
	}
	if _, err = file.Write(buf); err != nil {
		if err = file.Close(); err != nil {
			return fmt.Errorf("envelope: write envelope file: %w", err)
		}
		if err = os.Remove(tmp); err != nil {
			return fmt.Errorf("envelope: write envelope file: %w", err)
		}
		return fmt.Errorf("envelope: write envelope file: %w", err)
	}
	if err = file.Sync(); err != nil {
		if err = file.Close(); err != nil {
			return fmt.Errorf("envelope: sync envelope file: %w", err)
		}
		if err = os.Remove(tmp); err != nil {
			return fmt.Errorf("envelope: sync envelope file: %w", err)
		}
		return fmt.Errorf("envelope: sync envelope file: %w", err)
	}
	if err = file.Close(); err != nil {
		if err = os.Remove(tmp); err != nil {
			return fmt.Errorf("envelope: close envelope file: %w", err)
		}
		return fmt.Errorf("envelope: close envelope file: %w", err)
	}
	if err = os.Rename(tmp, name); err != nil {
		if err = os.Remove(tmp); err != nil {
			return fmt.Errorf("envelope: finalize envelope file: %w", err)
		}
		return fmt.Errorf("envelope: finalize envelope file: %w", err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("envelope: open envelope directory: %w", err)
	}
	if err = parent.Sync(); err != nil {
		if err = parent.Close(); err != nil {
			return fmt.Errorf("envelope: close envelope directory: %w", err)
		}
		return fmt.Errorf("envelope: sync envelope directory: %w", err)
	}
	if err = parent.Close(); err != nil {
		return fmt.Errorf("envelope: close envelope directory: %w", err)
	}
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "envelope: stored", log.String("file", fileName))
	}
	return nil
}

// Load reads the envelope for shard, rejects a changed KEK via the stored
// fingerprint, and returns the unwrapped DEK for the caller to build an Engine.
func (e *Envelope) Load(dir, fileName string) ([]byte, error) {
	if !e.enabled {
		return nil, nil
	}
	name := filepath.Join(dir, fileName)
	raw, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("envelope: read envelope %s: %w", fileName, err)
	}
	headerLen := 1 + envFingerprintLen
	if len(raw) < headerLen || raw[0] != envVersion {
		return nil, fmt.Errorf("envelope: unsupported envelope format %s", fileName)
	}
	fp := fingerprintBytes(e.kek)
	if !bytes.Equal(raw[1:headerLen], fp[:]) {
		return nil, fmt.Errorf("envelope: KEK fingerprint mismatch for %s; data was written with a different key", fileName)
	}
	dek, err := e.decrypt(raw[headerLen:])
	if err != nil {
		return nil, fmt.Errorf("envelope: unwrap DEK %s: %w", fileName, err)
	}
	e.dek = dek
	return dek, nil
}

func rnd() ([]byte, error) {
	b := make([]byte, keySize)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// fingerprintBytes is the truncated SHA-256 used as the KEK's identifier in the
// envelope header.
func fingerprintBytes(key []byte) [16]byte {
	var fp [16]byte
	sum := sha256.Sum256(key)
	copy(fp[:], sum[:16])
	return fp
}
