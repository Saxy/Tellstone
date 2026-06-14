/*
Package crypto
Tellstone Cloud-Native In-Memory Database
File: cipher.go
Description: CPU-agnostic, zero-allocation In-Memory Encryption engine

	using ChaCha20-Poly1305 with In-Place buffer mutation.

"Locked in silicon, opaque to the host."

Authors:

	Maximilian Hagen
*/
package crypto

import (
	"crypto/cipher"
	"crypto/rand"
	"errors"

	"github.com/Saxy/Tellstone/internal/log"
	"golang.org/x/crypto/chacha20poly1305"
)

var ErrDecryptionFailed = errors.New("cipher: authentication failed during decryption")

type Engine struct {
	aead    cipher.AEAD
	enabled bool
	logger  log.Logger
}

// NewEngine initializes the cryptographic state. If key is nil or empty,
// encryption runs in pass-through mode (disabled).
func NewEngine(key []byte, logger log.Logger) (*Engine, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	if len(key) == 0 {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "crypto engine initialized in pass-through mode (encryption disabled)")
		}
		return &Engine{enabled: false}, nil
	}
	// ChaCha20-Poly1305 requires exactly a 32-byte key
	if len(key) != 32 {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "failed to initialize crypto engine: key length mismatch",
				log.Int("provided_bytes", len(key)),
				log.Int("required_bytes", 32),
			)
		}
		return nil, errors.New("crypto: encryption key must be exactly 32 bytes")
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "failed to instantiate chacha20poly1305 cipher primitives")
		}
		return nil, err
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "crypto engine successfully initialized with active ChaCha20-Poly1305")
	}
	return &Engine{
		aead:    aead,
		enabled: true,
		logger:  logger,
	}, nil
}

// Enabled returns the operational state of the cipher engine.
func (e *Engine) Enabled() bool {
	return e.enabled
}

// EncryptInPlace encrypts the plaintext directly inside the provided session buffer.
// It appends a deterministic 12-byte Nonce and 16-byte Poly1305 tag to the payload.
// Destination format: [ Nonce (12B) ] + [ Ciphertext (Variable) ] + [ AuthTag (16B) ]
func (e *Engine) EncryptInPlace(dst, plaintext []byte) ([]byte, error) {
	if !e.enabled {
		return plaintext, nil
	}
	// 1. Ensure dst has enough capacity to hold Nonce + Plaintext + Overheads (Tag)
	neededCapacity := e.aead.NonceSize() + len(plaintext) + e.aead.Overhead()
	if cap(dst) < neededCapacity {
		// If the provided buffer window is too tight, we must expand it.
		// In production, our 4KB session buffer prevents this branch from ever hitting.
		if e.logger.Enabled(log.LevelWarn) {
			e.logger.Log(log.LevelWarn, "encryption dst buffer capacity insufficient, forcing allocation",
				log.Int("available_cap", cap(dst)),
				log.Int("required_cap", neededCapacity),
			)
		}
		dst = make([]byte, neededCapacity)
	} else {
		dst = dst[:neededCapacity]
	}
	// 2. Isolate the Nonce segment at the front of the output
	nonce := dst[:e.aead.NonceSize()]
	if _, err := rand.Read(nonce); err != nil {
		if e.logger.Enabled(log.LevelError) {
			e.logger.Log(log.LevelError, "cryptographic entropy source failure during nonce generation")
		}
		return nil, err
	}
	// 3. Perform In-Place Seal. The slice after the nonce has length 0 but
	//    enough capacity for ciphertext+tag, so Seal writes directly into dst
	//    without allocating.
	out := e.aead.Seal(dst[e.aead.NonceSize():e.aead.NonceSize()], nonce, plaintext, nil)
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "payload successfully encrypted in-place",
			log.Int("plaintext_len", len(plaintext)),
			log.Int("total_packet_len", e.aead.NonceSize()+len(out)),
		)
	}
	// Truncate dst to the exact size (nonce + ciphertext + tag)
	return dst[:e.aead.NonceSize()+len(out)], nil
}

// DecryptInPlace decrypts the data directly inside the cipher buffer window.
func (e *Engine) DecryptInPlace(ciphertext []byte) ([]byte, error) {
	if !e.enabled {
		return ciphertext, nil
	}
	nonceSize := e.aead.NonceSize()
	if len(ciphertext) < nonceSize+e.aead.Overhead() {
		if e.logger.Enabled(log.LevelWarn) {
			e.logger.Log(log.LevelWarn, "decryption rejected: block size underflow",
				log.Int("ciphertext_len", len(ciphertext)),
			)
		}
		return nil, ErrDecryptionFailed
	}
	nonce := ciphertext[:nonceSize]
	actualCiphertext := ciphertext[nonceSize:]
	// Allocate a destination slice with sufficient capacity for the plaintext.
	// Decrypting in‑place is not safe because the Go crypto library rejects
	// overlapping input/output buffers. We therefore allocate a fresh slice that
	// will be returned to the caller.
	dst := make([]byte, 0, len(actualCiphertext)-e.aead.Overhead())
	plaintext, err := e.aead.Open(dst, nonce, actualCiphertext, nil)
	if err != nil {
		if e.logger.Enabled(log.LevelError) {
			e.logger.Log(log.LevelError, "authentication failed during decryption (potential data tampering)")
		}
		return nil, ErrDecryptionFailed
	}
	return plaintext, nil
}

// DecryptInPlaceWithDst decrypts ciphertext into the provided dst slice without additional allocations.
// The dst slice must have enough capacity to hold the plaintext (len(ciphertext) - nonceSize - overhead).
// It returns the plaintext slice which shares the underlying array with dst.
func (e *Engine) DecryptInPlaceWithDst(dst, ciphertext []byte) ([]byte, error) {
	if !e.enabled {
		return ciphertext, nil
	}
	nonceSize := e.aead.NonceSize()
	if len(ciphertext) < nonceSize+e.aead.Overhead() {
		if e.logger.Enabled(log.LevelWarn) {
			e.logger.Log(log.LevelWarn, "dst-decryption rejected: block size underflow",
				log.Int("ciphertext_len", len(ciphertext)),
			)
		}
		return nil, ErrDecryptionFailed
	}
	nonce := ciphertext[:nonceSize]
	actualCiphertext := ciphertext[nonceSize:]
	// Ensure dst has the correct capacity; callers should provide a slice with sufficient cap.
	// The length is set to zero so Open will append the plaintext.
	dst = dst[:0]
	plaintext, err := e.aead.Open(dst, nonce, actualCiphertext, nil)
	if err != nil {
		if e.logger.Enabled(log.LevelError) {
			e.logger.Log(log.LevelError, "integrity check failed inside allocation-free decryption channel")
		}
		return nil, ErrDecryptionFailed
	}
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "payload successfully authenticated and decrypted via pooled buffer",
			log.Int("ciphertext_len", len(ciphertext)),
			log.Int("plaintext_len", len(plaintext)),
		)
	}
	return plaintext, nil
}
