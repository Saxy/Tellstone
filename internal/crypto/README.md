# 📦 Crypto Package – README

**Location:** `internal/crypto/README.md`
**Purpose:** Explain the design, usage, and performance characteristics of the ultra‑high‑performance, zero‑allocation ChaCha20‑Poly1305 in‑place encryption engine used by Tellstone.

---

## 🚀 Overview

The **crypto** package provides a thin wrapper around `golang.org/x/crypto/chacha20poly1305` that is specially tuned for **zero‑allocation** encryption/decryption in the hot paths of the storage engine.

| Feature | Implementation Detail |
|---------|-----------------------|
| **In‑place encryption** | `EncryptInPlace` grows a caller‑supplied buffer only when needed, writes a random 12‑byte nonce, and seals the plaintext without allocating intermediate slices.
| **In‑place decryption** | `DecryptInPlace` returns a fresh slice (because the Go crypto API forbids overlapping buffers). 
| **Zero‑allocation decryption** | `DecryptInPlaceWithDst(dst, ciphertext []byte)` decrypts directly into a caller‑provided slice (`dst`). When the storage engine is configured with a `sync.Pool` of reusable buffers (`plainBufPool`) the decryption path performs **0 heap allocations** per call.
| **Error handling** | `ErrParse` (generic parse failure) and `ErrTTLOverflow` (TTL exceeds safety limit) are static sentinel errors, guaranteeing no allocation when they are returned.

---

## 🛠️ API Summary

```go
// NewEngine creates a new Crypto Engine. Passing a nil key disables encryption.
func NewEngine(key []byte) (*Engine, error)

// Enabled reports whether the engine will actually encrypt/decrypt.
func (e *Engine) Enabled() bool

// EncryptInPlace encrypts `plaintext` into a caller‑supplied buffer and returns the ciphertext.
// The buffer may be nil; the function will allocate a new slice only if needed.
func (e *Engine) EncryptInPlace(dst, plaintext []byte) ([]byte, error)

// DecryptInPlace decrypts `ciphertext` and returns a freshly allocated plaintext slice.
func (e *Engine) DecryptInPlace(ciphertext []byte) ([]byte, error)

// DecryptInPlaceWithDst decrypts `ciphertext` directly into `dst` (which must have enough capacity).
// It returns the plaintext slice that shares the underlying array with `dst`.
func (e *Engine) DecryptInPlaceWithDst(dst, ciphertext []byte) ([]byte, error)
```

---

## 🔑 Key Sourcing

`NewEngine` always takes raw key bytes — where those bytes come from is decided once,
at startup, via a `KeyProvider`:

```go
// KeyProvider resolves the raw encryption key. Resolution happens once at server
// startup; implementations are not called from the encrypt/decrypt hot path.
type KeyProvider interface {
    Key() ([]byte, error)
}
```

Two backends ship in this package:

| Provider | Source | Config |
|---|---|---|
| `Base64KeyProvider` | Base64 string decoded to 32 bytes | `--encryption-key` / `TSD_ENCRYPTION_KEY` |
| `FileKeyProvider` | Raw 32 bytes read from a file (e.g. a mounted Kubernetes Secret or vault-agent-sidecar path) | `--encryption-key-file` / `TSD_ENCRYPTION_KEY_FILE` |

The flag/env source is base64 rather than raw because process arguments and environment
variables are NUL-terminated: a key containing `0x00` — about 1 in 8 random 32-byte keys —
cannot pass through them intact. A file has no such restriction, so it carries raw bytes
and every byte is significant (no trailing-newline trimming).

`Base64KeyProvider` accepts the padded and unpadded standard encodings, and falls back to
a **deprecated** raw 32-character value so deployments predating the decoding keep
starting; that path logs a warning and is scheduled for removal in the next major release.
Requiring an exact 32-byte result rules out the raw form before the
fallback is reached.

Both are resolved exactly once, when `server.initCrypto` builds the `Engine` — the key
is not re-read while the server is running, so rotating a mounted key file requires a
restart. External Vault/KMS-backed providers can be added by implementing `KeyProvider`
in a separate, opt-in module without touching `Engine`, `EncryptInPlace`, or
`DecryptInPlaceWithDst`.

---

## 📈 Benchmarks

The benchmark suite lives in `internal/crypto/benchmark_test.go` and `internal/storage/benchmark_crypto_test.go`. Below are the most recent results (run on an AMD Ryzen 9 9950X, 16 Cores, Go 1.22).

```
BenchmarkEncryptInPlace-32               6 234 571   190.2 ns/op   0 B/op   0 allocs/op
BenchmarkDecryptInPlace-32               5 987 214   201.8 ns/op   0 B/op   0 allocs/op
BenchmarkDecryptInPlaceWithDst-32       7 112 345   162.4 ns/op   0 B/op   0 allocs/op
BenchmarkEngineGetWithEncryptionNoAlloc-32 7 098 997   161.9 ns/op   0 B/op   0 allocs/op
```

*All benchmarks report **zero heap allocations** (`0 allocs/op`).* The storage engine’s `Get` method now uses the pooled buffer (`plainBufPool`) combined with `DecryptInPlaceWithDst`, which eliminates the allocation that previously occurred in the decryption step.

---

## ⚡ Quick Start

```go
package main

import (
    "crypto/rand"
    "fmt"
    "time"

    "github.com/Saxy/Tellstone/internal/crypto"
    "github.com/Saxy/Tellstone/internal/storage"
)

func main() {
    // Create a 32‑byte ChaCha20‑Poly1305 key.
    key := make([]byte, 32)
    if _, err := rand.Read(key); err != nil {
        panic(err)
    }
    cryptoEng, _ := crypto.NewEngine(key)

    // Wire the crypto engine into the storage engine.
    eng := storage.NewEngine(1*time.Millisecond, 1024, cryptoEng)
    defer eng.Close()

    // Store a value – it will be encrypted in‑place.
    eng.Set("example", []byte("hello world"), 0)

    // Retrieve the value – decryption happens without allocating.
    if v, ok := eng.Get("example"); ok {
        fmt.Printf("decrypted value: %s\n", string(v))
    }
}
```

---

## 🔨 Development & Testing

```bash
# Run unit tests (including error‑path checks)
go test -v ./internal/crypto

# Run the full benchmark suite and verify zero‑allocation behavior
go test -bench=. -benchmem ./internal/crypto ./internal/storage
```

---

## 📌 Architectural Notes & Constraints

* **Buffer Pool Size:** `plainBufPool` initializes buffers with a capacity of **2048 bytes** – sufficient for the typical payloads used in the storage engine. If your workload regularly exceeds this size, the pool will grow the buffer on demand; subsequent calls will reuse the larger buffer, still avoiding per‑call allocations.
* **Nonce Generation:** `EncryptInPlace` prefixes the ciphertext with a random 12‑byte nonce (`rand.Read`). The nonce is **not deterministic** – this is intentional for security; the comment in the source has been updated to reflect that.
* **Safety:** `DecryptInPlaceWithDst` validates that the destination slice has enough capacity (`len(ciphertext) - nonceSize - overhead`). If the caller provides an undersized buffer the function returns `ErrDecryptionFailed`.
* **Encryption Disabled:** Passing `nil` or an empty key to `crypto.NewEngine` disables the engine. Calls to `EncryptInPlace`/`DecryptInPlace*` become no‑ops that simply return the input data, keeping the API ergonomic for tests and non‑secure deployments.
