package storage

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
)

// BenchmarkEngineGetWithEncryption measures the allocation cost of Engine.Get when
// the engine is configured with an enabled crypto engine. The stored value is
// small enough (< 2KB) to fit into the stack buffer used by the decryption
// routine, so we expect zero heap allocations per Get call.
func BenchmarkEngineGetWithEncryptionNoAlloc(b *testing.B) {
	// Prepare a 32‑byte ChaCha20‑Poly1305 key.
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatalf("failed to generate crypto key: %v", err)
	}
	cryptoEng, err := crypto.NewEngine(key, nil)
	if err != nil {
		b.Fatalf("crypto engine init failed: %v", err)
	}

	// Create the storage engine with the crypto engine enabled.
	eng := NewEngine(1*time.Millisecond, 64, 0, nil, cryptoEng)
	defer eng.Close()

	// Insert a small value (well under the 2KB stack buffer threshold).
	keyStr := "benchmark_enc_key"
	val := []byte("benchmark_encrypted_value")
	eng.Set(keyStr, val, 0) // no TTL to keep it simple

	// Pre‑allocate a buffer for GetInto to write into; it will be reused across iterations.
	buf := make([]byte, len(val))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		n, ok := eng.GetInto(buf, keyStr)
		if !ok || n != len(val) || string(buf[:n]) != string(val) {
			b.Fatalf("unexpected GetInto result: ok=%v n=%d got=%s", ok, n, string(buf[:n]))
		}
	}
}
