package crypto

import (
	"crypto/rand"
	"fmt"
	"testing"
)

func benchSize(size int) string {
	return fmt.Sprintf("size=%d", size)
}

func benchKey(b *testing.B) []byte {
	b.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatalf("generate key: %v", err)
	}
	return key
}

// EncryptInPlace with a pre-sized, reused buffer measures the AEAD seal + nonce
// generation. The storage Set path additionally allocates the buffer once per
// call, so the full-write allocation cost is one make() on top of this.
func BenchmarkEncryptInPlace(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(benchSize(size), func(b *testing.B) {
			eng, err := NewEngine(benchKey(b), nil)
			if err != nil {
				b.Fatalf("engine init: %v", err)
			}
			plain := make([]byte, size)
			buf := make([]byte, 0, eng.aead.NonceSize()+size+eng.aead.Overhead())
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err = eng.EncryptInPlace(buf, plain); err != nil {
					b.Fatalf("encrypt: %v", err)
				}
			}
		})
	}
}

// DecryptInPlaceWithDst into a reused buffer mirrors the storage GET path and
// must stay allocation-free.
func BenchmarkDecryptInPlaceWithDst(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(benchSize(size), func(b *testing.B) {
			eng, err := NewEngine(benchKey(b), nil)
			if err != nil {
				b.Fatalf("engine init: %v", err)
			}
			plain := make([]byte, size)
			sealed, err := eng.EncryptInPlace(nil, plain)
			if err != nil {
				b.Fatalf("encrypt: %v", err)
			}
			dst := make([]byte, 0, size)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var open []byte
				open, err = eng.DecryptInPlaceWithDst(dst, sealed)
				if err != nil {
					b.Fatalf("decrypt: %v", err)
				}
				dst = open[:0]
			}
		})
	}
}

// DecryptInPlace allocates a fresh plaintext buffer per call. It is the
// allocating variant, used by the envelope unwrap path and the large-value GET
// fallback, so it is benchmarked separately from the zero-alloc GET path.
func BenchmarkDecryptInPlace(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(benchSize(size), func(b *testing.B) {
			eng, err := NewEngine(benchKey(b), nil)
			if err != nil {
				b.Fatalf("engine init: %v", err)
			}
			plain := make([]byte, size)
			sealed, err := eng.EncryptInPlace(nil, plain)
			if err != nil {
				b.Fatalf("encrypt: %v", err)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err = eng.DecryptInPlace(sealed); err != nil {
					b.Fatalf("decrypt: %v", err)
				}
			}
		})
	}
}
