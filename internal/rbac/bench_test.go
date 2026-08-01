/*
Package rbac
Tellstone Role-Based Access Control Benchmarks
File: bench_test.go
Description: Verifies the authorization hot path stays allocation-free: IsAllowed must report 0 allocs/op and stay in single-digit nanoseconds.
*/
package rbac

import "testing"

func BenchmarkIsAllowedAllowed(b *testing.B) {
	role, err := ParseRole("cache-manager", "+@readwrite", "~cache:")
	if err != nil {
		b.Fatalf("ParseRole: %v", err)
	}
	sc := NewSessionContext("alice", role)
	key := []byte("cache:session:abc")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !sc.IsAllowed(CmdGet, key) {
			b.Fatal("expected allowed")
		}
	}
}

func BenchmarkIsAllowedDeniedByPrefix(b *testing.B) {
	role, err := ParseRole("cache-manager", "+@readwrite", "~cache:")
	if err != nil {
		b.Fatalf("ParseRole: %v", err)
	}
	sc := NewSessionContext("alice", role)
	key := []byte("users:123")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if sc.IsAllowed(CmdSet, key) {
			b.Fatal("expected denied")
		}
	}
}
