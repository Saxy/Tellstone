/*
Package oauth
Tellstone OAuth Provider Registry
File: oauth_test.go
Description: Contract tests for the Provider interface. mockProvider doubles as the
compile-time check: if the interface grows a method, this mock stops compiling, forcing
the maintainer to revisit every registered provider.

Authors:

	Maximilian Hagen
*/
package oauth

import (
	"context"
	"errors"
	"testing"
)

// mockProvider implements Provider for tests and doubles as the compile-time
// contract check.
type mockProvider struct {
	cfg    Config
	claims Claims
	err    error
}

func (m *mockProvider) Config() Config { return m.cfg }

func (m *mockProvider) Verify(_ context.Context, _ []byte) (Claims, error) {
	return m.claims, m.err
}

var _ Provider = (*mockProvider)(nil)

func TestProviderContractVerify(t *testing.T) {
	claims := Claims{"email": {"alice@tellstone.io"}, "groups": {"dev", "qa"}}
	p := &mockProvider{claims: claims}
	got, err := p.Verify(context.Background(), []byte("token"))
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if len(got["email"]) != 1 || got["email"][0] != "alice@tellstone.io" {
		t.Errorf("email claim = %v, want single value", got["email"])
	}
	if len(got["groups"]) != 2 {
		t.Errorf("groups claim = %v, want two values preserved", got["groups"])
	}
}

func TestVerifyReturnsErrInvalidToken(t *testing.T) {
	// A provider signalling an authentication failure must surface the sentinel
	// so callers can classify it via errors.Is regardless of wrapping.
	p := &mockProvider{err: ErrInvalidToken}
	_, err := p.Verify(context.Background(), []byte("bad"))
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("errors.Is(err, ErrInvalidToken) = false, err = %v", err)
	}
}

func TestIsJWT(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want bool
	}{
		{"real-shaped jwt", []byte("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.sig"), true},
		{"minimal three segments", []byte("a.b.c"), true},
		{"two dots empty tails", []byte("a.."), false},
		{"two dots empty head", []byte("..c"), false},
		{"one dot", []byte("a.b"), false},
		{"no dots", []byte("sekret"), false},
		{"three dots", []byte("a.b.c.d"), false},
		{"bare two dots", []byte(".."), false},
		{"empty", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsJWT(tt.in); got != tt.want {
				t.Errorf("IsJWT(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
