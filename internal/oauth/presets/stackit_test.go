/*
Package presets
Tellstone OAuth Provider Presets
File: stackit_test.go
Description: Tests the STACKIT preset against an in-process OIDC identity provider: preset
defaults, delegation to the generic verifier, and passthrough of the groups claim STACKIT
id_tokens carry natively.

Authors:

	Maximilian Hagen
*/
package presets

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
)

func TestStackitPresetDefaults(t *testing.T) {
	srv, _ := startTestIDP(t)
	p, err := NewStackit(oauth.Config{Issuer: srv.URL, ClientID: "app"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("NewStackit() error = %v", err)
	}
	cfg := p.Config()
	if cfg.Issuer != srv.URL {
		t.Errorf("Issuer = %q, want overridden %q", cfg.Issuer, srv.URL)
	}
	want := []string{"openid", "email", "profile"}
	if len(cfg.Scopes) != len(want) {
		t.Errorf("Scopes = %v, want %v", cfg.Scopes, want)
	}
	for i := range want {
		if cfg.Scopes[i] != want[i] {
			t.Errorf("Scopes = %v, want %v", cfg.Scopes, want)
		}
	}
}

func TestStackitVerifyAcceptsValidToken(t *testing.T) {
	srv, priv := startTestIDP(t)
	p, err := NewStackit(oauth.Config{Issuer: srv.URL, ClientID: "app"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("NewStackit() error = %v", err)
	}

	token := signGoogleToken(t, priv, "test-google", srv.URL, map[string]any{
		"iss":    srv.URL,
		"aud":    "app",
		"exp":    time.Now().Add(time.Hour).Unix(),
		"sub":    "stackit-user-1",
		"email":  "svc@tellstone.io",
		"groups": []string{"admins"}, // STACKIT issues groups natively
	})
	claims, err := p.Verify(context.Background(), []byte(token))
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if got := claims["groups"]; len(got) != 1 || got[0] != "admins" {
		t.Errorf("groups claim = %v, want [admins] (passthrough, no normalization)", got)
	}
	if got := claims["email"]; len(got) != 1 || got[0] != "svc@tellstone.io" {
		t.Errorf("email claim = %v, want svc@tellstone.io", got)
	}
}

func TestStackitRejectsBadSignature(t *testing.T) {
	srv, _ := startTestIDP(t)
	p, err := NewStackit(oauth.Config{Issuer: srv.URL, ClientID: "app"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("NewStackit() error = %v", err)
	}

	// Sign with a key the IdP never published.
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	token := signGoogleToken(t, other, "test-google", srv.URL, map[string]any{
		"iss": srv.URL,
		"aud": "app",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	if _, err := p.Verify(context.Background(), []byte(token)); !errors.Is(err, oauth.ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
	}
}
