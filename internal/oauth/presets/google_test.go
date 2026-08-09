/*
Package presets
Tellstone OAuth Provider Presets
File: google_test.go
Description: Tests the Google preset against an in-process OIDC identity provider: preset
defaults, delegation to the generic verifier, and the hd-to-groups claim normalization.

Authors:

	Maximilian Hagen
*/
package presets

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
)

// startTestIDP serves OIDC discovery and a single-key JWKS backed by the given
// RSA private key, so tests can mint tokens the provider will accept.
func startTestIDP(t *testing.T) (*httptest.Server, *rsa.PrivateKey) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	const kid = "test-google"
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"issuer":   srv.URL,
				"jwks_uri": srv.URL + "/jwks",
			})
		case "/jwks":
			e := big.NewInt(int64(priv.E))
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "kid": kid,
				"n": b64std(priv.N.Bytes()),
				"e": b64std(e.Bytes()),
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, priv
}

func b64std(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// signGoogleToken mints an RS256 JWT signed by priv. Test-only; signing
// failures indicate a test bug and fail loudly.
func signGoogleToken(t *testing.T, priv *rsa.PrivateKey, kid, issuer string, claims map[string]any) string {
	t.Helper()
	hb, err := json.Marshal(map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	pb, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	head := b64std(hb)
	payload := b64std(pb)
	signed := head + "." + payload
	digest := sha256.Sum256([]byte(signed))
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signed + "." + b64std(sig)
}

func googleClaims(issuer string) map[string]any {
	return map[string]any{
		"iss":   issuer,
		"aud":   "app",
		"exp":   time.Now().Add(time.Hour).Unix(),
		"sub":   "google-user-1",
		"email": "alice@tellstone.io",
		"hd":    "tellstone.io", // Google carries the hosted domain here, not in groups
	}
}

func TestGooglePresetDefaults(t *testing.T) {
	srv, _ := startTestIDP(t)
	p, err := NewGoogle(oauth.Config{Issuer: srv.URL, ClientID: "app"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("NewGoogle() error = %v", err)
	}
	cfg := p.Config()
	if cfg.Issuer != srv.URL {
		t.Errorf("Issuer = %q, want overridden %q", cfg.Issuer, srv.URL)
	}
	// Scopes default to Google's when unset.
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

func TestGoogleVerifyNormalizesHD(t *testing.T) {
	srv, priv := startTestIDP(t)
	p, err := NewGoogle(oauth.Config{Issuer: srv.URL, ClientID: "app"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("NewGoogle() error = %v", err)
	}

	token := signGoogleToken(t, priv, "test-google", srv.URL, googleClaims(srv.URL))
	claims, err := p.Verify(context.Background(), []byte(token))
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if got := claims["groups"]; len(got) != 1 || got[0] != "tellstone.io" {
		t.Errorf("groups claim = %v, want hd normalized to [tellstone.io]", got)
	}
	if got := claims["email"]; len(got) != 1 || got[0] != "alice@tellstone.io" {
		t.Errorf("email claim = %v, want alice@tellstone.io", got)
	}
}

func TestGoogleRejectsBadSignature(t *testing.T) {
	srv, _ := startTestIDP(t)
	p, err := NewGoogle(oauth.Config{Issuer: srv.URL, ClientID: "app"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("NewGoogle() error = %v", err)
	}

	// Sign with a key the IdP never published.
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	token := signGoogleToken(t, other, "test-google", srv.URL, googleClaims(srv.URL))
	if _, err := p.Verify(context.Background(), []byte(token)); !errors.Is(err, oauth.ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
	}
}
