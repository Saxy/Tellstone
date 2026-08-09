/*
Package generic
Tellstone Generic OIDC Provider
File: generic_test.go
Description: End-to-end verification tests against an in-process OIDC identity provider
(httptest). Covers the happy path, every rejection branch, and key rotation without any
real network dependency.

Authors:

	Maximilian Hagen
*/
package generic

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
)

// idpKey pairs a private key (used to sign test tokens) with its public
// counterpart and kid.
type idpKey struct {
	kid  string
	priv crypto.Signer
	pub  crypto.PublicKey
}

// testIDP is an in-process OIDC identity provider whose key set can grow at
// runtime, simulating key rotation.
type testIDP struct {
	mu   sync.Mutex
	keys map[string]idpKey
	// jwksRequests counts /jwks fetches so tests can assert single-flight.
	jwksRequests atomic.Int64
}

func newTestIDP() (*testIDP, *httptest.Server) {
	idp := &testIDP{keys: make(map[string]idpKey)}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"issuer":   srv.URL,
				"jwks_uri": srv.URL + "/jwks",
			})
		case "/jwks":
			idp.jwksRequests.Add(1)
			idp.mu.Lock()
			keys := make([]idpKey, 0, len(idp.keys))
			for _, k := range idp.keys {
				keys = append(keys, k)
			}
			idp.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": jwksFromKeys(keys)})
		default:
			http.NotFound(w, r)
		}
	}))
	return idp, srv
}

func (i *testIDP) addKey(k idpKey) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys[k.kid] = k
}

func rsaKey(kid string) idpKey {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return idpKey{kid: kid, priv: priv, pub: &priv.PublicKey}
}

func ecKey(kid string) idpKey {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return idpKey{kid: kid, priv: priv, pub: &priv.PublicKey}
}

func jwksFromKeys(keys []idpKey) []jwk {
	out := make([]jwk, 0, len(keys))
	for _, k := range keys {
		switch pk := k.pub.(type) {
		case *rsa.PublicKey:
			e := big.NewInt(int64(pk.E))
			out = append(out, jwk{Kty: "RSA", Kid: k.kid, N: b64enc(pk.N.Bytes()), E: b64enc(e.Bytes())})
		case *ecdsa.PublicKey:
			out = append(out, jwk{Kty: "EC", Kid: k.kid, Crv: "P-256", X: b64enc(pk.X.Bytes()), Y: b64enc(pk.Y.Bytes())})
		}
	}
	return out
}

func b64enc(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// signJWT mints a token with the given key/algorithm. It is test-only, so a
// signing failure is a test bug and panics.
func signJWT(k idpKey, alg string, claims map[string]any) string {
	hb, err := json.Marshal(map[string]string{"alg": alg, "kid": k.kid, "typ": "JWT"})
	if err != nil {
		panic(err)
	}
	pb, err := json.Marshal(claims)
	if err != nil {
		panic(err)
	}
	head := b64enc(hb)
	payload := b64enc(pb)
	signed := head + "." + payload
	digest := sha256.Sum256([]byte(signed))

	var sig []byte
	switch alg {
	case "RS256":
		sig, err = rsa.SignPKCS1v15(rand.Reader, k.priv.(*rsa.PrivateKey), crypto.SHA256, digest[:])
	case "ES256":
		sig, err = k.priv.(*ecdsa.PrivateKey).Sign(rand.Reader, digest[:], crypto.SHA256)
	case "HS256":
		mac := hmac.New(sha256.New, []byte("symmetric-secret"))
		mac.Write([]byte(signed))
		sig = mac.Sum(nil)
	default:
		panic("signJWT: unsupported test algorithm " + alg)
	}
	if err != nil {
		panic(err)
	}
	return signed + "." + b64enc(sig)
}

// validClaims returns a token payload that passes every configured check.
func validClaims(issuer string) map[string]any {
	return map[string]any{
		"iss":    issuer,
		"aud":    "tellstone",
		"exp":    time.Now().Add(time.Hour).Unix(),
		"sub":    "user-1",
		"email":  "alice@tellstone.io",
		"groups": []any{"dev", "qa"},
	}
}

// newProvider spins up an IdP with one RSA key and constructs the provider
// against it. The returned key is the one the IdP published, so tests sign
// with it to reach the claim-validation branch rather than failing earlier on
// a signature mismatch.
func newProvider(t *testing.T) (*Provider, *testIDP, *httptest.Server, idpKey) {
	t.Helper()
	idp, srv := newTestIDP()
	key := rsaKey("key-1")
	idp.addKey(key)
	t.Cleanup(srv.Close)
	p, err := New(oauth.Config{Issuer: srv.URL, ClientID: "tellstone"}, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return p, idp, srv, key
}

func TestVerifyValidToken(t *testing.T) {
	p, _, srv, key := newProvider(t)
	claims, err := p.Verify(t.Context(), []byte(signJWT(key, "RS256", validClaims(srv.URL))))
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if got := claims["email"]; len(got) != 1 || got[0] != "alice@tellstone.io" {
		t.Errorf("email claim = %v, want single alice@tellstone.io", got)
	}
	if got := claims["groups"]; len(got) != 2 {
		t.Errorf("groups claim = %v, want both values preserved", got)
	}
	if got := claims["sub"]; len(got) != 1 || got[0] != "user-1" {
		t.Errorf("sub claim = %v, want user-1", got)
	}
}

func TestVerifyRejectsInvalidToken(t *testing.T) {
	p, _, srv, key := newProvider(t)

	// expired / wrong issuer / wrong audience are signed with the published key
	// so the signature passes and the claim validation is what rejects them.
	badClaims := func(mutate func(map[string]any)) map[string]any {
		c := validClaims(srv.URL)
		mutate(c)
		return c
	}

	cases := []struct {
		name  string
		token string
	}{
		{"wrong signature", signJWT(rsaKey("other"), "RS256", validClaims(srv.URL))},
		{"expired", signJWT(key, "RS256", badClaims(func(c map[string]any) {
			c["exp"] = time.Now().Add(-time.Hour).Unix()
		}))},
		{"wrong issuer", signJWT(key, "RS256", badClaims(func(c map[string]any) {
			c["iss"] = "https://unrelated.example.com"
		}))},
		{"wrong audience", signJWT(key, "RS256", badClaims(func(c map[string]any) {
			c["aud"] = "another-app"
		}))},
		{"disallowed algorithm", signJWT(key, "HS256", validClaims(srv.URL))},
		{"not a jwt", "not.a.jwt"},
		{"empty", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.Verify(t.Context(), []byte(tc.token))
			if !errors.Is(err, oauth.ErrInvalidToken) {
				t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestVerifyES256(t *testing.T) {
	p, idp, srv, _ := newProvider(t)
	ec := ecKey("ec-1")
	idp.addKey(ec)
	token := signJWT(ec, "ES256", validClaims(srv.URL))
	claims, err := p.Verify(t.Context(), []byte(token))
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if claims["sub"][0] != "user-1" {
		t.Errorf("sub claim = %v, want user-1", claims["sub"])
	}
}

func TestVerifyRefreshesOnUnknownKid(t *testing.T) {
	p, idp, srv, _ := newProvider(t)
	// A token signed by an as-yet-unknown key must trigger a JWKS refresh.
	rotated := rsaKey("key-2")
	idp.addKey(rotated)
	token := signJWT(rotated, "RS256", validClaims(srv.URL))
	claims, err := p.Verify(t.Context(), []byte(token))
	if err != nil {
		t.Fatalf("Verify() after rotation error = %v, want nil", err)
	}
	if claims["sub"][0] != "user-1" {
		t.Errorf("sub claim = %v, want user-1", claims["sub"])
	}
}

func TestVerifyRejectsUnknownKidAfterRefresh(t *testing.T) {
	p, _, srv, _ := newProvider(t)
	// key-2 is never published to the IdP, so even a refresh cannot resolve it.
	token := signJWT(rsaKey("key-2"), "RS256", validClaims(srv.URL))
	_, err := p.Verify(t.Context(), []byte(token))
	if !errors.Is(err, oauth.ErrInvalidToken) {
		t.Fatalf("Verify() error = %v, want ErrInvalidToken", err)
	}
}

func TestVerifyThrottlesUnknownKidRefresh(t *testing.T) {
	p, idp, srv, _ := newProvider(t)
	// The first rotation is picked up by a refresh.
	rotated := rsaKey("key-2")
	idp.addKey(rotated)
	token := signJWT(rotated, "RS256", validClaims(srv.URL))
	if _, err := p.Verify(t.Context(), []byte(token)); err != nil {
		t.Fatalf("first Verify after rotation error = %v, want nil", err)
	}
	// A second, even newer key arrives within the cooldown window: the refresh
	// is skipped, so its token is rejected rather than fetched.
	rotated2 := rsaKey("key-3")
	idp.addKey(rotated2)
	token2 := signJWT(rotated2, "RS256", validClaims(srv.URL))
	if _, err := p.Verify(t.Context(), []byte(token2)); !errors.Is(err, oauth.ErrInvalidToken) {
		t.Fatalf("Verify during cooldown error = %v, want ErrInvalidToken", err)
	}
	// A known key still verifies during the cooldown: the throttle only affects
	// the unknown-key path.
	known := signJWT(rotated, "RS256", validClaims(srv.URL))
	if _, err := p.Verify(t.Context(), []byte(known)); err != nil {
		t.Fatalf("Verify of cached key during cooldown error = %v, want nil", err)
	}
}

func TestVerifySingleFlightRefresh(t *testing.T) {
	p, idp, srv, _ := newProvider(t)
	before := idp.jwksRequests.Load()
	rotated := rsaKey("key-2")
	idp.addKey(rotated)
	token := signJWT(rotated, "RS256", validClaims(srv.URL))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claims, err := p.Verify(context.Background(), []byte(token))
			if err != nil {
				t.Errorf("Verify() error = %v, want nil", err)
				return
			}
			if claims["sub"][0] != "user-1" {
				t.Errorf("sub claim = %v, want user-1", claims["sub"])
			}
		}()
	}
	wg.Wait()

	// All concurrent verifications collapsed onto a single refresh.
	if got := idp.jwksRequests.Load() - before; got != 1 {
		t.Fatalf("jwks fetches = %d, want exactly 1 (single-flight)", got)
	}
}

func TestNewRejectsEmptyIssuer(t *testing.T) {
	if _, err := New(oauth.Config{}, log.NewNoOpLogger()); err == nil {
		t.Fatal("New() with empty issuer error = nil, want an error")
	}
}

func TestNewFailsFastOnUnreachableIssuer(t *testing.T) {
	// Nothing listens on port 1; the constructor must surface the discovery
	// failure instead of deferring it to the first Verify.
	if _, err := New(oauth.Config{Issuer: "http://127.0.0.1:1"}, log.NewNoOpLogger()); err == nil {
		t.Fatal("New() against unreachable issuer error = nil, want an error")
	}
}
