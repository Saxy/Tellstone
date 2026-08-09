/*
Package generic
Tellstone Generic OIDC Provider
File: generic.go
Description: Reference internal provider implementing the OAuth2/OIDC flow on stdlib only.
It discovers the identity provider's configuration, caches its JWKS, and verifies RS256 /
ES256 JWTs at AUTH time. Built-in providers follow this shape: a constructor that fetches
what it needs eagerly (so misconfiguration fails fast at startup, not at first AUTH) and a
Verify that stays local and allocation-light.

Authors:

	Maximilian Hagen
*/
package generic

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
)

const (
	// discoveryPath is appended to the issuer for RFC 8414 style discovery.
	discoveryPath = "/.well-known/openid-configuration"
	// refreshTimeout bounds every outbound call to the identity provider so a
	// hung IdP cannot stall AUTH indefinitely.
	refreshTimeout = 10 * time.Second
	// minRefreshInterval is the minimum time between JWKS refreshes. It bounds
	// how often a presented token can force a refresh: without it, unknown-kid
	// tokens would amplify into a refresh storm against the identity provider.
	minRefreshInterval = 30 * time.Second
)

// errRefreshCoolingDown is returned when a refresh is skipped because one
// completed within minRefreshInterval. Callers translate it into
// oauth.ErrInvalidToken: the key stays unknown, so the token cannot be trusted.
var errRefreshCoolingDown = errors.New("generic: jwks refresh cooling down")

// allowedAlgs is the signature algorithm allowlist. Everything else — notably
// the HS* family, which would accept the raw key material as a symmetric
// secret — is rejected outright to prevent algorithm-confusion attacks.
var allowedAlgs = map[string]bool{"RS256": true, "ES256": true}

// Provider verifies JWTs for a single OIDC identity provider.
//
// It is safe for concurrent use: jwks is the only mutable state and sits behind
// an RWMutex. Config and jwksURI are written once during New and read-only
// afterward.
type Provider struct {
	cfg       oauth.Config
	client    *http.Client
	jwksURI   string
	jwks      *jwksCache
	logger    log.Logger
	refreshMu sync.Mutex
	inFlight  *refreshAttempt
	refreshAt time.Time
}

// refreshAttempt tracks one in-flight JWKS refresh so concurrent callers wait
// for it and reuse its result rather than each hitting the identity provider.
// done is closed once the outcome is set; err is written before that close, so
// readers see a stable value after <-done.
type refreshAttempt struct {
	done chan struct{}
	err  error
}

// New builds a provider and, in the same call, discovers the IdP and fetches
// its keys. Doing the network round-trip here keeps the fail-fast property:
// a wrong issuer or unreachable IdP is reported at startup, before the first
// AUTH can ever fail on it.
func New(cfg oauth.Config, logger log.Logger) (*Provider, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("generic: oidc issuer is required")
	}
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	p := &Provider{
		cfg:    cfg,
		client: &http.Client{Timeout: refreshTimeout},
		jwks:   &jwksCache{keys: make(map[string]crypto.PublicKey)},
		logger: logger,
	}
	if err := p.refreshDiscovery(context.Background()); err != nil {
		return nil, fmt.Errorf("generic: oidc discovery for %q failed: %w", cfg.Issuer, err)
	}
	return p, nil
}

// Config returns the provider's static parameters.
func (p *Provider) Config() oauth.Config { return p.cfg }

// Verify authenticates a JWT and returns its claims.
//
// Authentication failures all surface as oauth.ErrInvalidToken. The single
// exception is a failed JWKS refresh, returned unwrapped: it means the IdP is
// unreachable, which is a transient condition the caller may want to treat
// differently from a rejected credential.
func (p *Provider) Verify(ctx context.Context, token []byte) (oauth.Claims, error) {
	headerB, payloadB, sig, err := splitToken(token)
	if err != nil {
		return nil, err
	}
	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err = json.Unmarshal(headerB, &hdr); err != nil {
		return nil, oauth.ErrInvalidToken
	}
	if !allowedAlgs[hdr.Alg] {
		return nil, oauth.ErrInvalidToken
	}
	if hdr.Kid == "" {
		return nil, oauth.ErrInvalidToken
	}
	key, ok := p.jwks.lookup(hdr.Kid)
	if !ok {
		// check if key may rotated
		if err = p.refreshJWKSThrottled(ctx); err != nil {
			// A refresh skipped by the cooldown surfaces as a rejection: the
			// key stays unknown, so the token cannot be trusted. Transient IdP
			// failures keep their own (unwrapped) error so callers can tell
			// "bad credential" apart from "identity provider down".
			if errors.Is(err, errRefreshCoolingDown) {
				return nil, oauth.ErrInvalidToken
			}
			return nil, err
		}
		key, ok = p.jwks.lookup(hdr.Kid)
		if !ok {
			return nil, oauth.ErrInvalidToken
		}
	}
	segments := strings.Split(string(token), ".")
	if err = verifySignature(hdr.Alg, key, []byte(segments[0]+"."+segments[1]), sig); err != nil {
		return nil, oauth.ErrInvalidToken
	}
	var raw map[string]any
	if err = json.Unmarshal(payloadB, &raw); err != nil {
		return nil, oauth.ErrInvalidToken
	}
	if err = validateClaims(raw, p.cfg); err != nil {
		return nil, oauth.ErrInvalidToken
	}
	return flatten(raw), nil
}

// refreshDiscovery fetches the OIDC discovery document and follows it to the
// JWKS endpoint.
func (p *Provider) refreshDiscovery(ctx context.Context) error {
	url := strings.TrimRight(p.cfg.Issuer, "/") + discoveryPath
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := p.getJSON(ctx, url, &doc); err != nil {
		return err
	}
	if doc.JWKSURI == "" {
		return errors.New("oauth: discovery document has no jwks_uri")
	}
	p.jwksURI = doc.JWKSURI
	return p.refreshJWKS(ctx)
}

// refreshJWKS re-fetches the key set. Used at construction and, on a missed
// kid, during Verify.
func (p *Provider) refreshJWKS(ctx context.Context) error {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := p.getJSON(ctx, p.jwksURI, &set); err != nil {
		return err
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		key, err := parseJWK(k)
		if err != nil {
			if p.logger.Enabled(log.LevelWarn) {
				p.logger.Log(log.LevelWarn, "oauth: skipping unusable jwk", log.String("kid", k.Kid), log.String("error", err.Error()))
			}
			continue
		}
		keys[k.Kid] = key
	}
	if len(keys) == 0 {
		return errors.New("oauth: jwks contained no usable keys")
	}
	p.jwks.mu.Lock()
	p.jwks.keys = keys
	p.jwks.mu.Unlock()
	return nil
}

// refreshJWKSThrottled re-fetches the key set subject to a minimum interval and
// single-flight semantics. Concurrent callers collapse into one refresh and
// share its result; a caller whose refresh would fall within
// minRefreshInterval of a completed attempt gets errRefreshCoolingDown instead
// of another fetch. refreshJWKS errors are returned unchanged.
func (p *Provider) refreshJWKSThrottled(ctx context.Context) error {
	p.refreshMu.Lock()
	if a := p.inFlight; a != nil {
		p.refreshMu.Unlock()
		select {
		case <-a.done:
			return a.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if time.Since(p.refreshAt) < minRefreshInterval {
		p.refreshMu.Unlock()
		return errRefreshCoolingDown
	}
	a := &refreshAttempt{done: make(chan struct{})}
	p.inFlight = a
	p.refreshMu.Unlock()
	a.err = p.refreshJWKS(ctx)
	p.refreshMu.Lock()
	p.refreshAt = time.Now()
	p.inFlight = nil
	close(a.done)
	p.refreshMu.Unlock()
	return a.err
}

func (p *Provider) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("generic: GET %s returned %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// JWTsCache holds the parsed public keys indexed by kid. The whole set is
// replaced on refresh; individual entries are never mutated in place.
type jwksCache struct {
	mu   sync.RWMutex
	keys map[string]crypto.PublicKey
}

func (c *jwksCache) lookup(kid string) (crypto.PublicKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	k, ok := c.keys[kid]
	return k, ok
}

// jwk is the JSON Web Key wire format for the RSA and EC key types Tellstone
// accepts.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseJWK converts a JWK to a crypto.PublicKey.
func parseJWK(k jwk) (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		n, err := b64(k.N)
		if err != nil {
			return nil, err
		}
		e, err := b64(k.E)
		if err != nil {
			return nil, err
		}
		// The exponent field is the big-endian integer; production keys use
		// 65537 (0x010001), but parse it generically.
		return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}, nil
	case "EC":
		if k.Crv != "P-256" {
			return nil, fmt.Errorf("generic: unsupported EC curve %q", k.Crv)
		}
		x, err := b64(k.X)
		if err != nil {
			return nil, err
		}
		y, err := b64(k.Y)
		if err != nil {
			return nil, err
		}
		return &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}, nil
	default:
		return nil, fmt.Errorf("generic: unsupported key type %q", k.Kty)
	}
}

// splitToken decodes the three JWT segments, mapping any decode failure to an
// authentication error.
func splitToken(token []byte) (header, payload, sig []byte, err error) {
	parts := strings.Split(string(token), ".")
	if len(parts) != 3 {
		return nil, nil, nil, oauth.ErrInvalidToken
	}
	if header, err = b64(parts[0]); err != nil {
		return nil, nil, nil, oauth.ErrInvalidToken
	}
	if payload, err = b64(parts[1]); err != nil {
		return nil, nil, nil, oauth.ErrInvalidToken
	}
	if sig, err = b64(parts[2]); err != nil {
		return nil, nil, nil, oauth.ErrInvalidToken
	}
	return header, payload, sig, nil
}

func b64(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("generic: empty base64 field")
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// verifySignature checks the token's signature against the selected key. The
// key type is asserted against the algorithm so a mismatched key can never be
// interpreted as a different algorithm.
func verifySignature(alg string, key crypto.PublicKey, signed, sig []byte) error {
	digest := sha256.Sum256(signed)
	switch alg {
	case "RS256":
		rsaKey, ok := key.(*rsa.PublicKey)
		if !ok {
			return errors.New("generic: key type does not match algorithm")
		}
		return rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, digest[:], sig)
	case "ES256":
		ecKey, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return errors.New("generic: key type does not match algorithm")
		}
		if !ecdsa.VerifyASN1(ecKey, digest[:], sig) {
			return errors.New("generic: ecdsa signature mismatch")
		}
		return nil
	}
	return errors.New("generic: unsupported algorithm")
}

// validateClaims enforces the token's temporal and scope constraints. exp and
// nbf are checked only when present; iss and aud are checked only when the
// operator configured them, so a bare issuer URL is enough to get going.
func validateClaims(raw map[string]any, cfg oauth.Config) error {
	now := time.Now().Unix()
	if exp, ok := num(raw["exp"]); ok && now >= exp {
		return errors.New("generic: token expired")
	}
	if nbf, ok := num(raw["nbf"]); ok && now < nbf {
		return errors.New("generic: token not yet valid")
	}
	if cfg.Issuer != "" {
		if iss, ok := raw["iss"].(string); !ok || iss != cfg.Issuer {
			return errors.New("generic: issuer mismatch")
		}
	}
	if cfg.ClientID != "" && !audContains(raw["aud"], cfg.ClientID) {
		return errors.New("generic: audience mismatch")
	}
	return nil
}

func num(v any) (int64, bool) {
	f, ok := v.(float64)
	if !ok {
		return 0, false
	}
	return int64(f), true
}

// audContains matches the audience claim whether the IdP emitted it as a
// single string or as a list (both are valid per the OIDC spec).
func audContains(aud any, want string) bool {
	switch a := aud.(type) {
	case string:
		return a == want
	case []any:
		for _, v := range a {
			if s, ok := v.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

// flatten normalizes decoded JSON claims into the map-of-slices shape the RBAC
// mapping consumes, preserving list claims (groups, roles) as-is.
func flatten(raw map[string]any) oauth.Claims {
	out := oauth.Claims{}
	for k, v := range raw {
		switch val := v.(type) {
		case string:
			out[k] = []string{val}
		case []any:
			var s []string
			for _, item := range val {
				if str, ok := item.(string); ok {
					s = append(s, str)
				}
			}
			if len(s) > 0 {
				out[k] = s
			}
		case float64:
			out[k] = []string{strconv.FormatInt(int64(val), 10)}
		case bool:
			out[k] = []string{strconv.FormatBool(val)}
		}
	}
	return out
}
