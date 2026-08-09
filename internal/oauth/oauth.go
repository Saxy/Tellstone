/*
Package oauth
Tellstone OAuth Provider Registry
File: oauth.go
Description: Defines the Provider contract and Claims type behind connection-time token
verification. Providers are pluggable: built-in presets (STACKIT, generic OIDC) and any
future third-party adapters satisfy this interface, and the registry in registry.go
resolves a configured provider name to its implementation.

Authors:

	Maximilian Hagen
*/
package oauth

import (
	"context"
	"errors"
	"time"
)

// VerifyTimeout bounds a single Provider.Verify call. Verification can reach
// the identity provider (a JWKS refresh on key rotation), so the AUTH workers
// in the RESP and binary listeners derive their context from it instead of an
// unbounded background context.
const VerifyTimeout = 10 * time.Second

// ErrInvalidToken is returned by a Provider.Verify when the credential cannot be
// authenticated. Callers translate it into a NOAUTH error and must still find it
// through errors.Is, so implementations should wrap rather than replace it.
var ErrInvalidToken = errors.New("oauth: token verification failed")

// Config carries the static, non-secret parameters a provider needs to validate
// tokens. Credentials (client secrets, private keys) deliberately live outside
// this struct: they are sourced from environment or files at startup, never
// baked into a preset or the RBAC policy file.
type Config struct {
	Issuer   string   // OIDC issuer or discovery base URL, e.g. https://iam.example.com
	ClientID string   // expected audience (aud) when set; empty skips the aud check
	Scopes   []string // scopes the provider issues; informational, not enforced here
}

// Claims is the normalized result of verification. Values are slices because a
// claim may repeat (the JWT spec allows it) or be a structured list such as
// "groups" or "roles"; single-value claims are one-element slices. This is the
// exact shape the RBAC claim-to-role mapping consumes, so implementations must
// not flatten lists.
type Claims map[string][]string

// Provider validates connection-time credentials and turns them into claims.
//
// Implementations must be safe for concurrent use: the same Provider instance
// is shared by every connection worker and may be asked to Verify concurrently.
type Provider interface {
	// Config returns the provider's static parameters.
	Config() Config

	// Verify authenticates token (a signed JWT, or an opaque token resolved via
	// introspection) and returns its claims. It returns ErrInvalidToken for any
	// authentication failure; transient errors (IdP unreachable, discovery
	// refresh failing) are returned unwrapped so callers can tell "bad
	// credential" apart from "identity provider down" and decide whether to
	// treat the connection as failed.
	Verify(ctx context.Context, token []byte) (Claims, error)
}

// IsJWT reports whether b has the shape of a signed JWT: three dot-separated
// base64url segments (header.payload.signature). The AUTH dispatchers in the
// RESP and binary listeners use it to route a presented secret to token
// verification instead of the bcrypt password path. Shape-only by design — a
// well-formed but forged token is still rejected by Verify.
func IsJWT(b []byte) bool {
	dots := 0
	for _, c := range b {
		if c == '.' {
			dots++
		}
	}
	// At least one non-empty segment each side of two dots, so a bare ".." or
	// "a.b" (three segments is impossible with two dots) cannot slip through.
	return dots == 2 && len(b) >= 5
}
