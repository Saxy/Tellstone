/*
Package resp
Tellstone RESP Listener
File: oauth_test.go
Description: End-to-end tests for bearer-token AUTH: a JWT-shaped secret is routed to the
oauth provider, whose claims resolve to a role via the policy's oauth.rules. Verified
tokens pin the role to the connection; forged tokens and tokens without a matching rule
are rejected (fail-closed).

Authors:

	Maximilian Hagen
*/
package resp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
	"github.com/Saxy/Tellstone/internal/rbac"
)

const oauthPolicy = `
roles:
  - name: admin
    rules: ["+@all", "~*"]
oauth:
  rules:
    - claim: groups
      match: "admins"
      role: admin
`

// stubProvider authenticates exactly one well-formed token and returns its
// claims; everything else fails as a bad credential.
type stubProvider struct {
	token  string
	claims oauth.Claims
}

func (p *stubProvider) Config() oauth.Config { return oauth.Config{} }

func (p *stubProvider) Verify(_ context.Context, token []byte) (oauth.Claims, error) {
	if string(token) != p.token {
		return nil, oauth.ErrInvalidToken
	}
	return p.claims, nil
}

func startOAuthServer(t *testing.T, prov oauth.Provider) (addr string) {
	t.Helper()
	policy, err := rbac.Parse([]byte(oauthPolicy))
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	addr = freeAddr(t)
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), nil, "", false,
		rbac.NewStore(policy, log.NewNoOpLogger()), prov, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return addr
}

func authCmd(token string) string {
	return fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(token), token)
}

func TestRESPServer_OAuthAuth(t *testing.T) {
	token := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSIsImdyb3VwcyI6WyJhZG1pbnMiXX0.sig"
	addr := startOAuthServer(t, &stubProvider{
		token:  token,
		claims: oauth.Claims{"sub": {"alice@tellstone.io"}, "groups": {"admins"}},
	})

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "GET before auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "-NOAUTH Authentication required\r\n")
	expectReply(t, conn, "AUTH wrong token", authCmd("a.b.c"), "-ERR invalid password\r\n")
	expectReply(t, conn, "AUTH valid token", authCmd(token), "+OK\r\n")
	// The token maps to the admin role (+@all): data commands are now allowed.
	expectReply(t, conn, "SET after oauth",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "+OK\r\n")
	expectReply(t, conn, "GET after oauth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$1\r\nv\r\n")
}

func TestRESPServer_OAuthDenyNoRoleMatch(t *testing.T) {
	// A valid token whose claims match no oauth.rules entry must be denied,
	// not silently granted a default role.
	addr := startOAuthServer(t, &stubProvider{
		token:  "a.b.c",
		claims: oauth.Claims{"sub": {"mallory@tellstone.io"}, "groups": {"everyone"}},
	})

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH unmapped token", authCmd("a.b.c"), "-ERR invalid password\r\n")
	expectReply(t, conn, "GET after denied token",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "-NOAUTH Authentication required\r\n")
}
