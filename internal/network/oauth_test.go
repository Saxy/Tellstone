/*
Package network
Tellstone Binary Listener
File: oauth_test.go
Description: End-to-end tests for bearer-token AUTH over the binary protocol: a
JWT-shaped password in a MsgAuth frame is routed to the oauth provider, whose claims
resolve to a role via the policy's oauth.rules. Verified tokens pin the role to the
connection; forged tokens and tokens without a matching rule are rejected (fail-closed).

Authors:

	Maximilian Hagen
*/
package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"strings"
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

func startOAuthServer(t *testing.T, prov oauth.Provider, store *fakeStore) string {
	t.Helper()
	policy, err := rbac.Parse([]byte(oauthPolicy))
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to obtain free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	srv := NewServer(addr, 0, nil, storeHandler(store), log.NewNoOpLogger(), nil, "",
		rbac.NewStore(policy, log.NewNoOpLogger()), prov, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	if err := waitForServer(addr, 2*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}
	return addr
}

func TestServerOAuthAuth(t *testing.T) {
	token := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSIsImdyb3VwcyI6WyJhZG1pbnMiXX0.sig"
	store := newFakeStore()
	addr := startOAuthServer(t, &stubProvider{
		token:  token,
		claims: oauth.Claims{"sub": {"alice@tellstone.io"}, "groups": {"admins"}},
	}, store)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	var reqBuf [32]byte
	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len("k")))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)
	copy(reqBuf[11:], "k")

	// GET before auth — rejected.
	resp := sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for GET before auth, got %v", resp.Type)
	}

	// Wrong token — rejected.
	resp = sendAndRecv(t, conn, MsgAuth, buildAuthPayload("a.b.c"))
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for wrong token, got %v", resp.Type)
	}

	// Valid token — accepted and the role is pinned.
	resp = sendAndRecv(t, conn, MsgAuth, buildAuthPayload(token))
	if resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk for valid token, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, ResponseOK) {
		t.Fatalf("expected OK payload, got %q", resp.Payload)
	}

	// GET after auth — allowed (admin role).
	store.set("k", []byte("value"))
	resp = sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse for GET after auth, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, []byte("value")) {
		t.Fatalf("expected 'value', got %q", resp.Payload)
	}
}

// TestClientOAuthLargeToken drives the packaged client through a bearer-token
// AUTH whose password exceeds the 512-byte stack buffer, exercising the heap
// fallback so real OIDC id_tokens (routinely >1 KB) can authenticate over the
// binary protocol. The pinned admin role then grants a GET.
func TestClientOAuthLargeToken(t *testing.T) {
	token := "eyJhbGciOiJSUzI1NiJ9." + strings.Repeat("x", 690) + ".sig"
	store := newFakeStore()
	addr := startOAuthServer(t, &stubProvider{
		token:  token,
		claims: oauth.Claims{"sub": {"alice@tellstone.io"}, "groups": {"admins"}},
	}, store)
	store.set("k", []byte("large-token-ok"))

	client, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	scratch := make([]byte, 4096)

	if err := client.Auth(token, scratch); err != nil {
		t.Fatalf("Auth with large token: %v", err)
	}
	got, err := client.Get([]byte("k"), scratch)
	if err != nil {
		t.Fatalf("Get after large-token auth: %v", err)
	}
	if !bytes.Equal(got, []byte("large-token-ok")) {
		t.Fatalf("got %q, want %q", got, "large-token-ok")
	}
}

// TestClientOAuthCredentialTooLong verifies the uint16 length-prefix guard:
// an AUTH credential beyond 65535 bytes must fail with a clear error instead
// of wrapping the length field and emitting a corrupt frame.
func TestClientOAuthCredentialTooLong(t *testing.T) {
	store := newFakeStore()
	addr := startOAuthServer(t, &stubProvider{
		token:  "ok",
		claims: oauth.Claims{"groups": {"admins"}},
	}, store)

	client, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	scratch := make([]byte, 4096)

	tooLong := strings.Repeat("x", math.MaxUint16+1)
	if err := client.Auth(tooLong, scratch); !errors.Is(err, ErrAuthCredentialsTooLong) {
		t.Fatalf("Auth: got %v, want ErrAuthCredentialsTooLong", err)
	}
	if err := client.AuthUser("alice", tooLong, scratch); !errors.Is(err, ErrAuthCredentialsTooLong) {
		t.Fatalf("AuthUser with long password: got %v, want ErrAuthCredentialsTooLong", err)
	}
	if err := client.AuthUser(tooLong, "pass", scratch); !errors.Is(err, ErrAuthCredentialsTooLong) {
		t.Fatalf("AuthUser with long username: got %v, want ErrAuthCredentialsTooLong", err)
	}

	// The boundary itself must still be accepted on the wire.
	if err := client.Auth(strings.Repeat("y", math.MaxUint16), scratch); err == nil ||
		errors.Is(err, ErrAuthCredentialsTooLong) {
		t.Fatalf("Auth at boundary: got %v, want a server reply, not ErrAuthCredentialsTooLong", err)
	}
}

func TestServerOAuthDenyNoRoleMatch(t *testing.T) {
	// A valid token whose claims match no oauth.rules entry must be denied,
	// not silently granted a default role.
	addr := startOAuthServer(t, &stubProvider{
		token:  "a.b.c",
		claims: oauth.Claims{"sub": {"mallory@tellstone.io"}, "groups": {"everyone"}},
	}, newFakeStore())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayload("a.b.c"))
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for unmapped token, got %v", resp.Type)
	}

	var reqBuf [32]byte
	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len("k")))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)
	copy(reqBuf[11:], "k")
	resp = sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for GET after denied token, got %v", resp.Type)
	}
}
