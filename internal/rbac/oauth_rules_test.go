/*
Package rbac
Tellstone Role-Based Access Control Tests
File: oauth_rules_test.go
Description: Verifies the policy's oauth.rules block: claim-to-role resolution with
first-match-wins semantics, exact and glob match patterns, fail-closed deny on no match,
and load-time rejection of malformed rules.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"errors"
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

const oauthRulesYAML = `
roles:
  - name: admin
    rules: ["+@all"]
  - name: reader
    rules: ["+@read", "~*"]
  - name: ci
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      match: "admins"
      role: admin
    - claim: email
      match: "*@tellstone.io"
      role: reader
    - claim: sub
      match: "ci-*"
      role: ci
`

func TestRoleForClaims(t *testing.T) {
	p, err := Parse([]byte(oauthRulesYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cases := []struct {
		name   string
		claims map[string][]string
		want   string // "" means deny (nil role)
	}{
		{"exact match", map[string][]string{"groups": {"admins"}}, "admin"},
		{"suffix glob", map[string][]string{"email": {"alice@tellstone.io"}}, "reader"},
		{"prefix glob", map[string][]string{"sub": {"ci-123"}}, "ci"},
		{"exact wins over later rule", map[string][]string{"groups": {"admins"}, "email": {"alice@tellstone.io"}}, "admin"},
		{"claim missing denies", map[string][]string{"other": {"x"}}, ""},
		{"value mismatch denies", map[string][]string{"groups": {"viewers"}}, ""},
		{"no oauth claims denies", map[string][]string{}, ""},
	}
	for _, tc := range cases {
		got := p.RoleForClaims(tc.claims)
		switch {
		case tc.want == "" && got != nil:
			t.Errorf("%s: RoleForClaims = %q, want deny (nil)", tc.name, got.Name)
		case tc.want != "" && (got == nil || got.Name != tc.want):
			t.Errorf("%s: RoleForClaims = %v, want %q", tc.name, roleName(got), tc.want)
		}
	}
}

func roleName(r *Role) string {
	if r == nil {
		return "<nil>"
	}
	return r.Name
}

func TestRoleForClaimsFirstMatchWins(t *testing.T) {
	// The same claim value matching two rules resolves to the first rule's role.
	doc := `
roles:
  - name: admin
    rules: ["+@all"]
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      match: "*"
      role: admin
    - claim: groups
      match: "admins"
      role: reader
`
	p, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := p.RoleForClaims(map[string][]string{"groups": {"admins"}})
	if got == nil || got.Name != "admin" {
		t.Fatalf("RoleForClaims = %v, want first rule's admin", roleName(got))
	}
}

func TestRoleForClaimsBareStar(t *testing.T) {
	doc := `
roles:
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      match: "*"
      role: reader
`
	p, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := p.RoleForClaims(map[string][]string{"groups": {"anything"}}); got == nil || got.Name != "reader" {
		t.Fatalf("RoleForClaims = %v, want reader for bare *", roleName(got))
	}
	// A claim present but with no values must not match bare "*".
	if got := p.RoleForClaims(map[string][]string{"groups": {}}); got != nil {
		t.Fatalf("RoleForClaims = %v, want deny for empty value list", roleName(got))
	}
}

func TestRoleForClaimsNoRulesDeniesAll(t *testing.T) {
	p, err := Parse([]byte(`
roles:
  - name: reader
    rules: ["+@read"]
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := p.RoleForClaims(map[string][]string{"groups": {"admins"}}); got != nil {
		t.Fatalf("RoleForClaims = %v, want deny when no oauth.rules are configured", roleName(got))
	}
}

func TestOAuthRulesParseErrors(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"unknown target role", `
roles:
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      match: "admins"
      role: ghost`},
		{"empty claim", `
roles:
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - match: "admins"
      role: reader`},
		{"empty match", `
roles:
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      role: reader`},
		{"empty role", `
roles:
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      match: "admins"`},
		{"middle wildcard", `
roles:
  - name: reader
    rules: ["+@read"]
oauth:
  rules:
    - claim: groups
      match: "a*b"
      role: reader`},
	}
	for _, tc := range cases {
		if _, err := Parse([]byte(tc.doc)); err == nil {
			t.Errorf("%s: expected a parse error, got none", tc.name)
		}
	}
}

func TestResolveOAuthToken(t *testing.T) {
	p, err := Parse([]byte(oauthRulesYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	store := NewStore(p, log.NewNoOpLogger())

	// Success: the sub claim becomes the session identity.
	session, subject := store.ResolveOAuthToken(func() (map[string][]string, error) {
		return map[string][]string{"sub": {"alice@tellstone.io"}, "groups": {"admins"}}, nil
	})
	if session == nil || session.RoleName != "admin" || session.Username != "alice@tellstone.io" {
		t.Fatalf("ResolveOAuthToken = %v/%q, want admin as alice@tellstone.io", session, subject)
	}
	if subject != "alice@tellstone.io" {
		t.Fatalf("subject = %q, want alice@tellstone.io", subject)
	}

	// A missing sub claim falls back to the default identity.
	session, subject = store.ResolveOAuthToken(func() (map[string][]string, error) {
		return map[string][]string{"email": {"ci@tellstone.io"}}, nil
	})
	if session == nil || session.RoleName != "reader" || session.Username != "default" || subject != "default" {
		t.Fatalf("ResolveOAuthToken = %v/%q, want reader as default", session, subject)
	}

	// A verification failure denies (fail-closed).
	session, subject = store.ResolveOAuthToken(func() (map[string][]string, error) {
		return nil, errors.New("verify failed")
	})
	if session != nil || subject != "" {
		t.Fatalf("ResolveOAuthToken with failing verify = %v/%q, want deny", session, subject)
	}

	// Claims matching no oauth.rules entry deny (fail-closed).
	session, subject = store.ResolveOAuthToken(func() (map[string][]string, error) {
		return map[string][]string{"sub": {"mallory@example.com"}, "groups": {"everyone"}}, nil
	})
	if session != nil || subject != "" {
		t.Fatalf("ResolveOAuthToken with unmapped claims = %v/%q, want deny", session, subject)
	}

	// A nil store denies without panicking.
	var nilStore *Store
	session, subject = nilStore.ResolveOAuthToken(func() (map[string][]string, error) {
		return map[string][]string{"groups": {"admins"}}, nil
	})
	if session != nil || subject != "" {
		t.Fatalf("nil store ResolveOAuthToken = %v/%q, want deny", session, subject)
	}
}
