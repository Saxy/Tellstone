/*
Package rbac
Tellstone Role-Based Access Control Tests
File: config_test.go
Description: Verifies the YAML/JSON policy loader: valid YAML and JSON parse to equivalent
policies, and malformed files fail with a load error instead of half-applying.
*/
package rbac

import (
	"os"
	"path/filepath"
	"testing"
)

const validYAML = `
roles:
  - name: reader
    rules: ["+@read", "~*"]
  - name: writer
    rules: ["+@readwrite", "~cache:*"]
users:
  - name: default
    nopass: true
    role: reader
  - name: service-a
    password: "$2a$10$4gEVZvJMaugLbxib9OnEzOTY2D6a50ja32pnyDktdigdoMGeJk/sm"
    role: writer
default_role: reader
`

func TestParseYAML(t *testing.T) {
	p, err := Parse([]byte(validYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(p.Roles) != 2 || len(p.Users) != 2 {
		t.Fatalf("expected 2 roles and 2 users, got %d/%d", len(p.Roles), len(p.Users))
	}
	if p.Default == nil || p.Default.Name != "reader" {
		t.Fatal("default_role must resolve to the reader role")
	}
	u := p.UserFor("service-a")
	if u == nil || u.Role != "writer" || string(u.PasswordHash) == "" {
		t.Fatalf("user record wrong: %+v", u)
	}
	def := p.UserFor("default")
	if def == nil || len(def.PasswordHash) != 0 {
		t.Fatal("nopass user must have an empty password hash")
	}
}

func TestParseJSON(t *testing.T) {
	jsonDoc := `{
		"roles": [{"name": "reader", "rules": ["+@read", "~*"]}],
		"users": [{"name": "default", "nopass": true, "role": "reader"}],
		"default_role": "reader"
	}`
	p, err := Parse([]byte(jsonDoc))
	if err != nil {
		t.Fatalf("Parse JSON: %v", err)
	}
	if p.RoleFor("default") == nil || !p.NoPassDefault() {
		t.Fatal("JSON config must produce the same policy as YAML")
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"malformed yaml", "roles: [unclosed"},
		{"duplicate role", "roles: [{name: r, rules: [\"+GET\"]}, {name: r, rules: [\"+GET\"]}]"},
		{"duplicate user", "users: [{name: u, role: r}, {name: u, role: r}]"},
		{"unknown role ref", "users: [{name: u, role: ghost}]"},
		{"unknown default role", "default_role: ghost"},
		{"bad rule", "roles: [{name: r, rules: [\"+BOGUS\"]}]"},
		{"password and nopass", "users: [{name: u, role: r, password: \"x\", nopass: true}]"},
		{"no password and no nopass", "users: [{name: u, role: r}]"},
		{"password not a bcrypt hash", "users: [{name: u, role: r, password: \"hunter2\"}]"},
		{"truncated bcrypt hash", "users: [{name: u, role: r, password: \"$2a$10$abcdefghijklmnopqrstuv\"}]"},
	}
	for _, tc := range cases {
		if _, err := Parse([]byte(tc.doc)); err == nil {
			t.Errorf("%s: expected a parse error, got none", tc.name)
		}
	}
}

func TestLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(validYAML), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	p, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if p.RoleFor("service-a") == nil {
		t.Fatal("loaded policy must resolve roles")
	}
	if _, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing file must error")
	}
}
