/*
Package rbac
Tellstone Role-Based Access Control
File: config.go
Description: Loads an RBAC policy from a YAML or JSON file into a PolicyStore. YAML is a
superset of JSON, so gopkg.in/yaml.v3 parses both formats — no second parser needed. Unknown
roles, malformed rules, and duplicate names fail the load so a bad file can never half-apply.

Authors:

	Maximilian Hagen
*/
package rbac

import (
	"fmt"
	"os"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// Config schema:
//
//	roles:
//	  - name: reader
//	    rules: ["+@read", "~*"]
//	users:
//	  - name: service-a
//	    password: "$2a$10$..."  # bcrypt hash; required unless nopass: true
//	    role: reader
//	default_role: reader / optional fallback for users without an explicit role
//	oauth:
//	  rules:                    # claim-to-role mapping for OAuth tokens, first match wins
//	    - claim: groups         # OIDC claim name to match against
//	      match: "admins"       # exact value, or leading/trailing "*" glob
//	      role: admin           # target role; must be defined above
type fileConfig struct {
	Roles []struct {
		Name  string   `yaml:"name"`
		Rules []string `yaml:"rules"`
	} `yaml:"roles"`
	Users []struct {
		Name     string `yaml:"name"`
		Password string `yaml:"password"`
		Role     string `yaml:"role"`
		Nopass   bool   `yaml:"nopass"`
	} `yaml:"users"`
	DefaultRole string `yaml:"default_role"`
	OAuth       struct {
		Rules []struct {
			Claim string `yaml:"claim"`
			Match string `yaml:"match"`
			Role  string `yaml:"role"`
		} `yaml:"rules"`
	} `yaml:"oauth"`
}

// LoadFile reads and parses the RBAC policy file at a path. The file may be
// YAML or JSON. Used both at startup and on SIGHUP reload.
func LoadFile(path string) (*PolicyStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rbac: read config: %w", err)
	}
	return Parse(data)
}

// Parse builds a PolicyStore from YAML or JSON bytes, validating every
// reference: duplicate names, unknown roles, and malformed rules are errors.
func Parse(data []byte) (*PolicyStore, error) {
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("rbac: parse config: %w", err)
	}
	return fc.build()
}

func (fc *fileConfig) build() (*PolicyStore, error) {
	p := &PolicyStore{
		Roles: make(map[string]*Role, len(fc.Roles)),
		Users: make(map[string]*User, len(fc.Users)),
	}
	for _, r := range fc.Roles {
		if r.Name == "" {
			return nil, fmt.Errorf("rbac: role without a name")
		}
		if _, dup := p.Roles[r.Name]; dup {
			return nil, fmt.Errorf("rbac: duplicate role %q", r.Name)
		}
		role, err := ParseRole(r.Name, r.Rules...)
		if err != nil {
			return nil, err
		}
		p.Roles[r.Name] = role
	}
	for _, u := range fc.Users {
		if u.Name == "" {
			return nil, fmt.Errorf("rbac: user without a name")
		}
		if _, dup := p.Users[u.Name]; dup {
			return nil, fmt.Errorf("rbac: duplicate user %q", u.Name)
		}
		if u.Role != "" {
			if _, ok := p.Roles[u.Role]; !ok {
				return nil, fmt.Errorf("rbac: user %q references unknown role %q", u.Name, u.Role)
			}
		}
		if u.Nopass && u.Password != "" {
			return nil, fmt.Errorf("rbac: user %q sets both password and nopass", u.Name)
		}
		var hash []byte
		if !u.Nopass {
			// A missing password silently turns a "passwordless" user into a
			// nopass one without the operator ever writing nopass — reject it
			// so the policy file says what it means.
			if u.Password == "" {
				return nil, fmt.Errorf("rbac: user %q has no password; set nopass: true for a passwordless user", u.Name)
			}
			if !isBcryptHash(u.Password) {
				return nil, fmt.Errorf("rbac: user %q password is not a valid bcrypt hash", u.Name)
			}
			hash = []byte(u.Password)
		}
		p.Users[u.Name] = &User{Role: u.Role, PasswordHash: hash}
	}
	if fc.DefaultRole != "" {
		r, ok := p.Roles[fc.DefaultRole]
		if !ok {
			return nil, fmt.Errorf("rbac: default_role %q is not a defined role", fc.DefaultRole)
		}
		p.Default = r
	}
	for _, r := range fc.OAuth.Rules {
		if r.Claim == "" {
			return nil, fmt.Errorf("rbac: oauth rule without a claim")
		}
		if r.Match == "" {
			return nil, fmt.Errorf("rbac: oauth rule for claim %q without a match pattern", r.Claim)
		}
		if r.Role == "" {
			return nil, fmt.Errorf("rbac: oauth rule for claim %q without a role", r.Claim)
		}
		role, ok := p.Roles[r.Role]
		if !ok {
			return nil, fmt.Errorf("rbac: oauth rule references unknown role %q", r.Role)
		}
		exact, prefix, suffix, err := parseMatch(r.Match)
		if err != nil {
			return nil, err
		}
		p.OAuthRules = append(p.OAuthRules, oauthRule{
			claim: r.Claim, exact: exact, prefix: prefix, suffix: suffix, role: role,
		})
	}
	return p, nil
}

// isBcryptHash sanity-checks that a config password looks like a bcrypt hash.
// bcrypt hashes are "$2[a|b|y]$<cost>$" plus 53 base64 chars; the length and
// prefix checks are cheap and catch pasted plaintext or truncated hashes. The
// full verification still happens at the first AUTH.
func isBcryptHash(h string) bool {
	if len(h) < 59 {
		return false
	}
	_, err := bcrypt.Cost([]byte(h))
	return err == nil
}
