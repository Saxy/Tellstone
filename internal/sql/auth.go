package sql

import (
	"context"
	"fmt"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
	"github.com/Saxy/Tellstone/internal/rbac"
	"golang.org/x/crypto/bcrypt"
)

// pgError is a protocol error carrying a SQLSTATE code (PG errcodes appendix A).
// Plain Go errors become the generic 58030 io_error; these are mapped 1:1.
type pgError struct {
	code string
	msg  string
}

func (e *pgError) Error() string { return e.msg }

// Standard errcodes used by the frontend.
const (
	errInvalidAuthorizationSpec = "28000"
	errInvalidPassword          = "28P01"
	errInsufficientPrivilege    = "42501"
	errIoError                  = "58030"
	errFeatureNotSupported      = "0A000"
	errSyntax                   = "42601"
	errUndefinedTable           = "42P01"
	errDuplicateKey             = "23505"
	errProtocolViolation        = "08P01"
	errAdminShutdown            = "57P01"
	errTooManyConnections       = "53300"
	errInvalidSQLStatement      = "26000"
	errInvalidCursorName        = "26P01"
)

// authState is the verified identity attached to a connection.
type authState struct {
	// session is non-nil when the connection authenticated against an RBAC
	// policy. Single-password and trust modes leave it nil.
	session *rbac.SessionContext
	// user is the identity used for audit and ACL LOG entries ("default" in
	// single-password mode, the mapped claim role for OAuth tokens).
	user string
}

// authenticate runs the startup auth exchange. It sends the cleartext
// password request unless no credential is configured (trust), then verifies
// the reply against the same bcrypt hashes the binary frontend uses, or routes
// a JWT-shaped password through the OAuth provider. The stored credential is
// always the bcrypt hash -- SCRAM/md5 verifiers cannot be derived from bcrypt,
// so those mechanisms are not offered (see ADR-012).
func (s *Server) authenticate(c *pgConn, params map[string]string) (*authState, *pgError) {
	user := params["user"]
	if user == "" {
		if s.policy == nil && s.requirePassHash == nil {
			user = "default"
		}
		return nil, &pgError{code: errInvalidAuthorizationSpec, msg: `no username given in startup packet`}
	}
	// Trust: no password or policy configured. Every connection is accepted.
	if s.requirePassHash == nil && s.policy == nil {
		return &authState{user: user}, nil
	}

	// Cleartext passwords may only cross an encrypted channel. Without TLS the
	// login is refused before the password leaves the client. SCRAM/md5 would
	// need a non-bcrypt verifier, so there is no plaintext-safe alternative.
	if c.tlsConn == nil {
		return nil, &pgError{
			code: errProtocolViolation,
			msg:  "cleartext password authentication requires a TLS connection (enable --pg-tls)",
		}
	}

	if err := sendMessage(c, msgAuthentication, frameAuthCleartext()); err != nil {
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	typ, payload, err := readFrame(c.r)
	if err != nil {
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	if typ != msgPassword {
		return nil, &pgError{code: errProtocolViolation, msg: fmt.Sprintf("expected PasswordMessage, got %q", typ)}
	}
	password := payload
	// PasswordMessage bodies are NUL-terminated strings; the terminator is not
	// part of the secret.
	if n := len(password); n > 0 && password[n-1] == 0 {
		password = password[:n-1]
	}

	// OAuth bearer path: a JWT-shaped password is a token, not a secret. It is
	// verified by the provider and mapped to a role from the claims before any
	// username lookup, mirroring the binary frontend.
	if s.oauth != nil && oauth.IsJWT(password) {
		verify := func() (map[string][]string, error) {
			ctx, cancel := context.WithTimeout(context.Background(), oauth.VerifyTimeout)
			defer cancel()
			return s.oauth.Verify(ctx, password)
		}
		session, name := s.policy.ResolveOAuthToken(verify)
		if session == nil {
			s.failAuth(c, user, "invalid oauth token")
			return nil, &pgError{code: errInvalidPassword, msg: fmt.Sprintf(`password authentication failed for user %q`, user)}
		}
		s.successAuth(c, name)
		return &authState{session: session, user: name}, nil
	}

	// RBAC password or single-password mode.
	var passHash []byte
	var session *rbac.SessionContext
	var name string
	if s.policy != nil {
		p := s.policy.Load()
		if p == nil {
			return nil, &pgError{code: errIoError, msg: "rbac policy not loaded"}
		}
		name = user
		u := p.UserFor(name)
		if u == nil {
			s.failAuth(c, name, "unknown user")
			return nil, &pgError{code: errInvalidPassword, msg: fmt.Sprintf(`password authentication failed for user %q`, user)}
		}
		passHash = u.PasswordHash
		session = rbac.NewSessionContext(name, p.RoleFor(name))
	} else {
		if user != "default" && user != "" {
			s.failAuth(c, user, "unknown user")
			return nil, &pgError{code: errInvalidPassword, msg: fmt.Sprintf(`password authentication failed for user %q`, user)}
		}
		name = "default"
		passHash = s.requirePassHash
	}
	ok := passHash == nil || bcrypt.CompareHashAndPassword(passHash, password) == nil
	if !ok {
		s.failAuth(c, name, "invalid password")
		return nil, &pgError{code: errInvalidPassword, msg: fmt.Sprintf(`password authentication failed for user %q`, user)}
	}
	s.successAuth(c, name)
	return &authState{session: session, user: name}, nil
}

func (s *Server) successAuth(c *pgConn, user string) {
	c.user = user
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "sql: client authenticated", log.String("user", user), log.String("remote_addr", c.remoteAddr))
	}
}

func (s *Server) failAuth(c *pgConn, user, reason string) {
	if s.policy != nil {
		s.policy.LogAuthFailure(user, c.remoteAddr, reason)
	}
	if s.audit != nil {
		s.audit.Record(audit.EventAuthFailure, "client authentication failed",
			log.String("user", user),
			log.String("remote_addr", c.remoteAddr),
			log.String("reason", reason),
			log.String("protocol", "sql"),
		)
	}
}
