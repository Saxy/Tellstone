package network

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/command"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
	"golang.org/x/crypto/bcrypt"
)

// TestRoleCodecRoundTrip verifies EncodeRoleArgs/DecodeRoleArgs recover the
// original token list, including empty lists and multi-byte UTF-8 tokens.
func TestRoleCodecRoundTrip(t *testing.T) {
	cases := [][][]byte{
		{},
		{[]byte("admin")},
		{[]byte("admin"), []byte("+get"), []byte("~*")},
		{[]byte("bob"), []byte("opérateur")},
	}
	for _, args := range cases {
		payload, ok := EncodeRoleArgs(args)
		if !ok {
			t.Fatalf("EncodeRoleArgs(%q) reported overflow", args)
		}
		got, ok := DecodeRoleArgs(payload, nil)
		if !ok {
			t.Fatalf("DecodeRoleArgs(%q) reported malformed", args)
		}
		if len(got) != len(args) {
			t.Fatalf("arg count: got %d want %d", len(got), len(args))
		}
		for i := range args {
			if !bytes.Equal(got[i], args[i]) {
				t.Fatalf("arg %d: got %q want %q", i, got[i], args[i])
			}
		}
	}
}

// TestRoleCodecOverflow verifies that a token over the 64 KiB length-prefix
// limit is rejected by the encoder instead of being silently truncated.
func TestRoleCodecOverflow(t *testing.T) {
	big := make([]byte, 65536)
	if _, ok := EncodeRoleArgs([][]byte{big}); ok {
		t.Fatal("expected args encoder to reject a >64 KiB token")
	}
	entry := RoleListEntry{Name: "r", Namespaces: [][]byte{big}}
	if _, ok := EncodeRoleListResponse([]RoleListEntry{entry}); ok {
		t.Fatal("expected list encoder to reject a >64 KiB namespace")
	}
}

// TestRoleCodecCountOverflow verifies that an argument or entry count that
// cannot fit the 16-bit count field is rejected instead of silently wrapping.
func TestRoleCodecCountOverflow(t *testing.T) {
	tooMany := make([][]byte, 65536)
	if _, ok := EncodeRoleArgs(tooMany); ok {
		t.Fatal("expected args encoder to reject a >64 KiB argument count")
	}
	entries := make([]RoleListEntry, 65536)
	if _, ok := EncodeRoleListResponse(entries); ok {
		t.Fatal("expected list encoder to reject a >64 KiB entry count")
	}
}

func TestRoleCodecMalformed(t *testing.T) {
	if _, ok := DecodeRoleArgs([]byte{0, 2, 0, 5}, nil); ok {
		t.Fatal("expected truncated payload to be rejected")
	}
	if _, ok := DecodeRoleArgs([]byte{0, 1, 0, 2, 'a'}, nil); ok {
		t.Fatal("expected trailing short token to be rejected")
	}
	if _, ok := DecodeRoleArgs([]byte{0, 1, 0, 2, 'a', 'b', 'c'}, nil); ok {
		t.Fatal("expected trailing garbage to be rejected")
	}
}

func TestRoleGetUserCodec(t *testing.T) {
	for _, u := range []RoleUser{
		{Role: "admin", HasPass: true},
		{Role: "", HasPass: false},
	} {
		decoded, ok := DecodeRoleGetUserResponse(EncodeRoleGetUserResponse(u))
		if !ok {
			t.Fatalf("decode of %+v failed", u)
		}
		if decoded != u {
			t.Fatalf("got %+v want %+v", decoded, u)
		}
	}
	if _, ok := DecodeRoleGetUserResponse([]byte{0, 4, 'a'}); ok {
		t.Fatal("expected truncated GETUSER response to be rejected")
	}
}

// encodeEntries packs entries, failing the test on encoder rejection.
func encodeEntries(t *testing.T, entries []RoleListEntry) []byte {
	t.Helper()
	payload, ok := EncodeRoleListResponse(entries)
	if !ok {
		t.Fatal("encode of LIST response failed")
	}
	return payload
}

func TestRoleListCodec(t *testing.T) {
	entries := []RoleListEntry{
		{Name: "admin", Commands: []string{"GET", "SET"}, Namespaces: [][]byte{}},
		{Name: "operator", Commands: []string{"GET"}, Namespaces: [][]byte{[]byte("users:")}},
	}
	decoded, ok := DecodeRoleListResponse(encodeEntries(t, entries))
	if !ok {
		t.Fatal("decode of LIST response failed")
	}
	if len(decoded) != len(entries) {
		t.Fatalf("entry count: got %d want %d", len(decoded), len(entries))
	}
	for i, e := range decoded {
		if e.Name != entries[i].Name {
			t.Fatalf("entry %d name: got %q want %q", i, e.Name, entries[i].Name)
		}
		if len(e.Commands) != len(entries[i].Commands) {
			t.Fatalf("entry %d command count mismatch", i)
		}
		if len(e.Namespaces) != len(entries[i].Namespaces) {
			t.Fatalf("entry %d namespace count mismatch", i)
		}
		for j := range e.Commands {
			if e.Commands[j] != entries[i].Commands[j] {
				t.Fatalf("entry %d command %d: got %q want %q", i, j, e.Commands[j], entries[i].Commands[j])
			}
		}
	}
}

// rbacNetworkPolicy builds the policy seeded into the RBAC test server.
func rbacNetworkPolicy(t *testing.T) *rbac.PolicyStore {
	t.Helper()
	admin, err := rbac.ParseRole("admin", "+@all", "~*")
	if err != nil {
		t.Fatalf("parse admin role: %v", err)
	}
	limited, err := rbac.ParseRole("limited", "+get", "~*")
	if err != nil {
		t.Fatalf("parse limited role: %v", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("sekret"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return &rbac.PolicyStore{
		Roles: map[string]*rbac.Role{"admin": admin, "limited": limited},
		Users: map[string]*rbac.User{
			"admin":   {Role: "admin", PasswordHash: hash},
			"limited": {Role: "limited"},
		},
		Default: admin,
	}
}

// rbacTestHandler mirrors server.networkHandler for the ROLE ops so the
// network layer's auth gating and wire codec are exercised end-to-end.
func rbacTestHandler(store *rbac.Store) func(msg *Message, c *command.Ctx) ([]byte, MessageType, error) {
	return func(msg *Message, c *command.Ctx) ([]byte, MessageType, error) {
		switch msg.Op {
		case OpGet:
			return ResponseOK, MsgResponse, nil
		case OpSet, OpDelete:
			return ResponseOK, MsgResponse, nil
		case OpRoleCreate:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) < 2 {
				return []byte("invalid ROLE CREATE arguments"), MsgError, nil
			}
			rules := make([]string, 0, len(args)-1)
			for _, r := range args[1:] {
				rules = append(rules, string(r))
			}
			if err := store.CreateRole(string(args[0]), rules); err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			return ResponseOK, MsgResponse, nil
		case OpRoleSetUser:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) < 2 {
				return []byte("invalid ROLE SETUSER arguments"), MsgError, nil
			}
			if len(args) == 2 {
				return []byte("ROLE SETUSER requires a '>password' or 'nopass' option"), MsgError, nil
			}
			hash, err := rbac.PasswordFromOpts(args[2:])
			if err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			if err := store.SetUser(string(args[0]), string(args[1]), hash); err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			return ResponseOK, MsgResponse, nil
		case OpRoleDelUser:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) != 1 {
				return []byte("invalid ROLE DELUSER arguments"), MsgError, nil
			}
			if err := store.DelUser(string(args[0])); err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			return ResponseOK, MsgResponse, nil
		case OpRoleDelete:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) != 1 {
				return []byte("invalid ROLE DELETE arguments"), MsgError, nil
			}
			if err := store.DeleteRole(string(args[0])); err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			return ResponseOK, MsgResponse, nil
		case OpRoleList:
			p := store.Load()
			entries := make([]RoleListEntry, 0, len(p.Roles))
			for name, r := range p.Roles {
				e := RoleListEntry{Name: name, Commands: r.GrantedCommands()}
				for _, ns := range r.Namespaces {
					e.Namespaces = append(e.Namespaces, append([]byte(nil), ns...))
				}
				entries = append(entries, e)
			}
			payload, ok := EncodeRoleListResponse(entries)
			if !ok {
				return []byte("role rule exceeds the 64 KiB wire limit"), MsgError, nil
			}
			return payload, MsgResponse, nil
		case OpRoleGetUser:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) != 1 {
				return []byte("invalid ROLE GETUSER arguments"), MsgError, nil
			}
			p := store.Load()
			u := p.UserFor(string(args[0]))
			if u == nil {
				return []byte("user does not exist"), MsgError, nil
			}
			return EncodeRoleGetUserResponse(RoleUser{Role: u.Role, HasPass: len(u.PasswordHash) > 0}), MsgResponse, nil
		default:
			return ResponseNotFound, MsgError, nil
		}
	}
}

func startRBACNetworkServer(t *testing.T) (addr string, store *rbac.Store) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr = l.Addr().String()
	_ = l.Close()
	store = rbac.NewStore(rbacNetworkPolicy(t), log.NewNoOpLogger())
	srv := NewServer(addr, 0, nil, rbacTestHandler(store), log.NewNoOpLogger(), nil, "", store, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	if err := waitForServer(addr, 2*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}
	return addr, store
}

// TestServerRBACAuthGating verifies the binary protocol denies data ops before
// AUTH, authenticates per-user, and gates ops on the session's role.
func TestServerRBACAuthGating(t *testing.T) {
	addr, _ := startRBACNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// GET before auth must be rejected.
	if resp := sendAndRecv(t, conn, MsgRequest, []byte{byte(OpGet), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 'k'}); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for GET before auth, got %v", resp.Type)
	}
	// Wrong password.
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("admin", "wrong")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for wrong password, got %v", resp.Type)
	}
	// Unknown user.
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("ghost", "x")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for unknown user, got %v", resp.Type)
	}
	// Correct credentials.
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("admin", "sekret")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk, got %v", resp.Type)
	}
	// ROLE CREATE now permitted (admin role).
	payload, err := roleRequestPayload(OpRoleCreate, [][]byte{[]byte("operator"), []byte("+get"), []byte("~*")})
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseOK) {
		t.Fatalf("expected ROLE CREATE OK, got %q type %v", resp.Value, resp.Type)
	}
}

// TestServerRBACAuthorizationDenial verifies that a limited user (GET only)
// gets ResponseNotAuthorized for ROLE and SET ops but may GET.
func TestServerRBACAuthorizationDenial(t *testing.T) {
	addr, _ := startRBACNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("limited", "anything")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk for nopass user, got %v", resp.Type)
	}
	payload, err := roleRequestPayload(OpRoleCreate, [][]byte{[]byte("operator"), []byte("+get")})
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for ROLE, got %q", resp.Value)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, []byte{byte(OpSet), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 'k', 'v'}); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for SET, got %q", resp.Value)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, []byte{byte(OpGet), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 'k'}); !bytes.Equal(resp.Value, ResponseOK) {
		t.Fatalf("expected OK for GET, got %q", resp.Value)
	}
}

// TestServerRBACMetrics verifies the authorization counters move in the binary
// protocol: failed AUTH bumps the auth-failure counter, denied ops bump the
// denial counter, and permitted ops bump the per-role executed counter.
func TestServerRBACMetrics(t *testing.T) {
	addr, store := startRBACNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Failed AUTH: wrong password for an existing user, then an unknown user.
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("admin", "wrong")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for wrong password, got %v", resp.Type)
	}
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("ghost", "x")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for unknown user, got %v", resp.Type)
	}
	// Successful AUTH as the get-only user.
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("limited", "anything")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk for nopass user, got %v", resp.Type)
	}
	// Permitted GET counts one executed command for the limited role.
	if resp := sendAndRecv(t, conn, MsgRequest, []byte{byte(OpGet), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 'k'}); !bytes.Equal(resp.Value, ResponseOK) {
		t.Fatalf("expected OK for GET, got %q", resp.Value)
	}
	// Denied SET bumps the denial counter.
	if resp := sendAndRecv(t, conn, MsgRequest, []byte{byte(OpSet), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 'k', 'v'}); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for SET, got %q", resp.Value)
	}
	// Denied ROLE CREATE bumps the denial counter again.
	payload, err := roleRequestPayload(OpRoleCreate, [][]byte{[]byte("operator"), []byte("+get")})
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for ROLE, got %q", resp.Value)
	}

	if got := store.AuthFailures(); got != 2 {
		t.Fatalf("AuthFailures = %d, want 2", got)
	}
	if got := store.DeniedCommands(); got != 2 {
		t.Fatalf("DeniedCommands = %d, want 2", got)
	}
	counts := store.RoleCommandCounts()
	if counts["limited"] != 1 {
		t.Fatalf("limited command count = %d, want 1", counts["limited"])
	}
	if counts["admin"] != 0 {
		t.Fatalf("admin command count = %d, want 0", counts["admin"])
	}
}

// TestClientRoleMethods exercises the client API against a live RBAC server.
func TestClientRoleMethods(t *testing.T) {
	addr, _ := startRBACNetworkServer(t)

	client, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	scratch := make([]byte, 4096)

	if err := client.AuthUser("admin", "sekret", scratch); err != nil {
		t.Fatalf("AuthUser: %v", err)
	}
	if err := client.RoleCreate("operator", []string{"+get", "~*"}, scratch); err != nil {
		t.Fatalf("RoleCreate: %v", err)
	}
	if err := client.RoleSetUser("bob", "operator", [][]byte{[]byte(">pw")}, scratch); err != nil {
		t.Fatalf("RoleSetUser: %v", err)
	}
	u, err := client.RoleGetUser("bob", scratch)
	if err != nil {
		t.Fatalf("RoleGetUser: %v", err)
	}
	if u.Role != "operator" || !u.HasPass {
		t.Fatalf("RoleGetUser: got %+v want operator with password", u)
	}
	entries, err := client.RoleList(scratch)
	if err != nil {
		t.Fatalf("RoleList: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("RoleList: got %d entries want 3 (admin, limited, operator)", len(entries))
	}
	// Bad credentials surface as an error, not a hang.
	badClient, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer badClient.Close()
	if err := badClient.AuthUser("admin", "nope", scratch); err == nil {
		t.Fatal("expected AuthUser to fail with wrong password")
	}
}

// TestClientRoleMethodsGating proves that a user created at runtime and bound
// to a limited role is gated exactly like a seeded one: GET passes, SET and
// non-matching namespaces are denied.
func TestClientRoleMethodsGating(t *testing.T) {
	addr, _ := startRBACNetworkServer(t)

	admin, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer admin.Close()
	scratch := make([]byte, 4096)
	if err := admin.AuthUser("admin", "sekret", scratch); err != nil {
		t.Fatalf("AuthUser: %v", err)
	}
	if err := admin.RoleCreate("limited2", []string{"+get", "~users:*"}, scratch); err != nil {
		t.Fatalf("RoleCreate: %v", err)
	}
	if err := admin.RoleSetUser("carol", "limited2", [][]byte{[]byte("nopass")}, scratch); err != nil {
		t.Fatalf("RoleSetUser: %v", err)
	}

	carol, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer carol.Close()
	if err := carol.AuthUser("carol", "anything", scratch); err != nil {
		t.Fatalf("AuthUser carol: %v", err)
	}
	// GET on a matching key passes through to the handler.
	if _, err := carol.Get([]byte("users:1"), scratch); err != nil {
		t.Fatalf("GET users:1 as carol: %v", err)
	}
	// SET is not in the role.
	if _, err := carol.Set([]byte("users:1"), []byte("x"), 0, scratch); err == nil {
		t.Fatal("expected SET to be denied for carol")
	}
	// Key outside the namespace whitelist.
	if _, err := carol.Get([]byte("accounts:1"), scratch); err == nil {
		t.Fatal("expected GET accounts:1 to be denied for carol")
	}
}
