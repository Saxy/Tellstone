package network

import (
	"bytes"
	"context"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/command"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// TestACLListCodec verifies EncodeACLListResponse/DecodeACLListResponse recover
// the original user list, including users with no explicit role and empty
// command or namespace lists.
func TestACLListCodec(t *testing.T) {
	users := []ACLUser{
		{Username: "admin", Role: "admin", HasPass: true, Commands: []string{"GET", "SET"}},
		{Username: "bob", Role: "", HasPass: false, Commands: nil, Namespaces: [][]byte{[]byte("users:")}},
	}
	payload, ok := EncodeACLListResponse(users)
	if !ok {
		t.Fatal("encode of ACL LIST response failed")
	}
	decoded, ok := DecodeACLListResponse(payload)
	if !ok {
		t.Fatal("decode of ACL LIST response failed")
	}
	if len(decoded) != len(users) {
		t.Fatalf("user count: got %d want %d", len(decoded), len(users))
	}
	for i, u := range decoded {
		if u.Username != users[i].Username || u.Role != users[i].Role || u.HasPass != users[i].HasPass {
			t.Fatalf("user %d: got %+v want %+v", i, u, users[i])
		}
		if len(u.Commands) != len(users[i].Commands) {
			t.Fatalf("user %d command count mismatch", i)
		}
		if len(u.Namespaces) != len(users[i].Namespaces) {
			t.Fatalf("user %d namespace count mismatch", i)
		}
		for j := range u.Commands {
			if u.Commands[j] != users[i].Commands[j] {
				t.Fatalf("user %d command %d: got %q want %q", i, j, u.Commands[j], users[i].Commands[j])
			}
		}
	}
}

// TestACLListCodecOverflow verifies a token over the 64 KiB length-prefix limit
// is rejected by the encoder instead of being silently truncated.
func TestACLListCodecOverflow(t *testing.T) {
	big := make([]byte, 65536)
	user := ACLUser{Username: "u", Commands: []string{string(big)}}
	if _, ok := EncodeACLListResponse([]ACLUser{user}); ok {
		t.Fatal("expected list encoder to reject a >64 KiB command")
	}
	user = ACLUser{Username: "u", Namespaces: [][]byte{big}}
	if _, ok := EncodeACLListResponse([]ACLUser{user}); ok {
		t.Fatal("expected list encoder to reject a >64 KiB namespace")
	}
}

// TestACLListCodecMalformed verifies truncated and trailing-garbage payloads are
// rejected.
func TestACLListCodecMalformed(t *testing.T) {
	if _, ok := DecodeACLListResponse([]byte{0, 1}); ok {
		t.Fatal("expected truncated payload to be rejected")
	}
	// One user whose role length overruns the buffer.
	enc, _ := EncodeACLListResponse([]ACLUser{{Username: "u", Role: "admin"}})
	if _, ok := DecodeACLListResponse(enc[:len(enc)-2]); ok {
		t.Fatal("expected truncated user payload to be rejected")
	}
	// A valid payload with a trailing byte must be rejected: the decoder is
	// required to consume the payload exactly, not just the prefix it knows.
	trailing := append(append([]byte(nil), enc...), 0xFF)
	if _, ok := DecodeACLListResponse(trailing); ok {
		t.Fatal("expected trailing-garbage payload to be rejected")
	}
}

// TestACLLogCodec verifies EncodeACLLogResponse/DecodeACLLogResponse recover
// the original entries, including empty reasons and an empty log.
func TestACLLogCodec(t *testing.T) {
	entries := []AuthLogEntry{
		{Timestamp: "2026-08-03T18:30:29+02:00", Username: "admin", RemoteAddr: "127.0.0.1:1", Reason: "invalid password"},
		{Timestamp: "2026-08-03T18:30:30+02:00", Username: "ghost", RemoteAddr: "127.0.0.1:2", Reason: "unknown user"},
		{Timestamp: "2026-08-03T18:30:31+02:00", Username: "nopass", RemoteAddr: "127.0.0.1:3", Reason: ""},
	}
	payload, ok := EncodeACLLogResponse(entries)
	if !ok {
		t.Fatal("encode of ACL LOG response failed")
	}
	decoded, ok := DecodeACLLogResponse(payload)
	if !ok {
		t.Fatal("decode of ACL LOG response failed")
	}
	if len(decoded) != len(entries) {
		t.Fatalf("entry count: got %d want %d", len(decoded), len(entries))
	}
	for i, e := range decoded {
		if e != entries[i] {
			t.Fatalf("entry %d: got %+v want %+v", i, e, entries[i])
		}
	}
}

// TestACLLogCodecMalformed verifies truncated and trailing-garbage ACL LOG
// payloads are rejected.
func TestACLLogCodecMalformed(t *testing.T) {
	if _, ok := DecodeACLLogResponse([]byte{0, 1}); ok {
		t.Fatal("expected truncated payload to be rejected")
	}
	enc, _ := EncodeACLLogResponse([]AuthLogEntry{{Timestamp: "t", Username: "u"}})
	if _, ok := DecodeACLLogResponse(enc[:len(enc)-2]); ok {
		t.Fatal("expected truncated entry payload to be rejected")
	}
	trailing := append(append([]byte(nil), enc...), 0xFF)
	if _, ok := DecodeACLLogResponse(trailing); ok {
		t.Fatal("expected trailing-garbage payload to be rejected")
	}
}

// aclTestHandler mirrors server.networkHandler for the ACL ops so the network
// layer's auth gating and wire codec are exercised end-to-end.
func aclTestHandler(store *rbac.Store) func(msg *Message, c *command.Ctx) ([]byte, MessageType, error) {
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
		case OpACLSetUser:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) < 2 {
				return []byte("invalid ACL SETUSER arguments"), MsgError, nil
			}
			if len(args) == 2 {
				return []byte("ACL SETUSER requires a '>password' or 'nopass' option"), MsgError, nil
			}
			hash, err := rbac.PasswordFromOpts(args[2:])
			if err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			if err := store.SetUser(string(args[0]), string(args[1]), hash); err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			return ResponseOK, MsgResponse, nil
		case OpACLDelUser:
			args, ok := DecodeRoleArgs(msg.Value, nil)
			if !ok || len(args) != 1 {
				return []byte("invalid ACL DELUSER arguments"), MsgError, nil
			}
			if err := store.DelUser(string(args[0])); err != nil {
				return []byte(err.Error()), MsgError, nil
			}
			return ResponseOK, MsgResponse, nil
		case OpACLList:
			p := store.Load()
			users := make([]ACLUser, 0, len(p.Users))
			for name, u := range p.Users {
				e := ACLUser{Username: name, Role: u.Role, HasPass: len(u.PasswordHash) > 0}
				if r := p.RoleFor(name); r != nil {
					e.Commands = r.GrantedCommands()
					for _, ns := range r.Namespaces {
						e.Namespaces = append(e.Namespaces, append([]byte(nil), ns...))
					}
				}
				users = append(users, e)
			}
			sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })
			payload, ok := EncodeACLListResponse(users)
			if !ok {
				return []byte("acl rule exceeds the 64 KiB wire limit"), MsgError, nil
			}
			return payload, MsgResponse, nil
		case OpACLLog:
			src := store.AuthLog()
			entries := make([]AuthLogEntry, 0, len(src))
			for _, e := range src {
				entries = append(entries, AuthLogEntry{
					Timestamp:  e.Timestamp.Format(time.RFC3339),
					Username:   e.Username,
					RemoteAddr: e.RemoteAddr,
					Reason:     e.Reason,
				})
			}
			payload, ok := EncodeACLLogResponse(entries)
			if !ok {
				return []byte("acl log exceeds the 64 KiB wire limit"), MsgError, nil
			}
			return payload, MsgResponse, nil
		default:
			return ResponseNotFound, MsgError, nil
		}
	}
}

func startACLNetworkServer(t *testing.T) (addr string, store *rbac.Store) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr = l.Addr().String()
	_ = l.Close()
	store = rbac.NewStore(rbacNetworkPolicy(t), log.NewNoOpLogger())
	srv := NewServer(addr, 0, nil, aclTestHandler(store), log.NewNoOpLogger(), nil, "", store, nil, newNoOpAudit())
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

// TestServerACLAuthGating verifies the binary protocol lets an admin manage
// ACL users end-to-end: SETUSER creates a bound user, LIST reflects it, and
// DELUSER removes it.
func TestServerACLAuthGating(t *testing.T) {
	addr, store := startACLNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// ACL ops before auth are rejected.
	payload, err := roleRequestPayload(OpACLList, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for ACL before auth, got %v", resp.Type)
	}
	// Authenticate as admin.
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("admin", "sekret")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk, got %v", resp.Type)
	}
	// Seed the role the user will be bound to.
	if err := store.CreateRole("operator", []string{"+get", "~users:*"}); err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	// ACL SETUSER bob.
	payload, err = roleRequestPayload(OpACLSetUser, [][]byte{[]byte("bob"), []byte("operator"), []byte(">hunter2")})
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseOK) {
		t.Fatalf("expected ACL SETUSER OK, got %q type %v", resp.Value, resp.Type)
	}
	if u := store.Load().UserFor("bob"); u == nil || u.Role != "operator" || len(u.PasswordHash) == 0 {
		t.Fatalf("bob after ACL SETUSER = %+v", u)
	}
	// ACL LIST reflects bob.
	payload, err = roleRequestPayload(OpACLList, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	resp := sendAndRecv(t, conn, MsgRequest, payload)
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse for ACL LIST, got %v", resp.Type)
	}
	users, ok := DecodeACLListResponse(resp.Value)
	if !ok {
		t.Fatal("malformed ACL LIST response")
	}
	if len(users) != 3 {
		t.Fatalf("ACL LIST user count = %d, want 3 (admin, bob, limited)", len(users))
	}
	// ACL DELUSER bob.
	payload, err = roleRequestPayload(OpACLDelUser, [][]byte{[]byte("bob")})
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseOK) {
		t.Fatalf("expected ACL DELUSER OK, got %q type %v", resp.Value, resp.Type)
	}
	if u := store.Load().UserFor("bob"); u != nil {
		t.Fatalf("bob still present after ACL DELUSER: %+v", u)
	}
}

// TestServerACLAuthorizationDenial verifies a limited user (GET only, no ACL
// bit) is denied ACL management with ResponseNotAuthorized.
func TestServerACLAuthorizationDenial(t *testing.T) {
	addr, _ := startACLNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("limited", "anything")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk for nopass user, got %v", resp.Type)
	}
	payload, err := roleRequestPayload(OpACLSetUser, [][]byte{[]byte("eve"), []byte("limited"), []byte("nopass")})
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for ACL SETUSER, got %q", resp.Value)
	}
	payload, err = roleRequestPayload(OpACLList, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for ACL LIST, got %q", resp.Value)
	}
	payload, err = roleRequestPayload(OpACLLog, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for ACL LOG, got %q", resp.Value)
	}
}

// TestServerACLLog verifies binary AUTH failures populate the store's ACL LOG
// buffer with the attempted username and reason, including unknown users.
func TestServerACLLog(t *testing.T) {
	addr, store := startACLNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("admin", "wrong")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for wrong password, got %v", resp.Type)
	}
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("ghost", "x")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for unknown user, got %v", resp.Type)
	}

	entries := store.AuthLog()
	if len(entries) != 2 {
		t.Fatalf("AuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Username != "admin" || entries[0].Reason != "invalid password" {
		t.Fatalf("entry 0 = %+v, want admin/invalid password", entries[0])
	}
	if entries[1].Username != "ghost" || entries[1].Reason != "unknown user" {
		t.Fatalf("entry 1 = %+v, want ghost/unknown user", entries[1])
	}

	// The wire path must expose the same entries: re-connect as admin (the
	// first connection was never authenticated) and fetch OpACLLog.
	admin, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer admin.Close()
	if resp := sendAndRecv(t, admin, MsgAuth, buildAuthPayloadWithUser("admin", "sekret")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk, got %v", resp.Type)
	}
	payload, err := roleRequestPayload(OpACLLog, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	resp := sendAndRecv(t, admin, MsgRequest, payload)
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse for ACL LOG, got %v", resp.Type)
	}
	wireEntries, ok := DecodeACLLogResponse(resp.Value)
	if !ok {
		t.Fatal("malformed ACL LOG response")
	}
	if len(wireEntries) != 2 {
		t.Fatalf("wire ACL LOG entry count = %d, want 2", len(wireEntries))
	}
	if wireEntries[0].Username != "admin" || wireEntries[0].Reason != "invalid password" {
		t.Fatalf("wire entry 0 = %+v, want admin/invalid password", wireEntries[0])
	}
	if wireEntries[1].Username != "ghost" || wireEntries[1].Reason != "unknown user" {
		t.Fatalf("wire entry 1 = %+v, want ghost/unknown user", wireEntries[1])
	}
}

// TestServerACLLogDenials verifies NOPERM denials reach the ACL LOG buffer over
// the binary protocol and survive the wire round-trip with the command and key
// folded into Reason, leaving the four-field entry encoding unchanged.
func TestServerACLLogDenials(t *testing.T) {
	addr, store := startACLNetworkServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("limited", "anything")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk for nopass user, got %v", resp.Type)
	}
	payload, err := roleRequestPayload(OpACLList, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	if resp := sendAndRecv(t, conn, MsgRequest, payload); !bytes.Equal(resp.Value, ResponseNotAuthorized) {
		t.Fatalf("expected ResponseNotAuthorized for ACL LIST, got %q", resp.Value)
	}

	entries := store.AuthLog()
	if len(entries) != 1 {
		t.Fatalf("AuthLog len = %d, want 1", len(entries))
	}
	if entries[0].Username != "limited" {
		t.Fatalf("entry 0 username = %q, want limited", entries[0].Username)
	}
	// OpACLList carries no key, so the folded Reason ends with an empty key.
	if want := "NOPERM command=ACL LIST key="; entries[0].Reason != want {
		t.Fatalf("entry 0 Reason = %q, want %q", entries[0].Reason, want)
	}

	admin, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer admin.Close()
	if resp := sendAndRecv(t, admin, MsgAuth, buildAuthPayloadWithUser("admin", "sekret")); resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk, got %v", resp.Type)
	}
	payload, err = roleRequestPayload(OpACLLog, nil)
	if err != nil {
		t.Fatalf("roleRequestPayload: %v", err)
	}
	resp := sendAndRecv(t, admin, MsgRequest, payload)
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse for ACL LOG, got %v", resp.Type)
	}
	wireEntries, ok := DecodeACLLogResponse(resp.Value)
	if !ok {
		t.Fatal("malformed ACL LOG response")
	}
	if len(wireEntries) != 1 {
		t.Fatalf("wire ACL LOG entry count = %d, want 1", len(wireEntries))
	}
	if wireEntries[0].Username != "limited" || wireEntries[0].Reason != entries[0].Reason {
		t.Fatalf("wire entry 0 = %+v, want it to match the store entry %+v", wireEntries[0], entries[0])
	}
}

// TestClientACLMethods exercises the ACL client API against a live server.
func TestClientACLMethods(t *testing.T) {
	addr, _ := startACLNetworkServer(t)

	client, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	scratch := make([]byte, 4096)

	if err := client.AuthUser("admin", "sekret", scratch); err != nil {
		t.Fatalf("AuthUser: %v", err)
	}
	if err := client.RoleCreate("operator", []string{"+get", "~users:*"}, scratch); err != nil {
		t.Fatalf("RoleCreate: %v", err)
	}
	if err := client.AclSetUser("bob", "operator", [][]byte{[]byte("nopass")}, scratch); err != nil {
		t.Fatalf("AclSetUser: %v", err)
	}
	users, err := client.AclList(scratch)
	if err != nil {
		t.Fatalf("AclList: %v", err)
	}
	if len(users) != 3 {
		t.Fatalf("AclList: got %d users want 3 (admin, bob, limited)", len(users))
	}
	var bob *ACLUser
	for i := range users {
		if users[i].Username == "bob" {
			bob = &users[i]
		}
	}
	if bob == nil || bob.Role != "operator" || bob.HasPass {
		t.Fatalf("bob = %+v, want role operator without password", bob)
	}
	if err := client.AclDelUser("bob", scratch); err != nil {
		t.Fatalf("AclDelUser: %v", err)
	}
	users, err = client.AclList(scratch)
	if err != nil {
		t.Fatalf("AclList after delete: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("AclList after delete: got %d users want 2", len(users))
	}

	// Trigger a failed AUTH (wrong password) on a second connection, then read
	// the resulting entry back through the client's AclLog.
	other, err := Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial second client: %v", err)
	}
	defer other.Close()
	if err := other.AuthUser("admin", "wrong", scratch); err == nil {
		t.Fatal("expected AuthUser with wrong password to fail")
	}
	entries, err := client.AclLog(scratch)
	if err != nil {
		t.Fatalf("AclLog: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("AclLog returned no entries after a failed AUTH")
	}
	last := entries[len(entries)-1]
	if last.Username != "admin" || last.Reason != "invalid password" {
		t.Fatalf("last log entry = %+v, want admin/invalid password", last)
	}
	if last.Timestamp == "" || last.RemoteAddr == "" {
		t.Fatalf("log entry missing timestamp or remote address: %+v", last)
	}
}

// TestServerACLLogPolicyNotLoaded covers the AUTH rejection that fires when the
// store holds no policy snapshot. It is still a failed AUTH, so it has to reach
// ACL LOG and count against the per-connection limit rather than returning bare.
func TestServerACLLogPolicyNotLoaded(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	// A store with no snapshot: Load() returns nil, the condition under test.
	store := rbac.NewStore(nil, log.NewNoOpLogger())
	srv := NewServer(addr, 0, nil, aclTestHandler(store), log.NewNoOpLogger(), nil, "", store, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	if err = waitForServer(addr, 2*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("alice", "whatever")); resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr, got %v", resp.Type)
	}

	entries := store.AuthLog()
	if len(entries) != 1 {
		t.Fatalf("AuthLog len = %d, want 1", len(entries))
	}
	if entries[0].Username != "alice" || entries[0].Reason != "policy not loaded" {
		t.Fatalf("entry 0 = %+v, want alice/policy not loaded", entries[0])
	}
	if got := store.AuthFailures(); got != 1 {
		t.Fatalf("AuthFailures = %d, want 1", got)
	}
}
