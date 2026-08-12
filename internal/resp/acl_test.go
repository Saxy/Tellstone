package resp

import (
	"context"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// startACLServer runs a RESP server on the seeded RBAC policy and returns the
// live store so tests can assert store-level effects of the ACL commands.
func startACLServer(t *testing.T) (addr string, store *rbac.Store) {
	t.Helper()
	addr = freeAddr(t)
	store = rbac.NewStore(rbacTestPolicy(t), log.NewNoOpLogger())
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), nil, "", false, store, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return addr, store
}

// TestRESPServer_ACLNotEnabled verifies the ACL command family is rejected
// when RBAC is disabled, mirroring the ROLE command's guard.
func TestRESPServer_ACLNotEnabled(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), nil, "", false, nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	conn := dialWithRetry(t, addr)
	defer conn.Close()
	expectReply(t, conn, "ACL LIST without RBAC",
		"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n", "-ERR RBAC is not enabled\r\n")
}

// TestRESPServer_ACLGating verifies the ACL command family needs the ACL
// command bit: the limited role (GET only) is denied with NOPERM.
func TestRESPServer_ACLGating(t *testing.T) {
	addr, _ := startACLServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH limited",
		"*3\r\n$4\r\nAUTH\r\n$7\r\nlimited\r\n$8\r\nwhatever\r\n", "+OK\r\n")
	expectReply(t, conn, "ACL LIST denied",
		"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n", "-NOPERM no permission for 'acl' command\r\n")
}

// TestRESPServer_ACLCommands exercises the SETUSER/DELUSER/LIST management
// surface end-to-end: create a role, bind a user via ACL SETUSER, list users,
// then delete the user and confirm the store dropped it.
func TestRESPServer_ACLCommands(t *testing.T) {
	addr, store := startACLServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH admin",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "+OK\r\n")

	// ACL SETUSER requires a role to bind to; create it first via ROLE CREATE.
	expectReply(t, conn, "ROLE CREATE operator",
		"*5\r\n$4\r\nROLE\r\n$6\r\nCREATE\r\n$8\r\noperator\r\n$4\r\n+get\r\n$8\r\n~users:*\r\n", "+OK\r\n")

	// ACL SETUSER with a password option.
	expectReply(t, conn, "ACL SETUSER bob",
		"*5\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$3\r\nbob\r\n$8\r\noperator\r\n$8\r\n>hunter2\r\n", "+OK\r\n")
	if u := store.Load().UserFor("bob"); u == nil || u.Role != "operator" || len(u.PasswordHash) == 0 {
		t.Fatalf("user bob after ACL SETUSER = %+v, want operator with password", u)
	}

	// ACL SETUSER without a password option is rejected, like ROLE SETUSER.
	expectReply(t, conn, "ACL SETUSER missing option",
		"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$3\r\neve\r\n$8\r\noperator\r\n",
		"-ERR acl|setuser requires a '>password' or 'nopass' option\r\n")
	if u := store.Load().UserFor("eve"); u != nil {
		t.Fatalf("user eve exists after rejected SETUSER: %+v", u)
	}

	// ACL LIST: seeded users admin/limited/nopass plus bob, sorted by name.
	admin, err := rbac.ParseRole("admin", "+@all", "~*")
	if err != nil {
		t.Fatalf("parse admin role: %v", err)
	}
	limited, err := rbac.ParseRole("limited", "+get", "~*")
	if err != nil {
		t.Fatalf("parse limited role: %v", err)
	}
	operator, err := rbac.ParseRole("operator", "+get", "~users:*")
	if err != nil {
		t.Fatalf("parse operator role: %v", err)
	}
	want := AppendArray(nil, 4)
	for _, u := range []struct {
		name    string
		role    string
		hasPass int64
		cmds    *rbac.Role
		ns      int
	}{
		{"admin", "admin", 1, admin, 0},
		{"bob", "operator", 1, operator, 1},
		{"limited", "limited", 0, limited, 0},
		{"nopass", "admin", 0, admin, 0},
	} {
		want = AppendArray(want, 5)
		want = AppendBulk(want, []byte(u.name))
		want = AppendBulk(want, []byte(u.role))
		want = AppendInt(want, u.hasPass)
		cmds := u.cmds.GrantedCommands()
		want = AppendArray(want, len(cmds))
		for _, n := range cmds {
			want = AppendBulk(want, []byte(n))
		}
		want = AppendArray(want, u.ns)
		if u.ns == 1 {
			want = AppendBulk(want, []byte("users:"))
		}
	}
	expectReply(t, conn, "ACL LIST",
		"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n", string(want))

	// ACL DELUSER removes the user from the store.
	expectReply(t, conn, "ACL DELUSER bob",
		"*3\r\n$3\r\nACL\r\n$7\r\nDELUSER\r\n$3\r\nbob\r\n", "+OK\r\n")
	if u := store.Load().UserFor("bob"); u != nil {
		t.Fatalf("user bob still present after ACL DELUSER: %+v", u)
	}

	// Unknown subcommand and wrong arity are rejected.
	expectReply(t, conn, "ACL unknown subcommand",
		"*3\r\n$3\r\nACL\r\n$3\r\nFOO\r\n$1\r\nx\r\n", "-ERR unknown acl subcommand 'FOO'\r\n")
	expectReply(t, conn, "ACL LOG wrong arity",
		"*3\r\n$3\r\nACL\r\n$3\r\nLOG\r\n$1\r\nx\r\n", "-ERR wrong number of arguments for 'acl|log' command\r\n")
}

// TestRESPServer_ACLLog verifies failed AUTH attempts are recorded in the ACL
// LOG buffer with username, remote address, and reason, and that ACL LOG
// renders them in chronological order without leaking password hashes.
func TestRESPServer_ACLLog(t *testing.T) {
	addr, store := startACLServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// Two failures: a wrong password for an existing user, then an unknown user.
	expectReply(t, conn, "AUTH wrong password",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$5\r\nwrong\r\n", "-ERR invalid password\r\n")
	expectReply(t, conn, "AUTH unknown user",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nghost\r\n$5\r\nwrong\r\n", "-ERR invalid password\r\n")
	expectReply(t, conn, "AUTH admin ok",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "+OK\r\n")

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

	// The reply must match exactly what the store's log renders.
	var want []byte
	want = AppendArray(want, len(entries))
	for _, e := range entries {
		want = AppendArray(want, 4)
		want = AppendBulk(want, []byte(e.Timestamp.Format(time.RFC3339)))
		want = AppendBulk(want, []byte(e.Username))
		want = AppendBulk(want, []byte(e.RemoteAddr))
		want = AppendBulk(want, []byte(e.Reason))
	}
	expectReply(t, conn, "ACL LOG",
		"*2\r\n$3\r\nACL\r\n$3\r\nLOG\r\n", string(want))
}

// TestRESPServer_ACLLogDenials verifies NOPERM denials land in the ACL LOG
// buffer alongside auth failures, and that folding the command and key into
// Reason keeps the reply at four fields per entry — the wire shape existing
// clients decode.
func TestRESPServer_ACLLogDenials(t *testing.T) {
	addr, store := startACLServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// The limited role grants +get only, so SET (keyed) and ACL (keyless) are
	// both denied, covering each deniedReply argument shape.
	expectReply(t, conn, "AUTH limited",
		"*3\r\n$4\r\nAUTH\r\n$7\r\nlimited\r\n$8\r\nwhatever\r\n", "+OK\r\n")
	expectReply(t, conn, "SET denied",
		"*3\r\n$3\r\nSET\r\n$6\r\nsecret\r\n$1\r\nv\r\n",
		"-NOPERM no permission for 'set' command on this key\r\n")
	expectReply(t, conn, "ACL LIST denied",
		"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n", "-NOPERM no permission for 'acl' command\r\n")

	entries := store.AuthLog()
	if len(entries) != 2 {
		t.Fatalf("AuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Username != "limited" || entries[0].Reason != "NOPERM command=set key=secret" {
		t.Fatalf("entry 0 = %+v, want limited/NOPERM command=set key=secret", entries[0])
	}
	if entries[1].Username != "limited" || entries[1].Reason != "NOPERM command=acl key=" {
		t.Fatalf("entry 1 = %+v, want limited/NOPERM command=acl key=", entries[1])
	}
	if entries[0].RemoteAddr == "" {
		t.Fatal("entry 0 has empty remote address")
	}

	// Re-AUTH as admin: the limited role cannot run ACL LOG itself.
	expectReply(t, conn, "AUTH admin",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "+OK\r\n")

	entries = store.AuthLog()
	var want []byte
	want = AppendArray(want, len(entries))
	for _, e := range entries {
		want = AppendArray(want, 4)
		want = AppendBulk(want, []byte(e.Timestamp.Format(time.RFC3339)))
		want = AppendBulk(want, []byte(e.Username))
		want = AppendBulk(want, []byte(e.RemoteAddr))
		want = AppendBulk(want, []byte(e.Reason))
	}
	expectReply(t, conn, "ACL LOG with denials",
		"*2\r\n$3\r\nACL\r\n$3\r\nLOG\r\n", string(want))
}
