package resp

import (
	"context"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
	"golang.org/x/crypto/bcrypt"
)

// rbacTestPolicy builds a two-role policy with a password-protected admin, a
// nopass admin, and a get-only limited user.
func rbacTestPolicy(t *testing.T) *rbac.PolicyStore {
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
		Roles: map[string]*rbac.Role{
			"admin":   admin,
			"limited": limited,
		},
		Users: map[string]*rbac.User{
			"admin":   {Role: "admin", PasswordHash: hash},
			"nopass":  {Role: "admin"},
			"limited": {Role: "limited"},
		},
		Default: admin,
	}
}

func startRBACServer(t *testing.T) (addr string) {
	t.Helper()
	addr = freeAddr(t)
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), nil, "",
		rbac.NewStore(rbacTestPolicy(t)))
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return addr
}

func TestRESPServer_RBACAuth(t *testing.T) {
	addr := startRBACServer(t)

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "GET before auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "-NOAUTH Authentication required\r\n")
	expectReply(t, conn, "AUTH wrong password",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$5\r\nwrong\r\n", "-ERR invalid password\r\n")
	expectReply(t, conn, "AUTH unknown user",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nghost\r\n$5\r\nwrong\r\n", "-ERR invalid password\r\n")
	expectReply(t, conn, "AUTH admin",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "+OK\r\n")
	expectReply(t, conn, "SET after auth",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "+OK\r\n")
}

func TestRESPServer_RBACNopassUser(t *testing.T) {
	addr := startRBACServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// A nopass user accepts any password (Redis ACL semantics).
	expectReply(t, conn, "AUTH nopass",
		"*3\r\n$4\r\nAUTH\r\n$6\r\nnopass\r\n$8\r\nwhatever\r\n", "+OK\r\n")
	expectReply(t, conn, "SET after nopass auth",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "+OK\r\n")
}

func TestRESPServer_RBACGating(t *testing.T) {
	addr := startRBACServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// "limited" role has only GET: SET and ROLE must be denied, GET allowed.
	expectReply(t, conn, "AUTH limited",
		"*3\r\n$4\r\nAUTH\r\n$7\r\nlimited\r\n$8\r\nwhatever\r\n", "+OK\r\n")
	expectReply(t, conn, "GET allowed",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$-1\r\n")
	expectReply(t, conn, "SET denied",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "-NOPERM no permission for 'set' command on this key\r\n")
	expectReply(t, conn, "ROLE denied",
		"*2\r\n$4\r\nROLE\r\n$4\r\nLIST\r\n", "-NOPERM no permission for 'role' command\r\n")
}

func TestRESPServer_RBACCommands(t *testing.T) {
	addr := startRBACServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH admin",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "+OK\r\n")

	// ROLE CREATE a new role with a command + namespace whitelist.
	expectReply(t, conn, "ROLE CREATE",
		"*5\r\n$4\r\nROLE\r\n$6\r\nCREATE\r\n$8\r\noperator\r\n$4\r\n+get\r\n$2\r\n~*\r\n", "+OK\r\n")

	// ROLE SETUSER with a password option.
	expectReply(t, conn, "ROLE SETUSER",
		"*5\r\n$4\r\nROLE\r\n$7\r\nSETUSER\r\n$3\r\nbob\r\n$8\r\noperator\r\n$8\r\n>hunter2\r\n", "+OK\r\n")

	// ROLE GETUSER reflects the assigned role and the presence of a password.
	expectReply(t, conn, "ROLE GETUSER",
		"*3\r\n$4\r\nROLE\r\n$7\r\nGETUSER\r\n$3\r\nbob\r\n",
		"*3\r\n$3\r\nbob\r\n$8\r\noperator\r\n:1\r\n")

	// ROLE LIST shows the seeded roles plus the new one. Expected bytes are
	// built with the same Append helpers the server uses, so the assertion
	// checks the wiring (sorted role names, command lists, namespace arrays).
	admin, err := rbac.ParseRole("admin", "+@all", "~*")
	if err != nil {
		t.Fatalf("parse admin role: %v", err)
	}
	limited, err := rbac.ParseRole("limited", "+get", "~*")
	if err != nil {
		t.Fatalf("parse limited role: %v", err)
	}
	want := AppendArray(nil, 3)
	want = AppendBulk(want, []byte("admin"))
	cmds := admin.GrantedCommands()
	want = AppendArray(want, len(cmds))
	for _, n := range cmds {
		want = AppendBulk(want, []byte(n))
	}
	want = AppendArray(want, 0)
	want = AppendBulk(want, []byte("limited"))
	limitedCmds := limited.GrantedCommands()
	want = AppendArray(want, len(limitedCmds))
	for _, n := range limitedCmds {
		want = AppendBulk(want, []byte(n))
	}
	want = AppendArray(want, 0)
	want = AppendBulk(want, []byte("operator"))
	want = AppendArray(want, 1)
	want = AppendBulk(want, []byte("GET"))
	want = AppendArray(want, 0)
	expectReply(t, conn, "ROLE LIST",
		"*2\r\n$4\r\nROLE\r\n$4\r\nLIST\r\n", string(want))
}

// TestRESPServer_RBACNamespacePrefix proves that a namespace-scoped role gates
// data commands end-to-end: GET inside the prefix passes, GET on a key outside
// it is denied, and SET is denied even inside the prefix. SETUSER with the
// explicit nopass option creates the passwordless user.
func TestRESPServer_RBACNamespacePrefix(t *testing.T) {
	addr := startRBACServer(t)
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH admin",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "+OK\r\n")
	expectReply(t, conn, "ROLE CREATE user-reader",
		"*5\r\n$4\r\nROLE\r\n$6\r\nCREATE\r\n$11\r\nuser-reader\r\n$4\r\n+get\r\n$8\r\n~users:*\r\n", "+OK\r\n")
	expectReply(t, conn, "ROLE SETUSER carol nopass",
		"*5\r\n$4\r\nROLE\r\n$7\r\nSETUSER\r\n$5\r\ncarol\r\n$11\r\nuser-reader\r\n$6\r\nnopass\r\n", "+OK\r\n")

	expectReply(t, conn, "AUTH carol",
		"*3\r\n$4\r\nAUTH\r\n$5\r\ncarol\r\n$8\r\nwhatever\r\n", "+OK\r\n")
	expectReply(t, conn, "GET inside prefix",
		"*2\r\n$3\r\nGET\r\n$7\r\nusers:1\r\n", "$-1\r\n")
	expectReply(t, conn, "GET outside prefix denied",
		"*2\r\n$3\r\nGET\r\n$10\r\naccounts:1\r\n", "-NOPERM no permission for 'get' command on this key\r\n")
	expectReply(t, conn, "SET inside prefix denied",
		"*3\r\n$3\r\nSET\r\n$7\r\nusers:1\r\n$1\r\nx\r\n", "-NOPERM no permission for 'set' command on this key\r\n")
}
