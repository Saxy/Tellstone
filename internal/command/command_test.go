/*
Package command
Tellstone Shared Command Layer
File: command_test.go
Description: Unit tests for the shared GET/SET/DEL dispatcher: storage semantics,
argument validation, EX/PX TTL parsing, the RBAC gate, and the per-role counter.
The reply assertions go through a capturing fake Reply so the tests are
transport-independent.

Authors:

	Maximilian Hagen
*/
package command

import (
	"errors"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

type fakeStore struct {
	m       map[string][]byte
	lastTTL time.Duration
	setErr  error
}

func newFakeStore() *fakeStore { return &fakeStore{m: make(map[string][]byte)} }

func (f *fakeStore) Get(key string) ([]byte, bool) {
	v, ok := f.m[key]
	return v, ok
}

func (f *fakeStore) Set(key string, value []byte, ttl time.Duration) error {
	f.m[key] = append([]byte(nil), value...)
	f.lastTTL = ttl
	return f.setErr
}

func (f *fakeStore) Delete(key string) bool {
	_, ok := f.m[key]
	delete(f.m, key)
	return ok
}

type replyKind int

const (
	kindNone replyKind = iota
	kindOK
	kindBulk
	kindNull
	kindInt
	kindDenied
	kindError
	kindStorageErr
)

type fakeReply struct {
	kind replyKind
	bulk []byte
	n    int64
	cmd  string
	msg  string
}

func (r *fakeReply) OK()               { r.kind = kindOK }
func (r *fakeReply) Bulk(b []byte)     { r.kind = kindBulk; r.bulk = append([]byte(nil), b...) }
func (r *fakeReply) Null()             { r.kind = kindNull }
func (r *fakeReply) Int(n int64)       { r.kind = kindInt; r.n = n }
func (r *fakeReply) Denied(cmd string) { r.kind = kindDenied; r.cmd = cmd }
func (r *fakeReply) ErrorMsg(s string) { r.kind = kindError; r.msg = s }
func (r *fakeReply) StorageErr(err error) {
	r.kind = kindStorageErr
	r.msg = err.Error()
}

// newNoOpAudit returns a disabled audit engine; Record() on it is a no-op.
func newNoOpAudit() *audit.LogEngine {
	e, _ := audit.NewLogEngine(false, nil, "stdout", log.NewNoOpLogger(), false, nil, nil)
	return e
}

// run executes one command with no RBAC identity (the binary frontend shape).
func run(store Store, args ...string) (*fakeReply, bool) {
	r := &fakeReply{}
	handled := Execute(&Ctx{
		Store: store,
		Args:  strArgs(args),
		Reply: r,
		Audit: newNoOpAudit(),
	})
	return r, handled
}

func strArgs(args []string) [][]byte {
	out := make([][]byte, 0, len(args))
	for _, a := range args {
		out = append(out, []byte(a))
	}
	return out
}

func TestLookup(t *testing.T) {
	for _, name := range []string{"GET", "get", "gEt", "Get"} {
		if Lookup([]byte(name)) == nil {
			t.Errorf("Lookup(%q) = nil, want GET", name)
		}
	}
	if Lookup([]byte("SET")) == nil {
		t.Error("Lookup(SET) = nil, want SET")
	}
	if Lookup([]byte("DEL")) == nil {
		t.Error("Lookup(DEL) = nil, want DEL")
	}
	if Lookup([]byte("PING")) != nil {
		t.Error("Lookup(PING) != nil, want nil")
	}
	if Lookup(nil) != nil {
		t.Error("Lookup(nil) != nil, want nil")
	}
}

func TestExecuteUnknown(t *testing.T) {
	store := newFakeStore()
	r, handled := run(store, "PING")
	if handled {
		t.Fatal("Execute(PING) handled = true, want false")
	}
	if r.kind != kindNone {
		t.Fatalf("reply kind = %v, want none", r.kind)
	}
}

func TestGet(t *testing.T) {
	store := newFakeStore()
	_ = store.Set("k", []byte("v"), 0)

	r, handled := run(store, "GET", "k")
	if !handled {
		t.Fatal("Execute(GET) handled = false, want true")
	}
	if r.kind != kindBulk || string(r.bulk) != "v" {
		t.Fatalf("GET hit = kind %v bulk %q, want bulk v", r.kind, r.bulk)
	}

	r, _ = run(store, "GET", "missing")
	if r.kind != kindNull {
		t.Fatalf("GET miss = kind %v, want null", r.kind)
	}

	r, _ = run(store, "GET", "k", "extra")
	if r.kind != kindError || r.msg != "ERR wrong number of arguments for 'get' command" {
		t.Fatalf("GET arity = kind %v msg %q, want arity error", r.kind, r.msg)
	}
}

func TestSet(t *testing.T) {
	store := newFakeStore()

	r, _ := run(store, "SET", "k", "v")
	if r.kind != kindOK {
		t.Fatalf("SET = kind %v, want OK", r.kind)
	}
	if got := string(store.m["k"]); got != "v" {
		t.Fatalf("stored value = %q, want v", got)
	}
	if store.lastTTL != 0 {
		t.Fatalf("SET without TTL = %v, want 0", store.lastTTL)
	}

	run(store, "SET", "k", "v", "EX", "2")
	if store.lastTTL != 2*time.Second {
		t.Fatalf("SET EX 2 = %v, want 2s", store.lastTTL)
	}

	run(store, "SET", "k", "v", "PX", "500")
	if store.lastTTL != 500*time.Millisecond {
		t.Fatalf("SET PX 500 = %v, want 500ms", store.lastTTL)
	}

	for _, args := range [][]string{
		{"SET", "k", "v", "XX", "2"},
		{"SET", "k", "v", "EX", "-1"},
		{"SET", "k", "v", "EX", "x"},
	} {
		r, _ := run(store, args...)
		if r.kind != kindError || r.msg != "ERR syntax error" {
			t.Fatalf("SET %v = kind %v msg %q, want syntax error", args, r.kind, r.msg)
		}
	}

	r, _ = run(store, "SET", "k", "v", "extra")
	if r.kind != kindError || r.msg != "ERR wrong number of arguments for 'set' command" {
		t.Fatalf("SET arity = kind %v msg %q, want arity error", r.kind, r.msg)
	}

	store.setErr = errors.New("disk full")
	r, _ = run(store, "SET", "k", "v")
	if r.kind != kindStorageErr || r.msg != "disk full" {
		t.Fatalf("SET storage failure = kind %v msg %q, want storage err", r.kind, r.msg)
	}
}

func TestSetTTLHint(t *testing.T) {
	// The binary frontend carries the expiry in the frame, not in EX/PX args.
	store := newFakeStore()
	r := &fakeReply{}
	Execute(&Ctx{
		Store: store,
		Args:  strArgs([]string{"SET", "k", "v"}),
		Reply: r,
		TTL:   3 * time.Second,
		Audit: newNoOpAudit(),
	})
	if r.kind != kindOK {
		t.Fatalf("SET with hint = kind %v, want OK", r.kind)
	}
	if store.lastTTL != 3*time.Second {
		t.Fatalf("SET hint TTL = %v, want 3s", store.lastTTL)
	}
}

func TestDel(t *testing.T) {
	store := newFakeStore()
	_ = store.Set("a", []byte("1"), 0)
	_ = store.Set("b", []byte("1"), 0)

	r, _ := run(store, "DEL", "a", "b", "missing")
	if r.kind != kindInt || r.n != 2 {
		t.Fatalf("DEL = kind %v count %d, want 2", r.kind, r.n)
	}
	if _, ok := store.m["a"]; ok {
		t.Error("DEL left key a behind")
	}

	r, _ = run(store, "DEL")
	if r.kind != kindError || r.msg != "ERR wrong number of arguments for 'del' command" {
		t.Fatalf("DEL arity = kind %v msg %q, want arity error", r.kind, r.msg)
	}
}

// rbacGate builds a Ctx with a policy store and a get-only "limited" session,
// mirroring how the RESP frontend calls Execute for every data command.
func rbacGate(store Store, args [][]byte, r *fakeReply) *Ctx {
	admin, err := rbac.ParseRole("admin", "+@all", "~*")
	if err != nil {
		panic(err)
	}
	limited, err := rbac.ParseRole("limited", "+get", "~*")
	if err != nil {
		panic(err)
	}
	policy := rbac.NewStore(&rbac.PolicyStore{
		Roles:   map[string]*rbac.Role{"admin": admin, "limited": limited},
		Default: admin,
	}, log.NewNoOpLogger())
	session := rbac.NewSessionContext("limited", limited)
	return &Ctx{
		Store:    store,
		Args:     args,
		Reply:    r,
		Policy:   policy,
		Session:  session,
		Remote:   "127.0.0.1:1234",
		Protocol: "resp",
		Audit:    newNoOpAudit(),
		Logger:   log.NewNoOpLogger(),
	}
}

func TestRBACGate(t *testing.T) {
	store := newFakeStore()
	_ = store.Set("k", []byte("v"), 0)
	r := &fakeReply{}
	c := rbacGate(store, strArgs([]string{"GET", "k"}), r)
	if !Execute(c) {
		t.Fatal("Execute(GET allowed) = false, want true")
	}
	if r.kind != kindBulk {
		t.Fatalf("GET allowed = kind %v, want bulk", r.kind)
	}

	// GET bumps the limited role's executed-command counter.
	if got := c.Policy.Load().Roles["limited"].Commands(); got != 1 {
		t.Fatalf("role commands = %d, want 1", got)
	}

	r = &fakeReply{}
	c = rbacGate(store, strArgs([]string{"SET", "k", "v"}), r)
	Execute(c)
	if r.kind != kindDenied || r.cmd != "set" {
		t.Fatalf("SET denied = kind %v cmd %q, want denied 'set'", r.kind, r.cmd)
	}
	if got := c.Policy.DeniedCommands(); got != 1 {
		t.Fatalf("denied counter = %d, want 1", got)
	}
}

func TestRBACGatePerKey(t *testing.T) {
	// DEL must gate every key, not just the first, and report the offending one.
	store := newFakeStore()
	r := &fakeReply{}
	c := rbacGate(store, strArgs([]string{"DEL", "a", "b"}), r)
	Execute(c)
	if r.kind != kindDenied || r.cmd != "del" {
		t.Fatalf("DEL denied = kind %v cmd %q, want denied 'del'", r.kind, r.cmd)
	}
	if got := c.Policy.DeniedCommands(); got != 1 {
		t.Fatalf("denied counter = %d, want 1 (one denial per command, not per key)", got)
	}
}

func TestRBACFailClosed(t *testing.T) {
	// A configured policy with no session must deny every data command.
	admin, err := rbac.ParseRole("admin", "+@all", "~*")
	if err != nil {
		t.Fatal(err)
	}
	policy := rbac.NewStore(&rbac.PolicyStore{
		Roles:   map[string]*rbac.Role{"admin": admin},
		Default: admin,
	}, log.NewNoOpLogger())
	store := newFakeStore()
	r := &fakeReply{}
	Execute(&Ctx{
		Store:    store,
		Args:     strArgs([]string{"GET", "k"}),
		Reply:    r,
		Policy:   policy,
		Remote:   "127.0.0.1:1234",
		Protocol: "resp",
		Audit:    newNoOpAudit(),
		Logger:   log.NewNoOpLogger(),
	})
	if r.kind != kindDenied {
		t.Fatalf("GET without session = kind %v, want denied", r.kind)
	}
}
