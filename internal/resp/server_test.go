package resp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/panjf2000/gnet/v2"
)

// newNoOpAudit returns a disabled audit engine so listener tests exercise the
// unguarded Record() hooks with zero audit output.
func newNoOpAudit() *audit.LogEngine {
	e, _ := audit.NewLogEngine(false, nil, "stdout", log.NewNoOpLogger(), false, nil, nil)
	return e
}

// fakeStore is a minimal in-memory Store. Like the real engine, it must COPY the key and
// value because the arguments handed to Set alias the server's network read buffer.
type fakeStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newFakeStore() *fakeStore { return &fakeStore{m: make(map[string][]byte)} }

func (f *fakeStore) Get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[key]
	return v, ok
}

func (f *fakeStore) Set(key string, value []byte, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[string([]byte(key))] = append([]byte(nil), value...)
	return nil
}

func (f *fakeStore) Delete(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestRESPServer_GetSetPingPipeline(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), nil, "", false, nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gnet.Stop(ctx, "tcp://"+addr)
	}()

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expect := func(name, send, want string) {
		t.Helper()
		if _, err := conn.Write([]byte(send)); err != nil {
			t.Fatalf("%s write: %v", name, err)
		}
		got := make([]byte, len(want))
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("%s read: %v", name, err)
		}
		if string(got) != want {
			t.Fatalf("%s: got %q want %q", name, got, want)
		}
	}

	expect("PING", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	expect("SET", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "+OK\r\n")
	expect("GET hit", "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$1\r\nv\r\n")
	expect("GET miss", "*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n", "$-1\r\n")
	// Pipelined SET + GET in a single write must return both replies in order.
	expect("pipeline",
		"*3\r\n$3\r\nSET\r\n$1\r\np\r\n$2\r\nhi\r\n*2\r\n$3\r\nGET\r\n$1\r\np\r\n",
		"+OK\r\n$2\r\nhi\r\n")
}

// expectReply writes send on conn and asserts the exact reply bytes.
func expectReply(t *testing.T, conn net.Conn, name, send, want string) {
	t.Helper()
	if _, err := conn.Write([]byte(send)); err != nil {
		t.Fatalf("%s write: %v", name, err)
	}
	got := make([]byte, len(want))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("%s read: %v", name, err)
	}
	if string(got) != want {
		t.Fatalf("%s: got %q want %q", name, got, want)
	}
}

func startServer(t *testing.T, requirePass string) (addr string) {
	t.Helper()
	addr = freeAddr(t)
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), nil, requirePass, false, nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return addr
}

func TestRESPServer_AuthRequired(t *testing.T) {
	addr := startServer(t, "sekret")

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "GET before auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "-NOAUTH Authentication required\r\n")
	expectReply(t, conn, "SET before auth",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "-NOAUTH Authentication required\r\n")
	expectReply(t, conn, "PING before auth",
		"*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	expectReply(t, conn, "AUTH wrong password",
		"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n", "-ERR invalid password\r\n")
	expectReply(t, conn, "GET after failed auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "-NOAUTH Authentication required\r\n")
	expectReply(t, conn, "AUTH correct password",
		"*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n", "+OK\r\n")
	expectReply(t, conn, "SET after auth",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", "+OK\r\n")
	expectReply(t, conn, "GET after auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$1\r\nv\r\n")

	// Auth state is per-connection: a fresh connection must authenticate again.
	conn2 := dialWithRetry(t, addr)
	defer conn2.Close()
	expectReply(t, conn2, "GET on second conn",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "-NOAUTH Authentication required\r\n")
}

func TestRESPServer_AuthPipeline(t *testing.T) {
	addr := startServer(t, "sekret")

	// AUTH is verified off the event loop by the worker pool. A GET pipelined
	// behind it in the same write must not run unauthenticated: its reply
	// arrives only after the async AUTH completes.
	conn := dialWithRetry(t, addr)
	defer conn.Close()
	expectReply(t, conn, "pipelined AUTH + GET",
		"*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"+OK\r\n$-1\r\n")

	// A pipelined SET after a failed AUTH must still be denied on a
	// connection that never authenticated.
	conn2 := dialWithRetry(t, addr)
	defer conn2.Close()
	expectReply(t, conn2, "pipelined failed AUTH + SET",
		"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
		"-ERR invalid password\r\n-NOAUTH Authentication required\r\n")
}

func TestRESPServer_QuitPreAuth(t *testing.T) {
	addr := startServer(t, "sekret")

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// QUIT is allowed before authentication and must close the connection after +OK.
	expectReply(t, conn, "QUIT before auth", "*1\r\n$4\r\nQUIT\r\n", "+OK\r\n")
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("QUIT deadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF after QUIT, got %v", err)
	}
}

func TestRESPServer_AuthWithUsername(t *testing.T) {
	addr := startServer(t, "sekret")

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH wrong username",
		"*3\r\n$4\r\nAUTH\r\n$5\r\nadmin\r\n$6\r\nsekret\r\n", "-ERR invalid password\r\n")
	expectReply(t, conn, "AUTH default user",
		"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$6\r\nsekret\r\n", "+OK\r\n")
	expectReply(t, conn, "GET after auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$-1\r\n")
}

func TestRESPServer_AuthNoPasswordConfigured(t *testing.T) {
	addr := startServer(t, "")

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// Without --require-pass every connection starts authenticated and AUTH is a no-op.
	expectReply(t, conn, "GET without auth",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "$-1\r\n")
	expectReply(t, conn, "AUTH no-op",
		"*2\r\n$4\r\nAUTH\r\n$8\r\nwhatever\r\n", "+OK\r\n")
}

func TestRESPServer_AuthWrongArity(t *testing.T) {
	addr := startServer(t, "sekret")

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	expectReply(t, conn, "AUTH no args",
		"*1\r\n$4\r\nAUTH\r\n", "-ERR wrong number of arguments for 'auth' command\r\n")
	expectReply(t, conn, "AUTH too many args",
		"*4\r\n$4\r\nAUTH\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
		"-ERR wrong number of arguments for 'auth' command\r\n")
}

func dialWithRetry(t *testing.T, addr string) net.Conn {
	t.Helper()
	var lastErr error
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("could not connect to %s: %v", addr, lastErr)
	return nil
}
