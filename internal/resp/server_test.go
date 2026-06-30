package resp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/panjf2000/gnet/v2"
)

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
	srv := NewServer(addr, newFakeStore(), log.NewNoOpLogger())
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
