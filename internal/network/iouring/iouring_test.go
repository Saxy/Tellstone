//go:build linux && io_uring

package iouring

import (
	"net"
	"testing"
	"time"
)

func startEcho(t testing.TB) *Server {
	t.Helper()
	srv, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	// Wait for the listener to be bound; Addr reports the assigned port.
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("listener did not bind")
	}
	t.Cleanup(srv.Stop)
	return srv
}

func TestEchoRoundTrip(t *testing.T) {
	srv := startEcho(t)
	c, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))

	msg := []byte("hello-io-uring")
	if _, err := c.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := c.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("echo = %q, want %q", buf, msg)
	}
}

func BenchmarkEcho(b *testing.B) {
	srv := startEcho(b)
	c, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer c.Close()

	msg := []byte("ping")
	buf := make([]byte, len(msg))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Read(buf); err != nil {
			b.Fatal(err)
		}
	}
}
