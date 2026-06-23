package network

import (
	"net"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

// TestServerEcho verifies that the Server processes a Ping message and responds with a Pong
// using the zero‑allocation Write/Read helpers.
func TestServerEcho(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to obtain free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	handler := func(msg *Message) ([]byte, MessageType, error) {
		if msg.Type == MsgPing {
			return msg.Payload, MsgPong, nil
		}
		return nil, 0, nil
	}
	srv := NewServer(addr, handler, log.NewNoOpLogger())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("client dial failed: %v", err)
	}
	defer conn.Close()
	if err := Write(conn, MsgPing, []byte("pingdata")); err != nil {
		t.Fatalf("client write failed: %v", err)
	}
	resp, err := ReadMessage(conn)
	if err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if resp.Type != MsgPong {
		t.Fatalf("expected MsgPong, got %v", resp.Type)
	}
	if string(resp.Payload) != "pingdata" {
		t.Fatalf("payload mismatch: got %s want %s", resp.Payload, "pingdata")
	}
	select {
	case err = <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	default:
		// server still running; test ends.
	}
}
