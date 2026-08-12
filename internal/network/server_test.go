package network

import (
	"bytes"
	"context"
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
)

// newNoOpAudit returns a disabled audit engine so listener tests exercise the
// unguarded Record() hooks with zero audit output.
func newNoOpAudit() *audit.LogEngine {
	e, _ := audit.NewLogEngine(false, nil, "stdout", log.NewNoOpLogger(), false, nil, nil)
	return e
}

// fakeStore is a minimal in-memory key-value store for testing binary protocol handlers.
type fakeStore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func newFakeStore() *fakeStore { return &fakeStore{m: make(map[string][]byte)} }

func (f *fakeStore) get(key string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.m[key]
	return v, ok
}

func (f *fakeStore) set(key string, value []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[string([]byte(key))] = append([]byte(nil), value...)
}

func (f *fakeStore) delete(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
}

// storeHandler is a Server handler that dispatches MsgRequest operations against
// a fakeStore and echoes MsgPing payloads.
func storeHandler(store *fakeStore) func(msg *Message) ([]byte, MessageType, error) {
	return func(msg *Message) ([]byte, MessageType, error) {
		switch msg.Type {
		case MsgPing:
			return msg.Payload, MsgPong, nil
		case MsgRequest:
			key := string(msg.Key)
			switch msg.Op {
			case OpGet:
				val, ok := store.get(key)
				if !ok {
					return ResponseNotFound, MsgError, nil
				}
				return val, MsgResponse, nil
			case OpSet:
				store.set(key, msg.Value)
				return ResponseOK, MsgResponse, nil
			case OpDelete:
				store.delete(key)
				return ResponseOK, MsgResponse, nil
			}
		}
		return ResponseInvalidOpCode, MsgError, nil
	}
}

// buildAuthPayload creates a MsgAuth wire payload in single-password mode (username empty).
func buildAuthPayload(password string) []byte {
	payloadLen := 2 + 0 + 2 + len(password)
	buf := make([]byte, payloadLen)
	binary.BigEndian.PutUint16(buf[0:2], 0)
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(password)))
	copy(buf[4:], password)
	return buf
}

// buildAuthPayloadWithUser creates a MsgAuth payload with a specific username.
func buildAuthPayloadWithUser(username, password string) []byte {
	payloadLen := 2 + len(username) + 2 + len(password)
	buf := make([]byte, payloadLen)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(username)))
	copy(buf[2:2+len(username)], username)
	pos := 2 + len(username)
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(password)))
	copy(buf[pos+2:], password)
	return buf
}

// sendAndRecv writes a raw frame and reads one full response message.
func sendAndRecv(t *testing.T, conn net.Conn, msgType MessageType, payload []byte) *Message {
	t.Helper()
	if err := Write(conn, msgType, payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := ReadMessage(conn)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	return resp
}

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
	srv := NewServer(addr, 0, nil, handler, log.NewNoOpLogger(), nil, "", nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	if err := waitForServer(addr, 2*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}
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
}

// TestServerTLSConfigRotation verifies that established TLS connections retain their
// original configuration while connections accepted after publication use the replacement.
func TestServerTLSConfigRotation(t *testing.T) {
	certA, keyA, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("generate certificate A: %v", err)
	}
	certB, keyB, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("generate certificate B: %v", err)
	}
	certFileA, keyFileA, err := writeTempCertFiles(certA, keyA)
	if err != nil {
		t.Fatalf("write certificate A: %v", err)
	}
	certFileB, keyFileB, err := writeTempCertFiles(certB, keyB)
	if err != nil {
		t.Fatalf("write certificate B: %v", err)
	}
	for _, path := range []string{certFileA, keyFileA, certFileB, keyFileB} {
		path := path
		t.Cleanup(func() { _ = os.Remove(path) })
	}

	cfgA, err := tlslib.BuildConfig(certFileA, keyFileA, "")
	if err != nil {
		t.Fatalf("build config A: %v", err)
	}
	cfgB, err := tlslib.BuildConfig(certFileB, keyFileB, "")
	if err != nil {
		t.Fatalf("build config B: %v", err)
	}
	configs, err := tlslib.NewConfigStore(cfgA)
	if err != nil {
		t.Fatalf("create config store: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	srv := NewServer(addr, 0, nil, pingHandler, log.NewNoOpLogger(), configs, "", nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	if err := waitForServer(addr, 2*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	connA, err := stdtls.Dial("tcp", addr, clientTLSConfig(t, certA))
	if err != nil {
		t.Fatalf("dial with certificate A trust: %v", err)
	}
	defer connA.Close()
	if got := sendAndRecv(t, connA, MsgPing, []byte("before")); got.Type != MsgPong {
		t.Fatalf("expected pong before rotation, got %v", got.Type)
	}

	if err := configs.Store(cfgB); err != nil {
		t.Fatalf("publish config B: %v", err)
	}
	if got := sendAndRecv(t, connA, MsgPing, []byte("after")); got.Type != MsgPong {
		t.Fatalf("existing connection failed after rotation: %v", got.Type)
	}

	connB, err := stdtls.Dial("tcp", addr, clientTLSConfig(t, certB))
	if err != nil {
		t.Fatalf("new connection did not receive certificate B: %v", err)
	}
	defer connB.Close()
	if got := sendAndRecv(t, connB, MsgPing, []byte("new")); got.Type != MsgPong {
		t.Fatalf("expected pong on new connection, got %v", got.Type)
	}

	if staleConn, err := stdtls.Dial("tcp", addr, clientTLSConfig(t, certA)); err == nil {
		_ = staleConn.Close()
		t.Fatal("new connection unexpectedly trusted the rotated-out certificate")
	}
}

func clientTLSConfig(t *testing.T, certPEM []byte) *stdtls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("append trusted certificate")
	}
	return &stdtls.Config{
		MinVersion: stdtls.VersionTLS13,
		MaxVersion: stdtls.VersionTLS13,
		RootCAs:    roots,
	}
}

// startAuthServer starts a server with the given requirePass and returns the address.
func startAuthServer(t *testing.T, requirePass string, handler func(msg *Message) ([]byte, MessageType, error)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to obtain free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	srv := NewServer(addr, 0, nil, handler, log.NewNoOpLogger(), nil, requirePass, nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	if err := waitForServer(addr, 2*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}
	return addr
}

// TestServerAuthFlow verifies the full authentication lifecycle:
// unauthenticated request rejected, PING allowed, wrong password, correct password.
func TestServerAuthFlow(t *testing.T) {
	store := newFakeStore()
	addr := startAuthServer(t, "hunter2", storeHandler(store))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// GET before auth — should be rejected with MsgAuthErr
	var reqBuf [32]byte
	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len("k")))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)
	copy(reqBuf[11:], "k")
	resp := sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for GET before auth, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, ResponseAuthErr) {
		t.Fatalf("expected auth error payload, got %q", resp.Payload)
	}

	// PING before auth — should be allowed (like Redis)
	resp = sendAndRecv(t, conn, MsgPing, []byte("ping"))
	if resp.Type != MsgPong {
		t.Fatalf("expected MsgPong for PING before auth, got %v", resp.Type)
	}

	// AUTH with wrong password
	resp = sendAndRecv(t, conn, MsgAuth, buildAuthPayload("wrongpass"))
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for wrong password, got %v", resp.Type)
	}

	// GET after failed auth — still rejected
	resp = sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for GET after failed auth, got %v", resp.Type)
	}

	// AUTH with correct password
	resp = sendAndRecv(t, conn, MsgAuth, buildAuthPayload("hunter2"))
	if resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, ResponseOK) {
		t.Fatalf("expected OK payload, got %q", resp.Payload)
	}

	// GET after auth — should succeed
	store.set("k", []byte("value"))
	resp = sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse for GET after auth, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, []byte("value")) {
		t.Fatalf("expected 'value', got %q", resp.Payload)
	}

	// Auth state is per-connection: fresh connection must auth again.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn2.Close()
	resp = sendAndRecv(t, conn2, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr on fresh connection, got %v", resp.Type)
	}
}

// TestServerAuthNoPassword verifies that when --require-pass is not set,
// all connections start authenticated and AUTH is a no-op.
func TestServerAuthNoPassword(t *testing.T) {
	store := newFakeStore()
	store.set("k", []byte("v"))
	addr := startAuthServer(t, "", storeHandler(store))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// GET without auth — should work immediately
	var reqBuf [32]byte
	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len("k")))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)
	copy(reqBuf[11:], "k")
	resp := sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse, got %v", resp.Type)
	}

	// AUTH is a no-op when no password is configured
	resp = sendAndRecv(t, conn, MsgAuth, buildAuthPayload("anything"))
	if resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk (no-op), got %v", resp.Type)
	}
}

// TestServerErrorFrameDistinctFromData verifies that a stored value beginning
// with "ERR " is returned verbatim in a MsgResponse frame, while real failures
// arrive in a MsgError frame — the frame type, not a payload prefix, signals an
// error on the wire.
func TestServerErrorFrameDistinctFromData(t *testing.T) {
	store := newFakeStore()
	store.set("k", []byte("ERR upstream timeout"))
	addr := startAuthServer(t, "", storeHandler(store))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	var reqBuf [32]byte
	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len("k")))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)
	copy(reqBuf[11:], "k")

	resp := sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse for a data value, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, []byte("ERR upstream timeout")) {
		t.Fatalf("stored value changed on the wire: got %q", resp.Payload)
	}

	// A miss is a real failure and must arrive in the dedicated error frame.
	resp = sendAndRecv(t, conn, MsgRequest, []byte{byte(OpGet), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 'x'})
	if resp.Type != MsgError {
		t.Fatalf("expected MsgError for a miss, got %v", resp.Type)
	}
	if !bytes.Equal(resp.Payload, ResponseNotFound) {
		t.Fatalf("expected NOT_FOUND payload, got %q", resp.Payload)
	}
}

// TestServerAuthWithUsername verifies username-aware auth.
func TestServerAuthWithUsername(t *testing.T) {
	store := newFakeStore()
	store.set("k", []byte("v"))
	addr := startAuthServer(t, "sekret", storeHandler(store))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Wrong username
	resp := sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("admin", "sekret"))
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for wrong username, got %v", resp.Type)
	}

	// Correct default user
	resp = sendAndRecv(t, conn, MsgAuth, buildAuthPayloadWithUser("default", "sekret"))
	if resp.Type != MsgAuthOk {
		t.Fatalf("expected MsgAuthOk for default user, got %v", resp.Type)
	}

	// GET after auth
	var reqBuf [32]byte
	reqBuf[0] = byte(OpGet)
	binary.BigEndian.PutUint16(reqBuf[1:3], uint16(len("k")))
	binary.BigEndian.PutUint64(reqBuf[3:11], 0)
	copy(reqBuf[11:], "k")
	resp = sendAndRecv(t, conn, MsgRequest, reqBuf[:11+1])
	if resp.Type != MsgResponse {
		t.Fatalf("expected MsgResponse, got %v", resp.Type)
	}
}

// TestServerAuthMalformedPayload verifies that a truncated MsgAuth payload is handled
// without crashing (returns auth error).
func TestServerAuthMalformedPayload(t *testing.T) {
	addr := startAuthServer(t, "sekret", storeHandler(newFakeStore()))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Payload too short (only 1 byte, needs at least 2 for usernameLen)
	resp := sendAndRecv(t, conn, MsgAuth, []byte{0x00})
	if resp.Type != MsgAuthErr {
		t.Fatalf("expected MsgAuthErr for truncated payload, got %v", resp.Type)
	}
}

func waitForServer(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("server not ready after %v", timeout)
}
