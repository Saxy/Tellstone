/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: starttls_test.go
Description: End-to-end coverage for upgrading RESP plaintext connections to TLS 1.3 while
preserving authentication boundaries, rejecting pipelined transition bytes, and retaining the
existing implicit-TLS listener behavior when STARTTLS is disabled.

Authors:

	Oghenefega Daniel Omajene
*/
package resp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
)

const startTLSRequest = "*1\r\n$8\r\nSTARTTLS\r\n"

func TestRESPServer_STARTTLSBeforeAuth(t *testing.T) {
	addr, _, _, certPEM := startRESPTLSServer(t, true, "sekret")
	raw := dialWithRetry(t, addr)

	expectReply(t, raw, "STARTTLS wrong arity",
		"*2\r\n$8\r\nSTARTTLS\r\n$3\r\nnow\r\n",
		"-ERR wrong number of arguments for 'starttls' command\r\n")
	expectReply(t, raw, "STARTTLS before AUTH", startTLSRequest, "+OK\r\n")

	conn := stdtls.Client(raw, startTLSClientConfig(t, certPEM))
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("STARTTLS handshake: %v", err)
	}

	expectReply(t, conn, "GET before AUTH",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"-NOAUTH Authentication required\r\n")
	expectReply(t, conn, "AUTH after STARTTLS",
		"*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n",
		"+OK\r\n")
	expectReply(t, conn, "PING after STARTTLS", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	expectReply(t, conn, "repeated STARTTLS", startTLSRequest,
		"-ERR connection is already encrypted\r\n")
}

func TestRESPServer_STARTTLSUsesRotatedCertificate(t *testing.T) {
	addr, srv, _, _ := startRESPTLSServer(t, true, "")
	raw := dialWithRetry(t, addr)

	rotatedCertPEM, rotatedKeyPEM := generateStartTLSCertificate(t)
	rotatedCfg := buildStartTLSConfig(t, rotatedCertPEM, rotatedKeyPEM)
	if err := srv.tlsConfigs.Store(rotatedCfg); err != nil {
		t.Fatalf("publish rotated TLS config: %v", err)
	}

	expectReply(t, raw, "STARTTLS after certificate rotation", startTLSRequest, "+OK\r\n")
	conn := stdtls.Client(raw, startTLSClientConfig(t, rotatedCertPEM))
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("STARTTLS handshake with rotated certificate: %v", err)
	}

	block, _ := pem.Decode(rotatedCertPEM)
	if block == nil {
		t.Fatal("decode rotated certificate")
	}
	peerCerts := conn.ConnectionState().PeerCertificates
	if len(peerCerts) == 0 || !bytes.Equal(peerCerts[0].Raw, block.Bytes) {
		t.Fatal("STARTTLS did not use the latest rotated certificate")
	}
}

func TestRESPServer_STARTTLSRejectsPipelining(t *testing.T) {
	addr, srv, store, _ := startRESPTLSServer(t, true, "")
	tests := []struct {
		name    string
		request string
		key     string
	}{
		{
			name:    "command before STARTTLS",
			request: "*3\r\n$3\r\nSET\r\n$6\r\nbefore\r\n$1\r\nx\r\n" + startTLSRequest,
			key:     "before",
		},
		{
			name:    "command after STARTTLS",
			request: startTLSRequest + "*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$1\r\nx\r\n",
			key:     "after",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialWithRetry(t, addr)
			defer conn.Close()
			if _, err := conn.Write([]byte(tt.request)); err != nil {
				t.Fatalf("write pipelined STARTTLS: %v", err)
			}
			if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatalf("set read deadline: %v", err)
			}
			var b [1]byte
			if n, err := conn.Read(b[:]); err == nil || n != 0 {
				t.Fatalf("pipelined STARTTLS should close without a reply: n=%d err=%v data=%q", n, err, b[:n])
			}
			if _, ok := store.Get(tt.key); ok {
				t.Fatal("plaintext command sharing a buffer with STARTTLS was executed")
			}
		})
	}
	if got := srv.ProtocolErrors(); got != uint64(len(tests)) {
		t.Fatalf("protocol error count: got %d want %d", got, len(tests))
	}
}

func TestRESPServer_ImplicitTLSRemainsDefault(t *testing.T) {
	addr, _, _, certPEM := startRESPTLSServer(t, false, "")
	conn, err := stdtls.Dial("tcp", addr, startTLSClientConfig(t, certPEM))
	if err != nil {
		t.Fatalf("implicit TLS dial: %v", err)
	}
	defer conn.Close()

	expectReply(t, conn, "PING over implicit TLS", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	expectReply(t, conn, "STARTTLS unavailable in implicit mode", startTLSRequest,
		"-ERR unknown command 'STARTTLS'\r\n")
}

func startRESPTLSServer(t *testing.T, startTLS bool, requirePass string) (string, *Server, *fakeStore, []byte) {
	t.Helper()
	certPEM, keyPEM := generateStartTLSCertificate(t)
	cfg := buildStartTLSConfig(t, certPEM, keyPEM)
	configs, err := tlslib.NewConfigStore(cfg)
	if err != nil {
		t.Fatalf("create TLS config store: %v", err)
	}

	addr := freeAddr(t)
	store := newFakeStore()
	srv := NewServer(addr, store, nil, log.NewNoOpLogger(), configs, requirePass, startTLS, nil)
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	probe := dialWithRetry(t, addr)
	_ = probe.Close()
	return addr, srv, store, certPEM
}

func buildStartTLSConfig(t *testing.T, certPEM, keyPEM []byte) *tlslib.Config {
	t.Helper()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	cfg, err := tlslib.BuildConfig(certPath, keyPath, "")
	if err != nil {
		t.Fatalf("build TLS config: %v", err)
	}
	return cfg
}

func generateStartTLSCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Tellstone STARTTLS Test"}},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func startTLSClientConfig(t *testing.T, certPEM []byte) *stdtls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("append trusted certificate")
	}
	return &stdtls.Config{
		MinVersion: stdtls.VersionTLS13,
		MaxVersion: stdtls.VersionTLS13,
		RootCAs:    roots,
		ServerName: "127.0.0.1",
	}
}
