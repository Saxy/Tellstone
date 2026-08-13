package network

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/command"
	"github.com/Saxy/Tellstone/internal/log"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
)

// benchServer holds a running plaintext gnet server for benchmarks.
type benchServer struct {
	srv   *Server
	addr  string
	ready chan struct{}
}

func startBenchServer(b *testing.B, handler func(msg *Message, c *command.Ctx) ([]byte, MessageType, error)) *benchServer {
	b.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to obtain free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	srv := NewServer(addr, 0, nil, handler, log.NewNoOpLogger(), nil, "", nil, nil, newNoOpAudit())
	go func() { _ = srv.ListenAndServe() }()
	if err := waitForServer(addr, 2*time.Second); err != nil {
		b.Fatalf("server not ready: %v", err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return &benchServer{srv: srv, addr: addr}
}

// benchTLS server holds a running TLS gnet server for benchmarks.
type benchTLSServer struct {
	srv     *Server
	addr    string
	certPEM []byte
	keyPEM  []byte
}

func startBenchTLSServer(b *testing.B, handler func(msg *Message, c *command.Ctx) ([]byte, MessageType, error)) *benchTLSServer {
	b.Helper()
	certPEM, keyPEM, err := generateSelfSignedCert()
	if err != nil {
		b.Fatalf("failed to generate cert: %v", err)
	}

	certFile, keyFile, err := writeTempCertFiles(certPEM, keyPEM)
	if err != nil {
		b.Fatalf("failed to write cert files: %v", err)
	}
	b.Cleanup(func() {
		os.Remove(certFile)
		os.Remove(keyFile)
	})

	tlsCfg, err := tlslib.BuildConfig(certFile, keyFile, "")
	if err != nil {
		b.Fatalf("failed to build TLS config: %v", err)
	}
	tlsConfigs, err := tlslib.NewConfigStore(tlsCfg)
	if err != nil {
		b.Fatalf("failed to create TLS config store: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to obtain free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	srv := NewServer(addr, 0, nil, handler, log.NewNoOpLogger(), tlsConfigs, "", nil, nil, newNoOpAudit())
	go func() {
		_ = srv.ListenAndServe()
	}()
	if err := waitForServer(addr, 2*time.Second); err != nil {
		b.Fatalf("server not ready: %v", err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return &benchTLSServer{srv: srv, addr: addr, certPEM: certPEM, keyPEM: keyPEM}
}

// pingHandler echoes back the payload — minimal work to measure transport overhead.
func pingHandler(msg *Message, c *command.Ctx) ([]byte, MessageType, error) {
	if msg.Type == MsgPing {
		return msg.Payload, MsgPong, nil
	}
	return nil, 0, nil
}

// buildPingFrame constructs a MsgPing frame with the given payload.
func buildPingFrame(payload []byte) []byte {
	totalLen := 1 + len(payload)
	var frame [5]byte
	binary.BigEndian.PutUint32(frame[:4], uint32(totalLen))
	frame[4] = byte(MsgPing)
	out := make([]byte, 5+len(payload))
	copy(out, frame[:])
	copy(out[5:], payload)
	return out
}

// BenchmarkGnetPlaintext benchmarks the plaintext gnet server with a single
// client performing sequential round-trips.
func BenchmarkGnetPlaintext(b *testing.B) {
	bs := startBenchServer(b, pingHandler)
	payload := []byte("benchmark_payload")
	frame := buildPingFrame(payload)

	conn, err := net.Dial("tcp", bs.addr)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Warm up: send one ping and read one pong.
	if _, err := conn.Write(frame); err != nil {
		b.Fatalf("warmup write failed: %v", err)
	}
	buf := make([]byte, 5+len(payload))
	if _, err := io.ReadFull(conn, buf); err != nil {
		b.Fatalf("warmup read failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(frame); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		if _, err := io.ReadFull(conn, buf); err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkGnetTLS benchmarks the TLS 1.3 gnet server with a single client
// performing sequential round-trips through crypto/tls.
func BenchmarkGnetTLS(b *testing.B) {
	bs := startBenchTLSServer(b, pingHandler)
	payload := []byte("benchmark_payload")
	frame := buildPingFrame(payload)

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(bs.certPEM)
	tlsCfg.RootCAs = pool
	conn, err := tls.Dial("tcp", bs.addr, tlsCfg)
	if err != nil {
		b.Fatalf("tls dial failed: %v", err)
	}
	defer conn.Close()

	// Warm up: handshake + one ping/pong.
	buf := make([]byte, 5+len(payload))
	if _, err := conn.Write(frame); err != nil {
		b.Fatalf("warmup write failed: %v", err)
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		b.Fatalf("warmup read failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(frame); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		if _, err := io.ReadFull(conn, buf); err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkGnetPlaintextParallel benchmarks the plaintext server under
// concurrent load with GOMAXPROCS clients.
func BenchmarkGnetPlaintextParallel(b *testing.B) {
	bs := startBenchServer(b, pingHandler)
	payload := []byte("benchdata")
	frame := buildPingFrame(payload)

	numCores := runtime.GOMAXPROCS(0)
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(core int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", bs.addr)
			if err != nil {
				b.Errorf("dial failed: %v", err)
				return
			}
			defer conn.Close()

			buf := make([]byte, 5+len(payload))
			iters := b.N / numCores
			if core < b.N%numCores {
				iters++
			}
			for j := 0; j < iters; j++ {
				if _, err := conn.Write(frame); err != nil {
					return
				}
				if _, err := io.ReadFull(conn, buf); err != nil {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	b.StopTimer()
}

// BenchmarkGnetTLSParallel benchmarks the TLS server under concurrent load
// with GOMAXPROCS clients, each performing TLS 1.3 round-trips.
func BenchmarkGnetTLSParallel(b *testing.B) {
	bs := startBenchTLSServer(b, pingHandler)
	payload := []byte("benchdata")
	frame := buildPingFrame(payload)

	numCores := runtime.GOMAXPROCS(0)
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(core int) {
			defer wg.Done()
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(bs.certPEM)
			tlsCfg := &tls.Config{
				MinVersion: tls.VersionTLS13,
				MaxVersion: tls.VersionTLS13,
				RootCAs:    pool,
			}
			conn, err := tls.Dial("tcp", bs.addr, tlsCfg)
			if err != nil {
				b.Errorf("tls dial failed: %v", err)
				return
			}
			defer conn.Close()

			buf := make([]byte, 5+len(payload))
			iters := b.N / numCores
			if core < b.N%numCores {
				iters++
			}
			for j := 0; j < iters; j++ {
				if _, err := conn.Write(frame); err != nil {
					return
				}
				if _, err := io.ReadFull(conn, buf); err != nil {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	b.StopTimer()
}

// generateSelfSignedCert creates a self-signed ECDSA P-256 certificate and key.
func generateSelfSignedCert() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Tellstone Benchmark"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

func writeTempCertFiles(certPEM, keyPEM []byte) (certFile, keyFile string, err error) {
	cf, err := os.CreateTemp("", "bench-cert-*.pem")
	if err != nil {
		return "", "", err
	}
	defer cf.Close()
	if _, err := cf.Write(certPEM); err != nil {
		return "", "", err
	}

	kf, err := os.CreateTemp("", "bench-key-*.pem")
	if err != nil {
		return "", "", err
	}
	defer kf.Close()
	if _, err := kf.Write(keyPEM); err != nil {
		return "", "", err
	}
	return cf.Name(), kf.Name(), nil
}
