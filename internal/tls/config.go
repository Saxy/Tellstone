/*
File: config.go
Description: Provides a gnet.Conn to net.Conn adapter for use with the TLS
handshake, and a config builder that constructs TLS configurations from
file paths. Supports TLS 1.3 and optional mTLS.
*/
package tls

import (
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// GnetConnAdapter wraps a gnet.Conn to satisfy net.Conn and expose the
// Peek/InboundBuffered methods that the TLS library requires for non-blocking
// handshake negotiation.
type GnetConnAdapter struct {
	c gnet.Conn
}

// NewGnetConnAdapter wraps a gnet.Conn so it can be passed to the TLS library.
func NewGnetConnAdapter(c gnet.Conn) *GnetConnAdapter {
	return &GnetConnAdapter{c: c}
}

func (a *GnetConnAdapter) Read(b []byte) (int, error) {
	buf, err := a.c.Peek(len(b))
	if len(buf) > 0 {
		n := copy(b, buf)
		_, _ = a.c.Discard(n)
		return n, nil
	}
	if err != nil {
		return 0, ErrNotEnough
	}
	return 0, nil
}

func (a *GnetConnAdapter) Write(b []byte) (int, error) {
	return a.c.Write(b)
}

func (a *GnetConnAdapter) Close() error {
	return a.c.Close()
}

func (a *GnetConnAdapter) LocalAddr() net.Addr  { return a.c.LocalAddr() }
func (a *GnetConnAdapter) RemoteAddr() net.Addr { return a.c.RemoteAddr() }

// SetDeadline is a no-op. Timeout enforcement is handled by the gnet event loop.
func (a *GnetConnAdapter) SetDeadline(t time.Time) error { return nil }

// SetReadDeadline is a no-op. Timeout enforcement is handled by the gnet event loop.
func (a *GnetConnAdapter) SetReadDeadline(t time.Time) error { return nil }

// SetWriteDeadline is a no-op. Timeout enforcement is handled by the gnet event loop.
func (a *GnetConnAdapter) SetWriteDeadline(t time.Time) error { return nil }

// Peek returns the next n bytes without advancing the read cursor.
// Required by the TLS library for non-blocking handshake negotiation.
func (a *GnetConnAdapter) Peek(n int) ([]byte, error) {
	return a.c.Peek(n)
}

// InboundBuffered returns the number of bytes currently buffered in the
// read buffer. Required by the TLS library for non-blocking handshake negotiation.
func (a *GnetConnAdapter) InboundBuffered() int {
	return a.c.InboundBuffered()
}

// BuildConfig constructs a *Config from cert/key/CA file paths.
// If certPath and keyPath are empty, returns (nil, nil) to signal TLS is disabled.
// If caPath is non-empty, client certificate verification is enabled (mTLS).
// Forces TLS 1.3 as the minimum version.
func BuildConfig(certPath, keyPath, caPath string) (*Config, error) {
	if (certPath == "") != (keyPath == "") {
		return nil, fmt.Errorf("tls: both cert and key are required")
	}
	if certPath == "" && keyPath == "" {
		return nil, nil
	}

	cert, err := LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("tls: failed to load certificate/key: %w", err)
	}

	cfg := &Config{
		Certificates: []Certificate{cert},
		MinVersion:   VersionTLS13,
		MaxVersion:   VersionTLS13,
	}

	if caPath != "" {
		caPEM, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("tls: failed to read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls: failed to parse CA certificate")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = RequireAndVerifyClientCert
	}

	return cfg, nil
}

// IsMTLS returns true if the config requires client certificate verification.
func IsMTLS(cfg *Config) bool {
	return cfg != nil && cfg.ClientAuth == RequireAndVerifyClientCert
}
