/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: handshake_test.go
Description: Coverage for enforcing the TLS handshake deadline without inbound traffic: silent
implicit-TLS connections, connections stalled after a STARTTLS acceptance, and connections stalled
mid-ClientHello are all reaped, while an established session survives well past the deadline.

Authors:

	Surafel Workayehu
*/
package resp

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
)

// sweepTestTimeout is the handshake deadline used by these tests. It is long enough that a loaded
// CI runner cannot mistake scheduling delay for a timeout, and short enough that four sequential
// tests stay well inside the package timeout. The sweep interval derives from it (deadline/10).
const sweepTestTimeout = 400 * time.Millisecond

func TestRESPServer_StalledSTARTTLSTimesOut(t *testing.T) {
	addr, _ := startHandshakeServer(t, true)
	raw := dialWithRetry(t, addr)
	defer raw.Close()

	// Timed from before the upgrade request: the server starts its deadline inside
	// upgradeToTLS, which cannot happen before this point, so a correct close can never look
	// early. Timing from after the +OK arrives would leave the assertion racing the round trip.
	start := time.Now()
	expectReply(t, raw, "STARTTLS", startTLSRequest, "+OK\r\n")
	// The upgrade is accepted and then the client goes silent: no ClientHello, no further
	// bytes, so nothing will ever call OnTraffic for this connection again.
	assertClosedAfterDeadline(t, raw, "connection stalled after STARTTLS", start)
}

func TestRESPServer_StalledClientHelloTimesOut(t *testing.T) {
	addr, _ := startHandshakeServer(t, false)
	// Implicit TLS is tracked in OnOpen, so the deadline starts at accept — time from before
	// the dial.
	start := time.Now()
	raw := dialWithRetry(t, addr)
	defer raw.Close()

	// A handshake record announcing 512 bytes of ClientHello, followed by two of them: the TLS
	// state machine parks on ErrNotEnough waiting for the rest, which never arrives.
	partialHello := []byte{0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00}
	if _, err := raw.Write(partialHello); err != nil {
		t.Fatalf("write partial ClientHello: %v", err)
	}
	assertClosedAfterDeadline(t, raw, "connection stalled mid-ClientHello", start)
}

func TestRESPServer_SilentImplicitTLSTimesOut(t *testing.T) {
	addr, _ := startHandshakeServer(t, false)
	start := time.Now()
	raw := dialWithRetry(t, addr)
	defer raw.Close()

	// Not a single byte is sent, so the deadline set at accept time is the only thing that can
	// release the socket and its 4 KiB TLS read buffer.
	assertClosedAfterDeadline(t, raw, "silent implicit-TLS connection", start)
}

func TestRESPServer_EstablishedConnectionSurvivesDeadline(t *testing.T) {
	addr, certPEM := startHandshakeServer(t, false)
	conn, err := stdtls.Dial("tcp", addr, startTLSClientConfig(t, certPEM))
	if err != nil {
		t.Fatalf("implicit TLS dial: %v", err)
	}
	defer conn.Close()

	expectReply(t, conn, "PING before the deadline", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
	// Idle across several sweeps: a completed handshake must be forgotten by the sweeper, not
	// closed by it.
	time.Sleep(3 * sweepTestTimeout)
	expectReply(t, conn, "PING after the deadline", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n")
}

func TestRESPServer_ConcurrentHandshakesUnderSweep(t *testing.T) {
	// Enough connections to spread across every event loop, so `track` runs on several loops
	// while the ticker goroutine sweeps. One connection at a time would leave the registry
	// boundary — the whole risk of this design — unexercised under -race.
	const stalledConns, liveConns = 24, 8
	addr, certPEM := startHandshakeServer(t, false)
	clientCfg := startTLSClientConfig(t, certPEM) // built here: the helper may call t.Fatal

	errs := make(chan error, stalledConns+liveConns)
	var wg sync.WaitGroup

	for i := 0; i < stalledConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errs <- fmt.Errorf("dial stalled connection: %w", err)
				return
			}
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(5 * sweepTestTimeout))
			var b [1]byte
			_, err = conn.Read(b[:])
			var netErr net.Error
			switch {
			case err == nil:
				errs <- fmt.Errorf("stalled connection got a reply %q instead of a close", b[:])
			case errors.As(err, &netErr) && netErr.Timeout():
				errs <- errors.New("stalled connection still open past the deadline")
			}
		}()
	}

	for i := 0; i < liveConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := stdtls.Dial("tcp", addr, clientCfg)
			if err != nil {
				errs <- fmt.Errorf("dial established connection: %w", err)
				return
			}
			defer conn.Close()
			if err := pingOnce(conn); err != nil {
				errs <- fmt.Errorf("ping before the deadline: %w", err)
				return
			}
			time.Sleep(3 * sweepTestTimeout)
			if err := pingOnce(conn); err != nil {
				errs <- fmt.Errorf("established session died while idle: %w", err)
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// pingOnce round-trips one PING. Unlike expectReply it returns the error instead of calling
// t.Fatalf, so it is safe to use from a goroutine other than the test's own.
func pingOnce(conn net.Conn) error {
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	var got [7]byte
	if _, err := io.ReadFull(conn, got[:]); err != nil {
		return err
	}
	if string(got[:]) != "+PONG\r\n" {
		return fmt.Errorf("got %q want %q", got[:], "+PONG\r\n")
	}
	return nil
}

// startHandshakeServer starts a TLS-enabled RESP server whose handshake deadline is
// sweepTestTimeout, returning its address and the certificate a client should trust. The deadline
// is installed before ListenAndServe because the sweeper goroutine reads it.
func startHandshakeServer(t *testing.T, startTLS bool) (string, []byte) {
	t.Helper()
	certPEM, keyPEM := generateStartTLSCertificate(t)
	configs, err := tlslib.NewConfigStore(buildStartTLSConfig(t, certPEM, keyPEM))
	if err != nil {
		t.Fatalf("create TLS config store: %v", err)
	}

	addr := freeAddr(t)
	srv := NewServer(addr, newFakeStore(), nil, log.NewNoOpLogger(), configs, "", startTLS, nil, nil, newNoOpAudit())
	srv.handshakeTimeout = sweepTestTimeout
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	probe := dialWithRetry(t, addr)
	_ = probe.Close()
	return addr, certPEM
}

// assertClosedAfterDeadline fails unless the server closed conn on its own and waited out the
// deadline first. start must be taken before whatever begins handshake tracking, so that the
// server's clock starts at or after it — otherwise a correct close can appear early and the test
// flakes. Without the lower bound, a regression that closed every TLS connection on accept would
// still pass.
func assertClosedAfterDeadline(t *testing.T, conn net.Conn, name string, start time.Time) {
	t.Helper()
	assertClosedByServer(t, conn, name)
	if elapsed := time.Since(start); elapsed < sweepTestTimeout {
		t.Fatalf("%s: closed after %v, before the %v deadline", name, elapsed, sweepTestTimeout)
	}
}

// assertClosedByServer fails unless the server closes conn on its own. The read deadline allows
// several sweep intervals of slack so a busy runner cannot fail the test.
func assertClosedByServer(t *testing.T, conn net.Conn, name string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * sweepTestTimeout)); err != nil {
		t.Fatalf("%s: set read deadline: %v", name, err)
	}
	var b [1]byte
	n, err := conn.Read(b[:])
	if n != 0 {
		t.Fatalf("%s: server replied %q instead of closing", name, b[:n])
	}
	if err == nil {
		t.Fatalf("%s: read returned no data and no error", name)
	}
	// Any non-timeout error means the peer went away (EOF on a graceful close, ECONNRESET when
	// the kernel discards a half-open socket); only the read deadline expiring proves the
	// server still holds the connection.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("%s: still open after %v (deadline %v)", name, 5*sweepTestTimeout, sweepTestTimeout)
	}
}

// BenchmarkHandshakeSweep measures the steady-state scan: entries whose handshake has neither
// completed nor expired, which is what every tick walks while connections are negotiating. The
// backing array is reused, so the sweep must not allocate.
func BenchmarkHandshakeSweep(b *testing.B) {
	const tracked = 1024
	h := &handshakeSweeper{}
	deadline := time.Now().Add(time.Hour)
	for i := 0; i < tracked; i++ {
		// A Conn that never completes its handshake. sweep reads only the atomic flag, so it
		// needs neither a socket nor a configuration.
		h.track(nil, tlslib.Server(nil, nil), deadline)
	}
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.sweep(now)
	}
}
