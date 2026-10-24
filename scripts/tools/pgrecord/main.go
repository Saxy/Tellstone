/*
Command pgrecord
Tellstone PostgreSQL Client Compatibility Recorder
File: main.go
Description: A PostgreSQL-aware proxy that sits between a client (psql, pgJDBC,
DataGrip) and a Tellstone PG-wire listener, and appends every SQL statement the
client sends to a JSONL transcript. The transcript is the golden input the
compatibility tests replay (internal/sql/transcript_test.go), so the SQL
surface is driven by what a real client actually sends rather than by guesswork.

The dev cluster refuses cleartext clients ("SSL is required on this
connection"), so the SQL never crosses a socket a byte relay could read. The
proxy therefore terminates TLS on the client leg with the cluster's own
certificate -- the client keeps the sslmode=require it was already using -- and
dials the upstream leg with TLS itself. Only the decrypted stream is parsed.

Startup-phase frames (SSLRequest, GSSENCRequest, StartupMessage) are
4-byte-length-prefixed and their first length byte is 0x00; typed messages
start with an ASCII type byte (>= 0x41). The negotiation frames are answered
by the proxy itself -- a JDBC client sends GSSENCRequest before it asks for
TLS -- and only the decrypted stream is parsed for typed messages, whose
SQL is read out of Query (Q) and Parse (P).

Usage:

	pgrecord --listen 127.0.0.1:55433 --upstream 127.0.0.1:47101 \
	    --out internal/sql/testdata/transcripts/datagrip-introspection.jsonl

Then point the client at the --listen address, keeping its TLS settings.
*/
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"crypto/tls"
)

// frontend message types whose payload carries SQL.
const (
	msgQuery = 'Q'
	msgParse = 'P'
)

// Protocol constants (frontend protocol 3.0 startup negotiation).
const (
	sslRequestCode    = 80877103
	gssEncRequestCode = 80877104
	cancelRequestCode = 80877102
)

func main() {
	listen := flag.String("listen", "127.0.0.1:55433", "address to listen on")
	upstream := flag.String("upstream", "127.0.0.1:47101", "Tellstone PG-wire address to proxy to")
	out := flag.String("out", "", "JSONL transcript file to append to (required)")
	tlsCert := flag.String("tls-cert", "", "certificate PEM served to the client (default: the local cluster's)")
	tlsKey := flag.String("tls-key", "", "private key PEM served to the client (default: the local cluster's)")
	upstreamTLS := flag.String("upstream-tls", "auto", "TLS to the upstream: auto, on or off")
	traceFlag := flag.Bool("trace", false, "log every protocol frame (direction, type, length) to stderr")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "pgrecord: --out is required")
		os.Exit(2)
	}

	srvTLS, err := clientTLSConfig(*tlsCert, *tlsKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgrecord: client TLS: %v\n", err)
		os.Exit(1)
	}
	if srvTLS == nil {
		fmt.Fprintln(os.Stderr, "pgrecord: no certificate, replying 'N' to SSLRequest: the client must be allowed to fall back to cleartext")
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgrecord: listen %s: %v\n", *listen, err)
		os.Exit(1)
	}
	defer ln.Close()
	fmt.Fprintf(os.Stderr, "pgrecord: listening on %s -> %s, writing %s\n", *listen, *upstream, *out)

	rec := &recorder{out: *out, trace: *traceFlag}
	for {
		client, err := ln.Accept()
		if err != nil {
			return
		}
		go rec.handle(client, *upstream, *upstreamTLS, srvTLS)
	}
}

// clientTLSConfig loads the certificate the proxy serves to clients. With no
// flags given it uses the local dev cluster's certificate, so a client keeps
// the TLS settings it already had -- only the port changes. It returns nil
// when no certificate is available, which makes the proxy answer SSLRequest
// with 'N' instead of pretending to terminate TLS.
func clientTLSConfig(certPath, keyPath string) (*tls.Config, error) {
	if certPath == "" && keyPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, nil
		}
		base := filepath.Join(home, ".tellstone", "local-cluster", "tls")
		certPath = filepath.Join(base, "cert.pem")
		keyPath = filepath.Join(base, "key.pem")
	}
	if certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("--tls-cert and --tls-key must be given together")
	}
	if _, err := os.Stat(certPath); err != nil {
		return nil, nil
	}
	if _, err := os.Stat(keyPath); err != nil {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

// dialUpstream opens the upstream connection. The dev cluster requires TLS,
// but it negotiates it the classic PostgreSQL way -- a plaintext SSLRequest,
// an 'S' reply, then the handshake on the same socket -- so a bare ClientHello
// is a protocol error rather than a handshake. A plaintext dev cluster
// (RBAC=0) or a real PostgreSQL that answers 'N' is reached in cleartext, which
// is what --upstream-tls=auto means.
func dialUpstream(addr, mode string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if mode == "off" {
		return dialer.Dial("tcp", addr)
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(sslRequestFrame()); err != nil {
		conn.Close()
		return nil, err
	}
	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		conn.Close()
		return nil, err
	}
	if reply[0] != 'S' {
		// The upstream refused TLS. auto accepts it and records in cleartext;
		// on refuses to, because the recording would be the cleartext the
		// cluster explicitly does not offer.
		if mode == "on" {
			conn.Close()
			return nil, fmt.Errorf("upstream refused TLS (replied %q)", reply[0])
		}
		return conn, nil
	}
	tconn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	if err := tconn.Handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	return tconn, nil
}

// sslRequestFrame is the 8-byte frame a client sends to negotiate TLS.
func sslRequestFrame() []byte {
	f := make([]byte, 8)
	binary.BigEndian.PutUint32(f[:4], 8)
	binary.BigEndian.PutUint32(f[4:], sslRequestCode)
	return f
}

// recorder appends statements to one transcript file under a mutex, so two
// connections writing at once cannot interleave a line.
type recorder struct {
	mu    sync.Mutex
	out   string
	trace bool
}

// tracef logs one protocol observation when --trace is set. Every frontend
// and backend frame after the startup negotiation is logged with its
// direction, type byte and length, which is what shows the last message a
// session carried before it closed. The millisecond timestamp is what turns
// the trace into a latency measurement: the gap between the server's final
// ReadyForQuery and the client's next Parse is client processing time, and
// the gap from Execute to the first response frame is the server's.
func (r *recorder) tracef(format string, args ...any) {
	if r.trace {
		rest := append([]any{time.Now().Format("15:04:05.000")}, args...)
		fmt.Fprintf(os.Stderr, "pgrecord: %s "+format+"\n", rest...)
	}
}

func (r *recorder) handle(client net.Conn, upstream, upstreamTLS string, srvTLS *tls.Config) {
	defer client.Close()
	server, err := dialUpstream(upstream, upstreamTLS)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgrecord: dial upstream %s: %v\n", upstream, err)
		return
	}
	defer server.Close()

	rd := bufio.NewReader(client)
	// Startup negotiation: a JDBC client (pgJDBC, DataGrip) sends a
	// GSSENCRequest before it even asks for TLS, and both negotiation frames
	// are answered here rather than forwarded -- a forwarded reply would
	// arrive as a raw byte in the relay, which pump cannot parse. The client
	// then sends its StartupMessage, which is the first frame forwarded
	// upstream.
	for {
		hdr, err := readHeader(rd)
		if err != nil {
			return
		}
		n := int(binary.BigEndian.Uint32(hdr))
		if n != 8 {
			// The StartupMessage (or a CancelRequest): forward it and let the
			// relay carry the session.
			body := make([]byte, n-4)
			if _, err := io.ReadFull(rd, body); err != nil {
				return
			}
			if _, err := server.Write(append(hdr, body...)); err != nil {
				return
			}
			r.tracef("c->s startup frame len=%d", n)
			r.relay(client, server, rd)
			return
		}
		body := make([]byte, 4)
		if _, err := io.ReadFull(rd, body); err != nil {
			return
		}
		switch binary.BigEndian.Uint32(body) {
		case sslRequestCode:
			// The client asked for TLS. Terminating it is the only way to see
			// the SQL that follows: upstream demands TLS and the cluster has
			// no cleartext listener to record against.
			r.tracef("ssl request from client (answered locally, TLS terminates here)")
			if srvTLS == nil {
				if _, err := client.Write([]byte{'N'}); err != nil {
					return
				}
			} else {
				if _, err := client.Write([]byte{'S'}); err != nil {
					return
				}
				tconn := tls.Server(client, srvTLS)
				if err := tconn.Handshake(); err != nil {
					fmt.Fprintf(os.Stderr, "pgrecord: client tls: %v\n", err)
					return
				}
				client = tconn
				rd = bufio.NewReader(tconn)
			}
		case gssEncRequestCode:
			// No GSSAPI support, which is what PostgreSQL answers when GSS
			// encryption is off. The client falls back to SSLRequest.
			if _, err := client.Write([]byte{'N'}); err != nil {
				return
			}
		default:
			// A CancelRequest (or any other short frame): forward it verbatim
			// and keep the session on its usual path.
			if _, err := server.Write(append(hdr, body...)); err != nil {
				return
			}
			r.relay(client, server, rd)
			return
		}
	}
}

// relay runs both directions until either ends: the backend half verbatim, the
// frontend half through pump so the SQL of each Q/P message is recorded. Whichever
// half ends first ends the session -- which is why --trace logs which one it was.
func (r *recorder) relay(client, server net.Conn, rd *bufio.Reader) {
	done := make(chan string, 2)
	go func() {
		if r.trace {
			r.backendTrace(client, server)
		} else {
			_, _ = io.Copy(client, server)
		}
		done <- "backend half ended"
	}()
	go func() {
		r.pump(rd, server)
		done <- "frontend half ended"
	}()
	r.tracef("relay stops: %s", <-done)
}

// pump forwards client -> server, recording the SQL of each Q/P message. It
// stops on any read or write error, which is the normal end of a session.
func (r *recorder) pump(rd *bufio.Reader, server net.Conn) {
	for {
		// Typed message: 1 type byte + 4-byte length. The startup phase is
		// over by now, so no frame begins with 0x00.
		var hdr [5]byte
		if _, err := io.ReadFull(rd, hdr[:]); err != nil {
			r.tracef("c->s stopped reading: %v", err)
			return
		}
		n := int(binary.BigEndian.Uint32(hdr[1:]))
		if n < 4 || n > 64<<20 {
			r.tracef("c->s bad frame length %d (type %q)", n, hdr[0])
			return
		}
		body := make([]byte, n-4)
		if _, err := io.ReadFull(rd, body); err != nil {
			r.tracef("c->s truncated %q body: %v", hdr[0], err)
			return
		}
		if _, err := server.Write(append(hdr[:], body...)); err != nil {
			r.tracef("c->s forward %q failed: %v", hdr[0], err)
			return
		}
		r.tracef("c->s %q len=%d", hdr[0], n)
		switch hdr[0] {
		case msgQuery:
			if q, ok := cstring(body); ok {
				r.record(q)
			}
		case msgParse:
			// Parse: statement name (cstring), then the query (cstring).
			_, rest, _ := consumeCString(body)
			if q, ok := cstring(rest); ok {
				r.record(q)
			}
		}
	}
}

// backendTrace copies server -> client as parsed frames when --trace is set,
// so the last message a session carried is visible; without --trace the copy
// stays a plain io.Copy in relay.
func (r *recorder) backendTrace(dst, src net.Conn) {
	for {
		var hdr [5]byte
		if _, err := io.ReadFull(src, hdr[:]); err != nil {
			r.tracef("s->c stopped reading: %v", err)
			return
		}
		n := int(binary.BigEndian.Uint32(hdr[1:]))
		if n < 4 || n > 64<<20 {
			r.tracef("s->c bad frame length %d (type %q)", n, hdr[0])
			return
		}
		body := make([]byte, n-4)
		if _, err := io.ReadFull(src, body); err != nil {
			r.tracef("s->c truncated %q body: %v", hdr[0], err)
			return
		}
		if _, err := dst.Write(append(hdr[:], body...)); err != nil {
			r.tracef("s->c forward %q failed: %v", hdr[0], err)
			return
		}
		r.tracef("s->c %q len=%d", hdr[0], n)
	}
}

// readHeader reads the 4-byte length prefix of a startup-phase frame.
func readHeader(rd *bufio.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return nil, err
	}
	return hdr[:], nil
}

// record appends one SQL statement to the transcript as a JSON string line.
func (r *recorder) record(sql string) {
	if strings.TrimSpace(sql) == "" {
		return
	}
	b, err := json.Marshal(sql)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	f, err := os.OpenFile(r.out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgrecord: open %s: %v\n", r.out, err)
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

// cstring reads a NUL-terminated string from b, returning it without the NUL.
func cstring(b []byte) (string, bool) {
	s, _, ok := consumeCString(b)
	return s, ok
}

// consumeCString splits b at its first NUL, returning the string, the remainder
// after the NUL, and whether a NUL was found.
func consumeCString(b []byte) (string, []byte, bool) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], true
		}
	}
	return "", nil, false
}
