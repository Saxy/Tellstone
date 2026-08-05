/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: handshake.go
Description: Traffic-independent enforcement of the TLS handshake deadline. Connections handed to
the TLS state machine — on accept for implicit TLS, on upgrade for STARTTLS — are registered here
and swept from the gnet engine ticker, so a client that stops sending after the transition cannot
hold a socket, its read buffer, and its TLS state past the deadline.

Authors:

	Surafel Workayehu
*/
package resp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
	"github.com/panjf2000/gnet/v2"
)

// pendingHandshake tracks one connection that has been handed to the TLS state machine but has
// not completed its handshake yet.
//
// c, tlsConn, and deadline are published once by the event loop that owns the connection and are
// never mutated afterwards, so the sweeper goroutine only ever reads immutable fields plus two
// concurrency-safe calls: gnet.Conn.Close enqueues the close on the owning event loop, and
// tlslib.Conn.HandshakeCompleted is a single atomic load. *connState is deliberately absent here —
// it is owned by its event loop and must not be read from the ticker.
type pendingHandshake struct {
	c        gnet.Conn
	tlsConn  *tlslib.Conn
	deadline time.Time
	// done is set by the event loop in OnClose so the sweeper forgets a connection that died
	// before handshaking instead of reporting it as a timeout. TCP health probes and port
	// scanners take exactly that path, and they must not produce a warning per probe.
	done atomic.Bool
}

// handshakeSweeper enforces the TLS handshake deadline independently of inbound traffic. Checking
// the deadline in OnTraffic alone is not enough: a client that completes the STARTTLS exchange (or
// opens an implicit-TLS connection) and then sends nothing never triggers another event, so its
// socket, read buffer, and TLS state are held until the client gives up — an unauthenticated
// resource-exhaustion path. gnet v2.10 has no per-connection timer, and its unix connections
// reject SetReadDeadline with ErrUnsupportedOp, which leaves the engine ticker as the only place a
// silent socket can be reaped from.
//
// Event loops only append; the sweeper is the only remover. That keeps the accept path to a single
// append under an uncontended mutex, and the backing array is reused across sweeps, so a steady
// state of n concurrent handshakes settles at zero allocations per tick.
type handshakeSweeper struct {
	mu      sync.Mutex
	pending []*pendingHandshake
}

// track registers c for deadline enforcement and returns the entry so the connection can mark
// itself done when it closes. Called once per TLS connection — on accept for implicit TLS, on
// upgrade for STARTTLS — never per request.
func (h *handshakeSweeper) track(c gnet.Conn, tlsConn *tlslib.Conn, deadline time.Time) *pendingHandshake {
	p := &pendingHandshake{c: c, tlsConn: tlsConn, deadline: deadline}
	h.mu.Lock()
	h.pending = append(h.pending, p)
	h.mu.Unlock()
	return p
}

// sweep closes every tracked connection that has not completed its handshake by now, drops the
// entries that no longer need watching, and returns how many connections it closed.
//
// A completed handshake is forgotten by the first sweep that observes it, which is why an
// established connection can never be closed by a later sweep. Closing an already-closed gnet
// connection is a no-op — the event loop ignores stale connections — so losing the race against a
// concurrent close costs nothing.
func (h *handshakeSweeper) sweep(now time.Time) int {
	closed := 0
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := 0; i < len(h.pending); {
		p := h.pending[i]
		switch {
		case p.done.Load() || p.tlsConn.HandshakeCompleted():
			// Nothing left to enforce; the entry is dropped below.
		case now.Before(p.deadline):
			i++
			continue
		default:
			// Close only enqueues the close on the owning event loop, so the sweeper never
			// waits on a socket while holding the mutex.
			_ = p.c.Close()
			closed++
		}
		// Swap-delete: sweep order carries no meaning, so a removal must not shift the tail.
		// Clearing the vacated slot drops the last reference to the connection.
		last := len(h.pending) - 1
		h.pending[i] = h.pending[last]
		h.pending[last] = nil
		h.pending = h.pending[:last]
	}
	return closed
}

// OnTick sweeps the pre-handshake registry. gnet runs it on a dedicated goroutine started
// alongside the event loops, never on one of them, so it must touch only concurrency-safe state —
// see handshakeSweeper. The interval is a tenth of the deadline: a stalled socket is then reaped
// within 10% of it, and a single knob (handshakeTimeout) shrinks both for tests.
func (s *Server) OnTick() (time.Duration, gnet.Action) {
	if n := s.handshakes.sweep(time.Now()); n > 0 && s.logger.Enabled(log.LevelWarn) {
		s.logger.Log(log.LevelWarn, "resp: closing connections that missed the TLS handshake deadline",
			log.Int("connections", n),
		)
	}
	return s.handshakeTimeout / 10, gnet.None
}
