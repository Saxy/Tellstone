/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: server.go
Description: Optional gnet event-loop server speaking RESP2, reusing the shared storage engine
via a small Store interface. Supports PING, GET, SET (with optional EX/PX), DEL, and AUTH;
unknown commands return an error without dropping the connection. Exists so Tellstone can be
driven by standard Redis tooling (redis-benchmark, memtier_benchmark) for cross-system
comparison. Supports optional implicit TLS 1.3 or an explicit STARTTLS in-place upgrade via the
internal TLS library. When a server password is configured (--require-pass / TSD_REQUIRE_PASS), connections must
authenticate via AUTH before issuing commands other than PING and QUIT.

Authors:

	Maximilian Hagen
*/
package resp

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
	"github.com/Saxy/Tellstone/internal/rbac"
	"github.com/Saxy/Tellstone/internal/shard"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
	"github.com/panjf2000/gnet/v2"
	"golang.org/x/crypto/bcrypt"
)

const tlsHandshakeTimeout = 10 * time.Second

// maxAuthFails closes the connection after this many failed AUTH attempts,
// throttling bcrypt-fuelled credential stuffing (mirrors the binary listener).
const maxAuthFails = 3

// numAuthWorkers and authQueueCap bound the bcrypt worker pool. bcrypt at cost
// 10 takes ~50-80ms, so a flood of AUTH commands must not block the gnet event
// loop: workers run the compare off-loop and dispatchAuth fails fast when the
// queue is saturated instead of stalling the connection.
const (
	numAuthWorkers = 4
	authQueueCap   = 256
)

// Store is the subset of the storage engine the RESP server needs. *storage.Engine satisfies
// it directly, which keeps this package decoupled and easy to test with a fake.
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, ttl time.Duration) error
	Delete(key string)
}

// authJob carries one AUTH verification request from the event loop to the
// bcrypt worker pool. password is a private copy: the worker must not read the
// gnet buffer after the event loop has moved on. passHash nil marks a nopass
// user that accepts any password (Redis ACL semantics).
type authJob struct {
	c        gnet.Conn
	password []byte
	passHash []byte
	username string
	reason   string
	session  *rbac.SessionContext
	// oauth marks a bearer-token verification: password holds the raw JWT,
	// passHash is nil, and the session is built only after claims resolve to a
	// role via the policy's oauth.rules.
	oauth bool
}

// connState holds per-connection scratch buffers reused across OnTraffic calls so the hot
// path stays allocation-free, plus the assigned shard index for per-shard metrics.
type connState struct {
	out     []byte
	args    [][]byte
	shardID int
	tlsConn *tlslib.Conn
	readBuf []byte
	// pending is the sweeper entry enforcing this connection's handshake deadline without
	// inbound traffic. It is non-nil whenever tlsConn is non-nil — both are installed
	// together, on accept for implicit TLS and in upgradeToTLS for STARTTLS.
	pending       *pendingHandshake
	authenticated bool
	// session is the RBAC context pinned at AUTH time. nil when RBAC is
	// disabled (the zero-overhead no-op path) or before authentication.
	session    *rbac.SessionContext
	remoteAddr string
	// closeAfterReply is set by dispatch (QUIT) so the traffic loop flushes the pending
	// replies and then returns gnet.Close instead of keeping the connection open.
	closeAfterReply bool
	// upgradeTLS is set only after a valid STARTTLS command. The plaintext traffic loop
	// owns the transition so +OK can be flushed before TLS consumes the next inbound byte.
	upgradeTLS bool
	// authPending is set while an AUTH verification is in flight in the worker
	// pool. While set, the traffic loops return gnet.None so pipelined commands
	// cannot run unauthenticated; the worker's Wake clears it and re-triggers
	// processing. authFails counts failed AUTH attempts against maxAuthFails.
	authPending bool
	authFails   int
}

// Server is an edge-triggered RESP2 listener backed by gnet.
type Server struct {
	gnet.BuiltinEventEngine
	addr       string
	store      Store
	logger     log.Logger
	tlsConfigs *tlslib.ConfigStore
	startTLS   bool
	// eng and ready let Shutdown reach the running gnet engine: OnBoot fires once the event
	// loop is accepting connections and hands us the Engine handle we need to stop it; ready
	// is closed at that point so a concurrent Shutdown call can block until it's safe to stop.
	eng              gnet.Engine
	ready            chan struct{}
	connectedClients uint64
	totalConnections uint64
	bytesRead        uint64
	bytesWritten     uint64
	protocolErrors   uint64
	shards           []*shard.Shard
	nextConn         uint64
	// requirePassHash is the bcrypt hash of the server password. nil means AUTH is not
	// required and every connection starts authenticated (zero-overhead no-op path).
	// Ignored when a policy store is configured — per-user RBAC supersedes it.
	requirePassHash []byte
	// policy is the atomic RBAC policy store. nil means RBAC is disabled and no
	// authorization checks run (zero-overhead no-op path). When set, AUTH resolves
	// per-user bcrypt hashes and every data command is gated by the session.
	policy *rbac.Store

	// oauth is the token-verification provider for bearer-token AUTH. nil keeps
	// the password-only paths; when set, a JWT-shaped AUTH secret is verified
	// here and mapped to a role through the policy store (fail-closed on both
	// a bad token and a claim set that maps to no role). It is read-only after
	// construction, so workers may share it without locks.
	oauth oauth.Provider

	// authJobs and workerWg back the bcrypt worker pool, created only when a
	// password or a policy is configured (see NewServer). authWorker goroutines
	// consume authJobs off the event loop; workerWg lets Shutdown drain them.
	authJobs chan authJob
	workerWg sync.WaitGroup

	// audit is the shared audit engine. Always non-nil: without --enable-audit
	// it is a disabled no-op whose Record() costs one bool comparison, so the
	// hooks below call it without a nil guard.
	audit *audit.LogEngine

	// handshakes enforces handshakeTimeout on connections that stop sending after being handed
	// to the TLS state machine (see handshake.go). handshakeTimeout is tlsHandshakeTimeout;
	// tests shrink it to keep the sweep observable without waiting out the production deadline.
	handshakes       handshakeSweeper
	handshakeTimeout time.Duration
}

// NewServer creates a RESP server bound to addr that dispatches commands to store.
// shards is optional — if nil, per-shard metrics are not tracked.
// tlsConfigs is optional — if nil, plaintext TCP is used. When configured, each
// accepted connection atomically loads the latest immutable TLS configuration.
// requirePass is optional — if empty, AUTH is a no-op and connections start authenticated;
// otherwise it is hashed once at startup and clients must AUTH before issuing commands.
// startTLS keeps the RESP listener plaintext until a client successfully issues STARTTLS.
// policy is optional — if nil, RBAC is disabled and every authenticated command is allowed;
// otherwise AUTH resolves per-user credentials and sessions gate data commands.
// audit is the shared audit engine; it must be non-nil (pass a disabled engine when
// audit logging is off) and is always called without a nil guard.
func NewServer(addr string, store Store, shards []*shard.Shard, logger log.Logger, tlsConfigs *tlslib.ConfigStore, requirePass string, startTLS bool, policy *rbac.Store, provider oauth.Provider, audit *audit.LogEngine) *Server {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	var passHash []byte
	if requirePass != "" && policy == nil {
		var err error
		passHash, err = bcrypt.GenerateFromPassword([]byte(requirePass), bcrypt.DefaultCost)
		if err != nil {
			// bcrypt only fails on invalid cost or a password over 72 bytes — a
			// misconfiguration that must surface at startup, not at first AUTH.
			panic("resp: invalid --require-pass value: " + err.Error())
		}
	}
	s := &Server{
		addr:            addr,
		store:           store,
		shards:          shards,
		logger:          logger,
		tlsConfigs:      tlsConfigs,
		startTLS:        startTLS,
		requirePassHash: passHash,
		policy:          policy,
		oauth:           provider,
		ready:           make(chan struct{}),
		audit:           audit,

		handshakeTimeout: tlsHandshakeTimeout,
	}
	// The worker pool exists only when authentication is actually required, so
	// the zero-overhead no-password path spawns no goroutines.
	if passHash != nil || policy != nil || provider != nil {
		s.authJobs = make(chan authJob, authQueueCap)
		for i := 0; i < numAuthWorkers; i++ {
			s.workerWg.Add(1)
			go s.authWorker()
		}
	}
	return s
}

// ListenAndServe starts the multi-reactor epoll event loop (blocking).
func (s *Server) ListenAndServe() error {
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "resp: event-driven engine initializing", log.String("address", s.addr))
	}
	opts := []gnet.Option{gnet.WithMulticore(true), gnet.WithLogger(log.NewGnetAdapter(s.logger))}
	if s.tlsConfigs.Load() != nil {
		// The ticker exists only to enforce the TLS handshake deadline, so a plaintext listener
		// keeps the original configuration: no ticker goroutine, no timer, no OnTick calls.
		opts = append(opts, gnet.WithTicker(true))
	}
	return gnet.Run(s, "tcp://"+s.addr, opts...)
}

// Shutdown gracefully stops the event loop, waiting for in-flight connections to drain or
// ctx to expire. It blocks until ListenAndServe has reached OnBoot, so it is safe to call
// concurrently with ListenAndServe from another goroutine (e.g. a signal handler).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ready:
	case <-ctx.Done():
		if s.authJobs != nil {
			close(s.authJobs)
		}
		s.workerWg.Wait()
		return ctx.Err()
	}
	// Stop the engine first: this synchronously shuts down all event-loop
	// goroutines, so no more concurrent sends to s.authJobs can occur.
	err := s.eng.Stop(ctx)
	if s.authJobs != nil {
		close(s.authJobs)
	}
	s.workerWg.Wait()
	return err
}

// connectionAuth resolves the starting authentication state for a new connection.
// Without RBAC this mirrors --require-pass: authenticated unless a password is
// required. With RBAC, connections start authenticated only when a nopass
// "default" user exists, pinned to that user's effective role (Redis semantics).
func (s *Server) connectionAuth() (bool, *rbac.SessionContext) {
	if s.policy != nil {
		p := s.policy.Load()
		if p != nil && p.NoPassDefault() {
			return true, rbac.NewSessionContext("default", p.RoleFor("default"))
		}
		return false, nil
	}
	return s.requirePassHash == nil, nil
}

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	close(s.ready)
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, 1)
	atomic.AddUint64(&s.totalConnections, 1)
	shardID := -1
	if len(s.shards) > 0 {
		sid := atomic.AddUint64(&s.nextConn, 1) - 1
		sid = sid % uint64(len(s.shards))
		shardID = int(sid)
		s.shards[sid].IncConnectedClients()
		s.shards[sid].IncTotalConnections()
	}
	authenticated, session := s.connectionAuth()
	st := &connState{
		out:           make([]byte, 0, 4096),
		args:          make([][]byte, 0, 8),
		shardID:       shardID,
		authenticated: authenticated,
		session:       session,
		remoteAddr:    c.RemoteAddr().String(),
	}
	if tlsCfg := s.tlsConfigs.Load(); tlsCfg != nil && !s.startTLS {
		adapter := tlslib.NewGnetConnAdapter(c)
		st.tlsConn = tlslib.Server(adapter, tlsCfg)
		st.readBuf = make([]byte, 0, 4096)
		st.pending = s.handshakes.track(c, st.tlsConn, time.Now().Add(s.handshakeTimeout))
	}
	c.SetContext(st)
	s.audit.Record(audit.EventConnect, "client connected",
		log.String("remote_addr", st.remoteAddr),
		log.String("protocol", "resp"),
		log.Int("shard_id", shardID),
	)
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "resp: client connected", log.String("remote_addr", st.remoteAddr))
	}
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, ^uint64(0))
	var remoteAddr string
	if st, ok := c.Context().(*connState); ok {
		remoteAddr = st.remoteAddr
		if st.pending != nil {
			// Let the sweeper forget this connection instead of reporting it as a handshake
			// timeout once the deadline passes: a client is free to hang up mid-handshake,
			// and TCP health probes do it on every check.
			st.pending.done.Store(true)
		}
		if st.shardID >= 0 && st.shardID < len(s.shards) {
			s.shards[st.shardID].DecConnectedClients()
		}
	}
	s.audit.Record(audit.EventDisconnect, "client disconnected",
		log.String("remote_addr", remoteAddr),
		log.String("protocol", "resp"),
	)
	if s.logger.Enabled(log.LevelDebug) {
		fields := []log.Field{log.String("remote_addr", remoteAddr)}
		if err != nil {
			fields = append(fields, log.String("error", err.Error()))
		}
		s.logger.Log(log.LevelDebug, "resp: client disconnected", fields...)
	}
	return gnet.None
}

// OnTraffic parses every complete command currently buffered, batches all replies into a
// single write, and advances the inbound buffer once — which makes pipelined workloads
// (redis-benchmark -P / memtier --pipeline) amortize syscalls.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	st, _ := c.Context().(*connState)
	if st == nil {
		authenticated, session := s.connectionAuth()
		st = &connState{
			out:           make([]byte, 0, 4096),
			args:          make([][]byte, 0, 8),
			authenticated: authenticated,
			session:       session,
			remoteAddr:    c.RemoteAddr().String(),
		}
		c.SetContext(st)
	}

	if st.tlsConn != nil {
		// The sweeper enforces the same deadline without traffic; this keeps closing a
		// stalled handshake on the spot when the client does send something.
		if !st.tlsConn.HandshakeCompleted() && time.Now().After(st.pending.deadline) {
			return gnet.Close
		}
		return s.onTrafficTLS(c, st)
	}
	return s.onTrafficPlaintext(c, st)
}

// onTrafficTLS reads decrypted application data from the TLS connection,
// parses RESP frames, and writes encrypted responses.
const maxTLSReadBuf = 64 << 20 // 64 MB — hard ceiling for a single RESP request over TLS.

func (s *Server) onTrafficTLS(c gnet.Conn, st *connState) gnet.Action {
	for {
		if len(st.readBuf) == cap(st.readBuf) {
			if cap(st.readBuf) >= maxTLSReadBuf {
				return gnet.Close
			}
			newCap := cap(st.readBuf) * 2
			if newCap > maxTLSReadBuf {
				newCap = maxTLSReadBuf
			}
			grown := make([]byte, len(st.readBuf), newCap)
			copy(grown, st.readBuf)
			st.readBuf = grown
		}
		n, err := st.tlsConn.Read(st.readBuf[len(st.readBuf):cap(st.readBuf)])
		if n > 0 {
			st.readBuf = st.readBuf[:len(st.readBuf)+n]
			if action := s.handleDecryptedResp(st, c); action != gnet.None {
				return action
			}
		}
		if err != nil {
			if errors.Is(err, tlslib.ErrNotEnough) {
				return gnet.None
			}
			return gnet.Close
		}
	}
}

// processBuffer parses and dispatches every complete command in src, appending
// replies to st.out. It is the shared core of the plaintext and TLS traffic
// paths, whose only differences are the buffer ownership and the STARTTLS
// upgrade. A STARTTLS upgrade flushes the replies and installs TLS state
// before returning. The results tell the caller what to flush: bad means the
// connection must close (malformed frame or failed upgrade), upgraded means
// the replies were already written and the caller must not flush again.
func (s *Server) processBuffer(st *connState, c gnet.Conn, src []byte) (consumed int, upgraded bool, bad bool) {
	st.out = st.out[:0]
	for consumed < len(src) {
		args, n, perr := Parse(src[consumed:], st.args)
		if perr != nil {
			if errors.Is(perr, errIncomplete) {
				break
			}
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "resp: malformed frame; closing connection",
					log.String("remote_addr", c.RemoteAddr().String()),
				)
			}
			return consumed, false, true
		}
		st.args = args[:0]
		consumed += n
		var async bool
		st.out, async = s.dispatch(st, c, args, st.out)
		if async {
			// AUTH was dispatched to the bcrypt worker pool. Stop parsing
			// pipelined commands so they cannot run unauthenticated; the
			// worker's Wake writes the reply and re-triggers processing.
			break
		}
		if st.upgradeTLS {
			if s.upgradeToTLS(c, st, consumed) == gnet.Close {
				return consumed, false, true
			}
			return consumed, true, false
		}
		if st.closeAfterReply {
			// QUIT: stop parsing pipelined commands; flush replies, then close below.
			break
		}
	}
	return consumed, false, false
}

// handleDecryptedResp parses RESP commands from decrypted plaintext and
// writes encrypted responses through the TLS connection. It returns gnet.Close
// on protocol or write errors so the caller propagates the close.
func (s *Server) handleDecryptedResp(st *connState, c gnet.Conn) gnet.Action {
	if st.authPending {
		// An AUTH verification is in flight; the worker's Wake writes the
		// reply and triggers this path again once it completes.
		return gnet.None
	}
	consumed, _, bad := s.processBuffer(st, c, st.readBuf)
	if bad {
		return gnet.Close
	}
	if consumed == 0 {
		return gnet.None
	}
	if len(st.out) > 0 {
		if _, err := st.tlsConn.Write(st.out); err != nil {
			return gnet.Close
		}
		n := uint64(len(st.out))
		atomic.AddUint64(&s.bytesWritten, n)
		if st.shardID >= 0 && st.shardID < len(s.shards) {
			s.shards[st.shardID].AddBytesWritten(n)
		}
	}
	n := uint64(consumed)
	atomic.AddUint64(&s.bytesRead, n)
	if st.shardID >= 0 && st.shardID < len(s.shards) {
		s.shards[st.shardID].AddBytesRead(n)
	}
	remaining := copy(st.readBuf, st.readBuf[consumed:])
	st.readBuf = st.readBuf[:remaining]
	if st.closeAfterReply {
		return gnet.Close
	}
	return gnet.None
}

// hasPipelinedSTARTTLS validates the transition before dispatching any command in the
// inbound buffer. Checking only when STARTTLS reaches dispatch would allow commands before
// it in the same buffer to execute even though the connection is then rejected.
func (s *Server) hasPipelinedSTARTTLS(buf []byte, scratch [][]byte) bool {
	consumed := 0
	for consumed < len(buf) {
		commandStart := consumed
		args, n, err := Parse(buf[consumed:], scratch)
		if err != nil {
			return false
		}
		consumed += n
		if len(args) == 1 && EqualFold(args[0], "STARTTLS") {
			return commandStart != 0 || consumed != len(buf)
		}
	}
	return false
}

// onTrafficPlaintext is the original zero-copy plaintext path.
func (s *Server) onTrafficPlaintext(c gnet.Conn, st *connState) gnet.Action {
	if st.authPending {
		// An AUTH verification is in flight; the worker's Wake writes the
		// reply and triggers this path again once it completes.
		return gnet.None
	}
	buf, err := c.Peek(-1)
	if err != nil {
		return gnet.Close
	}
	if s.startTLS && s.hasPipelinedSTARTTLS(buf, st.args) {
		atomic.AddUint64(&s.protocolErrors, 1)
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "resp: pipelined STARTTLS rejected",
				log.String("remote_addr", st.remoteAddr),
			)
		}
		return gnet.Close
	}
	consumed, upgraded, bad := s.processBuffer(st, c, buf)
	if bad {
		return gnet.Close
	}
	if upgraded {
		// upgradeToTLS already flushed the replies, discarded the consumed bytes,
		// and installed TLS state; the TLS path takes over from here.
		return gnet.None
	}
	if consumed == 0 {
		return gnet.None
	}
	if len(st.out) > 0 {
		if _, err := c.Write(st.out); err != nil {
			return gnet.Close
		}
		n := uint64(len(st.out))
		atomic.AddUint64(&s.bytesWritten, n)
		if st.shardID >= 0 && st.shardID < len(s.shards) {
			s.shards[st.shardID].AddBytesWritten(n)
		}
	}
	if _, err := c.Discard(consumed); err != nil {
		return gnet.Close
	}
	n := uint64(consumed)
	atomic.AddUint64(&s.bytesRead, n)
	if st.shardID >= 0 && st.shardID < len(s.shards) {
		s.shards[st.shardID].AddBytesRead(n)
	}
	if st.closeAfterReply {
		return gnet.Close
	}
	return gnet.None
}

// upgradeToTLS flushes the plaintext acceptance reply before installing TLS state. The
// configuration is loaded at upgrade time, not accept time, so an idle plaintext connection
// observes the latest atomically rotated certificate when it eventually upgrades.
func (s *Server) upgradeToTLS(c gnet.Conn, st *connState, consumed int) gnet.Action {
	tlsCfg := s.tlsConfigs.Load()
	if tlsCfg == nil {
		return gnet.Close
	}
	if _, err := c.Write(st.out); err != nil {
		return gnet.Close
	}
	if err := c.Flush(); err != nil {
		return gnet.Close
	}
	if _, err := c.Discard(consumed); err != nil {
		return gnet.Close
	}

	written := uint64(len(st.out))
	atomic.AddUint64(&s.bytesWritten, written)
	read := uint64(consumed)
	atomic.AddUint64(&s.bytesRead, read)
	if st.shardID >= 0 && st.shardID < len(s.shards) {
		s.shards[st.shardID].AddBytesWritten(written)
		s.shards[st.shardID].AddBytesRead(read)
	}

	adapter := tlslib.NewGnetConnAdapter(c)
	st.tlsConn = tlslib.Server(adapter, tlsCfg)
	st.readBuf = make([]byte, 0, 4096)
	st.pending = s.handshakes.track(c, st.tlsConn, time.Now().Add(s.handshakeTimeout))
	st.upgradeTLS = false
	return gnet.None
}

// dispatch executes a single command and appends its RESP reply to out. The
// second return is true when an AUTH was dispatched to the bcrypt worker pool,
// in which case no reply was appended and the caller must stop processing the
// current buffer until the worker's Wake delivers it.
//
// Lookup keys use a zero-copy unsafe string over the argument bytes (which alias the gnet read
// buffer): this is safe because Get does not retain the key, and Set clones the key and copies
// the value before storing them.
func (s *Server) dispatch(st *connState, c gnet.Conn, args [][]byte, out []byte) ([]byte, bool) {
	if len(args) == 0 {
		return AppendError(out, "ERR empty command"), false
	}
	cmd := args[0]
	if EqualFold(cmd, shard.CmdAuth) {
		return s.auth(st, c, args, out)
	}
	// STARTTLS precedes the authentication gate so credentials can remain encrypted.
	// Its authenticated check stays in the default case below to avoid adding the
	// optional feature branch to successful GET, SET, DEL, PING, and COMMAND dispatch.
	if !st.authenticated {
		if s.startTLS && EqualFold(cmd, "STARTTLS") {
			return s.dispatchSTARTTLS(st, args, out), false
		}
		// Unauthenticated connections may only issue AUTH, PING, and QUIT (Redis semantics).
		if !EqualFold(cmd, shard.CmdPing) && !EqualFold(cmd, "QUIT") {
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "resp: command rejected, client not authenticated",
					log.String("remote_addr", st.remoteAddr),
					log.String("command", string(cmd)),
				)
			}
			return AppendError(out, "NOAUTH Authentication required"), false
		}
	}
	// The command and key are converted zero-copy over the gnet buffer and
	// consumed synchronously by the audit encoder before the buffer advances.
	user := "default"
	if st.session != nil {
		user = st.session.Username
	}
	key := ""
	if len(args) >= 2 {
		key = *(*string)(unsafe.Pointer(&args[1]))
	}
	s.audit.Record(audit.EventCommand, "command dispatched",
		log.String("command", *(*string)(unsafe.Pointer(&cmd))),
		log.String("key", key),
		log.String("user", user),
		log.String("remote_addr", st.remoteAddr),
		log.String("protocol", "resp"),
	)
	switch {
	case EqualFold(cmd, shard.CmdGet):
		if len(args) != 2 {
			return AppendError(out, "ERR wrong number of arguments for 'get' command"), false
		}
		if !s.authorized(st, rbac.CmdGet, args[1]) {
			return s.deniedReply(st, "get", args[1], false, out), false
		}
		s.countCommand(st)
		key := *(*string)(unsafe.Pointer(&args[1]))
		val, ok := s.store.Get(key)
		if !ok {
			return AppendNullBulk(out), false
		}
		return AppendBulk(out, val), false

	case EqualFold(cmd, shard.CmdSet):
		if len(args) != 3 && len(args) != 5 {
			return AppendError(out, "ERR wrong number of arguments for 'set' command"), false
		}
		if !s.authorized(st, rbac.CmdSet, args[1]) {
			return s.deniedReply(st, "set", args[1], false, out), false
		}
		s.countCommand(st)
		key := *(*string)(unsafe.Pointer(&args[1]))
		ttl, ok := parseSetTTL(args)
		if !ok {
			return AppendError(out, "ERR syntax error"), false
		}
		if err := s.store.Set(key, args[2], ttl); err != nil {
			return AppendError(out, "ERR "+err.Error()), false
		}
		return append(out, respOK...), false

	case EqualFold(cmd, shard.CmdDel):
		if len(args) < 2 {
			return AppendError(out, "ERR wrong number of arguments for 'del' command"), false
		}
		for _, k := range args[1:] {
			if !s.authorized(st, rbac.CmdDel, k) {
				return s.deniedReply(st, "del", k, false, out), false
			}
		}
		s.countCommand(st)
		var n int64
		for _, k := range args[1:] {
			ks := *(*string)(unsafe.Pointer(&k))
			if _, ok := s.store.Get(ks); ok {
				s.store.Delete(ks)
				n++
			}
		}
		return AppendInt(out, n), false

	case EqualFold(cmd, shard.CmdPing):
		if len(args) >= 2 {
			return AppendBulk(out, args[1]), false
		}
		return append(out, respPong...), false

	case EqualFold(cmd, shard.CmdCommand):
		// redis-cli / some tools probe COMMAND DOCS|COUNT at startup; an empty array keeps
		// the session alive without implementing the introspection surface.
		return append(out, "*0\r\n"...), false

	case EqualFold(cmd, shard.CmdRole):
		if !s.authorizedCmd(st, rbac.CmdRole) {
			return s.deniedReply(st, "role", nil, true, out), false
		}
		s.countCommand(st)
		return s.role(st, args, out), false

	case EqualFold(cmd, shard.CmdACL):
		if !s.authorizedCmd(st, rbac.CmdACL) {
			return s.deniedReply(st, "acl", nil, true, out), false
		}
		s.countCommand(st)
		return s.acl(st, args, out), false

	case EqualFold(cmd, "QUIT"):
		st.closeAfterReply = true
		return append(out, respOK...), false

	default:
		if s.startTLS && EqualFold(cmd, "STARTTLS") {
			return s.dispatchSTARTTLS(st, args, out), false
		}
		return AppendError(out, "ERR unknown command '"+string(cmd)+"'"), false
	}
}

// authorized reports whether the connection may run the RBAC command cmd on
// key. When RBAC is disabled (no policy store) every command is allowed and
// the check is a single pointer comparison — the zero-overhead no-op path.
func (s *Server) authorized(st *connState, cmd uint16, key []byte) bool {
	if s.policy == nil {
		return true
	}
	return st.session != nil && st.session.IsAllowed(cmd, key)
}

// authorizedCmd is authorized for keyless commands: the namespace whitelist is
// not consulted, only the command bit (e.g. ROLE for the ROLE command family).
func (s *Server) authorizedCmd(st *connState, cmd uint16) bool {
	if s.policy == nil {
		return true
	}
	return st.session != nil && st.session.AllowsCommand(cmd)
}

// countCommand records one permitted command against the connection's pinned
// role. It is a no-op when RBAC is disabled (the zero-overhead path) and is
// called once per dispatched command, never per key.
func (s *Server) countCommand(st *connState) {
	if s.policy != nil && st.session != nil {
		st.session.CountCommand()
	}
}

// countDenied records one authorization denial (NOPERM). The denial branches
// in dispatch only run when RBAC is enabled, so this is a no-op path otherwise.
func (s *Server) countDenied() {
	if s.policy != nil {
		s.policy.IncDenied()
	}
}

// deniedReply records one RBAC denial (NOPERM) in the ACL LOG buffer and the
// audit trail, and logs the blocked command with the pinned user identity so
// operators see who was denied. key holds the offending key for keyed commands
// and is nil for keyless ones (ROLE/ACL have no key scope); keyless omits the
// key-scoped suffix from the reply.
func (s *Server) deniedReply(st *connState, cmd string, key []byte, keyless bool, out []byte) []byte {
	s.countDenied()
	user := ""
	if st.session != nil {
		user = st.session.Username
	}
	// keyStr aliases the read buffer, so it stays valid only for this frame.
	// LogDenied concatenates it into Reason, which copies, and Record encodes it
	// synchronously — neither retains the alias past the call.
	keyStr := *(*string)(unsafe.Pointer(&key))
	if s.policy != nil {
		s.policy.LogDenied(user, st.remoteAddr, cmd, keyStr)
	}
	s.audit.Record(audit.EventACLDeny, "command denied by rbac policy",
		log.String("user", user),
		log.String("command", cmd),
		log.String("key", keyStr),
		log.String("remote_addr", st.remoteAddr),
		log.String("protocol", "resp"),
	)
	if s.logger.Enabled(log.LevelWarn) {
		fields := []log.Field{
			log.String("remote_addr", st.remoteAddr),
			log.String("command", cmd),
		}
		if st.session != nil {
			fields = append(fields, log.String("username", st.session.Username))
		}
		s.logger.Log(log.LevelWarn, "resp: command denied by rbac policy", fields...)
	}
	if keyless {
		return AppendError(out, "NOPERM no permission for '"+cmd+"' command")
	}
	return AppendError(out, "NOPERM no permission for '"+cmd+"' command on this key")
}

func (s *Server) dispatchSTARTTLS(st *connState, args [][]byte, out []byte) []byte {
	if len(args) != 1 {
		return AppendError(out, "ERR wrong number of arguments for 'starttls' command")
	}
	if st.tlsConn != nil {
		return AppendError(out, "ERR connection is already encrypted")
	}
	if s.tlsConfigs.Load() == nil {
		return AppendError(out, "ERR TLS not configured")
	}
	st.upgradeTLS = true
	return append(out, respOK...)
}

// auth handles the AUTH command in both single-password (AUTH <password>) and ACL
// (AUTH <username> <password>) forms. When RBAC is enabled, per-user bcrypt hashes
// from the policy store are verified instead of the single --require-pass password;
// otherwise, when no password is configured, AUTH is a backward-compatible no-op
// that replies +OK. The second return is true when the verification was dispatched
// to the bcrypt worker pool: the reply arrives later via the worker's Wake, so the
// caller must not append or flush anything for this command.
func (s *Server) auth(st *connState, c gnet.Conn, args [][]byte, out []byte) ([]byte, bool) {
	if len(args) != 2 && len(args) != 3 {
		return AppendError(out, "ERR wrong number of arguments for 'auth' command"), false
	}
	if s.policy != nil {
		// A JWT-shaped secret is a bearer token, not a password: route it to
		// the oauth provider (which maps claims to a role) before any username
		// lookup. Only the two-argument AUTH form carries a token.
		if s.oauth != nil && len(args) == 2 && oauth.IsJWT(args[1]) {
			return s.authOAuth(st, c, args, out)
		}
		return s.authRBAC(st, c, args, out)
	}
	if s.requirePassHash == nil {
		return append(out, respOK...), false
	}
	// Single-password mode knows only the implicit "default" user; per-user
	// identities and the ACL command family live in the RBAC/ACL path above.
	// Derive the username once so the unknown-user and invalid-password
	// branches report the same identity.
	username := "default"
	if len(args) == 3 {
		username = string(args[1])
	}
	if username != "default" {
		return s.authFailed(st, username, "unknown user", out), false
	}
	password := make([]byte, len(args[len(args)-1]))
	copy(password, args[len(args)-1])
	if !s.dispatchAuth(c, username, "invalid password", password, s.requirePassHash, nil) {
		return AppendError(out, "ERR invalid password"), false
	}
	st.authPending = true
	return out, true
}

// authRBAC authenticates against the policy store's per-user bcrypt hashes.
// A nopass user accepts any password (Redis semantics); users with a hash are
// verified off the event loop by the worker pool. On success the resolved role
// is pinned to the connection as a SessionContext; a user without an assignable
// role gets a deny-all session (fail-closed).
func (s *Server) authRBAC(st *connState, c gnet.Conn, args [][]byte, out []byte) ([]byte, bool) {
	username := "default"
	if len(args) == 3 {
		username = string(args[1])
	}
	p := s.policy.Load()
	if p == nil {
		return s.authFailed(st, username, "policy not loaded", out), false
	}
	u := p.UserFor(username)
	if u == nil {
		return s.authFailed(st, username, "unknown user", out), false
	}
	if len(u.PasswordHash) == 0 {
		// nopass user: authentication succeeds without any hashing.
		st.authenticated = true
		st.session = rbac.NewSessionContext(username, p.RoleFor(username))
		s.audit.Record(audit.EventAuthSuccess, "client authenticated",
			log.String("user", username),
			log.String("remote_addr", st.remoteAddr),
			log.String("protocol", "resp"),
		)
		if s.logger.Enabled(log.LevelDebug) {
			s.logger.Log(log.LevelDebug, "resp: client authenticated",
				log.String("remote_addr", st.remoteAddr),
				log.String("username", username),
			)
		}
		return append(out, respOK...), false
	}
	// The session is built from the same snapshot that yielded the hash; the
	// *Role and the hash it references are immutable across hot-swaps, so the
	// worker may use them after the event loop has moved on.
	password := make([]byte, len(args[len(args)-1]))
	copy(password, args[len(args)-1])
	if !s.dispatchAuth(c, username, "invalid password", password, u.PasswordHash, rbac.NewSessionContext(username, p.RoleFor(username))) {
		return AppendError(out, "ERR invalid password"), false
	}
	st.authPending = true
	return out, true
}

// authOAuth authenticates a bearer JWT against the configured provider. The
// token's claims resolve to a role via the policy's oauth.rules on the worker
// pool; a token that fails verification or maps to no rule is rejected
// (fail-closed). The connection's identity is the token's sub claim.
func (s *Server) authOAuth(st *connState, c gnet.Conn, args [][]byte, out []byte) ([]byte, bool) {
	token := make([]byte, len(args[1]))
	copy(token, args[1])
	if !s.dispatchOAuth(c, token) {
		return AppendError(out, "ERR invalid password"), false
	}
	st.authPending = true
	return out, true
}

// dispatchOAuth submits a bearer-token verification to the worker pool. token
// is a private copy owned by the job. Returns false when the pool is saturated
// so the caller fails AUTH synchronously instead of stalling the connection.
func (s *Server) dispatchOAuth(c gnet.Conn, token []byte) bool {
	select {
	case s.authJobs <- authJob{c: c, password: token, oauth: true, reason: "invalid token"}:
		return true
	default:
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "resp: auth worker pool saturated, rejecting AUTH",
				log.String("remote_addr", c.RemoteAddr().String()),
			)
		}
		return false
	}
}

// authFailed records a rejected AUTH attempt — bumping the store-wide counter
// and appending to the ACL LOG buffer when RBAC is enabled — writes it to the
// audit trail, counts it against the per-connection maxAuthFails limit (closing
// the connection when reached), logs it, and appends the RESP error reply.
func (s *Server) authFailed(st *connState, username, reason string, out []byte) []byte {
	if s.policy != nil {
		s.policy.LogAuthFailure(username, st.remoteAddr, reason)
	}
	s.audit.Record(audit.EventAuthFailure, "authentication failed",
		log.String("user", username),
		log.String("remote_addr", st.remoteAddr),
		log.String("reason", reason),
		log.String("protocol", "resp"),
	)
	st.authFails++
	if s.logger.Enabled(log.LevelWarn) {
		s.logger.Log(log.LevelWarn, "resp: failed AUTH attempt",
			log.String("remote_addr", st.remoteAddr),
			log.Int("attempts", st.authFails),
		)
	}
	if st.authFails >= maxAuthFails {
		st.closeAfterReply = true
	}
	return AppendError(out, "ERR invalid password")
}

// dispatchAuth submits an AUTH verification job to the bounded worker pool.
// Returns false when the pool is saturated so the caller fails the AUTH
// synchronously instead of stalling the connection.
func (s *Server) dispatchAuth(c gnet.Conn, username, reason string, password, passHash []byte, session *rbac.SessionContext) bool {
	select {
	case s.authJobs <- authJob{c: c, password: password, passHash: passHash, session: session, username: username, reason: reason}:
		return true
	default:
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "resp: auth worker pool saturated, rejecting AUTH",
				log.String("remote_addr", c.RemoteAddr().String()),
			)
		}
		return false
	}
}

// authWorker is a background goroutine that runs bcrypt verification off the
// gnet event loop. On completion it wakes the connection to deliver the result
// and write the response, so the loop never blocks on a compare.
func (s *Server) authWorker() {
	defer s.workerWg.Done()
	for job := range s.authJobs {
		// A nil passHash marks a nopass user that accepts any password
		// (Redis ACL semantics). In single-password mode passHash is never
		// nil because the workers only run when a hash or a policy exists.
		success := job.passHash == nil || bcrypt.CompareHashAndPassword(job.passHash, job.password) == nil
		if job.oauth {
			// Bearer-token path: the session is unknown until the claims are
			// verified and mapped to a role, so it is resolved here rather than
			// pinned at dispatch time. The provider is concurrency-safe; the
			// store maps claims to a role off a lock-free atomic snapshot.
			job.session, job.username = s.policy.ResolveOAuthToken(func() (map[string][]string, error) {
				ctx, cancel := context.WithTimeout(context.Background(), oauth.VerifyTimeout)
				defer cancel()
				return s.oauth.Verify(ctx, job.password)
			})
			success = job.session != nil
		}
		_ = job.c.Wake(func(c gnet.Conn, _ error) error {
			st, _ := c.Context().(*connState)
			if st == nil {
				return nil
			}
			st.authPending = false
			var reply []byte
			if success {
				st.authenticated = true
				st.session = job.session
				s.audit.Record(audit.EventAuthSuccess, "client authenticated",
					log.String("user", job.username),
					log.String("remote_addr", st.remoteAddr),
					log.String("protocol", "resp"),
				)
				if s.logger.Enabled(log.LevelDebug) {
					s.logger.Log(log.LevelDebug, "resp: client authenticated",
						log.String("remote_addr", st.remoteAddr),
						log.String("username", job.username),
					)
				}
				reply = append(reply, respOK...)
			} else {
				reply = s.authFailed(st, job.username, job.reason, nil)
			}
			var writeErr error
			if st.tlsConn != nil {
				_, writeErr = st.tlsConn.Write(reply)
			} else {
				_, writeErr = c.Write(reply)
			}
			if writeErr != nil {
				if s.logger.Enabled(log.LevelError) {
					s.logger.Log(log.LevelError, "resp: auth reply write failed",
						log.String("error", writeErr.Error()),
					)
				}
				_ = c.Close()
				return nil
			}
			n := uint64(len(reply))
			atomic.AddUint64(&s.bytesWritten, n)
			if st.shardID >= 0 && st.shardID < len(s.shards) {
				s.shards[st.shardID].AddBytesWritten(n)
			}
			if st.closeAfterReply {
				_ = c.Close()
				return nil
			}
			_ = c.Wake(nil)
			return nil
		})
	}
}

// parseSetTTL extracts the TTL from a SET command. Returns (0, true) for a plain 3-arg SET,
// the parsed duration for a valid "EX <s>" / "PX <ms>" 5-arg SET, and (_, false) on a syntax
// error.
func parseSetTTL(args [][]byte) (time.Duration, bool) {
	if len(args) == 3 {
		return 0, true
	}
	v, err := strconv.Atoi(unsafe.String(unsafe.SliceData(args[4]), len(args[4])))
	if err != nil || v < 0 {
		return 0, false
	}
	switch {
	case EqualFold(args[3], "EX"):
		return time.Duration(v) * time.Second, true
	case EqualFold(args[3], "PX"):
		return time.Duration(v) * time.Millisecond, true
	default:
		return 0, false
	}
}

func (s *Server) ConnectedClients() uint64 { return atomic.LoadUint64(&s.connectedClients) }
func (s *Server) TotalConnections() uint64 { return atomic.LoadUint64(&s.totalConnections) }
func (s *Server) BytesRead() uint64        { return atomic.LoadUint64(&s.bytesRead) }
func (s *Server) BytesWritten() uint64     { return atomic.LoadUint64(&s.bytesWritten) }
func (s *Server) ProtocolErrors() uint64   { return atomic.LoadUint64(&s.protocolErrors) }
