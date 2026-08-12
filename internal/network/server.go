/*
Package network
Tellstone Secure Event-Driven Networking Package
File: server.go
Description: Implements an ultra‑high‑performance, zero‑allocation TCP server using an edge‑triggered epoll event‑loop (gnet). Handles incoming messages, dispatches them to storage, and writes responses. Supports optional TLS 1.3 transport encryption via the internal TLS library.

Authors:

	Maximilian Hagen
*/
package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
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

const defaultAddr = "127.0.0.1:9988"
const defaultMaxMsgSize = 16 * 1024 * 1024
const maxAuthFails = 3
const numAuthWorkers = 4

type authJob struct {
	c        gnet.Conn
	password []byte
	passHash []byte
	// session is the RBAC context pinned on success. nil when RBAC is disabled
	// (single-password mode).
	session *rbac.SessionContext
	// username and reason identify the attempt for the ACL LOG entry recorded
	// when verification fails; username is empty in single-password mode.
	username string
	reason   string
	// oauth marks a bearer-token verification: password holds the raw JWT,
	// passHash is nil, and the session is built only after claims resolve to a
	// role via the policy's oauth.rules.
	oauth bool
}

// connState holds per-connection state. When TLS is enabled, tlsConn wraps the
// raw gnet connection with TLS 1.3 encryption via the internal TLS library. readBuf is a
// reusable scratch buffer for TLS Read calls to avoid per-traffic allocations.
// authenticated tracks whether the client has passed AUTH (always true when no
// server password is configured, so the hot path is branch-predictable).
type connState struct {
	shardID       uint64
	authenticated bool
	// session is the RBAC context pinned at AUTH time. nil when RBAC is
	// disabled (the zero-overhead no-op path) or before authentication.
	session         *rbac.SessionContext
	remoteAddr      string
	authFails       int
	authPending     bool
	closeAfterReply bool
	tlsConn         *tlslib.Conn
	readBuf         []byte
}

type Server struct {
	gnet.BuiltinEventEngine
	addr       string
	handler    func(msg *Message) ([]byte, MessageType, error)
	logger     log.Logger
	maxMsgSize uint64
	tlsConfigs *tlslib.ConfigStore

	// eng and ready let Shutdown reach the running gnet engine: OnBoot fires once the event
	// loop is accepting connections and hands us the Engine handle we need to stop it; ready
	// is closed at that point so a concurrent Shutdown call can block until it's safe to stop.
	eng   gnet.Engine
	ready chan struct{}

	connectedClients uint64
	totalConnections uint64
	bytesRead        uint64
	bytesWritten     uint64
	protocolErrors   uint64
	handlerErrors    uint64

	shards   []*shard.Shard
	nextConn uint64

	// requirePassHash is the bcrypt hash of the server password. nil means AUTH is not
	// required and every connection starts authenticated (zero-overhead no-op path).
	// Ignored when a policy store is configured — per-user RBAC supersedes it.
	requirePassHash []byte

	// policy is the atomic RBAC policy store. nil means RBAC is disabled and no
	// authorization checks run (zero-overhead no-op path). When set, AUTH resolves
	// per-user bcrypt hashes and every data op is gated by the session.
	policy *rbac.Store

	// oauth is the token-verification provider for bearer-token AUTH. nil keeps
	// the password-only paths; when set, a JWT-shaped AUTH secret is verified
	// here and mapped to a role through the policy store (fail-closed on both
	// a bad token and a claim set that maps to no role). It is read-only after
	// construction, so workers may share it without locks.
	oauth oauth.Provider

	// audit is the shared audit engine. Always non-nil: without --enable-audit
	// it is a disabled no-op whose Record() costs one bool comparison, so the
	// hooks below call it without a nil guard.
	audit *audit.LogEngine

	authJobs chan authJob
	workerWg sync.WaitGroup
}

// NewServer initializes an edge-triggered networking server engine instance.
// It applies defensive configuration defaults before spawning infrastructure.
// shards is optional — if nil, per-shard metrics are not tracked.
// tlsConfigs is optional — if nil, plaintext TCP is used. When configured, each
// accepted connection atomically loads the latest immutable TLS configuration.
// requirePass is optional — if empty, AUTH is a no-op and connections start authenticated;
// otherwise it is hashed at startup and clients must AUTH before issuing data commands.
// policy is optional — if nil, RBAC is disabled and every authenticated op is allowed;
// otherwise AUTH resolves per-user credentials and sessions gate data ops.
// provider is optional — if nil, bearer-token AUTH is disabled and AUTH stays
// password-only; when set it verifies JWT-shaped secrets and maps their claims to
// roles through the policy store (which must therefore also be set).
// audit is the shared audit engine; it must be non-nil (pass a disabled engine when
// audit logging is off) and is always called without a nil guard.
func NewServer(
	addr string,
	maxMsgSize uint64,
	shards []*shard.Shard,
	handler func(msg *Message) ([]byte, MessageType, error),
	logger log.Logger,
	tlsConfigs *tlslib.ConfigStore,
	requirePass string,
	policy *rbac.Store,
	provider oauth.Provider,
	audit *audit.LogEngine) *Server {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	if addr == "" {
		if logger.Enabled(log.LevelDebug) {
			logger.Log(log.LevelDebug, "addr is nil using defaultAddr instead", log.String("listen to addr", defaultAddr))
		}
		addr = defaultAddr
	}
	if maxMsgSize == 0 {
		maxMsgSize = defaultMaxMsgSize
	}
	var passHash []byte
	if requirePass != "" && policy == nil {
		var err error
		passHash, err = bcrypt.GenerateFromPassword([]byte(requirePass), bcrypt.DefaultCost)
		if err != nil {
			panic("network: invalid --require-pass value: " + err.Error())
		}
	}
	s := &Server{
		addr:            addr,
		handler:         handler,
		logger:          logger,
		maxMsgSize:      maxMsgSize,
		tlsConfigs:      tlsConfigs,
		ready:           make(chan struct{}),
		shards:          shards,
		requirePassHash: passHash,
		policy:          policy,
		oauth:           provider,
		audit:           audit,
	}
	if passHash != nil || policy != nil || provider != nil {
		s.authJobs = make(chan authJob, 256)
		for i := 0; i < numAuthWorkers; i++ {
			s.workerWg.Add(1)
			go s.authWorker()
		}
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "tcp server created", log.Int("max_msg_size", int(maxMsgSize)))
	}
	return s
}

// ListenAndServe starts the multi-reactor epoll event loop.
func (s *Server) ListenAndServe() error {
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "network: event-driven engine initializing", log.String("address", s.addr))
	}
	return gnet.Run(s, "tcp://"+s.addr, gnet.WithMulticore(true), gnet.WithLogger(log.NewGnetAdapter(s.logger)))
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

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	close(s.ready)
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, 1)
	atomic.AddUint64(&s.totalConnections, 1)
	var sid uint64
	if len(s.shards) > 0 {
		sid = atomic.AddUint64(&s.nextConn, 1) - 1
		sid = sid % uint64(len(s.shards))
		s.shards[sid].IncConnectedClients()
		s.shards[sid].IncTotalConnections()
	}
	st := &connState{
		shardID:       sid,
		authenticated: s.requirePassHash == nil && s.policy == nil,
		remoteAddr:    c.RemoteAddr().String(),
	}
	if tlsCfg := s.tlsConfigs.Load(); tlsCfg != nil {
		adapter := tlslib.NewGnetConnAdapter(c)
		st.tlsConn = tlslib.Server(adapter, tlsCfg)
		st.readBuf = make([]byte, 0, 4096)
	}
	c.SetContext(st)
	s.audit.Record(audit.EventConnect, "client connected",
		log.String("remote_addr", st.remoteAddr),
		log.String("protocol", "binary"),
		log.Uint64("shard_id", sid),
	)
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "network: client connected",
			log.String("remote_addr", st.remoteAddr),
			log.Uint64("shard_id", sid),
		)
	}
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, ^uint64(0))
	var remoteAddr string
	if st, ok := c.Context().(*connState); ok {
		remoteAddr = st.remoteAddr
		if int(st.shardID) < len(s.shards) {
			s.shards[st.shardID].DecConnectedClients()
		}
	}
	s.audit.Record(audit.EventDisconnect, "client disconnected",
		log.String("remote_addr", remoteAddr),
		log.String("protocol", "binary"),
	)
	if s.logger.Enabled(log.LevelDebug) {
		fields := []log.Field{log.String("remote_addr", remoteAddr)}
		if err != nil {
			fields = append(fields, log.String("error", err.Error()))
		}
		s.logger.Log(log.LevelDebug, "network: client disconnected", fields...)
	}
	return gnet.None
}

// OnTraffic handles incoming bytes on the socket asynchronously and lock-free.
// When TLS is enabled, encrypted bytes are decrypted via the internal TLS library before
// protocol parsing. The handshake is driven automatically by the first Read/Write
// calls on the TLS connection.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	st, _ := c.Context().(*connState)
	if st == nil {
		st = &connState{}
		c.SetContext(st)
	}
	if st.tlsConn != nil {
		return s.onTrafficTLS(c, st)
	}
	return s.onTrafficPlaintext(c, st)
}

// onTrafficTLS reads decrypted application data from the TLS connection, parses
// our binary protocol frames, dispatches them to the handler, and writes
// encrypted responses.
func (s *Server) onTrafficTLS(c gnet.Conn, st *connState) gnet.Action {
	for {
		n, err := st.tlsConn.Read(st.readBuf[len(st.readBuf):cap(st.readBuf)])
		if n > 0 {
			st.readBuf = st.readBuf[:len(st.readBuf)+n]
			if action := s.handleDecryptedFrames(c, st); action != gnet.None {
				return action
			}
		}
		if err != nil {
			if errors.Is(err, tlslib.ErrNotEnough) {
				return gnet.None
			}
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "tls read failed",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
	}
}

// handleDecryptedFrames parses zero or more Tellstone binary protocol frames
// from plaintext data and dispatches each to the handler. Responses are written
// through the TLS connection for automatic encryption. It returns gnet.Close
// on decode, handler, or TLS write errors so the caller propagates the close.
func (s *Server) handleDecryptedFrames(c gnet.Conn, st *connState) gnet.Action {
	if st.authPending {
		return gnet.None
	}
	var msg Message
	offset := 0
	for offset < len(st.readBuf) {
		msg = Message{}
		payloadLen, err := Decode(st.readBuf[offset:], s.maxMsgSize, &msg)
		if err != nil {
			if errors.Is(err, errShortRead) {
				break
			}
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "protocol decoding failed catastrophically",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		totalPacketLen := 5 + payloadLen
		atomic.AddUint64(&s.bytesRead, uint64(totalPacketLen))
		if len(s.shards) > 0 && int(st.shardID) < len(s.shards) {
			s.shards[st.shardID].AddBytesRead(uint64(totalPacketLen))
		}
		if s.handler != nil {
			respType, respPayload, skipHandler, dispatched := s.gateMessage(c, st, &msg)
			if dispatched {
				offset += totalPacketLen
				break
			}
			if action := s.runHandler(st.tlsConn, st, &msg, respType, respPayload, skipHandler, "failed to write tls response frame"); action != gnet.None {
				return action
			}
		}
		offset += totalPacketLen
	}
	if offset > 0 {
		remaining := copy(st.readBuf, st.readBuf[offset:])
		st.readBuf = st.readBuf[:remaining]
	}
	return gnet.None
}

// onTrafficPlaintext handles the original zero-copy plaintext path. Raw bytes
// are peeked directly from the gnet ring buffer, parsed, and responses are
// written back through gnet without any intermediate copies.
func (s *Server) onTrafficPlaintext(c gnet.Conn, st *connState) gnet.Action {
	if st.authPending {
		return gnet.None
	}
	var msg Message
	for {
		buf, err := c.Peek(-1)
		if err != nil {
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "peek failed to return n bytes",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		msg = Message{}
		payloadLen, err := Decode(buf, s.maxMsgSize, &msg)
		if err != nil {
			if errors.Is(err, errShortRead) {
				break
			}
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "protocol decoding failed catastrophically",
					log.String("remote_addr", c.RemoteAddr().String()),
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		totalPacketLen := 5 + payloadLen
		atomic.AddUint64(&s.bytesRead, uint64(totalPacketLen))
		if len(s.shards) > 0 {
			if int(st.shardID) < len(s.shards) {
				s.shards[st.shardID].AddBytesRead(uint64(totalPacketLen))
			}
		}
		if s.handler != nil {
			respType, respPayload, skipHandler, dispatched := s.gateMessage(c, st, &msg)
			if dispatched {
				s.discardFrame(c, totalPacketLen)
				return gnet.None
			}
			if action := s.runHandler(c, st, &msg, respType, respPayload, skipHandler, "failed to write network response frame"); action != gnet.None {
				return action
			}
		}
		s.discardFrame(c, totalPacketLen)
	}
	return gnet.None
}

// gateMessage authorizes one decoded frame against the connection state: it runs
// the AUTH path (dispatching bcrypt to the worker pool when needed) and applies
// the authentication and RBAC gates. skipHandler reports that respType and
// respPayload already hold the reply and the handler must not run; dispatched
// reports that the AUTH job was sent to the worker pool, so the caller must
// consume the frame without writing a response.
func (s *Server) gateMessage(c gnet.Conn, st *connState, msg *Message) (respType MessageType, respPayload []byte, skipHandler, dispatched bool) {
	if msg.Type == MsgAuth {
		result := s.handleAuthMessage(c, st, msg.Value)
		if result.dispatched {
			return 0, nil, false, true
		}
		return result.respType, result.respPayload, true, false
	}
	if !st.authenticated && msg.Type != MsgPing {
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "network: command rejected, client not authenticated",
				log.String("remote_addr", st.remoteAddr),
				log.String("command", msg.Op.String()),
			)
		}
		return MsgAuthErr, ResponseAuthErr, true, false
	}
	if s.policy != nil && !s.opAuthorized(*msg, st) {
		s.policy.IncDenied()
		user := ""
		if st.session != nil {
			user = st.session.Username
		}
		cmd := msg.Op.String()
		keyStr := string(msg.Key)
		s.policy.LogDenied(user, st.remoteAddr, cmd, keyStr)
		s.audit.Record(audit.EventACLDeny, "command denied by rbac policy",
			log.String("user", user),
			log.String("command", cmd),
			log.String("key", keyStr),
			log.String("remote_addr", st.remoteAddr),
			log.String("protocol", "binary"),
		)
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "network: command denied by rbac policy",
				log.String("remote_addr", st.remoteAddr),
				log.String("command", cmd),
				log.String("key", keyStr),
			)
		}
		return MsgError, ResponseNotAuthorized, true, false
	}
	return 0, nil, false, false
}

// runHandler executes the application handler for one frame unless the gate
// supplied a reply, then writes the response and accounts the bytes against the
// connection and its shard. writeError names the failing path for the log. It
// returns gnet.Close on handler or write errors and when the connection is
// marked to close after the reply.
func (s *Server) runHandler(w io.Writer, st *connState, msg *Message, respType MessageType, respPayload []byte, skipHandler bool, writeError string) gnet.Action {
	if !skipHandler {
		// PING is not gated by RBAC and never counted as a role command, keeping
		// per-role counts symmetric with the RESP data commands.
		if s.policy != nil && st.session != nil && msg.Type != MsgPing {
			st.session.CountCommand()
		}
		// The key is converted zero-copy (aliasing the gnet buffer) and consumed
		// synchronously by the encoder, mirroring the dispatch path in server.go.
		// The unsafe string holds a slice header over the buffer that stays valid
		// until the frame is discarded below.
		keyStr := *(*string)(unsafe.Pointer(&msg.Key))
		user := "default"
		if st.session != nil {
			user = st.session.Username
		}
		s.audit.Record(audit.EventCommand, "command dispatched",
			log.String("command", msg.Op.String()),
			log.String("key", keyStr),
			log.String("user", user),
			log.String("remote_addr", st.remoteAddr),
			log.String("protocol", "binary"),
		)
		var err error
		respPayload, respType, err = s.handler(msg)
		if err != nil {
			atomic.AddUint64(&s.handlerErrors, 1)
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "application handler returned execution error",
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
	}
	if respPayload != nil {
		if err := Write(w, respType, respPayload); err != nil {
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, writeError,
					log.String("error", err.Error()),
				)
			}
			return gnet.Close
		}
		n := uint64(5 + len(respPayload))
		atomic.AddUint64(&s.bytesWritten, n)
		if len(s.shards) > 0 && int(st.shardID) < len(s.shards) {
			s.shards[st.shardID].AddBytesWritten(n)
		}
	}
	if st.closeAfterReply {
		return gnet.Close
	}
	return gnet.None
}

// discardFrame consumes one decoded frame from the gnet ring buffer and, when
// that fails, counts a protocol error and logs a warning. totalPacketLen is the
// full on-wire size including the 5-byte header.
func (s *Server) discardFrame(c gnet.Conn, totalPacketLen int) {
	_, err := c.Discard(totalPacketLen)
	if err != nil {
		atomic.AddUint64(&s.protocolErrors, 1)
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "discarding packages not possible",
				log.Int("total packet length", totalPacketLen),
				log.String("error", err.Error()),
			)
		}
	}
}

// authFailed records a rejected AUTH attempt in the ACL LOG buffer and the
// audit trail, increments the per-connection fail counter, and marks the
// connection for closure when the rate limit is exceeded. Both recordings live
// here rather than at the call sites so a new failure path cannot reach one
// sink without the other, which would let ACL LOG and the audit trail drift.
func (s *Server) authFailed(st *connState, username, reason string) []byte {
	st.authFails++
	if s.policy != nil {
		s.policy.LogAuthFailure(username, st.remoteAddr, reason)
	}
	s.audit.Record(audit.EventAuthFailure, "authentication failed",
		log.String("user", username),
		log.String("remote_addr", st.remoteAddr),
		log.String("reason", reason),
		log.String("protocol", "binary"),
	)
	if s.logger.Enabled(log.LevelWarn) {
		s.logger.Log(log.LevelWarn, "network: failed AUTH attempt",
			log.String("remote_addr", st.remoteAddr),
			log.Int("attempts", st.authFails),
		)
	}
	if st.authFails >= maxAuthFails {
		st.closeAfterReply = true
	}
	return ResponseAuthErr
}

// authResult conveys the outcome of synchronous AUTH processing.
type authResult struct {
	respPayload []byte
	respType    MessageType
	dispatched  bool // true when bcrypt was sent to the worker pool
}

var tokenPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// handleAuthMessage consolidates the MsgAuth branch shared by the TLS and
// plaintext OnTraffic paths. It handles the no-password bypass, fast-rejects
// malformed payloads and unknown usernames, validates the username, makes a
// copy of the password for the worker, and submits the job via dispatchAuth. It
// returns an authResult: dispatched == true means the caller must consume the
// current frame and skip response writing; otherwise respPayload/respType hold
// the synchronous result.
func (s *Server) handleAuthMessage(c gnet.Conn, st *connState, value []byte) authResult {
	if s.requirePassHash == nil && s.policy == nil {
		return authResult{respPayload: ResponseOK, respType: MsgAuthOk}
	}
	username, password, malformed := parseAuthPayload(value)
	if malformed {
		// A truncated frame is still a rejected AUTH: record it like any other
		// failure so ACL LOG shows attempted-but-undeliverable credentials. The
		// username is unknown, so the entry carries an empty name.
		return authResult{respPayload: s.authFailed(st, "", "malformed request"), respType: MsgAuthErr}
	}
	// A JWT-shaped secret is a bearer token, not a password: route it to the
	// oauth provider (which maps claims to a role) before any username lookup.
	if s.oauth != nil && len(username) == 0 && oauth.IsJWT(password) {
		buf := tokenPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.Write(password)
		if s.dispatchOAuth(c, buf.Bytes()) {
			st.authPending = true
			return authResult{dispatched: true}
		}
		return authResult{respPayload: ResponseAuthErr, respType: MsgAuthErr}
	}
	var (
		passHash []byte
		session  *rbac.SessionContext
		name     string
		reason   string
	)
	if s.policy != nil {
		p := s.policy.Load()
		if p == nil {
			// Recorded like any other rejection rather than returned bare: this
			// is still a failed AUTH, so it belongs in ACL LOG and the audit
			// trail and must count against the per-connection limit. The RESP
			// listener reports it under the same reason.
			return authResult{respPayload: s.authFailed(st, string(username), "policy not loaded"), respType: MsgAuthErr}
		}
		name = "default"
		if len(username) > 0 {
			name = string(username)
		}
		u := p.UserFor(name)
		if u == nil {
			// Unknown usernames fail synchronously; the worker never sees them,
			// so the rejection is recorded here on the synchronous path.
			return authResult{respPayload: s.authFailed(st, name, "unknown user"), respType: MsgAuthErr}
		}
		// Empty hash marks a nopass user that accepts any password (Redis
		// ACL semantics). The session is built from the same snapshot that
		// yielded the hash, and the *Role it references is immutable across
		// hot-swaps.
		passHash = u.PasswordHash
		session = rbac.NewSessionContext(name, p.RoleFor(name))
		reason = "invalid password"
	} else {
		if len(username) > 0 && string(username) != "default" {
			return authResult{respPayload: s.authFailed(st, string(username), "unknown user"), respType: MsgAuthErr}
		}
		passHash = s.requirePassHash
	}
	passwordCopy := make([]byte, len(password))
	copy(passwordCopy, password)
	if s.dispatchAuth(c, name, reason, passwordCopy, passHash, session) {
		st.authPending = true
		return authResult{dispatched: true}
	}
	return authResult{respPayload: ResponseAuthErr, respType: MsgAuthErr}
}

// authWorker is a background goroutine that runs bcrypt verification off the
// gnet event loop. On completion it wakes the connection on the event loop to
// deliver the result and write the response.
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
			// store maps claim to a role of a lock-free atomic snapshot.
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
			var respPayload []byte
			var respType MessageType
			if success {
				st.authenticated = true
				st.session = job.session
				// Single-password mode never sets job.username; the implicit
				// identity there is "default", mirroring the AUTH payload rule.
				user := job.username
				if user == "" {
					user = "default"
				}
				s.audit.Record(audit.EventAuthSuccess, "client authenticated",
					log.String("user", user),
					log.String("remote_addr", st.remoteAddr),
					log.String("protocol", "binary"),
				)
				respPayload, respType = ResponseOK, MsgAuthOk
				if s.logger.Enabled(log.LevelDebug) {
					s.logger.Log(log.LevelDebug, "network: client authenticated",
						log.String("remote_addr", st.remoteAddr),
					)
				}
			} else {
				respPayload, respType = s.authFailed(st, job.username, job.reason), MsgAuthErr
			}
			var writeErr error
			if st.tlsConn != nil {
				writeErr = Write(st.tlsConn, respType, respPayload)
			} else {
				writeErr = Write(c, respType, respPayload)
			}
			if writeErr != nil {
				if s.logger.Enabled(log.LevelError) {
					s.logger.Log(log.LevelError, "network: auth write failed",
						log.String("error", writeErr.Error()),
					)
				}
				_ = c.Close()
				return nil
			}
			n := uint64(5 + len(respPayload))
			atomic.AddUint64(&s.bytesWritten, n)
			if len(s.shards) > 0 && int(st.shardID) < len(s.shards) {
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

// dispatchAuth submits an auth verification job to the bounded worker pool.
// Returns true if the job was accepted, false if the pool is saturated.
func (s *Server) dispatchAuth(c gnet.Conn, username, reason string, password, passHash []byte, session *rbac.SessionContext) bool {
	select {
	case s.authJobs <- authJob{c: c, password: password, passHash: passHash, session: session, username: username, reason: reason}:
		return true
	default:
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "network: auth worker pool saturated, rejecting AUTH",
				log.String("remote_addr", c.RemoteAddr().String()),
			)
		}
		return false
	}
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
			s.logger.Log(log.LevelWarn, "network: auth worker pool saturated, rejecting AUTH",
				log.String("remote_addr", c.RemoteAddr().String()),
			)
		}
		return false
	}
}

// opAuthorized gates a data op against the connection's pinned RBAC session.
// PING and AUTH always pass; ROLE admin ops require the ROLE command bit; data
// ops map their OpCode to the registered command and check the bit plus the key
// namespace whitelist. Sessions without a role deny everything (fail-closed).
func (s *Server) opAuthorized(msg Message, st *connState) bool {
	switch msg.Type {
	case MsgPing, MsgAuth:
		return true
	default:
		break
	}
	if st.session == nil {
		return false
	}
	switch msg.Op {
	// ROLE admin ops are keyless: the namespace whitelist must not be
	// consulted, only the command bit (mirrors RESP's authorizedCmd).
	case OpRoleCreate, OpRoleSetUser, OpRoleDelUser, OpRoleDelete, OpRoleList, OpRoleGetUser:
		return st.session.AllowsCommand(rbac.CmdRole)
	// ACL admin ops gate on the ACL command bit, a sibling of ROLE: a role
	// granted only +role cannot manage ACL users and vice versa.
	case OpACLSetUser, OpACLDelUser, OpACLList, OpACLLog:
		return st.session.AllowsCommand(rbac.CmdACL)
	case OpGet:
		return st.session.IsAllowed(rbac.CmdGet, msg.Key)
	case OpSet:
		return st.session.IsAllowed(rbac.CmdSet, msg.Key)
	case OpDelete:
		return st.session.IsAllowed(rbac.CmdDel, msg.Key)
	default:
		return false
	}
}

// parseAuthPayload extracts username and password from the MsgAuth wire format:
//
//	[2B usernameLen][username bytes][2B passwordLen][password bytes]
//
// malformed is true when the payload is truncated. (nil, nil) with malformed=false
// is a valid frame with an empty username ("default" user).
func parseAuthPayload(value []byte) (username, password []byte, malformed bool) {
	if len(value) < 2 {
		return nil, nil, true
	}
	usernameLen := int(binary.BigEndian.Uint16(value[:2]))
	pos := 2
	if len(value) < pos+usernameLen+2 {
		return nil, nil, true
	}
	username = value[pos : pos+usernameLen]
	pos += usernameLen
	passwordLen := int(binary.BigEndian.Uint16(value[pos : pos+2]))
	pos += 2
	if len(value) < pos+passwordLen {
		return nil, nil, true
	}
	password = value[pos : pos+passwordLen]
	return
}

func (s *Server) ConnectedClients() uint64 { return atomic.LoadUint64(&s.connectedClients) }
func (s *Server) TotalConnections() uint64 { return atomic.LoadUint64(&s.totalConnections) }
func (s *Server) BytesRead() uint64        { return atomic.LoadUint64(&s.bytesRead) }
func (s *Server) BytesWritten() uint64     { return atomic.LoadUint64(&s.bytesWritten) }
func (s *Server) ProtocolErrors() uint64   { return atomic.LoadUint64(&s.protocolErrors) }
func (s *Server) HandlerErrors() uint64    { return atomic.LoadUint64(&s.handlerErrors) }
