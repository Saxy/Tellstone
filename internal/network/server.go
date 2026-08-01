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
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Saxy/Tellstone/internal/log"
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
func NewServer(addr string, maxMsgSize uint64, shards []*shard.Shard, handler func(msg *Message) ([]byte, MessageType, error), logger log.Logger, tlsConfigs *tlslib.ConfigStore, requirePass string, policy *rbac.Store) *Server {
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
	}
	if passHash != nil || policy != nil {
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
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, ^uint64(0))
	if st, ok := c.Context().(*connState); ok && int(st.shardID) < len(s.shards) {
		s.shards[st.shardID].DecConnectedClients()
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
			var (
				respType    MessageType
				respPayload []byte
				skipHandler bool
			)
			if msg.Type == MsgAuth {
				result := s.handleAuthMessage(c, st, msg.Value)
				if result.dispatched {
					offset += totalPacketLen
					break
				}
				respPayload, respType = result.respPayload, result.respType
				skipHandler = true
			} else if !st.authenticated && msg.Type != MsgPing {
				respPayload, respType = ResponseAuthErr, MsgAuthErr
				skipHandler = true
			} else if s.policy != nil && !s.opAuthorized(msg, st) {
				s.policy.IncDenied()
				respPayload, respType = ResponseNotAuthorized, MsgError
				skipHandler = true
			}
			if !skipHandler {
				// PING is not gated by RBAC and never counted as a role command,
				// keeping per-role counts symmetric with the RESP data commands.
				if s.policy != nil && st.session != nil && msg.Type != MsgPing {
					st.session.CountCommand()
				}
				respPayload, respType, err = s.handler(&msg)
			}
			if err != nil {
				atomic.AddUint64(&s.handlerErrors, 1)
				if s.logger.Enabled(log.LevelWarn) {
					s.logger.Log(log.LevelWarn, "application handler returned execution error",
						log.String("error", err.Error()),
					)
				}
				return gnet.Close
			}
			if respPayload != nil {
				if err = Write(st.tlsConn, respType, respPayload); err != nil {
					if s.logger.Enabled(log.LevelError) {
						s.logger.Log(log.LevelError, "failed to write tls response frame",
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
			var (
				respType    MessageType
				respPayload []byte
				skipHandler bool
			)
			if msg.Type == MsgAuth {
				result := s.handleAuthMessage(c, st, msg.Value)
				if result.dispatched {
					_, err = c.Discard(totalPacketLen)
					if err != nil {
						atomic.AddUint64(&s.protocolErrors, 1)
						if s.logger.Enabled(log.LevelWarn) {
							s.logger.Log(log.LevelWarn, "discarding packages not possible",
								log.Int("total packet length", totalPacketLen),
								log.String("error", err.Error()),
							)
						}
					}
					return gnet.None
				}
				respPayload, respType = result.respPayload, result.respType
				skipHandler = true
			} else if !st.authenticated && msg.Type != MsgPing {
				respPayload, respType = ResponseAuthErr, MsgAuthErr
				skipHandler = true
			} else if s.policy != nil && !s.opAuthorized(msg, st) {
				s.policy.IncDenied()
				respPayload, respType = ResponseNotAuthorized, MsgError
				skipHandler = true
			}
			if !skipHandler {
				// PING is not gated by RBAC and never counted as a role command,
				// keeping per-role counts symmetric with the RESP data commands.
				if s.policy != nil && st.session != nil && msg.Type != MsgPing {
					st.session.CountCommand()
				}
				respPayload, respType, err = s.handler(&msg)
			}
			if err != nil {
				atomic.AddUint64(&s.handlerErrors, 1)
				if s.logger.Enabled(log.LevelWarn) {
					s.logger.Log(log.LevelWarn, "application handler returned execution error",
						log.String("error", err.Error()),
					)
				}
				return gnet.Close
			}
			if respPayload != nil {
				if err = Write(c, respType, respPayload); err != nil {
					if s.logger.Enabled(log.LevelError) {
						s.logger.Log(log.LevelError, "failed to write network response frame",
							log.String("error", err.Error()),
						)
					}
					return gnet.Close
				}
				n := uint64(5 + len(respPayload))
				atomic.AddUint64(&s.bytesWritten, n)
				if len(s.shards) > 0 {
					if int(st.shardID) < len(s.shards) {
						s.shards[st.shardID].AddBytesWritten(n)
					}
				}
			}
			if st.closeAfterReply {
				return gnet.Close
			}
		}
		_, err = c.Discard(totalPacketLen)
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
	return gnet.None
}

// authFailed logs a rejected AUTH attempt, increments the per-connection fail
// counter, and marks the connection for closure when the rate limit is exceeded.
func (s *Server) authFailed(st *connState) []byte {
	if s.policy != nil {
		s.policy.IncAuthFailure()
	}
	st.authFails++
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

// dummyAuthHash is a fixed bcrypt hash verified against an unknown user's
// password so a failed AUTH for a nonexistent username takes as long as one for
// a real user — otherwise response latency leaks which users exist. The worker
// comparison always fails against it (mirrors the RESP dummyAuthHash).
var dummyAuthHash = []byte("$2a$10$cwFksVIrb4lyV/GA2fAmWeUFmAkmYlUGwkxVoF9r3Ccaus0H5LdOW")

// handleAuthMessage consolidates the MsgAuth branch shared by the TLS and
// plaintext OnTraffic paths. It handles the no-password bypass, fast-rejects
// malformed payloads, validates the username, makes a copy of the password for
// the worker, and submits the job via dispatchAuth. It returns an authResult:
// dispatched == true means the caller must consume the current frame and skip
// response writing; otherwise respPayload/respType hold the synchronous result.
func (s *Server) handleAuthMessage(c gnet.Conn, st *connState, value []byte) authResult {
	if s.requirePassHash == nil && s.policy == nil {
		return authResult{respPayload: ResponseOK, respType: MsgAuthOk}
	}
	username, password, malformed := parseAuthPayload(value)
	if malformed {
		return authResult{respPayload: ResponseAuthErr, respType: MsgAuthErr}
	}
	var (
		passHash []byte
		session  *rbac.SessionContext
	)
	if s.policy != nil {
		p := s.policy.Load()
		if p == nil {
			return authResult{respPayload: ResponseAuthErr, respType: MsgAuthErr}
		}
		name := "default"
		if len(username) > 0 {
			name = string(username)
		}
		u := p.UserFor(name)
		if u == nil {
			// An unknown username must cost the same bcrypt work as a wrong
			// password for a real user — otherwise AUTH latency leaks which
			// usernames exist. Dispatch the job against a fixed dummy hash so
			// the worker's comparison fails normally (see dummyAuthHash).
			passHash = dummyAuthHash
		} else {
			// Empty hash marks a nopass user that accepts any password (Redis
			// ACL semantics). The session is built from the same snapshot that
			// yielded the hash, and the *Role it references is immutable across
			// hot-swaps.
			passHash = u.PasswordHash
			session = rbac.NewSessionContext(name, p.RoleFor(name))
		}
	} else {
		if len(username) > 0 && string(username) != "default" {
			return authResult{respPayload: s.authFailed(st), respType: MsgAuthErr}
		}
		passHash = s.requirePassHash
	}
	passwordCopy := make([]byte, len(password))
	copy(passwordCopy, password)
	if s.dispatchAuth(c, passwordCopy, passHash, session) {
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
				respPayload, respType = ResponseOK, MsgAuthOk
				if s.logger.Enabled(log.LevelDebug) {
					s.logger.Log(log.LevelDebug, "network: client authenticated",
						log.String("remote_addr", st.remoteAddr),
					)
				}
			} else {
				respPayload, respType = s.authFailed(st), MsgAuthErr
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
func (s *Server) dispatchAuth(c gnet.Conn, password, passHash []byte, session *rbac.SessionContext) bool {
	select {
	case s.authJobs <- authJob{c: c, password: password, passHash: passHash, session: session}:
		return true
	default:
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
	}
	if st.session == nil {
		return false
	}
	switch msg.Op {
	// ROLE admin ops are keyless: the namespace whitelist must not be
	// consulted, only the command bit (mirrors RESP's authorizedCmd).
	case OpRoleCreate, OpRoleSetUser, OpRoleDelUser, OpRoleDelete, OpRoleList, OpRoleGetUser:
		return st.session.AllowsCommand(rbac.CmdRole)
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
