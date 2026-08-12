/*
Package server
Tellstone Cloud-Native In-Memory Database
File: server.go
Description: Top-level server orchestration: initializes the shared-nothing shards, router, binary-protocol listener, optional RESP/TLS listener, and metrics server. Handles graceful shutdown on SIGINT/SIGTERM.

Authors:

	Maximilian Hagen
*/
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/metrics"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/oauth"
	"github.com/Saxy/Tellstone/internal/oauth/generic"
	"github.com/Saxy/Tellstone/internal/oauth/presets"
	"github.com/Saxy/Tellstone/internal/persistence"
	"github.com/Saxy/Tellstone/internal/rbac"
	"github.com/Saxy/Tellstone/internal/resp"
	"github.com/Saxy/Tellstone/internal/router"
	"github.com/Saxy/Tellstone/internal/shard"
	tlslib "github.com/Saxy/Tellstone/internal/tls"
)

type RouterStore struct {
	router *router.Router
}

func (rs *RouterStore) Get(key string) ([]byte, bool) {
	resp := rs.router.Dispatch(shard.CmdGet, key, nil, 0)
	return resp.Value, resp.OK
}

func (rs *RouterStore) Set(key string, value []byte, ttl time.Duration) error {
	resp := rs.router.Dispatch(shard.CmdSet, key, value, ttl)
	return resp.Err
}

func (rs *RouterStore) Delete(key string) {
	rs.router.Dispatch(shard.CmdDel, key, nil, 0)
}

type Server struct {
	app         *tellstone.App
	router      *router.Router
	shards      []*shard.Shard
	netSrv      *network.Server
	respSrv     *resp.Server
	metricsSrv  *http.Server
	tlsConfigs  *tlslib.ConfigStore
	tlsReloader *tlslib.Reloader
	// policy is the atomic RBAC policy store shared by the binary and RESP
	// listeners. nil means RBAC is disabled and both servers keep their
	// legacy zero-overhead paths. SIGHUP swaps a fresh snapshot into it.
	policy *rbac.Store
	// oauth is the configured token-verification provider, built once at
	// startup from --oauth-provider / --oauth-issuer. nil means token auth is
	// disabled and both listeners keep their password-only AUTH paths. It is
	// read-only after init, so it can be shared safely across workers.
	oauth oauth.Provider
	// audit is the shared audit engine. It is always non-nil: when
	// --enable-audit is not set it is a disabled no-op whose Record() costs a
	// single bool comparison, so listeners never guard the call.
	audit *audit.LogEngine
}

func NewServer(app *tellstone.App) *Server {
	return &Server{
		app: app,
	}
}

func (s *Server) Run() error {
	logger := s.app.GetLogger()
	cfg := s.app.GetConfig()
	key, cryptoEngine, err := s.initCrypto()
	if err != nil {
		return fmt.Errorf("crypto init: %w", err)
	}
	tlsReloaderStarted := false
	if cfg.TLSEnabled() {
		s.tlsReloader, err = tlslib.NewReloader(cfg.GetTLSCert(), cfg.GetTLSKey(), cfg.GetTLSCA())
		if err != nil {
			return fmt.Errorf("tls config: %w", err)
		}
		defer func() {
			if !tlsReloaderStarted {
				_ = s.tlsReloader.Close()
			}
		}()
		s.tlsConfigs = s.tlsReloader.Configs()
	}
	if err = s.initRBAC(); err != nil {
		return fmt.Errorf("rbac init: %w", err)
	}
	if err = s.initOAuth(); err != nil {
		return fmt.Errorf("oauth init: %w", err)
	}
	if err = s.initAudit(key, cryptoEngine); err != nil {
		return fmt.Errorf("audit init: %w", err)
	}
	// After initAudit so the engine has resolved the destination and the key
	// that seals it, and before any listener starts so the restored history is
	// in place by the time a client can read ACL LOG.
	s.seedAuditReplay()
	if err = s.initShards(key, cryptoEngine); err != nil {
		return fmt.Errorf("shard init: %w", err)
	}
	s.netSrv = network.NewServer(
		cfg.GetAddr(),
		cfg.GetMaxMsgSize(),
		s.shards,
		s.networkHandler,
		logger,
		s.tlsConfigs,
		cfg.GetRequirePass(),
		s.policy,
		s.oauth,
		s.audit,
	)
	if cfg.MetricsEnabled() {
		s.startMetricsServer(s.netSrv)
	}
	if cfg.RESPEnabled() {
		s.startRESPServer()
	}

	if s.tlsReloader != nil {
		reloadCtx, cancelReload := context.WithCancel(context.Background())
		tlsReloaderStarted = true
		reloadDone := make(chan struct{})
		go func() {
			defer close(reloadDone)
			if reloadErr := s.tlsReloader.Run(reloadCtx, logger); reloadErr != nil && logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: tls certificate watcher stopped",
					log.String("error", reloadErr.Error()),
				)
			}
		}()
		defer func() {
			cancelReload()
			<-reloadDone
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			// Hot-reload the RBAC policy on SIGHUP. Atomic swap: a rejected
			// file leaves the running policy untouched.
			s.reloadRBAC()
		}
	}()
	go func() {
		<-ctx.Done()
		stop()
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "server: shutdown signal received, draining connections")
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.GetShutdownTimeout())
		defer cancel()
		s.shutdown(shutdownCtx)
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "server: shutdown complete")
		}
	}()

	if err = s.netSrv.ListenAndServe(); err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = nil
		} else if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "server: tcp error", log.String("error", err.Error()))
		}
	}
	// Stop the SIGHUP watcher so its goroutine exits with Run instead of
	// lingering until the process dies.
	signal.Stop(hup)
	close(hup)
	return err
}

// initRBAC loads the --rbac-config policy file once at startup. No file
// configured means RBAC is disabled (legacy behavior, zero overhead). A
// configured-but-unreadable or invalid file aborts startup: a server that
// silently runs without the operator's ACLs is a security hole.
func (s *Server) initRBAC() error {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	path := cfg.GetRBACConfig()
	if path == "" {
		return nil
	}
	policy, err := rbac.LoadFile(path)
	if err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "server: rbac policy load failed", log.String("error", err.Error()))
		}
		return err
	}
	s.policy = rbac.NewStore(policy, logger)
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "server: rbac policy loaded", log.String("path", path))
	}
	return nil
}

// initOAuth builds the token-verification provider from the --oauth-* flags.
// No provider configured leaves the provider nil and the password-only AUTH
// paths intact (zero overhead). A configured-but-invalid provider aborts
// startup, and so does token auth without --rbac-config: a token can only map
// to a role through the policy's oauth.rules, so enabling it without a policy
// would silently deny every connection.
func (s *Server) initOAuth() error {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	issuer := cfg.GetOAuthIssuer()
	if cfg.GetOAuthProvider() == "" && issuer == "" {
		return nil
	}
	if s.policy == nil {
		return errors.New("--oauth-provider requires --rbac-config: tokens map to roles via the policy's oauth.rules")
	}
	ocfg := oauth.Config{Issuer: issuer, ClientID: cfg.GetOAuthClientID()}
	if ocfg.ClientID == "" {
		return errors.New("--oauth-client-id is required: an empty client id disables audience (aud) validation")
	}
	var (
		p   oauth.Provider
		err error
	)
	switch cfg.GetOAuthProvider() {
	case "google":
		p, err = presets.NewGoogle(ocfg, logger)
	case "stackit":
		p, err = presets.NewStackit(ocfg, logger)
	case "":
		p, err = generic.New(ocfg, logger)
	default:
		return fmt.Errorf("unknown --oauth-provider %q (want google, stackit, or empty for generic OIDC)", cfg.GetOAuthProvider())
	}
	if err != nil {
		return err
	}
	s.oauth = p
	if logger.Enabled(log.LevelInfo) {
		providerName := cfg.GetOAuthProvider()
		if providerName == "" {
			providerName = "generic"
		}
		logger.Log(log.LevelInfo, "oauth provider initialized",
			log.String("provider", providerName),
			log.String("issuer", issuer),
		)
	}
	return nil
}

// reloadRBAC re-reads the policy file on SIGHUP and swaps it into the store in
// one atomic operation. A bad file is rejected and the running policy stays in
// effect; only a fully valid snapshot is ever published.
func (s *Server) reloadRBAC() {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	path := cfg.GetRBACConfig()
	if path == "" || s.policy == nil {
		return
	}
	policy, err := rbac.LoadFile(path)
	if err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "server: rbac policy reload rejected, keeping running policy",
				log.String("error", err.Error()))
		}
		return
	}
	s.policy.Reload(policy)
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "server: rbac policy reloaded", log.String("path", path))
	}
}

func (s *Server) shutdown(ctx context.Context) {
	logger := s.app.GetLogger()
	if s.respSrv != nil {
		if err := s.respSrv.Shutdown(ctx); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: resp server shutdown error", log.String("error", err.Error()))
			}
		}
	}
	if s.metricsSrv != nil {
		if err := s.metricsSrv.Shutdown(ctx); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: metrics server shutdown error", log.String("error", err.Error()))
			}
		}
	}
	if err := s.netSrv.Shutdown(ctx); err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "server: tcp server shutdown error", log.String("error", err.Error()))
		}
	}
	for _, sh := range s.shards {
		if err := sh.Stop(ctx); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: shard shutdown error",
					log.Uint64("shard_id", uint64(sh.ID)),
					log.String("error", err.Error()),
				)
			}
		}
	}
	// Both listeners are stopped above, so no Record() call can race the
	// file close. A disabled engine's Close() is a no-op returning nil.
	if err := s.audit.Close(); err != nil && logger.Enabled(log.LevelError) {
		logger.Log(log.LevelError, "server: audit log close error", log.String("error", err.Error()))
	}
}

// initCrypto resolves the encryption key and builds the shared at-rest cipher
// engine. The key comes from whichever KeyProvider matches the configured source —
// a file when --encryption-key-file is set, otherwise the base64 flag value — and
// is read exactly once here, never from the encrypt/decrypt hot path. The raw key
// is returned alongside the engine so envelope mode can hand it to each shard as a
// KEK. Both are nil when encryption is disabled, leaving callers in pass-through
// mode.
func (s *Server) initCrypto() ([]byte, *crypto.Engine, error) {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	if !cfg.EncryptionEnabled() {
		return nil, nil, nil
	}
	var provider crypto.KeyProvider
	switch {
	case cfg.GetEncryptionKeyFile() != "":
		provider = crypto.NewFileKeyProvider(cfg.GetEncryptionKeyFile())
	default:
		provider = crypto.NewBase64KeyProvider(cfg.GetEncryptionKey(), logger)
	}
	key, err := provider.Key()
	if err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "encryption key resolution failed", log.String("error", err.Error()))
		}
		return nil, nil, fmt.Errorf("encryption key resolution: %w", err)
	}
	cryptoEngine, err := crypto.NewEngine(key, logger)
	if err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "server: crypto engine setup failed", log.String("error", err.Error()))
		}
		return nil, nil, fmt.Errorf("crypto engine initialization: %w", err)
	}
	return key, cryptoEngine, nil
}

// initAudit constructs the shared audit engine from the --enable-audit,
// --audit-log-path, and --audit-events flags. The engine is always non-nil: a
// disabled --enable-audit yields the no-op engine, so every listener hook can
// call Record() unconditionally. cryptoEngine is nil when encryption is off;
// the zero-value crypto.Engine keeps the file writer's encryption path inert.
// In envelope mode the audit engine derives its own DEK engine, leaving the
// caller's cryptoEngine untouched for the shards.
func (s *Server) initAudit(key []byte, cryptoEngine *crypto.Engine) error {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	var engine *crypto.Engine
	if cryptoEngine != nil {
		engine = cryptoEngine
	}
	var err error
	s.audit, err = audit.NewLogEngine(
		cfg.AuditEnabled(),
		audit.ParseEventTypes(strings.Join(cfg.AuditLogEvents(), ",")),
		cfg.AuditLogPath(),
		logger,
		cfg.EncryptionEnabled() && cfg.EnvelopeEnabled(),
		key,
		engine,
	)
	return err
}

// seedAuditReplay restores the ACL LOG buffer from the audit files a previous
// run left behind, so ACL LOG survives a restart instead of starting empty.
//
// With RBAC disabled there is no buffer to seed. Every other precondition —
// audit enabled, records going to a directory rather than stdout, and the key
// that seals them — belongs to the audit engine, so it decides what is
// replayable. Recovery is best-effort and never blocks startup: an unreadable
// or unrecoverable log simply leaves the buffer empty.
func (s *Server) seedAuditReplay() {
	if s.policy == nil {
		return
	}
	logger := s.app.GetLogger()
	replayed := s.audit.ReplayAuthLog(rbac.DefaultAuthLogCap)
	if len(replayed) == 0 {
		return
	}
	entries := make([]rbac.AuthLogEntry, len(replayed))
	for i, r := range replayed {
		entries[i] = rbac.AuthLogEntry{
			Timestamp:  r.Timestamp,
			Username:   r.Username,
			RemoteAddr: r.RemoteAddr,
			Reason:     r.Reason,
		}
	}
	s.policy.SeedAuthLog(entries)
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "server: restored ACL LOG history from the audit log",
			log.String("dir", s.app.GetConfig().AuditLogPath()),
			log.Int("entries", len(entries)),
		)
	}
}

func (s *Server) initShards(key []byte, cryptoEngine *crypto.Engine) error {
	cfg := s.app.GetConfig()
	numShards := cfg.GetNumShards()
	logger := s.app.GetLogger()

	var store *persistence.Storage
	if cfg.PersistenceEnabled() {
		var err error
		store, err = persistence.NewStorage(true, logger, cfg.GetPersistenceDir())
		if err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: persistence initialization failed", log.String("error", err.Error()))
			}
			return fmt.Errorf("persistence initialization: %w", err)
		}
	}

	s.shards = make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		sh, err := shard.Run(shard.ID(i), cfg, key, cryptoEngine, logger, store)
		if err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: shard initialization failed",
					log.String("error", err.Error()), log.String("shard", fmt.Sprintf("%d", i)))
			}
			return fmt.Errorf("shard %d init: %w", i, err)
		}
		s.shards[i] = sh
	}
	s.router = router.New(s.shards)
	if logger.Enabled(log.LevelInfo) {
		p := "disabled"
		if store != nil {
			p = "enabled"
		}
		logger.Log(log.LevelInfo, "server: shards initialized",
			log.Int("num_shards", numShards),
			log.String("persistence", p),
		)
	}
	return nil
}

func (s *Server) startMetricsServer(srv *network.Server) {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	metricsAddr := cfg.GetMetricsAddr()
	shardCollectors := make([]*metrics.Collector, len(s.shards))
	for i, sh := range s.shards {
		shardCollectors[i] = metrics.NewShardCollector(uint32(sh.ID), sh, sh.Engine, srv, logger)
	}
	var tlsMetrics metrics.TLSMetrics
	if s.tlsReloader != nil {
		tlsMetrics = s.tlsReloader
	}
	// A nil *rbac.Store must not be boxed into the RBACMetrics interface: the
	// scrape path treats a nil interface as "RBAC disabled", but a typed nil
	// would pass the nil check and then panic on RoleCommandCounts.
	var rbacMetrics metrics.RBACMetrics
	if s.policy != nil {
		rbacMetrics = s.policy
	}
	aggregateCollector := metrics.NewAggregateCollector(shardCollectors, srv, tlsMetrics, rbacMetrics)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		aggregateCollector.WritePrometheus(w)
	})
	httpSrv := &http.Server{
		Addr:         metricsAddr,
		Handler:      mux,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	s.metricsSrv = httpSrv
	go func() {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "server: telemetry infrastructure online", log.String("addr", metricsAddr))
		}
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server: metrics server encountered an error", log.String("error", err.Error()))
			}
		}
	}()
}

func (s *Server) startRESPServer() {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	store := &RouterStore{router: s.router}
	respSrv := resp.NewServer(
		cfg.GetRESPAddr(),
		store,
		s.shards,
		logger,
		s.tlsConfigs,
		cfg.GetRequirePass(),
		cfg.RESPStartTLSEnabled(),
		s.policy,
		s.oauth,
		s.audit,
	)
	s.respSrv = respSrv
	go func() {
		if err := respSrv.ListenAndServe(); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "server:resp server encountered an error", log.String("error", err.Error()))
			}
		}
	}()
}

func (s *Server) networkHandler(msg *network.Message) ([]byte, network.MessageType, error) {
	if msg.Type == network.MsgPing {
		return nil, network.MsgPong, nil
	}
	keyStr := *(*string)(unsafe.Pointer(&msg.Key))
	switch msg.Op {
	case network.OpGet:
		resp := s.router.Dispatch(shard.CmdGet, keyStr, nil, 0)
		if !resp.OK {
			return network.ResponseNotFound, network.MsgError, nil
		}
		return resp.Value, network.MsgResponse, nil
	case network.OpSet:
		if len(msg.Key) == 0 {
			return network.ResponseEmptyKey, network.MsgError, nil
		}
		ttlDuration := time.Duration(msg.TTL) * time.Millisecond
		resp := s.router.Dispatch(shard.CmdSet, keyStr, msg.Value, ttlDuration)
		if resp.Err != nil {
			if s.app.GetLogger().Enabled(log.LevelError) {
				s.app.GetLogger().Log(log.LevelError, "server: failed to store inside storage engine", log.String("error", resp.Err.Error()))
			}
			return network.ResponseStorageFailure, network.MsgError, nil
		}
		return network.ResponseOK, network.MsgResponse, nil
	case network.OpDelete:
		s.router.Dispatch(shard.CmdDel, keyStr, nil, 0)
		return network.ResponseOK, network.MsgResponse, nil
	case network.OpRoleCreate, network.OpRoleSetUser, network.OpRoleDelUser,
		network.OpRoleDelete, network.OpRoleList, network.OpRoleGetUser:
		// RBAC is disabled without --rbac-config; the RESP layer rejects ROLE
		// with "RBAC is not enabled" and the binary layer must do the same
		// instead of panicking on a nil policy store.
		if s.policy == nil {
			return roleReply(fmt.Errorf("rbac not enabled"))
		}
		switch msg.Op {
		case network.OpRoleCreate:
			return s.roleCreate(msg)
		case network.OpRoleSetUser:
			return s.roleSetUser(msg)
		case network.OpRoleDelUser:
			return s.roleDelUser(msg)
		case network.OpRoleDelete:
			return s.roleDelete(msg)
		case network.OpRoleList:
			return s.roleList(msg)
		default:
			return s.roleGetUser(msg)
		}
	case network.OpACLSetUser, network.OpACLDelUser, network.OpACLList, network.OpACLLog:
		if s.policy == nil {
			return roleReply(fmt.Errorf("rbac not enabled"))
		}
		switch msg.Op {
		case network.OpACLSetUser:
			return s.aclSetUser(msg)
		case network.OpACLDelUser:
			return s.aclDelUser(msg)
		case network.OpACLList:
			return s.aclList(msg)
		default:
			return s.aclLog(msg)
		}
	default:
		return network.ResponseInvalidOpCode, network.MsgError, nil
	}
}

// roleReply wraps a ROLE result: ResponseOK in a MsgResponse frame on success,
// the error detail in a MsgError frame otherwise. The client surfaces the
// latter as an error without tearing down the connection, so a failed admin op
// never kicks the client.
func roleReply(err error) ([]byte, network.MessageType, error) {
	if err == nil {
		return network.ResponseOK, network.MsgResponse, nil
	}
	return []byte(err.Error()), network.MsgError, nil
}

func (s *Server) roleCreate(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) < 2 {
		return roleReply(fmt.Errorf("invalid ROLE CREATE arguments"))
	}
	rules := make([]string, 0, len(args)-1)
	for _, r := range args[1:] {
		rules = append(rules, string(r))
	}
	return roleReply(s.policy.CreateRole(string(args[0]), rules))
}

func (s *Server) roleSetUser(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) < 2 {
		return roleReply(fmt.Errorf("invalid ROLE SETUSER arguments"))
	}
	if len(args) == 2 {
		return roleReply(fmt.Errorf("ROLE SETUSER requires a '>password' or 'nopass' option"))
	}
	passHash, err := rbac.PasswordFromOpts(args[2:])
	if err != nil {
		return roleReply(err)
	}
	return roleReply(s.policy.SetUser(string(args[0]), string(args[1]), passHash))
}

func (s *Server) roleDelUser(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) != 1 {
		return roleReply(fmt.Errorf("invalid ROLE DELUSER arguments"))
	}
	return roleReply(s.policy.DelUser(string(args[0])))
}

func (s *Server) roleDelete(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) != 1 {
		return roleReply(fmt.Errorf("invalid ROLE DELETE arguments"))
	}
	return roleReply(s.policy.DeleteRole(string(args[0])))
}

func (s *Server) roleList(msg *network.Message) ([]byte, network.MessageType, error) {
	p := s.policy.Load()
	if p == nil {
		return roleReply(fmt.Errorf("rbac policy not loaded"))
	}
	entries := make([]network.RoleListEntry, 0, len(p.Roles))
	for name, r := range p.Roles {
		e := network.RoleListEntry{Name: name, Commands: r.GrantedCommands()}
		for _, ns := range r.Namespaces {
			e.Namespaces = append(e.Namespaces, append([]byte(nil), ns...))
		}
		entries = append(entries, e)
	}
	// Map iteration is unordered; sort by name so identical policies produce
	// a stable, name-ordered response (mirrors the RESP LIST handler).
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	payload, ok := network.EncodeRoleListResponse(entries)
	if !ok {
		return roleReply(fmt.Errorf("role rule exceeds the 64 KiB wire limit"))
	}
	return payload, network.MsgResponse, nil
}

func (s *Server) roleGetUser(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) != 1 {
		return roleReply(fmt.Errorf("invalid ROLE GETUSER arguments"))
	}
	p := s.policy.Load()
	if p == nil {
		return roleReply(fmt.Errorf("rbac policy not loaded"))
	}
	u := p.UserFor(string(args[0]))
	if u == nil {
		return roleReply(fmt.Errorf("user '%s' does not exist", args[0]))
	}
	return network.EncodeRoleGetUserResponse(network.RoleUser{Role: u.Role, HasPass: len(u.PasswordHash) > 0}), network.MsgResponse, nil
}

// aclSetUser handles OpACLSetUser, the binary ACL alias of ROLE SETUSER.
func (s *Server) aclSetUser(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) < 2 {
		return roleReply(fmt.Errorf("invalid ACL SETUSER arguments"))
	}
	if len(args) == 2 {
		return roleReply(fmt.Errorf("ACL SETUSER requires a '>password' or 'nopass' option"))
	}
	passHash, err := rbac.PasswordFromOpts(args[2:])
	if err != nil {
		return roleReply(err)
	}
	return roleReply(s.policy.SetUser(string(args[0]), string(args[1]), passHash))
}

func (s *Server) aclDelUser(msg *network.Message) ([]byte, network.MessageType, error) {
	args, ok := network.DecodeRoleArgs(msg.Value, nil)
	if !ok || len(args) != 1 {
		return roleReply(fmt.Errorf("invalid ACL DELUSER arguments"))
	}
	return roleReply(s.policy.DelUser(string(args[0])))
}

// aclList handles OpACLList, returning one entry per user with the username,
// bound role, password presence, and the role's commands and namespace
// whitelist — never a password hash (mirrors the RESP ACL LIST handler).
func (s *Server) aclList(msg *network.Message) ([]byte, network.MessageType, error) {
	p := s.policy.Load()
	if p == nil {
		return roleReply(fmt.Errorf("rbac policy not loaded"))
	}
	users := make([]network.ACLUser, 0, len(p.Users))
	for name, u := range p.Users {
		e := network.ACLUser{Username: name, Role: u.Role, HasPass: len(u.PasswordHash) > 0}
		// Effective permissions come from RoleFor: the explicit assignment or
		// the Default role for unassigned / role-deleted users, matching the
		// RESP ACL LIST handler.
		if r := p.RoleFor(name); r != nil {
			e.Commands = r.GrantedCommands()
			for _, ns := range r.Namespaces {
				e.Namespaces = append(e.Namespaces, append([]byte(nil), ns...))
			}
		}
		users = append(users, e)
	}
	// Map iteration is unordered; sort by username so identical policies
	// produce a stable, name-ordered response (mirrors the RESP LIST handler).
	sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })
	payload, ok := network.EncodeACLListResponse(users)
	if !ok {
		return roleReply(fmt.Errorf("acl rule exceeds the 64 KiB wire limit"))
	}
	return payload, network.MsgResponse, nil
}

// aclLog handles OpACLLog, returning the recent security-event buffer — rejected
// AUTH attempts and denied commands — in chronological order with timestamp,
// username, remote address, and reason, the binary twin of the RESP ACL LOG
// handler. Entries are already ordered by the store, so no sort is needed.
func (s *Server) aclLog(msg *network.Message) ([]byte, network.MessageType, error) {
	src := s.policy.AuthLog()
	entries := make([]network.AuthLogEntry, 0, len(src))
	for _, e := range src {
		entries = append(entries, network.AuthLogEntry{
			Timestamp:  e.Timestamp.Format(time.RFC3339),
			Username:   e.Username,
			RemoteAddr: e.RemoteAddr,
			Reason:     e.Reason,
		})
	}
	payload, ok := network.EncodeACLLogResponse(entries)
	if !ok {
		return roleReply(fmt.Errorf("acl log exceeds the 64 KiB wire limit"))
	}
	return payload, network.MsgResponse, nil
}
