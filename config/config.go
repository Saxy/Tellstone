/*
Package config
Tellstone Cloud-Native In-Memory Database
File: config.go
Description: Loads server configuration from command‑line flags (with environment‑variable fallbacks) into a Config struct.

Authors:

	Maximilian Hagen
*/
package config

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster"
	"github.com/Saxy/Tellstone/internal/log"
)

type Config struct {
	addr              string
	enableMetrics     bool
	metricsAddr       string
	logLevel          log.Level
	evictTicker       time.Duration
	evictSlots        uint32
	enableEncryption  bool
	enableEnvelope    bool
	encryptionKey     string
	encryptionKeyFile string
	traceRatio        float64
	maxMsgSize        uint64
	maxMemBytes       uint64
	enableRESP        bool
	respAddr          string
	respStartTLS      bool
	shutdownTimeout   time.Duration
	numShards         int
	enablePersistence bool
	persistenceDir    string
	tlsCert           string
	tlsKey            string
	tlsCA             string
	requirePass       string
	rbacConfig        string
	enableAuditLog    bool
	auditLogPath      string
	auditLogEvents    string
	oauthProvider     string
	oauthIssuer       string
	oauthClientID     string
	snapshotInterval  time.Duration
	snapshotBytes     uint64
	// Cluster mode (Raft consensus per region).
	clusterMode bool
	nodeID      uint64
	peerAddr    string
	peers       string
	// Placement driver / TSO (phase 2). nodeRole selects the deployment
	// topology per ADR-010 §5: hybrid runs data shards plus an embedded
	// etcd member, pd runs only the embedded member, data skips the embed
	// entirely and dials the external PD at pdAddr. pdClientAddr/pdPeerAddr
	// hold the *resolved* etcd endpoints (override or derived from --addr).
	nodeRole           string
	pdAddr             string
	pdClientAddr       string
	pdPeerAddr         string
	pdMembers          string
	pdDir              string
	tsoMinBatch        uint64
	tsoHeadroomSeconds uint
	tsoRefillThreshold int
}

func getEnv[T any](key string, fallback T) T {
	val, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}
	var res T
	switch p := any(&res).(type) {
	case *string:
		*p = val
	case *int:
		if i, err := strconv.Atoi(val); err == nil {
			*p = i
		} else {
			return fallback
		}
	case *uint:
		if u, err := strconv.ParseUint(val, 10, 64); err == nil {
			*p = uint(u)
		} else {
			return fallback
		}
	case *bool:
		if b, err := strconv.ParseBool(val); err == nil {
			*p = b
		} else {
			return fallback
		}
	case *float64:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			*p = f
		} else {
			return fallback
		}
	case *time.Duration:
		if d, err := time.ParseDuration(val); err == nil {
			*p = d
		} else {
			return fallback
		}
	default:
		return fallback
	}

	return res
}

// LoadConfig parses command‑line flags (with environment‑variable fallbacks) and
// returns a fully populated Config struct. All flags include concise, professional
// descriptions that are displayed when the binary is executed with `-h`.
//
// The function also respects the following environment variables, allowing configuration
// via container orchestration tools or CI pipelines:
//
//		TSD_ADDR            – server listen address (default "127.0.0.1:9988")
//		TSD_LOG_LEVEL       – log verbosity (debug, info, warn, error, fatal)
//		TSD_EVICT_INTERVAL  – eviction ticker interval (e.g. "500ms", "2s")
//		TSD_EVICT_SLOTS     – number of slots in the timing‑wheel chronometer
//		TSD_ENCRYPTION_KEY  – optional base‑64 encoded 32-byte symmetric key for data encryption
//		TSD_ENCRYPTION_KEY_FILE – path to a file holding the raw 32-byte key; mutually exclusive with TSD_ENCRYPTION_KEY
//		TSD_TRACE_RATIO     – OpenTelemetry sampling ratio in the range [0.0‑1.0]
//		TSD_MAX_MSG_SIZE	- optional parameter to define the maximum msg size
//		TSD_METRICS_ADDR    – Prometheus HTTP exporter address (e.g. ":9100")
//		TSD_MAX_MEM_BYTES   – optional engine memory ceiling (e.g. "512MiB"; 0 = unlimited)
//		TSD_ENABLE_RESP     – boolean to enable the Redis-compatible RESP listener (default: false)
//		TSD_RESP_ADDR       – RESP listener address (default 127.0.0.1:6379)
//		TSD_RESP_STARTTLS   – allow RESP clients to upgrade plaintext connections to TLS
//		TSD_ENABLE_METRICS  – boolean to activate the Prometheus exporter (default: false)
//	 	TSD_ENABLE_ENCRYPTION  – boolean to enforce data-at-rest encryption (default: false)
//	 	TSD_ENABLE_ENVELOPE    – boolean to enable envelope encryption (KEK wraps a per-shard DEK; default: false)
//		TSD_SHUTDOWN_TIMEOUT – max wait for graceful shutdown on SIGINT/SIGTERM (default: 10s)
//		TSD_NUM_SHARDS      – number of shared-nothing shards (default: GOMAXPROCS)
//		TSD_ENABLE_PERSISTENCE – boolean to enable WAL persistence (default: false)
//		TSD_PERSISTENCE_DIR  – directory for persistence data files (default: ~/.local/share/tellstone/data)
//		TSD_TLS_CERT         – path to TLS certificate file (PEM; empty = TLS disabled)
//		TSD_TLS_KEY          – path to TLS private key file (PEM)
//		TSD_TLS_CA           – path to CA certificate for client verification (enables mTLS)
//		TSD_REQUIRE_PASS     – server password required by AUTH (empty = no authentication)
//		TSD_RBAC_CONFIG      – path to a YAML/JSON RBAC policy file (roles, users, default_role)
//		TSD_ENABLE_AUDIT	 - boolean to enable audit logging (default: false)
//		TSD_AUDIT_LOG_PATH	 - path where the audit log file(s) will be created (default: stdout)
//		TSD_AUDIT_EVENTS	 - comma-seperated event types on which audit events should be logged (default: auth,acl)
//		TSD_OAUTH_PROVIDER   - OAuth provider preset (google|stackit|empty for generic OIDC)
//		TSD_OAUTH_ISSUER	 - OIDC issuer / discovery base URL of the OAuth provider
//		TSD_OAUTH_CLIENT_ID	 - OAuth2 client ID used as the expected token audience
//
// args are the command-line arguments to parse (typically os.Args[1:]); pass nil for an
// environment-only / default configuration. A fresh flag.FlagSet is used, so LoadConfig is
// free of global state and safe to call repeatedly (e.g. from tests).
func LoadConfig(args []string) *Config {
	cfg := new(Config)
	fs := flag.NewFlagSet("tellstone", flag.ContinueOnError)

	// Network listening address.
	fs.StringVar(
		&cfg.addr,
		"addr",
		getEnv("TSD_ADDR", "127.0.0.1:9988"),
		"TCP listen address (default: 127.0.0.1:9988)",
	)
	fs.BoolVar(
		&cfg.enableMetrics,
		"enable-metrics",
		getEnv("TSD_ENABLE_METRICS", false),
		"Enable the Prometheus HTTP metrics exporter (default: false)",
	)
	fs.StringVar(
		&cfg.metricsAddr,
		"metrics-addr",
		getEnv("TSD_METRICS_ADDR", ":9100"),
		"Prometheus HTTP metrics exporter address (default: :9100)",
	)
	// Log level – accepts values: debug, info, warn, error, fatal.
	var logLevel string
	fs.StringVar(
		&logLevel,
		"log-level",
		getEnv("TSD_LOG_LEVEL", "info"),
		"Log verbosity (debug|info|warn|error|fatal) (default: info)",
	)
	// Chronometer eviction ticker interval.
	fs.DurationVar(
		&cfg.evictTicker,
		"evict-interval",
		getEnv("TSD_EVICT_INTERVAL", time.Second),
		"Interval between eviction scans (default: 1s)",
	)
	// Number of slots in the timing‑wheel chronometer (default derived from config).
	var evictSlots uint
	fs.UintVar(
		&evictSlots,
		"evict-slots",
		getEnv("TSD_EVICT_SLOTS", uint(256)),
		"Number of slots in the chronometer wheel (default: 256)",
	)
	fs.BoolVar(
		&cfg.enableEncryption,
		"enable-encryption",
		getEnv("TSD_ENABLE_ENCRYPTION", false),
		"Enforce symmetric encryption for data at rest (default: false)",
	)
	// Optional encryption key for data at rest.
	fs.StringVar(
		&cfg.encryptionKey,
		"encryption-key",
		getEnv("TSD_ENCRYPTION_KEY", ""),
		"Base‑64 encoded 32-byte encryption key; empty disables encryption (default: none)",
	)
	// Optional file-sourced encryption key, e.g. a mounted Kubernetes Secret or a
	// vault-agent-sidecar-rendered path. Mutually exclusive with --encryption-key.
	// The file carries raw bytes rather than base64: a NUL cannot survive a process
	// argument or environment variable, but a file has no such restriction.
	fs.StringVar(
		&cfg.encryptionKeyFile,
		"encryption-key-file",
		getEnv("TSD_ENCRYPTION_KEY_FILE", ""),
		"Path to a file holding the raw (unencoded) 32-byte encryption key; empty disables (default: none)",
	)
	// Envelope encryption: the configured key becomes a KEK that wraps a per-shard
	// random DEK. Opt-in; the legacy single-key mode remains the default.
	fs.BoolVar(
		&cfg.enableEnvelope,
		"enable-envelope",
		getEnv("TSD_ENABLE_ENVELOPE", false),
		"Envelope encryption: wrap a per-shard random DEK with the configured key (default: false)",
	)
	// OpenTelemetry trace sampling ratio.
	fs.Float64Var(
		&cfg.traceRatio,
		"trace-ratio",
		getEnv("TSD_TRACE_RATIO", 0.0),
		"OTel sampling ratio in [0.0‑1.0] (default: 0.0 – disabled)",
	)
	// Maximum message size for the network server (human‑readable).
	// Accepts KiB, MiB, GiB (binary) or KB, MB, GB (decimal) suffixes.
	// A value of 0 means the server will use its built‑in default (16 MiB).
	var maxMsgSize ByteSize
	// Apply env var if present so the flag gets the same default.
	if env := os.Getenv("TSD_MAX_MSG_SIZE"); env != "" {
		_ = maxMsgSize.Set(env) // ignore errors – malformed env yields zero (default)
	}
	fs.Var(
		&maxMsgSize,
		"max-msg-size",
		"Maximum network message size (e.g. 16MiB, 1GiB, 0 = use default 16MiB)",
	)
	// Total engine memory ceiling, distinct from the per-message size limit.
	// 0 means unlimited on 64-bit (the engine applies a safety cap on 32-bit).
	var maxMemBytes ByteSize
	if env := os.Getenv("TSD_MAX_MEM_BYTES"); env != "" {
		_ = maxMemBytes.Set(env)
	}
	fs.Var(
		&maxMemBytes,
		"max-mem-bytes",
		"Total engine memory ceiling (e.g. 512MiB, 4GiB, 0 = unlimited)",
	)
	// Optional RESP2 (Redis-compatible) listener for benchmarking against Redis/Dragonfly/etc.
	fs.BoolVar(
		&cfg.enableRESP,
		"enable-resp",
		getEnv("TSD_ENABLE_RESP", false),
		"Enable the Redis-compatible RESP listener (default: false)",
	)
	fs.StringVar(
		&cfg.respAddr,
		"resp-addr",
		getEnv("TSD_RESP_ADDR", "127.0.0.1:6379"),
		"RESP listener address (default: 127.0.0.1:6379)",
	)
	fs.BoolVar(
		&cfg.respStartTLS,
		"resp-starttls",
		getEnv("TSD_RESP_STARTTLS", false),
		"Allow RESP clients to upgrade plaintext connections with STARTTLS (default: false)",
	)
	// Maximum time graceful shutdown waits for in-flight connections to drain after
	// SIGINT/SIGTERM before forcing termination.
	fs.DurationVar(
		&cfg.shutdownTimeout,
		"shutdown-timeout",
		getEnv("TSD_SHUTDOWN_TIMEOUT", 10*time.Second),
		"Max time to wait for graceful shutdown on SIGINT/SIGTERM (default: 10s)",
	)
	// Number of shared-nothing shards (one goroutine + one lock-free map per shard).
	fs.IntVar(
		&cfg.numShards,
		"shards",
		getEnv("TSD_NUM_SHARDS", 0),
		"Number of shared-nothing shards (0 = GOMAXPROCS). Each shard = 1 goroutine + 1 lock-free map",
	)
	// Optional write-ahead log persistence per shard.
	fs.BoolVar(
		&cfg.enablePersistence,
		"enable-persistence",
		getEnv("TSD_ENABLE_PERSISTENCE", false),
		"Enable write-ahead log persistence for crash recovery (default: false)",
	)
	fs.StringVar(
		&cfg.persistenceDir,
		"persistence-dir",
		getEnv("TSD_PERSISTENCE_DIR", ""),
		"Directory for persistence data files (default: ~/.local/share/tellstone/data on Linux)",
	)
	// TLS configuration for transport encryption.
	fs.StringVar(
		&cfg.tlsCert,
		"tls-cert",
		getEnv("TSD_TLS_CERT", ""),
		"Path to watched TLS certificate file (PEM); empty disables TLS (default: none)",
	)
	fs.StringVar(
		&cfg.tlsKey,
		"tls-key",
		getEnv("TSD_TLS_KEY", ""),
		"Path to watched TLS private key file (PEM); required when tls-cert is set",
	)
	fs.StringVar(
		&cfg.tlsCA,
		"tls-ca",
		getEnv("TSD_TLS_CA", ""),
		"Path to watched CA certificate for client verification (PEM); enables mTLS when set",
	)
	// Optional server password enforced via the RESP AUTH command.
	fs.StringVar(
		&cfg.requirePass,
		"require-pass",
		getEnv("TSD_REQUIRE_PASS", ""),
		"Password clients must supply via AUTH; empty disables authentication (default: none)",
	)
	// Optional RBAC policy file. When set, per-user authentication and
	// role-based access control replace the single --require-pass password, and
	// SIGHUP re-reads the file for hot-reload.
	fs.StringVar(
		&cfg.rbacConfig,
		"rbac-config",
		getEnv("TSD_RBAC_CONFIG", ""),
		"Path to YAML/JSON RBAC policy file (roles, users, default_role); empty disables RBAC (default: none)",
	)
	fs.BoolVar(
		&cfg.enableAuditLog,
		"enable-audit",
		getEnv("TSD_ENABLE_AUDIT", false),
		"Enable Audit logging (default: false)",
	)
	fs.StringVar(
		&cfg.auditLogPath,
		"audit-log-path",
		getEnv("TSD_AUDIT_LOG_PATH", "stdout"),
		"Path where Audit logging files should be created (default: stdout)",
	)
	fs.StringVar(
		&cfg.auditLogEvents,
		"audit-events",
		getEnv("TSD_AUDIT_EVENTS", "auth,acl"),
		"Comma-separated event types which should be logged, possible events: auth, acl, connect, disconnect, command, all for all event logging (default: auth,acl)",
	)
	// Optional OAuth provider for connection-time token authentication. The
	// provider name selects a preset (google, stackit); empty selects the
	// generic OIDC verifier driven by --oauth-issuer. Token validation needs no
	// client secret — issuer and audience suffice — so none is accepted here.
	fs.StringVar(
		&cfg.oauthProvider,
		"oauth-provider",
		getEnv("TSD_OAUTH_PROVIDER", ""),
		"OAuth provider preset (google|stackit); empty selects generic OIDC via --oauth-issuer (default: none)",
	)
	fs.StringVar(
		&cfg.oauthIssuer,
		"oauth-issuer",
		getEnv("TSD_OAUTH_ISSUER", ""),
		"OIDC issuer / discovery base URL of the OAuth provider (default: none)",
	)
	fs.StringVar(
		&cfg.oauthClientID,
		"oauth-client-id",
		getEnv("TSD_OAUTH_CLIENT_ID", ""),
		"OAuth2 client ID; expected token audience (default: none)",
	)
	fs.DurationVar(
		&cfg.snapshotInterval,
		"snapshot-interval",
		getEnv("TSD_SNAPSHOT_INTERVAL", time.Duration(0)),
		"Interval between periodic snapshots (e.g. 5m); 0 disables periodic snapshots (default: 0)",
	)
	var snapshotBytes ByteSize
	if env := os.Getenv("TSD_SNAPSHOT_BYTES"); env != "" {
		_ = snapshotBytes.Set(env)
	}
	fs.Var(
		&snapshotBytes,
		"snapshot-bytes",
		"WAL size threshold that triggers a snapshot (e.g. 64MiB); 0 disables size-based snapshots (default: 0, disabled)",
	)
	// Cluster mode: Raft consensus per region.
	fs.BoolVar(
		&cfg.clusterMode,
		"cluster-mode",
		getEnv("TSD_CLUSTER_MODE", false),
		"Enable Raft consensus per region (default: false)",
	)
	fs.Uint64Var(
		&cfg.nodeID,
		"node-id",
		getEnv("TSD_NODE_ID", uint64(0)),
		"Unique node identifier; required (non-zero) when --cluster-mode is enabled and must appear in --peers (default: 0)",
	)
	fs.StringVar(
		&cfg.peerAddr,
		"peer-addr",
		getEnv("TSD_PEER_ADDR", "0.0.0.0:9989"),
		"Raft transport listen address (default: 0.0.0.0:9989)",
	)
	fs.StringVar(
		&cfg.peers,
		"peers",
		getEnv("TSD_PEERS", ""),
		"Comma-separated list of peer addresses for initial cluster bootstrap (default: none)",
	)
	// Placement driver / TSO (phase 2, ADR-010).
	fs.StringVar(
		&cfg.nodeRole,
		"node-role",
		getEnv("TSD_NODE_ROLE", "hybrid"),
		"Cluster deployment role: hybrid (data shards + embedded PD), pd (PD member only), or data (external PD via --pd-addr) (default: hybrid)",
	)
	fs.StringVar(
		&cfg.pdAddr,
		"pd-addr",
		getEnv("TSD_PD_ADDR", ""),
		"External placement driver address; required when --node-role=data (default: none)",
	)
	fs.StringVar(
		&cfg.pdClientAddr,
		"pd-client-addr",
		getEnv("TSD_PD_CLIENT_ADDR", ""),
		"Override the embedded etcd client address; derived from --addr port +10000 when unset (default: none)",
	)
	fs.StringVar(
		&cfg.pdPeerAddr,
		"pd-peer-addr",
		getEnv("TSD_PD_PEER_ADDR", ""),
		"Override the embedded etcd peer address; derived from --addr port +20000 when unset (default: none)",
	)
	fs.StringVar(
		&cfg.pdMembers,
		"pd-members",
		getEnv("TSD_PD_MEMBERS", ""),
		"Comma-separated ID@addr list of placement driver members; defaults to --peers. Set when data-role nodes share the Raft membership (default: --peers)",
	)
	fs.StringVar(
		&cfg.pdDir,
		"pd-dir",
		getEnv("TSD_PD_DIR", ""),
		"Data directory for the embedded placement driver; <persistence-dir>/pd when set, otherwise ./pd (default: derived)",
	)
	fs.Uint64Var(
		&cfg.tsoMinBatch,
		"tso-min-batch",
		getEnv("TSD_TSO_MIN_BATCH", uint64(1000)),
		"Minimum number of timestamps per TSO pool refill batch (default: 1000)",
	)
	fs.UintVar(
		&cfg.tsoHeadroomSeconds,
		"tso-headroom-seconds",
		getEnv("TSD_TSO_HEADROOM_SECONDS", uint(30)),
		"Seconds of write headroom a full TSO pool provides at the observed write rate (default: 30)",
	)
	fs.IntVar(
		&cfg.tsoRefillThreshold,
		"tso-refill-threshold",
		getEnv("TSD_TSO_REFILL_THRESHOLD", 20),
		"Remaining-pool percentage that triggers a refill request (default: 20)",
	)
	// Custom usage output to guide operators.
	fs.Usage = func() {
		println("Tellstone server – high-performance in-memory database")
		println("Usage: tellstone [options]")
		println("Options:")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)

	// Resolve derived values after parsing so command-line flags can override defaults.
	cfg.logLevel = log.ParseLogLevel(logLevel)
	cfg.evictSlots = uint32(evictSlots)
	cfg.maxMsgSize = uint64(maxMsgSize)
	cfg.maxMemBytes = uint64(maxMemBytes)
	if cfg.numShards < 1 {
		cfg.numShards = runtime.NumCPU()
	}
	if cfg.maxMsgSize == 0 {
		cfg.maxMsgSize = 16 * 1024 * 1024
	}
	cfg.snapshotBytes = uint64(snapshotBytes)
	// Validate TLS configuration: cert and key must be provided together.
	if (cfg.tlsCert == "") != (cfg.tlsKey == "") {
		panic("tellstone: --tls-cert and --tls-key must both be provided")
	}
	// mTLS requires a valid TLS base (cert + key).
	if cfg.tlsCA != "" && cfg.tlsCert == "" {
		panic("tellstone: --tls-ca requires --tls-cert and --tls-key")
	}
	if cfg.respStartTLS && cfg.tlsCert == "" {
		panic("tellstone: --resp-starttls requires --tls-cert and --tls-key")
	}
	// The raw key and file-sourced key are alternative KeyProvider backends;
	// supplying both leaves the intended source ambiguous.
	if cfg.encryptionKey != "" && cfg.encryptionKeyFile != "" {
		panic("tellstone: --encryption-key and --encryption-key-file are mutually exclusive")
	}
	// Refuse to start rather than silently downgrade: NewEngine treats an empty key as
	// pass-through, so without this the server would serve plaintext after the operator
	// explicitly asked for encryption.
	if cfg.enableEncryption && cfg.encryptionKey == "" && cfg.encryptionKeyFile == "" {
		panic("tellstone: --enable-encryption requires --encryption-key or --encryption-key-file")
	}
	// Envelope encryption is a mode of --enable-encryption, not a substitute for it.
	if cfg.enableEnvelope && !cfg.enableEncryption {
		panic("tellstone: --enable-envelope requires --enable-encryption")
	}
	// Snapshot options require persistence to be enabled; without it the WAL
	// and snapshot files are never created so the flags are meaningless.
	if !cfg.enablePersistence && (cfg.snapshotInterval > 0 || cfg.snapshotBytes > 0) {
		panic("tellstone: --snapshot-interval and --snapshot-bytes require --enable-persistence")
	}
	// Cluster mode requires a parseable, non-empty bootstrap membership that
	// includes this node. Parsing here — with the same ParsePeers the server
	// startup path uses — rejects malformed membership at flag-validation
	// time (empty lists, comma-only input, duplicate IDs, explicit ID 0)
	// instead of surfacing as a broken raft cluster after StartNode.
	if cfg.clusterMode {
		peers, err := cluster.ParsePeers(cfg.peers)
		if err != nil {
			panic(fmt.Sprintf("tellstone: --peers: %v", err))
		}
		if len(peers) == 0 {
			panic("tellstone: --cluster-mode requires --peers with at least one peer address")
		}
		if cfg.nodeID == 0 {
			panic("tellstone: --cluster-mode requires --node-id to be set")
		}
		seen := make(map[uint64]struct{}, len(peers))
		localFound := false
		for _, p := range peers {
			if p.ID == 0 {
				panic("tellstone: --peers contains peer ID 0; IDs must be non-zero")
			}
			if _, dup := seen[p.ID]; dup {
				panic(fmt.Sprintf("tellstone: --peers contains duplicate node ID %d", p.ID))
			}
			seen[p.ID] = struct{}{}
			if p.ID == cfg.nodeID {
				localFound = true
			}
		}
		if !localFound {
			panic("tellstone: --node-id is not present in --peers; the local node must be part of the bootstrap membership")
		}
	}

	// Placement driver / TSO (phase 2, ADR-010 §5–§7). Roles only exist in
	// cluster mode; the data role replaces the embedded etcd member with an
	// external --pd-addr endpoint.
	switch cfg.nodeRole {
	case "hybrid", "pd", "data":
	default:
		panic(fmt.Sprintf("tellstone: --node-role must be hybrid, pd, or data (got %q)", cfg.nodeRole))
	}
	if cfg.nodeRole != "hybrid" && !cfg.clusterMode {
		panic(fmt.Sprintf("tellstone: --node-role=%s requires --cluster-mode", cfg.nodeRole))
	}
	if cfg.nodeRole == "data" && cfg.pdAddr == "" {
		panic("tellstone: --node-role=data requires --pd-addr pointing at an external placement driver")
	}
	if cfg.nodeRole != "data" && cfg.pdAddr != "" {
		panic("tellstone: --pd-addr connects to an external placement driver and requires --node-role=data")
	}
	if cfg.tsoMinBatch < 1 {
		panic("tellstone: --tso-min-batch must be at least 1")
	}
	if cfg.tsoHeadroomSeconds < 1 {
		panic("tellstone: --tso-headroom-seconds must be at least 1")
	}
	if cfg.tsoRefillThreshold < 1 || cfg.tsoRefillThreshold > 99 {
		panic("tellstone: --tso-refill-threshold must be between 1 and 99")
	}
	// The PD member list defaults to the Raft peer list (homogeneous
	// clusters); an explicit --pd-members splits the two when data-role
	// nodes share Raft membership without hosting an etcd member.
	if cfg.pdMembers != "" && (!cfg.clusterMode || cfg.nodeRole == "data") {
		panic("tellstone: --pd-members requires --cluster-mode with a hybrid or pd node role")
	}
	pdDirBase := cfg.pdDir
	if pdDirBase == "" && cfg.persistenceDir != "" {
		pdDirBase = filepath.Join(cfg.persistenceDir, "pd")
	}
	if pdDirBase == "" {
		pdDirBase = "./pd"
	}
	cfg.pdDir = pdDirBase

	// Resolve the embedded etcd endpoints (ADR-010 §6): an explicit
	// override or the data port +10000/+20000. Resolved endpoints must not
	// collide with any configured listen address (own data and Raft ports,
	// every --peers entry) — a derived port silently swallowing another
	// channel would fail far from the cause. Wildcard binds (0.0.0.0)
	// collide on port regardless of the remote host.
	if cfg.clusterMode && cfg.nodeRole != "data" {
		configured := map[string]string{
			cfg.addr:     "--addr",
			cfg.peerAddr: "--peer-addr",
		}
		members, perr := cluster.ParsePeers(cfg.peers)
		if perr != nil {
			panic(fmt.Sprintf("tellstone: --peers: %v", perr))
		}
		for _, p := range members {
			configured[p.Addr] = "--peers"
		}
		cfg.pdClientAddr = resolvePDEndpoint(cfg.pdClientAddr, cfg.addr, 10000, "--pd-client-addr")
		cfg.pdPeerAddr = resolvePDEndpoint(cfg.pdPeerAddr, cfg.addr, 20000, "--pd-peer-addr")
		for _, endpoint := range []string{cfg.pdClientAddr, cfg.pdPeerAddr} {
			for addr, flag := range configured {
				if pdEndpointsCollide(endpoint, addr) {
					panic(fmt.Sprintf(
						"tellstone: resolved PD endpoint %s collides with %s %s; adjust the port or set an explicit override",
						endpoint, flag, addr))
				}
			}
		}

		// The local node hosts an etcd member in these roles, so it must
		// appear in the effective member list under its own node ID.
		memberList := cfg.pdMembers
		if memberList == "" {
			memberList = cfg.peers
		}
		pdPeers, merr := cluster.ParsePeers(memberList)
		if merr != nil {
			panic(fmt.Sprintf("tellstone: --pd-members: %v", merr))
		}
		localMember := false
		for _, p := range pdPeers {
			if p.ID == cfg.nodeID {
				localMember = true
				break
			}
		}
		if !localMember {
			panic("tellstone: --node-id is not present in the PD membership; hybrid and pd roles must host an embedded etcd member")
		}
	}
	return cfg
}

// hostPort splits and range-checks a host:port address.
func hostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("port %q out of range", portStr)
	}
	return host, port, nil
}

// resolvePDEndpoint returns the effective embedded-etcd endpoint: the
// explicit override when set, otherwise the data address with the port
// shifted by delta (ADR-010 §6).
func resolvePDEndpoint(override, dataAddr string, delta int, flag string) string {
	source, port := dataAddr, 0
	if override != "" {
		source = override
	}
	host, p, err := hostPort(source)
	if err != nil {
		panic(fmt.Sprintf("tellstone: %s %q is not a valid host:port: %v", flag, source, err))
	}
	if override != "" {
		port = p
	} else {
		port = p + delta
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// pdEndpointsCollide reports whether two host:port endpoints would bind the
// same socket, treating wildcard hosts (empty, 0.0.0.0, ::) as matching any
// counterpart on the same port.
func pdEndpointsCollide(a, b string) bool {
	ha, pa, errA := hostPort(a)
	hb, pb, errB := hostPort(b)
	if errA != nil || errB != nil {
		return false // malformed addresses are rejected at their own flag site
	}
	wildcard := func(h string) bool { return h == "" || h == "0.0.0.0" || h == "::" }
	sameHost := ha == hb || wildcard(ha) || wildcard(hb)
	return sameHost && pa == pb
}

func (cfg *Config) GetAddr() string                   { return cfg.addr }
func (cfg *Config) MetricsEnabled() bool              { return cfg.enableMetrics }
func (cfg *Config) GetMetricsAddr() string            { return cfg.metricsAddr }
func (cfg *Config) GetLogLevel() log.Level            { return cfg.logLevel }
func (cfg *Config) GetEvictTicker() time.Duration     { return cfg.evictTicker }
func (cfg *Config) GetEvictSlots() uint32             { return cfg.evictSlots }
func (cfg *Config) EncryptionEnabled() bool           { return cfg.enableEncryption }
func (cfg *Config) EnvelopeEnabled() bool             { return cfg.enableEnvelope }
func (cfg *Config) GetEncryptionKey() string          { return cfg.encryptionKey }
func (cfg *Config) GetEncryptionKeyFile() string      { return cfg.encryptionKeyFile }
func (cfg *Config) GetTraceRatio() float64            { return cfg.traceRatio }
func (cfg *Config) GetMaxMsgSize() uint64             { return cfg.maxMsgSize }
func (cfg *Config) GetMaxMemBytes() uint64            { return cfg.maxMemBytes }
func (cfg *Config) RESPEnabled() bool                 { return cfg.enableRESP }
func (cfg *Config) GetRESPAddr() string               { return cfg.respAddr }
func (cfg *Config) RESPStartTLSEnabled() bool         { return cfg.respStartTLS }
func (cfg *Config) GetShutdownTimeout() time.Duration { return cfg.shutdownTimeout }
func (cfg *Config) GetNumShards() int                 { return cfg.numShards }
func (cfg *Config) PersistenceEnabled() bool          { return cfg.enablePersistence }
func (cfg *Config) GetPersistenceDir() string         { return cfg.persistenceDir }
func (cfg *Config) TLSEnabled() bool                  { return cfg.tlsCert != "" && cfg.tlsKey != "" }
func (cfg *Config) GetTLSCert() string                { return cfg.tlsCert }
func (cfg *Config) GetTLSKey() string                 { return cfg.tlsKey }
func (cfg *Config) GetTLSCA() string                  { return cfg.tlsCA }
func (cfg *Config) GetRequirePass() string            { return cfg.requirePass }
func (cfg *Config) GetRBACConfig() string             { return cfg.rbacConfig }
func (cfg *Config) MTLSEnabled() bool {
	return cfg.tlsCert != "" && cfg.tlsKey != "" && cfg.tlsCA != ""
}
func (cfg *Config) AuditEnabled() bool                 { return cfg.enableAuditLog }
func (cfg *Config) AuditLogPath() string               { return cfg.auditLogPath }
func (cfg *Config) AuditLogEvents() []string           { return strings.Split(cfg.auditLogEvents, ",") }
func (cfg *Config) GetOAuthProvider() string           { return cfg.oauthProvider }
func (cfg *Config) GetOAuthIssuer() string             { return cfg.oauthIssuer }
func (cfg *Config) GetOAuthClientID() string           { return cfg.oauthClientID }
func (cfg *Config) GetSnapshotInterval() time.Duration { return cfg.snapshotInterval }
func (cfg *Config) GetSnapshotBytes() uint64           { return cfg.snapshotBytes }
func (cfg *Config) ClusterMode() bool                  { return cfg.clusterMode }
func (cfg *Config) GetNodeID() uint64                  { return cfg.nodeID }
func (cfg *Config) GetPeerAddr() string                { return cfg.peerAddr }
func (cfg *Config) GetPeers() string                   { return cfg.peers }
func (cfg *Config) GetNodeRole() string                { return cfg.nodeRole }
func (cfg *Config) GetPDAddr() string                  { return cfg.pdAddr }
func (cfg *Config) GetPDClientAddr() string            { return cfg.pdClientAddr }
func (cfg *Config) GetPDPeerAddr() string              { return cfg.pdPeerAddr }

// GetPDMembers returns the effective PD member list: --pd-members when set,
// otherwise the Raft --peers list.
func (cfg *Config) GetPDMembers() string {
	if cfg.pdMembers != "" {
		return cfg.pdMembers
	}
	return cfg.peers
}
func (cfg *Config) GetPDDir() string       { return cfg.pdDir }
func (cfg *Config) GetTSOMinBatch() uint64 { return cfg.tsoMinBatch }
func (cfg *Config) GetTSOHeadroom() time.Duration {
	return time.Duration(cfg.tsoHeadroomSeconds) * time.Second
}
func (cfg *Config) GetTSORefillThreshold() int { return cfg.tsoRefillThreshold }
