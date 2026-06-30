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
	"os"
	"strconv"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

type Config struct {
	addr             string
	enableMetrics    bool
	metricsAddr      string
	logLevel         log.Level
	evictTicker      time.Duration
	evictSlots       uint32
	enableEncryption bool
	encryptionKey    string
	traceRatio       float64
	maxMsgSize       uint64
	maxMemBytes      uint64
	enableRESP       bool
	respAddr         string
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
//		TSD_ENCRYPTION_KEY  – optional base‑64 symmetric key for data encryption
//		TSD_TRACE_RATIO     – OpenTelemetry sampling ratio in the range [0.0‑1.0]
//		TSD_MAX_MSG_SIZE	- optional parameter to define the maximum msg size
//		TSD_METRICS_ADDR    – Prometheus HTTP exporter address (e.g. ":9100")
//		TSD_MAX_MEM_BYTES   – optional engine memory ceiling (e.g. "512MiB"; 0 = unlimited)
//		TSD_ENABLE_RESP     – boolean to enable the Redis-compatible RESP listener (default: false)
//		TSD_RESP_ADDR       – RESP listener address (default 127.0.0.1:6379)
//		TSD_ENABLE_METRICS  – boolean to activate the Prometheus exporter (default: false)
//	 	TSD_ENABLE_ENCRYPTION  – boolean to enforce data-at-rest encryption (default: false)
//
// args are the command-line arguments to parse (typically os.Args[1:]); pass nil for an
// environment-only / default configuration. A fresh flag.FlagSet is used so LoadConfig is
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
		"Base‑64 encoded encryption key; empty disables encryption (default: none)",
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
	// Optional RESP2 (Redis-compatible) listener, for benchmarking against Redis/Dragonfly/etc.
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
	return cfg
}

func (cfg *Config) GetAddr() string               { return cfg.addr }
func (cfg *Config) MetricsEnabled() bool          { return cfg.enableMetrics }
func (cfg *Config) GetMetricsAddr() string        { return cfg.metricsAddr }
func (cfg *Config) GetLogLevel() log.Level        { return cfg.logLevel }
func (cfg *Config) GetEvictTicker() time.Duration { return cfg.evictTicker }
func (cfg *Config) GetEvictSlots() uint32         { return cfg.evictSlots }
func (cfg *Config) EncryptionEnabled() bool       { return cfg.enableEncryption }
func (cfg *Config) GetEncryptionKey() string      { return cfg.encryptionKey }
func (cfg *Config) GetTraceRatio() float64        { return cfg.traceRatio }
func (cfg *Config) GetMaxMsgSize() uint64         { return cfg.maxMsgSize }
func (cfg *Config) GetMaxMemBytes() uint64        { return cfg.maxMemBytes }
func (cfg *Config) RESPEnabled() bool             { return cfg.enableRESP }
func (cfg *Config) GetRESPAddr() string           { return cfg.respAddr }
