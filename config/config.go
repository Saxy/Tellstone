package config

import (
	"flag"
	"os"
	"strconv"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

type Config struct {
	Addr          string
	LogLevel      log.Level
	EvictTicker   time.Duration
	EvictSlots    uint32
	EncryptionKey string
	TraceRatio    float64
	MaxMsgSize    uint32
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
//	TSD_ADDR            – server listen address (default "127.0.0.1:9988")
//	TSD_LOG_LEVEL       – log verbosity (debug, info, warn, error, fatal)
//	TSD_EVICT_INTERVAL  – eviction ticker interval (e.g. "500ms", "2s")
//	TSD_EVICT_SLOTS     – number of slots in the timing‑wheel chronometer
//	TSD_ENCRYPTION_KEY  – optional base‑64 symmetric key for data encryption
//	TSD_TRACE_RATIO     – OpenTelemetry sampling ratio in the range [0.0‑1.0]
//	TSD_MAX_MSG_SIZE	- optional parameter to define the maximum msg size
//
// The flag definitions below intentionally repeat the default values in the help text
// to improve discoverability for end users.
func LoadConfig() *Config {
	cfg := new(Config)

	// Network listening address.
	flag.StringVar(
		&cfg.Addr,
		"addr",
		getEnv("TSD_ADDR", "127.0.0.1:9988"),
		"TCP listen address (default: 127.0.0.1:9988)",
	)
	// Log level – accepts values: debug, info, warn, error, fatal.
	var logLevel string
	flag.StringVar(
		&logLevel,
		"log-level",
		getEnv("TSD_LOG_LEVEL", "info"),
		"Log verbosity (debug|info|warn|error|fatal) (default: info)",
	)
	cfg.LogLevel = log.ParseLogLevel(logLevel)
	// Chronometer eviction ticker interval.
	flag.DurationVar(
		&cfg.EvictTicker,
		"evict-interval",
		getEnv("TSD_EVICT_INTERVAL", time.Second),
		"Interval between eviction scans (default: 1s)",
	)
	// Number of slots in the timing‑wheel chronometer (default derived from config).
	var evictSlots uint
	flag.UintVar(
		&evictSlots,
		"evict-slots",
		getEnv("TSD_EVICT_SLOTS", uint(256)),
		"Number of slots in the chronometer wheel (default: 256)",
	)
	cfg.EvictSlots = uint32(evictSlots)
	// Optional encryption key for data at rest.
	flag.StringVar(
		&cfg.EncryptionKey,
		"encryption-key",
		getEnv("TSD_ENCRYPTION_KEY", ""),
		"Base‑64 encoded encryption key; empty disables encryption (default: none)",
	)
	// OpenTelemetry trace sampling ratio.
	flag.Float64Var(
		&cfg.TraceRatio,
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
	flag.Var(
		&maxMsgSize,
		"max-msg-size",
		"Maximum network message size (e.g. 16MiB, 1GiB, 0 = use default 16MiB)",
	)
	cfg.MaxMsgSize = uint32(maxMsgSize)
	// Custom usage output to guide operators.
	flag.Usage = func() {
		println("Tellstone server – high‑performance in‑memory database")
		println("Usage: tellstone [options]")
		println("Options:")
		flag.PrintDefaults()
	}
	// Parse flags without inheriting any arguments from the testing framework.
	// Using an empty slice ensures that only the flags defined above are considered.
	_ = flag.CommandLine.Parse([]string{})
	return cfg
}
