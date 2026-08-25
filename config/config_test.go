// Package config_test provides unit tests for the public configuration utilities.

package config

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGetEnvPrimitives(t *testing.T) {
	// string
	os.Setenv("TEST_STR", "hello")
	if got := getEnv("TEST_STR", "fallback"); got != "hello" {
		t.Fatalf("expected string env to be 'hello', got %v", got)
	}
	os.Unsetenv("TEST_STR")
	if got := getEnv("TEST_STR", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback string, got %v", got)
	}

	// int
	os.Setenv("TEST_INT", "42")
	if got := getEnv("TEST_INT", 0); got != 42 {
		t.Fatalf("expected int env to be 42, got %v", got)
	}
	os.Unsetenv("TEST_INT")
	if got := getEnv("TEST_INT", 7); got != 7 {
		t.Fatalf("expected fallback int, got %v", got)
	}

	// uint
	os.Setenv("TEST_UINT", "13")
	if got := getEnv("TEST_UINT", uint(13)); got != uint(13) {
		t.Fatalf("expected uint env to be 13, got %v", got)
	}
	os.Unsetenv("TEST_UINT")
	if got := getEnv("TEST_UINT", uint(5)); got != uint(5) {
		t.Fatalf("expected fallback uint, got %v", got)
	}

	// uint32
	os.Setenv("TEST_UINT32", "99")
	if got := getEnv("TEST_UINT32", uint32(99)); got != uint32(99) {
		t.Fatalf("expected uint32 env to be 99, got %v", got)
	}
	os.Unsetenv("TEST_UINT32")
	if got := getEnv("TEST_UINT32", uint32(3)); got != uint32(3) {
		t.Fatalf("expected fallback uint32, got %v", got)
	}

	// bool
	os.Setenv("TEST_BOOL", "true")
	if got := getEnv("TEST_BOOL", false); got != true {
		t.Fatalf("expected bool env true, got %v", got)
	}
	os.Unsetenv("TEST_BOOL")
	if got := getEnv("TEST_BOOL", true); got != true { // fallback true
		t.Fatalf("expected fallback bool true, got %v", got)
	}

	// float64
	os.Setenv("TEST_FLOAT", "3.14")
	if got := getEnv("TEST_FLOAT", 0.0); got != 3.14 {
		t.Fatalf("expected float env 3.14, got %v", got)
	}
	os.Unsetenv("TEST_FLOAT")
	if got := getEnv("TEST_FLOAT", 2.71); got != 2.71 {
		t.Fatalf("expected fallback float 2.71, got %v", got)
	}

	// time.Duration
	os.Setenv("TEST_DUR", "1500ms")
	if got := getEnv("TEST_DUR", time.Second); got != 1500*time.Millisecond {
		t.Fatalf("expected duration 1500ms, got %v", got)
	}
	os.Unsetenv("TEST_DUR")
	if got := getEnv("TEST_DUR", 2*time.Second); got != 2*time.Second {
		t.Fatalf("expected fallback duration 2s, got %v", got)
	}
}

func TestLoadConfigDefaultsAndEnv(t *testing.T) {
	// Ensure a clean environment.
	os.Unsetenv("TSD_ADDR")
	os.Unsetenv("TSD_LOG_LEVEL")
	os.Unsetenv("TSD_EVICT_INTERVAL")
	os.Unsetenv("TSD_EVICT_SLOTS")
	os.Unsetenv("TSD_ENCRYPTION_KEY")
	os.Unsetenv("TSD_TRACE_RATIO")

	cfg := LoadConfig(nil)

	if cfg.GetAddr() != "127.0.0.1:9988" {
		t.Fatalf("default Addr mismatch: %s", cfg.GetAddr())
	}
	if cfg.GetLogLevel() != 1 { // LevelInfo = 1
		t.Fatalf("default LogLevel mismatch: %d", cfg.GetLogLevel())
	}
	if cfg.GetEvictTicker() != time.Second {
		t.Fatalf("default EvictTicker mismatch: %v", cfg.GetEvictTicker())
	}
	if cfg.GetEvictSlots() != 256 {
		t.Fatalf("default EvictSlots mismatch: %d", cfg.GetEvictSlots())
	}
	if cfg.GetEncryptionKey() != "" {
		t.Fatalf("default EncryptionKey should be empty, got %s", cfg.GetEncryptionKey())
	}
	if cfg.GetTraceRatio() != 0.0 {
		t.Fatalf("default TraceRatio mismatch: %f", cfg.GetTraceRatio())
	}
	if cfg.RESPEnabled() {
		t.Fatalf("RESP should be disabled by default")
	}
	if cfg.GetRESPAddr() != "127.0.0.1:6379" {
		t.Fatalf("default RESP addr mismatch: %s", cfg.GetRESPAddr())
	}
	wantShards := runtime.NumCPU()
	if cfg.GetNumShards() != wantShards {
		t.Fatalf("default NumShards mismatch: %d (expected %d)", cfg.GetNumShards(), wantShards)
	}

	// Now set environment variables to override defaults.
	os.Setenv("TSD_ADDR", "0.0.0.0:7777")
	os.Setenv("TSD_LOG_LEVEL", "debug")
	os.Setenv("TSD_EVICT_INTERVAL", "500ms")
	os.Setenv("TSD_EVICT_SLOTS", "512")
	os.Setenv("TSD_ENCRYPTION_KEY", "mykey")
	os.Setenv("TSD_TRACE_RATIO", "0.25")
	os.Setenv("TSD_NUM_SHARDS", "16")

	cfg = LoadConfig(nil)

	if cfg.GetAddr() != "0.0.0.0:7777" {
		t.Fatalf("env Addr mismatch: %s", cfg.GetAddr())
	}
	if cfg.GetLogLevel() != 0 { // LevelDebug = 0
		t.Fatalf("env LogLevel mismatch: %d", cfg.GetLogLevel())
	}
	if cfg.GetEvictTicker() != 500*time.Millisecond {
		t.Fatalf("env EvictTicker mismatch: %v", cfg.GetEvictTicker())
	}
	if cfg.GetEvictSlots() != 512 {
		t.Fatalf("env EvictSlots mismatch: %d", cfg.GetEvictSlots())
	}
	if cfg.GetEncryptionKey() != "mykey" {
		t.Fatalf("env EncryptionKey mismatch: %s", cfg.GetEncryptionKey())
	}
	if cfg.GetTraceRatio() != 0.25 {
		t.Fatalf("env TraceRatio mismatch: %f", cfg.GetTraceRatio())
	}
	if cfg.GetNumShards() != 16 {
		t.Fatalf("env NumShards mismatch: %d (expected 16)", cfg.GetNumShards())
	}

	// Clean up env so subsequent tests/packages see a pristine environment.
	os.Unsetenv("TSD_ADDR")
	os.Unsetenv("TSD_LOG_LEVEL")
	os.Unsetenv("TSD_EVICT_INTERVAL")
	os.Unsetenv("TSD_EVICT_SLOTS")
	os.Unsetenv("TSD_ENCRYPTION_KEY")
	os.Unsetenv("TSD_TRACE_RATIO")
	os.Unsetenv("TSD_NUM_SHARDS")

	// Persistence defaults should be disabled.
	if cfg.PersistenceEnabled() {
		t.Fatalf("persistence should be disabled by default")
	}
	if cfg.GetPersistenceDir() != "" {
		t.Fatalf("default PersistenceDir should be empty, got %s", cfg.GetPersistenceDir())
	}

	// Set persistence env vars.
	t.Setenv("TSD_ENABLE_PERSISTENCE", "true")
	t.Setenv("TSD_PERSISTENCE_DIR", "/tmp/test-persist")

	cfg = LoadConfig(nil)

	if !cfg.PersistenceEnabled() {
		t.Fatalf("persistence should be enabled via env")
	}
	if cfg.GetPersistenceDir() != "/tmp/test-persist" {
		t.Fatalf("persistence dir mismatch: %s", cfg.GetPersistenceDir())
	}
}

func TestTLSDefaultsDisabled(t *testing.T) {
	t.Setenv("TSD_TLS_CERT", "")
	t.Setenv("TSD_TLS_KEY", "")
	t.Setenv("TSD_TLS_CA", "")

	cfg := LoadConfig(nil)

	if cfg.TLSEnabled() {
		t.Fatalf("TLS should be disabled by default")
	}
	if cfg.MTLSEnabled() {
		t.Fatalf("mTLS should be disabled by default")
	}
	if cfg.GetTLSCert() != "" {
		t.Fatalf("default TLSCert should be empty, got %s", cfg.GetTLSCert())
	}
	if cfg.GetTLSKey() != "" {
		t.Fatalf("default TLSKey should be empty, got %s", cfg.GetTLSKey())
	}
	if cfg.GetTLSCA() != "" {
		t.Fatalf("default TLSCA should be empty, got %s", cfg.GetTLSCA())
	}
}

func TestTLSEnabledCertAndKey(t *testing.T) {
	cfg := LoadConfig([]string{
		"--tls-cert", "/path/to/cert.pem",
		"--tls-key", "/path/to/key.pem",
	})

	if !cfg.TLSEnabled() {
		t.Fatalf("TLS should be enabled when cert and key are set")
	}
	if cfg.MTLSEnabled() {
		t.Fatalf("mTLS should not be enabled without CA")
	}
	if cfg.GetTLSCert() != "/path/to/cert.pem" {
		t.Fatalf("TLSCert mismatch: %s", cfg.GetTLSCert())
	}
	if cfg.GetTLSKey() != "/path/to/key.pem" {
		t.Fatalf("TLSKey mismatch: %s", cfg.GetTLSKey())
	}
	if cfg.GetTLSCA() != "" {
		t.Fatalf("TLSCA should be empty without CA flag")
	}
}

func TestMTLSEnabledCertKeyCA(t *testing.T) {
	cfg := LoadConfig([]string{
		"--tls-cert", "/path/to/cert.pem",
		"--tls-key", "/path/to/key.pem",
		"--tls-ca", "/path/to/ca.pem",
	})

	if !cfg.TLSEnabled() {
		t.Fatalf("TLS should be enabled when mTLS is enabled")
	}
	if !cfg.MTLSEnabled() {
		t.Fatalf("mTLS should be enabled when cert, key, and CA are set")
	}
	if cfg.GetTLSCA() != "/path/to/ca.pem" {
		t.Fatalf("TLSCA mismatch: %s", cfg.GetTLSCA())
	}
}

func TestTLSPanicCertOnly(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when only tls-cert is set")
		}
	}()
	LoadConfig([]string{"--tls-cert", "/path/to/cert.pem"})
}

func TestTLSPanicKeyOnly(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when only tls-key is set")
		}
	}()
	LoadConfig([]string{"--tls-key", "/path/to/key.pem"})
}

func TestTLSPanicCAOnly(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when only tls-ca is set")
		}
	}()
	LoadConfig([]string{"--tls-ca", "/path/to/ca.pem"})
}

func TestEncryptionKeyFileFlagAndEnv(t *testing.T) {
	t.Setenv("TSD_ENCRYPTION_KEY", "")
	cfg := LoadConfig([]string{"--encryption-key-file", "/path/to/key"})
	if cfg.GetEncryptionKeyFile() != "/path/to/key" {
		t.Fatalf("EncryptionKeyFile mismatch: %s", cfg.GetEncryptionKeyFile())
	}
	if cfg.GetEncryptionKey() != "" {
		t.Fatalf("EncryptionKey should be empty when only the file flag is set")
	}

	t.Setenv("TSD_ENCRYPTION_KEY_FILE", "/env/key")
	cfg = LoadConfig(nil)
	if cfg.GetEncryptionKeyFile() != "/env/key" {
		t.Fatalf("EncryptionKeyFile env mismatch: %s", cfg.GetEncryptionKeyFile())
	}
}

func TestEncryptionKeyPanicWhenBothSourcesSet(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when both --encryption-key and --encryption-key-file are set")
		}
	}()
	LoadConfig([]string{"--encryption-key", "raw-key-value", "--encryption-key-file", "/path/to/key"})
}

// An empty key puts the crypto engine in pass-through mode, so accepting this
// configuration would serve plaintext while the operator believes encryption is on.
func TestEnableEncryptionPanicWithoutKeySource(t *testing.T) {
	t.Setenv("TSD_ENCRYPTION_KEY", "")
	t.Setenv("TSD_ENCRYPTION_KEY_FILE", "")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when --enable-encryption is set with no key source")
		}
	}()
	LoadConfig([]string{"--enable-encryption"})
}

func TestEnableEncryptionAcceptsEitherKeySource(t *testing.T) {
	t.Setenv("TSD_ENCRYPTION_KEY", "")
	t.Setenv("TSD_ENCRYPTION_KEY_FILE", "")

	cfg := LoadConfig([]string{"--enable-encryption", "--encryption-key", "raw-key-value"})
	if !cfg.EncryptionEnabled() || cfg.GetEncryptionKey() != "raw-key-value" {
		t.Fatal("raw key source should be accepted with --enable-encryption")
	}

	cfg = LoadConfig([]string{"--enable-encryption", "--encryption-key-file", "/path/to/key"})
	if !cfg.EncryptionEnabled() || cfg.GetEncryptionKeyFile() != "/path/to/key" {
		t.Fatal("file key source should be accepted with --enable-encryption")
	}
}

func TestEnableEnvelopePanicWithoutEncryption(t *testing.T) {
	t.Setenv("TSD_ENABLE_ENCRYPTION", "false")
	t.Setenv("TSD_ENCRYPTION_KEY", "")
	t.Setenv("TSD_ENCRYPTION_KEY_FILE", "")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when --enable-envelope is set without --enable-encryption")
		}
	}()
	LoadConfig([]string{"--enable-envelope"})
}

func TestEnableEnvelopeRequiresKeySource(t *testing.T) {
	t.Setenv("TSD_ENCRYPTION_KEY", "")
	t.Setenv("TSD_ENCRYPTION_KEY_FILE", "")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when --enable-envelope has no key source")
		}
	}()
	LoadConfig([]string{"--enable-encryption", "--enable-envelope"})
}

func TestEnableEnvelopeAcceptedWithKey(t *testing.T) {
	t.Setenv("TSD_ENCRYPTION_KEY", "")
	t.Setenv("TSD_ENCRYPTION_KEY_FILE", "")
	cfg := LoadConfig([]string{"--enable-encryption", "--enable-envelope", "--encryption-key", "raw-key-value"})
	if !cfg.EnvelopeEnabled() {
		t.Fatal("envelope mode should be enabled")
	}
}

func TestTLSEnvVars(t *testing.T) {
	t.Setenv("TSD_TLS_CERT", "/env/cert.pem")
	t.Setenv("TSD_TLS_KEY", "/env/key.pem")
	t.Setenv("TSD_TLS_CA", "/env/ca.pem")

	cfg := LoadConfig(nil)

	if !cfg.TLSEnabled() {
		t.Fatalf("TLS should be enabled via env vars")
	}
	if !cfg.MTLSEnabled() {
		t.Fatalf("mTLS should be enabled via env vars")
	}
	if cfg.GetTLSCert() != "/env/cert.pem" {
		t.Fatalf("TLSCert env mismatch: %s", cfg.GetTLSCert())
	}
	if cfg.GetTLSKey() != "/env/key.pem" {
		t.Fatalf("TLSKey env mismatch: %s", cfg.GetTLSKey())
	}
	if cfg.GetTLSCA() != "/env/ca.pem" {
		t.Fatalf("TLSCA env mismatch: %s", cfg.GetTLSCA())
	}
}

func TestTLSFlagsOverrideEnvVars(t *testing.T) {
	t.Setenv("TSD_TLS_CERT", "/env/cert.pem")
	t.Setenv("TSD_TLS_KEY", "/env/key.pem")

	cfg := LoadConfig([]string{
		"--tls-cert", "/flag/cert.pem",
		"--tls-key", "/flag/key.pem",
	})

	if cfg.GetTLSCert() != "/flag/cert.pem" {
		t.Fatalf("flag should override env for cert: %s", cfg.GetTLSCert())
	}
	if cfg.GetTLSKey() != "/flag/key.pem" {
		t.Fatalf("flag should override env for key: %s", cfg.GetTLSKey())
	}
}

func TestRESPStartTLSDefaultsDisabled(t *testing.T) {
	t.Setenv("TSD_RESP_STARTTLS", "")
	cfg := LoadConfig(nil)
	if cfg.RESPStartTLSEnabled() {
		t.Fatal("RESP STARTTLS should be disabled by default")
	}
}

func TestRESPStartTLSFlagAndEnv(t *testing.T) {
	cfg := LoadConfig([]string{
		"--tls-cert", "/path/to/cert.pem",
		"--tls-key", "/path/to/key.pem",
		"--resp-starttls",
	})
	if !cfg.RESPStartTLSEnabled() {
		t.Fatal("RESP STARTTLS should be enabled by flag")
	}

	t.Setenv("TSD_TLS_CERT", "/env/cert.pem")
	t.Setenv("TSD_TLS_KEY", "/env/key.pem")
	t.Setenv("TSD_RESP_STARTTLS", "true")
	cfg = LoadConfig(nil)
	if !cfg.RESPStartTLSEnabled() {
		t.Fatal("RESP STARTTLS should be enabled by environment")
	}
}

func TestRESPStartTLSPanicWithoutTLS(t *testing.T) {
	t.Setenv("TSD_TLS_CERT", "")
	t.Setenv("TSD_TLS_KEY", "")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when RESP STARTTLS is enabled without TLS material")
		}
	}()
	LoadConfig([]string{"--resp-starttls"})
}

func TestRequirePassDefaultEmpty(t *testing.T) {
	cfg := LoadConfig(nil)
	if cfg.GetRequirePass() != "" {
		t.Fatalf("require-pass should default to empty, got %q", cfg.GetRequirePass())
	}
}

func TestRequirePassFlag(t *testing.T) {
	cfg := LoadConfig([]string{"--require-pass", "hunter2"})
	if cfg.GetRequirePass() != "hunter2" {
		t.Fatalf("require-pass flag mismatch: %q", cfg.GetRequirePass())
	}
}

func TestRequirePassEnvVar(t *testing.T) {
	t.Setenv("TSD_REQUIRE_PASS", "envpass")
	cfg := LoadConfig(nil)
	if cfg.GetRequirePass() != "envpass" {
		t.Fatalf("require-pass env mismatch: %q", cfg.GetRequirePass())
	}
}

func TestRBACConfigEnvVar(t *testing.T) {
	t.Setenv("TSD_RBAC_CONFIG", "/env/policy.yaml")
	cfg := LoadConfig(nil)
	if cfg.GetRBACConfig() != "/env/policy.yaml" {
		t.Fatalf("rbac-config env mismatch: %q", cfg.GetRBACConfig())
	}
}

func TestGetMaxMsgSizeDefault(t *testing.T) {
	t.Setenv("TSD_MAX_MSG_SIZE", "")
	cfg := LoadConfig(nil)
	if cfg.GetMaxMsgSize() != 16*1024*1024 {
		t.Fatalf("default maxMsgSize = %d, want %d", cfg.GetMaxMsgSize(), 16*1024*1024)
	}
}

func TestGetMaxMsgSizeZeroExplicit(t *testing.T) {
	t.Setenv("TSD_MAX_MSG_SIZE", "0")
	cfg := LoadConfig(nil)
	if cfg.GetMaxMsgSize() != 16*1024*1024 {
		t.Fatalf("maxMsgSize(0) = %d, want %d", cfg.GetMaxMsgSize(), 16*1024*1024)
	}
}

func TestGetMaxMsgSizeNonZero(t *testing.T) {
	cfg := LoadConfig([]string{"--max-msg-size", "32MiB"})
	if cfg.GetMaxMsgSize() != 32*1024*1024 {
		t.Fatalf("maxMsgSize(32MiB) = %d, want %d", cfg.GetMaxMsgSize(), 32*1024*1024)
	}
}

// tryLoadClusterConfig runs LoadConfig in cluster mode and returns the
// parsed config plus the panic message ("" when validation passed).
func tryLoadClusterConfig(extra ...string) (cfg *Config, msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	args := append([]string{"--cluster-mode"}, extra...)
	return LoadConfig(args), ""
}

// TestClusterMembershipValidation pins the flag-time rejection of invalid
// bootstrap membership: every case below must fail fast with a descriptive
// panic instead of surfacing as a broken raft cluster after StartNode.
func TestClusterMembershipValidation(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing peers",
			args: []string{"--node-id", "1"},
			want: "at least one peer address",
		},
		{
			name: "comma-only peers",
			args: []string{"--peers", ",,,", "--node-id", "1"},
			want: "at least one peer address",
		},
		{
			name: "node-id zero",
			args: []string{"--peers", "1@127.0.0.1:9001", "--node-id", "0"},
			want: "requires --node-id",
		},
		{
			name: "node-id absent from membership",
			args: []string{"--peers", "1@127.0.0.1:9001,2@127.0.0.1:9002", "--node-id", "3"},
			want: "not present in --peers",
		},
		{
			name: "duplicate peer ids",
			args: []string{"--peers", "1@127.0.0.1:9001,1@127.0.0.1:9002", "--node-id", "1"},
			want: "duplicate node ID 1",
		},
		{
			name: "explicit zero peer id",
			args: []string{"--peers", "5@127.0.0.1:9001,0@127.0.0.1:9002", "--node-id", "5"},
			want: "peer ID 0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, msg := tryLoadClusterConfig(tc.args...)
			if msg == "" {
				t.Fatalf("LoadConfig(%v): expected panic containing %q, got none", tc.args, tc.want)
			}
			if !strings.Contains(msg, tc.want) {
				t.Fatalf("panic message %q does not contain %q", msg, tc.want)
			}
		})
	}

	t.Run("valid membership passes", func(t *testing.T) {
		cfg, msg := tryLoadClusterConfig(
			"--peers", "1@127.0.0.1:9001,2@127.0.0.1:9002",
			"--node-id", "2",
		)
		if msg != "" {
			t.Fatalf("valid membership rejected: %s", msg)
		}
		if cfg.GetNodeID() != 2 {
			t.Fatalf("node-id: got %d, want 2", cfg.GetNodeID())
		}
		if cfg.GetPeers() != "1@127.0.0.1:9001,2@127.0.0.1:9002" {
			t.Fatalf("peers: got %q", cfg.GetPeers())
		}
	})
}

// tryLoadConfig runs LoadConfig and returns the parsed config plus the
// panic message ("" when validation passed).
func tryLoadConfig(args ...string) (cfg *Config, msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	return LoadConfig(args), ""
}

// TestNodeRoleValidation pins the phase-2 role rules from ADR-010 §5:
// roles exist only in cluster mode, data requires an external PD, and the
// external-PD flag is exclusive to the data role.
func TestNodeRoleValidation(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "unknown role",
			args: []string{"--node-role", "observer"},
			want: "must be hybrid, pd, or data",
		},
		{
			name: "data role outside cluster mode",
			args: []string{"--node-role", "data", "--pd-addr", "127.0.0.1:2379"},
			want: "requires --cluster-mode",
		},
		{
			name: "pd role outside cluster mode",
			args: []string{"--node-role", "pd"},
			want: "requires --cluster-mode",
		},
		{
			name: "data role without external pd",
			args: []string{"--cluster-mode", "--node-role", "data",
				"--peers", "1@127.0.0.1:9001", "--node-id", "1"},
			want: "requires --pd-addr",
		},
		{
			name: "external pd flag on hybrid role",
			args: []string{"--cluster-mode", "--pd-addr", "127.0.0.1:2379",
				"--peers", "1@127.0.0.1:9001", "--node-id", "1"},
			want: "requires --node-role=data",
		},
		{
			name: "data role with malformed pd-addr (no port)",
			args: []string{"--cluster-mode", "--node-role", "data",
				"--pd-addr", "127.0.0.1",
				"--peers", "1@127.0.0.1:9001", "--node-id", "1"},
			want: "malformed",
		},
		{
			name: "data role with non-numeric pd-addr port",
			args: []string{"--cluster-mode", "--node-role", "data",
				"--pd-addr", "127.0.0.1:abc",
				"--peers", "1@127.0.0.1:9001", "--node-id", "1"},
			want: "out of range",
		},
		{
			name: "data role with zero pd-addr port",
			args: []string{"--cluster-mode", "--node-role", "data",
				"--pd-addr", "127.0.0.1:0",
				"--peers", "1@127.0.0.1:9001", "--node-id", "1"},
			want: "out of range",
		},
		{
			name: "data role with out-of-range pd-addr port",
			args: []string{"--cluster-mode", "--node-role", "data",
				"--pd-addr", "127.0.0.1:70000",
				"--peers", "1@127.0.0.1:9001", "--node-id", "1"},
			want: "out of range",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, msg := tryLoadConfig(tc.args...)
			if !strings.Contains(msg, tc.want) {
				t.Fatalf("panic message %q does not contain %q", msg, tc.want)
			}
		})
	}

	t.Run("defaults are hybrid with no pd flags", func(t *testing.T) {
		cfg, msg := tryLoadConfig()
		if msg != "" {
			t.Fatalf("default config rejected: %s", msg)
		}
		if cfg.GetNodeRole() != "hybrid" {
			t.Fatalf("node role: got %q, want hybrid", cfg.GetNodeRole())
		}
		if cfg.GetPDAddr() != "" {
			t.Fatalf("pd addr: got %q, want empty", cfg.GetPDAddr())
		}
		if cfg.GetTSOMinBatch() != 1000 || cfg.GetTSOHeadroom() != 30*time.Second ||
			cfg.GetTSORefillThreshold() != 20 {
			t.Fatalf("tso defaults: batch=%d headroom=%v threshold=%d",
				cfg.GetTSOMinBatch(), cfg.GetTSOHeadroom(), cfg.GetTSORefillThreshold())
		}
	})
}

// TestPDAddressResolution verifies ADR-010 §6 addressing: deterministic
// derivation from the data port, explicit overrides, and startup rejection
// of endpoints that would swallow another configured listener.
func TestPDAddressResolution(t *testing.T) {
	t.Run("derived ports follow data+10000/+20000", func(t *testing.T) {
		cfg, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7000",
			"--peer-addr", "127.0.0.1:7001",
			"--peers", "1@127.0.0.1:9001,2@127.0.0.1:9002",
			"--node-id", "1",
		)
		if msg != "" {
			t.Fatalf("valid cluster config rejected: %s", msg)
		}
		if got := cfg.GetPDClientAddr(); got != "127.0.0.1:17000" {
			t.Fatalf("client addr: got %q, want 127.0.0.1:17000", got)
		}
		if got := cfg.GetPDPeerAddr(); got != "127.0.0.1:27000" {
			t.Fatalf("peer addr: got %q, want 127.0.0.1:27000", got)
		}
	})

	t.Run("overrides win over derivation", func(t *testing.T) {
		cfg, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7000",
			"--pd-client-addr", "127.0.0.1:12379",
			"--pd-peer-addr", "127.0.0.1:12380",
			"--peers", "1@127.0.0.1:9001",
			"--node-id", "1",
		)
		if msg != "" {
			t.Fatalf("valid cluster config rejected: %s", msg)
		}
		if got := cfg.GetPDClientAddr(); got != "127.0.0.1:12379" {
			t.Fatalf("client addr: got %q, want override 127.0.0.1:12379", got)
		}
		if got := cfg.GetPDPeerAddr(); got != "127.0.0.1:12380" {
			t.Fatalf("peer addr: got %q, want override 127.0.0.1:12380", got)
		}
	})

	t.Run("derived endpoint colliding with a peer data port is rejected", func(t *testing.T) {
		// Data port 7000 derives client port 17000; peer 3 already uses it.
		_, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7000",
			"--peers", "1@127.0.0.1:9001,3@127.0.0.1:17000",
			"--node-id", "1",
		)
		if !strings.Contains(msg, "collides") {
			t.Fatalf("expected collision panic, got %q", msg)
		}
	})

	t.Run("wildcard bind collides across hosts on same port", func(t *testing.T) {
		// Default --peer-addr binds 0.0.0.0:<raft>; a PD endpoint on that
		// port must be rejected even though the hosts differ textually.
		_, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7989", // client would derive to 17989
			"--peer-addr", "0.0.0.0:9989",
			"--pd-client-addr", "10.0.0.1:9989", // different host, same port
			"--peers", "1@127.0.0.1:9001",
			"--node-id", "1",
		)
		if !strings.Contains(msg, "collides") {
			t.Fatalf("expected wildcard collision panic, got %q", msg)
		}
	})

	t.Run("malformed override rejected", func(t *testing.T) {
		_, msg := tryLoadConfig(
			"--cluster-mode",
			"--pd-client-addr", "no-port-here",
			"--peers", "1@127.0.0.1:9001",
			"--node-id", "1",
		)
		if !strings.Contains(msg, "--pd-client-addr") {
			t.Fatalf("expected malformed-address panic naming the flag, got %q", msg)
		}
	})
}

// TestTSOHeadroomBounds covers the overflow guard on --tso-headroom-seconds:
// the largest duration representable in seconds is accepted, the first value
// above it is rejected before it can become a negative time.Duration.
func TestTSOHeadroomBounds(t *testing.T) {
	const maxSeconds = math.MaxInt64 / int64(time.Second) // 9223372036
	t.Run("maximum accepted value", func(t *testing.T) {
		if _, msg := tryLoadConfig("--tso-headroom-seconds", strconv.FormatInt(maxSeconds, 10)); msg != "" {
			t.Fatalf("max accepted headroom rejected: %s", msg)
		}
	})
	t.Run("first rejected value overflows", func(t *testing.T) {
		_, msg := tryLoadConfig("--tso-headroom-seconds", strconv.FormatInt(maxSeconds+1, 10))
		if !strings.Contains(msg, "too large") {
			t.Fatalf("expected overflow panic, got %q", msg)
		}
	})
}

// TestPDMembersAndDirValidation covers ADR-010 §5 membership split and the
// --pd-dir fallback chain.
func TestPDMembersAndDirValidation(t *testing.T) {
	t.Run("pd-members outside cluster mode rejected", func(t *testing.T) {
		_, msg := tryLoadConfig("--pd-members", "1@127.0.0.1:9001")
		if !strings.Contains(msg, "--pd-members") {
			t.Fatalf("expected pd-members panic, got %q", msg)
		}
	})

	t.Run("pd-members with data role rejected", func(t *testing.T) {
		_, msg := tryLoadConfig(
			"--cluster-mode", "--node-role", "data",
			"--pd-addr", "127.0.0.1:2379",
			"--peers", "1@127.0.0.1:9001",
			"--node-id", "1",
			"--pd-members", "1@127.0.0.1:9001",
		)
		if !strings.Contains(msg, "--pd-members") {
			t.Fatalf("expected pd-members panic, got %q", msg)
		}
	})

	t.Run("local node must host a member in hybrid role", func(t *testing.T) {
		_, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7000",
			"--peers", "1@127.0.0.1:9001,2@127.0.0.1:9002",
			"--node-id", "2",
			"--pd-members", "1@127.0.0.1:9001", // excludes local node 2
		)
		if !strings.Contains(msg, "PD membership") {
			t.Fatalf("expected missing-local-member panic, got %q", msg)
		}
	})

	t.Run("effective members default to peers", func(t *testing.T) {
		cfg, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7000",
			"--peers", "1@127.0.0.1:9001,2@127.0.0.1:9002",
			"--node-id", "1",
		)
		if msg != "" {
			t.Fatalf("valid config rejected: %s", msg)
		}
		if got := cfg.GetPDMembers(); got != cfg.GetPeers() {
			t.Fatalf("members: got %q, want %q", got, cfg.GetPeers())
		}
	})

	t.Run("explicit members override peers default", func(t *testing.T) {
		cfg, msg := tryLoadConfig(
			"--cluster-mode",
			"--addr", "127.0.0.1:7000",
			"--peers", "1@127.0.0.1:9001,3@127.0.0.1:9003",
			"--node-id", "1",
			"--pd-members", "1@127.0.0.1:9001,2@127.0.0.1:9101",
		)
		if msg != "" {
			t.Fatalf("valid config rejected: %s", msg)
		}
		if got := cfg.GetPDMembers(); got != "1@127.0.0.1:9001,2@127.0.0.1:9101" {
			t.Fatalf("members: got %q", got)
		}
	})

	t.Run("pd-dir fallback chain", func(t *testing.T) {
		cfg, _ := tryLoadConfig()
		if got := cfg.GetPDDir(); got != "./pd" {
			t.Fatalf("bare default dir: got %q, want ./pd", got)
		}
		cfg, _ = tryLoadConfig("--persistence-dir", "/data/ts")
		if got := cfg.GetPDDir(); got != "/data/ts/pd" {
			t.Fatalf("persistence-derived dir: got %q, want /data/ts/pd", got)
		}
		cfg, _ = tryLoadConfig("--persistence-dir", "/data/ts", "--pd-dir", "/fast/ssd")
		if got := cfg.GetPDDir(); got != "/fast/ssd" {
			t.Fatalf("explicit dir: got %q, want /fast/ssd", got)
		}
	})
}
