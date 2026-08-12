// Package config_test provides unit tests for the public configuration utilities.

package config

import (
	"os"
	"runtime"
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
