// Package config_test provides unit tests for the public configuration utilities.

package config

import (
	"flag"
	"os"
	"testing"
	"time"
)

// resetFlagSet restores the default flag.CommandLine between test runs.
func resetFlagSet() {
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
}

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

	resetFlagSet()
	cfg := loadConfig()

	if cfg.Addr != "127.0.0.1:9988" {
		t.Fatalf("default Addr mismatch: %s", cfg.Addr)
	}
	if cfg.LogLevel != 1 { // LevelInfo = 1
		t.Fatalf("default LogLevel mismatch: %d", cfg.LogLevel)
	}
	if cfg.EvictTicker != time.Second {
		t.Fatalf("default EvictTicker mismatch: %v", cfg.EvictTicker)
	}
	if cfg.EvictSlots != 256 {
		t.Fatalf("default EvictSlots mismatch: %d", cfg.EvictSlots)
	}
	if cfg.EncryptionKey != "" {
		t.Fatalf("default EncryptionKey should be empty, got %s", cfg.EncryptionKey)
	}
	if cfg.TraceRatio != 0.0 {
		t.Fatalf("default TraceRatio mismatch: %f", cfg.TraceRatio)
	}

	// Now set environment variables to override defaults.
	os.Setenv("TSD_ADDR", "0.0.0.0:7777")
	os.Setenv("TSD_LOG_LEVEL", "debug")
	os.Setenv("TSD_EVICT_INTERVAL", "500ms")
	os.Setenv("TSD_EVICT_SLOTS", "512")
	os.Setenv("TSD_ENCRYPTION_KEY", "mykey")
	os.Setenv("TSD_TRACE_RATIO", "0.25")

	resetFlagSet()
	cfg = loadConfig()

	if cfg.Addr != "0.0.0.0:7777" {
		t.Fatalf("env Addr mismatch: %s", cfg.Addr)
	}
	if cfg.LogLevel != 0 { // LevelDebug = 0
		t.Fatalf("env LogLevel mismatch: %d", cfg.LogLevel)
	}
	if cfg.EvictTicker != 500*time.Millisecond {
		t.Fatalf("env EvictTicker mismatch: %v", cfg.EvictTicker)
	}
	if cfg.EvictSlots != 256 {
		t.Fatalf("env EvictSlots mismatch: %d", cfg.EvictSlots)
	}
	if cfg.EncryptionKey != "mykey" {
		t.Fatalf("env EncryptionKey mismatch: %s", cfg.EncryptionKey)
	}
	if cfg.TraceRatio != 0.25 {
		t.Fatalf("env TraceRatio mismatch: %f", cfg.TraceRatio)
	}
}
