package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

func TestTranslateLevelToSlog(t *testing.T) {
	cases := []struct {
		lvl      log.Level
		expected slog.Level
	}{
		{log.LevelDebug, slog.LevelDebug},
		{log.LevelInfo, slog.LevelInfo},
		{log.LevelWarn, slog.LevelWarn},
		{log.LevelError, slog.LevelError},
		{log.LevelFatal, slog.LevelError}, // default fallback maps fatal to error
	}
	for _, c := range cases {
		got := translateLevelToSlog(c.lvl)
		if got != c.expected {
			t.Fatalf("translateLevelToSlog(%v) = %v, want %v", c.lvl, got, c.expected)
		}
	}
}

func TestSlogAdapterEnabled(t *testing.T) {
	l := NewSlogLogger(log.LevelInfo)
	if l.Enabled(log.LevelDebug) {
		t.Fatalf("logger should not be enabled for Debug level when set to Info")
	}
	if !l.Enabled(log.LevelInfo) {
		t.Fatalf("logger should be enabled for Info level")
	}
	if !l.Enabled(log.LevelWarn) {
		t.Fatalf("logger should be enabled for Warn level")
	}
	if !l.Enabled(log.LevelError) {
		t.Fatalf("logger should be enabled for Error level")
	}
}

func captureStdout(f func()) (string, error) {
	// Save original stdout
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = w
	f()
	w.Close()
	os.Stdout = old
	// Read output
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	r.Close()
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func TestSlogAdapterLogOutput(t *testing.T) {
	// Capture stdout while logging
	out, err := captureStdout(func() {
		logger := NewSlogLogger(log.LevelDebug)
		// Log a message with fields
		logger.Log(log.LevelInfo, "test message", log.String("key", "value"), log.Int("num", 42))
	})
	if err != nil {
		t.Fatalf("failed to capture stdout: %v", err)
	}
	if out == "" {
		t.Fatalf("expected log output, got empty string")
	}
	// Parse JSON log entry (slog JSON handler writes one line per entry)
	var entry map[string]any
	if err := json.Unmarshal([]byte(out), &entry); err != nil {
		t.Fatalf("failed to unmarshal json log: %v; raw: %s", err, out)
	}
	// Verify message and fields are present
	if msg, ok := entry["msg"].(string); !ok || msg != "test message" {
		t.Fatalf("unexpected msg field: %v", entry["msg"])
	}
	if v, ok := entry["key"].(string); !ok || v != "value" {
		t.Fatalf("expected key field to be 'value', got %v", entry["key"])
	}
	// JSON numbers are decoded as float64
	if n, ok := entry["num"].(float64); !ok || int(n) != 42 {
		t.Fatalf("expected num field to be 42, got %v", entry["num"])
	}
	// Verify level field exists (type may be string or numeric)
	if _, ok := entry["level"]; !ok {
		t.Fatalf("log entry missing level field")
	}
}


func TestSlogAdapterLogSuppressedBelowThreshold(t *testing.T) {
	out, err := captureStdout(func() {
		logger := NewSlogLogger(log.LevelInfo) // Info threshold
		logger.Log(log.LevelDebug, "debug should be suppressed")
	})
	if err != nil {
		t.Fatalf("capture error: %v", err)
	}
	if out != "" {
		t.Fatalf("expected no output for debug below threshold, got: %s", out)
	}
}
