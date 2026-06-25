package log

import "testing"

func TestParseLogLevel(t *testing.T) {
    cases := []struct {
        in   string
        want Level
    }{
        {"debug", LevelDebug},
        {"DEBUG", LevelDebug},
        {"info", LevelInfo},
        {"INFO", LevelInfo},
        {"warn", LevelWarn},
        {"WARN", LevelWarn},
        {"warning", LevelWarn},
        {"error", LevelError},
        {"ERROR", LevelError},
        {"fatal", LevelFatal},
        {"FATAL", LevelFatal},
        {"unknown", LevelInfo}, // fallback
    }
    for _, c := range cases {
        if got := ParseLogLevel(c.in); got != c.want {
            t.Fatalf("ParseLogLevel(%q) = %v, want %v", c.in, got, c.want)
        }
    }
}

func TestNoOpLogger(t *testing.T) {
    l := NewNoOpLogger()
    // Enabled should always be false regardless of level.
    for lvl := LevelDebug; lvl <= LevelFatal; lvl++ {
        if l.Enabled(lvl) {
            t.Fatalf("NoOpLogger.Enabled(%v) = true, want false", lvl)
        }
    }
    // Log should not panic; just call it.
    defer func() {
        if r := recover(); r != nil {
            t.Fatalf("NoOpLogger.Log panicked: %v", r)
        }
    }()
    l.Log(LevelInfo, "test message")
}
