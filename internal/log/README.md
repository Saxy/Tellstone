# 📦 Log Package – README

**Location:** `internal/log/README.md`

**Purpose:** Provide a minimal, zero‑dependency logging abstraction used throughout Tellstone. The package defines a lightweight `Level` enumeration, a structured `Field` type for key‑value pairs, and a `Logger` interface. A no‑op implementation (`NewNoOpLogger`) is supplied for contexts where logging is optional or should be disabled without incurring allocation overhead.

---

## 🚀 Overview

The `log` package is intentionally tiny:

- **No external dependencies** – only the Go standard library.
- **Zero allocation in the hot‑path** – constructing a log entry involves only stack‑allocated structs.
- **Pluggable back‑ends** – any type implementing the `Logger` interface can be injected (e.g., a console logger, a structured JSON logger, or the provided no‑op logger).

### Core Types

| Symbol | Description |
|--------|-------------|
| `Level` | Logging severity (`Debug`, `Info`, `Warn`, `Error`, `Fatal`). |
| `FieldType` | Data type of a structured field (`String`, `Int`, `Bool`, `Float`). |
| `Field` | A single key‑value pair used to enrich log messages. |
| `Logger` | Interface used by the rest of the code base. |
| `NewNoOpLogger` | Returns a logger that silently discards all messages. |

---

## ⚡ Quick Start

```go
package main

import (
    "time"
    "github.com/Saxy/Tellstone/internal/log"
)

// A very small example logger that prints to stdout.
type simpleLogger struct{}

func (s *simpleLogger) Enabled(level log.Level) bool { return true }
func (s *simpleLogger) Log(level log.Level, msg string, fields ...log.Field) {
    // Very basic formatting – production code would use a proper logger.
    fmt.Printf("%s [%d] %s", time.Now().Format(time.RFC3339), level, msg)
    for _, f := range fields {
        fmt.Printf(" %s=", f.Key)
        switch f.Type {
        case log.TypeString:
            fmt.Printf("%s", f.StrVal)
        case log.TypeInt:
            fmt.Printf("%d", f.IntVal)
        case log.TypeBool:
            fmt.Printf("%t", f.BoolVal)
        case log.TypeFloat:
            fmt.Printf("%f", f.FloatVal)
        }
    }
    fmt.Println()
}

func main() {
    var logger log.Logger = &simpleLogger{}
    logger.Log(log.LevelInfo, "starting server", log.String("env", "prod"), log.Int("port", 8080))

    // Switch to a no‑op logger when the binary runs in quiet mode.
    logger = log.NewNoOpLogger()
    logger.Log(log.LevelInfo, "this will never be printed")
}
```

---

## 🛠️ API Summary

```go
// Levels – ordered from most verbose to most severe.
const (
    log.LevelDebug Level = iota
    log.LevelInfo
    log.LevelWarn
    log.LevelError
    log.LevelFatal
)

// Field constructors – convenient helpers for structured data.
func String(key, val string) log.Field
func Int(key string, val int) log.Field
func Int64(key string, val int64) log.Field // alias to Int with int conversion
func Bool(key string, val bool) log.Field

// Logger interface – used throughout the codebase.
type Logger interface {
    Enabled(level Level) bool
    Log(level Level, msg string, fields ...Field)
}

// No‑op implementation – useful for tests or when logging is disabled.
func NewNoOpLogger() Logger
```

---

## 📂 Package Contents

```
log.go   – Level enums, Field definition, helper constructors, Logger interface.
noop.go  – No‑op logger implementation (`NewNoOpLogger`).
README.md – This documentation.
```

---

## 🔨 Development & Testing

The package contains no runtime tests because the behaviour is trivial, but you can verify the interface contracts with a simple compile‑time check:

```bash
go test ./internal/log -run TestDummy -v
```

(An empty test file can be added if CI requires a test target.)

---

## 📌 Design Considerations

- **Zero Allocation:** All `Field` values are stored directly in the struct; no heap allocation occurs when constructing log entries.
- **Extensibility:** By depending only on the `Logger` interface, callers remain agnostic to the actual logging backend. Swap implementations without recompilation.
- **Performance‑First:** The `Enabled` method allows callers to guard expensive log‑generation logic, ensuring that even the cost of formatting strings is avoided when the level is disabled.
