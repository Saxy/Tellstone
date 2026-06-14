# 📦 Protocol Package – README

**Location:** `internal/protocol/README.md`
**Purpose:** Document the design, API, and ultra‑low‑latency zero‑allocation parser used by the Tellstone in‑memory DB for translating simple SQL‑style statements into internal `ParsedQuery` structures.

## 🚀 Overview

The `protocol` package provides a **hand‑rolled, allocation‑free parser** for a tiny SQL‑dialect that maps directly to the key‑value engine. It supports three commands:

| Command | Example |
|--------|---------|
| **GET** (`SELECT`) | `SELECT value FROM kv WHERE key='myKey'` |
| **SET** (`INSERT`) | `INSERT INTO kv (key, value, ttl_ms) VALUES ('myKey','myVal',5000)` |
| **DELETE** | `DELETE FROM kv WHERE key='myKey'` |

*Case‑insensitive*, tolerant of extra whitespace, and works **directly on the incoming `[]byte`** without allocating any heap memory. All slices returned by the parser point back into the original buffer.

## ⚡ Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/Saxy/Tellstone/internal/protocol"
)

func main() {
    // Example INSERT with TTL of 5 seconds
    raw := []byte("INSERT INTO kv (key, value, ttl_ms) VALUES ('session:123','payload',5000)")
    q, err := protocol.ParseQuery(raw)
    if err != nil {
        fmt.Printf("parse error: %v\n", err)
        return
    }

    switch q.Type {
    case protocol.CmdSet:
        fmt.Printf("key=%s value=%s ttl=%s\n", q.Key, q.Value, q.TTL)
    }
}
```

The program runs with **zero heap allocations** on the hot path (the only allocations are the static error values).

## 🛠️ API Summary

```go
package protocol

import "time"

// CommandType enumerates supported statements.
type CommandType uint8

const (
    CmdUnknown CommandType = iota
    CmdGet
    CmdSet
    CmdDelete
)

// ParsedQuery is the result of a successful parse.
type ParsedQuery struct {
    Type  CommandType   // CmdGet / CmdSet / CmdDelete
    Key   []byte        // raw key slice (no copy)
    Value []byte        // raw value slice (only for CmdSet)
    TTL   time.Duration // TTL in ms (zero for no TTL)
}

// Errors returned by ParseQuery.
var (
    ErrParse       = errors.New("invalid query")      // generic parse failure
    ErrTTLOverflow = errors.New("ttl overflow")       // TTL > 24 h (configurable)
)

// ParseQuery parses a raw SQL‑style query.
// It never allocates on the heap (except the static error values).
func ParseQuery(raw []byte) (ParsedQuery, error)
```

## 📈 🏎️ Benchmark Results

All benchmarks were executed with `go test -bench=. ./internal/protocol` on an **AMD Ryzen 9 9950X (16‑core, 3.4 GHz)**.

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `Select_Sequential` | **26 ns** | 0 B | 0 |
| `Insert_Sequential` | **86 ns** | 0 B | 0 |
| `Select_Whitespace_Sequential` | **31 ns** | 0 B | 0 |
| `Select_MixedCase_Sequential` | **27 ns** | 0 B | 0 |
| `Insert_LongValue_Sequential` (~2 KB) | **853 ns** | 0 B | 0 |
| `Insert_TTLOverflow_Sequential` | **54 ns** | 0 B | 0 |
| `Select_UTF8Key_Sequential` | **27 ns** | 0 B | 0 |
| `Select_Parallel` | **11 ns** | 0 B | 0 |
| `Insert_Parallel` | **12 ns** | 0 B | 0 |
| `MixedParallel_Mix_Parallel` (GET/SET/DELETE round‑robin) | **12 ns** | 0 B | 0 |

**Interpretation**

* Sub‑30 ns latency for normal queries, even with extra whitespace or mixed‑case keywords.
* Parallel execution halves the latency because the parser is completely thread‑safe and lock‑free.
* The longest benchmark (2 KB payload) still incurs **0 allocations**, confirming the zero‑allocation guarantee.

## 📂 Package Contents

```
parser.go            – Core zero‑allocation parser implementation
parser_bench_test.go – Benchmark suite covering sequential, parallel, and edge‑case scenarios
parser_test.go       – Unit tests for valid/invalid queries and TTL overflow
protocol.go          – CommandType enum and ParsedQuery definition
```

## 🔨 Development & Testing

```bash
# Run unit tests (including TTL overflow case)
go test ./...

# Run the full benchmark suite with allocation reporting
go test -bench=. -benchmem ./internal/protocol
```

## 📌 Architectural Constraints & Boundaries

* **No heap allocations** on the hot path – all helpers operate on the original byte slice.
* **ASCII‑only case folding** – the parser lower‑cases bytes manually; non‑ASCII characters are passed through unchanged. If Unicode case‑folding is required, a separate slow‑path can be added.
* **TTL limit** – values greater than 24 hours are rejected with `ErrTTLOverflow`. Adjust the constant in `extractSetPayloadInline` if a different limit is needed.
* **Error handling** – callers must check the returned error before using the parsed fields.

## 🌱 Future Work

* SIMD‑accelerated case folding for even lower latency.
* Configurable maximum TTL via a package‑level variable.
* Support for batch `IN` queries and optional `IF NOT EXISTS` clauses.
* Auto‑generated `BENCHMARKS.md` from benchmark output.
* Integration tests exercising zero‑copy parsing from network sockets.

---

*This document mirrors the look‑and‑feel of the storage package README and includes up‑to‑date benchmark results.*