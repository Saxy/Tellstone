# Persistence Package -- README

**Location:** `internal/persistence/README.md`
**Purpose:** Explain the design, usage, and performance characteristics of the write-ahead log (WAL) persistence layer used by Tellstone for crash recovery.

---

## Overview

The **persistence** package provides a per-shard, append-only write-ahead log (WAL) that enables Tellstone to recover in-memory state after an unclean shutdown. Each shard owns an independent `.db` file, so there is zero cross-shard coordination during writes -- consistent with Tellstone's shared-nothing architecture.

| Feature | Implementation Detail |
|---------|-----------------------|
| **Per-shard WAL files** | Each shard writes to its own file (`shard_000.db`, `shard_001.db`, ...), eliminating lock contention between shards. |
| **Append-only binary format** | Records are appended in a compact binary layout: 16-byte little-endian header (`keyLen [4B] + valLen [4B] + ttlNano [8B]`) followed by key and value bytes. |
| **Zero-allocation hot path** | `Write` uses a stack-allocated `[16]byte` header and makes no heap allocations. Benchmarked at **0 B/op, 0 allocs/op**. |
| **TTL-aware replay** | `LoadShard` skips records whose TTL has already expired, so only live keys are restored into the engine. |
| **Pass-through mode** | When persistence is disabled, `Write` is a no-op return and no files are opened -- zero overhead on the hot path. |

---

## Record Format

```
Offset  Size  Field       Encoding
------  ----  ----------  --------
0       4     keyLen      uint32 little-endian
4       4     valLen      uint32 little-endian
8       8     ttlNano     int64  little-endian (0 = no expiry)
16      var   key         raw bytes (keyLen)
16+var  var   value       raw bytes (valLen)
```

A TTL value of `0` in the on-disk format means "no expiry". The `LoadShard` function treats `ttlNano == 0` as a live key with no time-to-live.

---

## API Summary

```go
// NewStorage creates a new persistence Storage. If enabled is false, a pass-through
// (no-op) instance is returned. If dir is empty, the platform-specific default is used.
func NewStorage(enabled bool, logger log.Logger, dir string) *Storage

// Enabled reports whether this storage instance will actually write to disk.
func (s *Storage) Enabled() bool

// OpenShard opens (or creates) the WAL file for the given shard.
// Must be called before Write or LoadShard for that shard.
func (s *Storage) OpenShard(shardID uint32) error

// Write appends a single record to the shard's WAL file.
// Returns nil immediately when persistence is disabled.
func (s *Storage) Write(shardID uint32, key string, value []byte, ttl time.Time) error

// LoadShard replays all records from the shard's WAL file into the given engine,
// skipping expired keys.
func (s *Storage) LoadShard(shardID uint32, engine *storage.Engine) error
```

---

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    "github.com/Saxy/Tellstone/internal/persistence"
    "github.com/Saxy/Tellstone/internal/storage"
)

func main() {
    // Create a persistence store in a temporary directory.
    store := persistence.NewStorage(true, nil, "/tmp/tellstone-data")

    // Open the WAL file for shard 0.
    if err := store.OpenShard(0); err != nil {
        panic(err)
    }

    // Write records -- zero heap allocations.
    store.Write(0, "user:1", []byte("Alice"), time.Time{})
    store.Write(0, "user:2", []byte("Bob"), time.Now().Add(time.Hour))

    // Create a fresh engine and replay the WAL.
    engine := storage.NewEngine(0, 0, 0, nil, nil)
    defer engine.Close()

    if err := store.LoadShard(0, engine); err != nil {
        panic(err)
    }

    val, ok := engine.Get("user:1")
    fmt.Printf("recovered: %s (found=%v)\n", string(val), ok)
}
```

---

## Benchmarks

The benchmark suite lives in `internal/persistence/benchmark_test.go`. Below are the most recent results (AMD Ryzen 9 9950X, Go 1.26):

```
BenchmarkWriteSequential-32          1884082      676.7 ns/op       0 B/op     0 allocs/op
BenchmarkWriteWithTTL-32             1685334      691.4 ns/op       0 B/op     0 allocs/op
BenchmarkWriteParallel-32            1232047      972.5 ns/op      15 B/op     1 allocs/op
BenchmarkLoadShardSmall-32            25046      47957 ns/op    23528 B/op     413 allocs/op
BenchmarkLoadShardSequential-32         218     5468021 ns/op  3645012 B/op   40083 allocs/op
BenchmarkWriteThenLoad-32              2077      569924 ns/op   405645 B/op    4021 allocs/op
```

Key observations:
- **Write (hot path): 0 allocs/op** -- the stack-allocated header and direct file writes produce zero heap pressure.
- **Parallel write: 1 alloc/op** -- the single allocation comes from key formatting in the benchmark harness (`fmt.Sprintf`), not from `Write` itself.
- **Load: ~48us for 100 records, ~5.5ms for 10k records** -- acceptable for a startup-only path. Allocations come from per-record buffer creation (`header`, `key`, `val`) which is inherent to sequential file parsing.

---

## Architecture

### Integration with Shards

Persistence is wired into Tellstone's shard layer (`internal/shard/runner.go`):

1. **Startup:** When the server starts with persistence enabled, each shard calls `OpenShard` and `LoadShard` to replay its WAL file into the in-memory engine.
2. **Write path:** After every successful `Engine.Set()`, the shard calls `Persistence.Write()` to append the record to disk. Errors are logged but do not fail the SET (the data is already in memory).
3. **Shutdown:** On graceful shutdown, in-memory state is preserved. Uncommitted WAL entries (if any) are irrelevant because the engine is consistent at shutdown.

### Platform-Specific Default Directories

| OS | Default Directory |
|----|-------------------|
| Linux | `~/.local/share/tellstone/data` |
| macOS | `~/Library/Application Support/tellstone/data` |
| Windows | `%APPDATA%/tellstone/data` |

Override with `--persistence-dir` or `TSD_PERSISTENCE_DIR`.

---

## Development & Testing

```bash
# Run unit tests
go test -v ./internal/persistence/

# Run benchmarks
go test -bench=. -benchmem ./internal/persistence/

# Run tests with race detector
go test -race ./internal/persistence/
```

---

## Architectural Notes & Constraints

* **No compaction (yet):** The WAL is append-only. Over time, duplicate keys accumulate in the file. `LoadShard` replays all records in order, so the final in-memory state is correct (last-write-wins). A future compaction pass can reclaim space.
* **Deletes are not persisted:** `DEL` operations only remove the key from the in-memory engine. On restart, deleted keys will not reappear (they were never written to the WAL). If delete persistence is needed in the future, a tombstone record type can be added to the binary format.
* **File handles:** One open file per shard. The file is opened `O_RDWR` to support both `Write` (append) and `LoadShard` (read from start). The OS page cache handles buffering, so no userspace `bufio.Writer` is used.
* **Crash safety:** Because the WAL is append-only and each record is written atomically (header + key + value in a single `Write` call sequence), a partial write at crash time results in a truncated record that `LoadShard` will detect as a read error. Future work can add a checksum footer to make this explicit.
