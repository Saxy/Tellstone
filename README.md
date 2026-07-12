# Tellstone <img src="tsd_logo.svg" width="100" height="100" alt="Tellstone logo" style="vertical-align: middle;">

[![CI](https://github.com/Saxy/Tellstone/actions/workflows/ci.yml/badge.svg)](https://github.com/Saxy/Tellstone/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.26-blue)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache--2.0-green)](LICENSE)

**Tellstone** is an ultra‑high‑performance, cloud‑native **in‑memory key/value store** written
entirely in **Go**. It speaks two protocols over TCP — a compact custom **binary protocol** and
a **Redis‑compatible (RESP2)** protocol — on top of a **shared-nothing (SN) storage engine**
with optional TTL eviction, at‑rest encryption, and write-ahead log persistence.

```
       +---------------------------------------------+
       |             Your K8s Cluster                |
       |                                             |
       |  [App Pod] --( binary :9988 / RESP :6379 )->|
       |                                             |
       |     +---------------------------------+     |
       |     |        TELLSTONE CORE           |     |
       |     |  (N Shards, each a goroutine +  |     |
       |     |   sync.RWMutex + map[string]Item)|    |
       |     |  FNV-1a hash → O(1) dispatch    |     |
       |     +---------------------------------+     |
       +---------------------------------------------+
```

## Why Tellstone?

Many managed databases (PostgreSQL, MySQL, …) become bottlenecks under high‑frequency
workloads. Tellstone offers a **lean, modern, memory‑efficient buffer** that:

* **Zero‑Copy Binary Protocol** – Direct binary messages avoid text parsing / Protobuf overhead.
* **Redis‑Compatible** – An optional RESP2 listener lets you drive Tellstone with `redis-cli`,
  `redis-benchmark`, `memtier_benchmark`, and existing Redis client libraries (GET/SET/PING/DEL).
* **Shared-Nothing Engine** – N independent shards, each containing one `map[string]Item` plus a
  `sync.RWMutex`. Keys are pinned to a shard via FNV-1a hashing so the lock is almost never
  contended. No cross-shard coordination, no channel round-trips, no per-request allocations.
* **Configurable TTL Eviction** – An active timing‑wheel (chronometer) evicts expired keys in
  O(1); lazy eviction on read backs it up.
* **Optional At‑Rest Encryption** – ChaCha20‑Poly1305, off by default.
* **Write-Ahead Log Persistence** – Per-shard append-only WAL for crash recovery. SET and DEL operations are persisted (deletes as tombstones) and replayed on restart. Zero-allocation on the hot path (`Write` = 0 allocs/op). Disabled by default.
* **Metrics & Tracing** – Built‑in Prometheus exporter and optional OpenTelemetry tracing.

### Core Architecture

| Layer | Package | Notes |
|---|---|---|
| Binary protocol | `internal/network` | `MsgRequest`/`MsgResponse` frames (`GET`/`SET`/`DEL`, TTL, key, value) |
| RESP2 protocol | `internal/resp` | Redis‑compatible listener reusing the same engine |
| Request router | `internal/router` | FNV-1a hash → O(1) shard dispatch |
| Shard runner | `internal/shard` | Shared-nothing shard: synchronous `Execute()`, per-shard `sync.RWMutex` |
| Storage engine | `internal/storage` | Single-map engine, TTL eviction via timing wheel |
| Persistence | `internal/persistence` | Per-shard append-only WAL, zero-alloc write path |
| Crypto | `internal/crypto` | Optional ChaCha20‑Poly1305 |
| Metrics / tracing | `internal/metrics`, `internal/trace` | Prometheus text exporter, OTLP/gRPC tracing |

---

## Getting Started

### Prerequisites
* **Go 1.26+**
* Optional: [`task`](https://taskfile.dev) (go‑task) for the shortcuts below
* Optional (for RESP benchmarking): `redis-cli`, `memtier_benchmark`, or `redis-tools`

### Build

```bash
task build          # → ./bin/tellstone   (or: go build -o bin/tellstone ./cmd/tellstone)
```

### Run

```bash
task run            # binary protocol on 127.0.0.1:9988
task run:resp       # binary on :9988  +  Redis-compatible RESP on :6379
```

Or run the binary directly with flags / environment variables:

```bash
./bin/tellstone --addr 127.0.0.1:9988 --enable-resp --resp-addr 127.0.0.1:6379
TSD_ADDR=127.0.0.1:9988 TSD_ENABLE_RESP=true ./bin/tellstone
```

If a previous run got killed uncleanly and left a server stuck on a port (`address already in
use`), find and stop it with:

```bash
task kill                          # checks :19988, :6379, :6060 and any bin/tellstone process
task kill PORTS="9988" NAME=myapp  # override the ports/name to search for
```

Works on Linux and macOS (`lsof`/`pgrep`/`pkill`, no OS-specific tooling).

### Configuration

Every option is available as a flag and an environment variable.

| Flag                  | Env                     | Default          | Description                                              |
|-----------------------|-------------------------|------------------|----------------------------------------------------------|
| `--addr`              | `TSD_ADDR`              | `127.0.0.1:9988` | Binary‑protocol listen address                           |
| `--enable-resp`       | `TSD_ENABLE_RESP`       | `false`          | Enable the Redis‑compatible RESP listener                |
| `--resp-addr`         | `TSD_RESP_ADDR`         | `127.0.0.1:6379` | RESP listen address                                      |
| `--shards`            | `TSD_NUM_SHARDS`        | `0` (auto = CPU) | Number of shared-nothing shards                          |
| `--max-msg-size`      | `TSD_MAX_MSG_SIZE`      | `16MiB`          | Per‑message size limit                                   |
| `--max-mem-bytes`     | `TSD_MAX_MEM_BYTES`     | `0` (unlimited)  | Total engine memory ceiling                              |
| `--evict-interval`    | `TSD_EVICT_INTERVAL`    | `1s`             | Chronometer tick interval (`0` disables active eviction) |
| `--evict-slots`       | `TSD_EVICT_SLOTS`       | `256`            | Timing‑wheel slot count                                  |
| `--enable-encryption` | `TSD_ENABLE_ENCRYPTION` | `false`          | Enable ChaCha20‑Poly1305 at‑rest encryption              |
| `--encryption-key`    | `TSD_ENCRYPTION_KEY`    | _(none)_         | 32‑byte key (required when encryption is on)             |
| `--enable-metrics`    | `TSD_ENABLE_METRICS`    | `false`          | Enable the Prometheus exporter                           |
| `--metrics-addr`      | `TSD_METRICS_ADDR`      | `:9100`          | Prometheus exporter address (`/metrics`)                 |
| `--trace-ratio`       | `TSD_TRACE_RATIO`       | `0.0`            | OpenTelemetry sample ratio (`0` disables)                |
| `--enable-persistence`| `TSD_ENABLE_PERSISTENCE`| `false`          | Enable write-ahead log persistence for crash recovery    |
| `--persistence-dir`   | `TSD_PERSISTENCE_DIR`   | _(platform)_     | Directory for WAL data files                             |
| `--shutdown-timeout`  | `TSD_SHUTDOWN_TIMEOUT`  | `10s`            | Max wait for graceful shutdown on SIGINT/SIGTERM         |

Runtime tuning (environment only): `TSD_GC_PERCENT` (default `-1`, GC off for a zero‑GC hot
path), `TSD_MEM_LIMIT_BYTES` (soft heap ceiling), `TSD_ENABLE_PROFILING` (serves `pprof` on
`127.0.0.1:6060`).

---

## Using Tellstone

### Redis‑compatible (RESP) — easiest

Start with `task run:resp`, then use any Redis client:

```bash
redis-cli -p 6379 PING            # PONG
redis-cli -p 6379 SET foo bar     # OK
redis-cli -p 6379 GET foo         # "bar"
redis-cli -p 6379 SET k v EX 60   # OK (60s TTL)
redis-cli -p 6379 DEL foo         # (integer) 1
```

Supported commands today: **`PING`, `GET`, `SET` (with `EX`/`PX`), `DEL`**. Unknown commands
return a `-ERR` reply without dropping the connection.

### Native binary protocol (Go client)

The native protocol is the fastest path. Use the bundled client in `internal/network`:

```go
import "github.com/Saxy/Tellstone/internal/network"

c, _ := network.Dial("127.0.0.1:9988", 2*time.Second)
defer c.Close()

scratch := make([]byte, 4096)              // reusable response buffer (zero-alloc)
c.Set([]byte("hello"), []byte("world"), 0, scratch)   // ttlMs=0 → no expiry
val, _ := c.Get([]byte("hello"), scratch)             // val == "world"
```

A runnable example lives in `cmd/example/client`.

---

## Benchmarks

> **Methodology matters.** A naive local benchmark runs the load generator on the same cores as
> the server's event loops, so the two contend for CPU and the latency tail balloons. All tasks
> below **pin the server and the load generator to disjoint core sets** (`taskset`) so the
> numbers reflect the server, not scheduler contention. For absolute comparisons, run the load
> generator on a separate host.

### Native binary protocol

```bash
task bench:native       # pinned: server cpu0-15, generator cpu16-31
```

### Redis‑compatible (RESP) via memtier

```bash
task bench:resp                 # latency run (pipeline=1)
task bench:resp:pipeline        # throughput ceiling (pipeline=16)
task bench:resp:hits            # preload then read-heavy (realistic ~100% hit rate)
task bench:resp:correctness     # preload then read back — proves GET returns what SET stored
```

Override workload knobs on the command line, e.g.:

```bash
task bench:resp PIPELINE=16 DURATION=30 CONNS=50 RATIO=1:4 KEYSPACE=1000000
```

You can point `memtier_benchmark`/`redis-benchmark` at `:6379` directly and run the **identical
command** against Redis, Dragonfly, Valkey (or `--protocol=memcache_text` against memcached) for
an apples‑to‑apples comparison.

### Reference results

Single host (32 cores, Linux), server pinned to cores 0-15 (GOMAXPROCS=16), memtier pinned to
cores 16-31, `--ratio=1:9`, 8 threads, 100 connections each, pipeline 8, 200k keys preloaded
(Gaussian distribution, zero miss rate):

| System | Throughput | vs Redis | p50 | p99 | p99.9 |
|--------|-----------|----------|-----|-----|-------|
| **Tellstone** | **4.70M ops/s** | **4.7x** | **1.23ms** | **3.38ms** | **4.67ms** |
| Redis 8.8 | 0.99M ops/s | 1.0x | 6.59ms | 10.56ms | 18.69ms |
| Valkey 7.2 | 1.01M ops/s | 1.0x | 6.34ms | 13.25ms | 23.30ms |

Without core pinning (all 32 cores shared, `GOMAXPROCS=32`):

| System | Throughput | vs Redis | p50 | p99 | p99.9 |
|--------|-----------|----------|-----|-----|-------|
| **Tellstone** | **2.21M ops/s** | **2.1x** | **2.82ms** | **4.67ms** | **7.14ms** |
| Redis 8.8 | 1.07M ops/s | 1.0x | 6.34ms | 10.56ms | 18.69ms |
| Valkey 7.2 | 0.99M ops/s | 1.0x | 6.66ms | 13.25ms | 23.30ms |

Tellstone delivers **2.1-4.7x higher throughput** than Redis/Valkey on the same workload
with **56-81% lower** p50 latency. The gap widens with core isolation because Tellstone's
shared-nothing design scales linearly with dedicated cores, while Redis's single-threaded
event loop cannot utilze more than one.

Native binary protocol throughput (without pipelining, read-heavy):

| Connections | Throughput | p50 |
|-------------|-----------|-----|
| 32 | 940K RPS | 99us |
| 200 | 940K RPS | 99us |
| 1000 | 1.47M RPS | 470us |
| 2000 | 1.35M RPS | 1.2ms |

> Numbers are environment-specific; reproduce with the tasks above.

---

## Development

```bash
task test           # go test ./...
task test:race      # go test -race ./...
task vet            # go vet ./...
task check          # vet + race tests (run before committing)
task fmt            # format
```

### Continuous Integration

Pull requests and pushes to `main` trigger the [CI workflow](.github/workflows/ci.yml):

- **Build** — `go build ./...`
- **Vet** — `go vet ./...`
- **Test** — `go test ./...`
- **Race tests** — `go test -race ./...`

Benchmarks are not run automatically on every push due to resource constraints.
Run them locally with `task bench:native` or `task bench:resp:precise`.

### Observability
* **Metrics:** `task run:resp` with `--enable-metrics` exposes Prometheus text at
  `http://<metrics-addr>/metrics` (default `:9100`).

### Profiling

Two independent workflows, both built on the stock Go toolchain (`pprof` / `trace`). Neither
assumes a specific core count, OS, or machine — every variable below is overridable on the CLI,
so the same commands work on a laptop, a CI runner, or a dedicated benchmarking host.

**1) Profile a package's benchmarks directly** — no server involved, good for isolating one
function (e.g. the storage engine or the RESP parser):

```bash
task profile:pkg                                          # ./internal/storage/..., all benchmarks
task profile:pkg PKG=./internal/resp/... BENCH=BenchmarkParseGet
task profile:view FILE=tmp/profile/cpu.out                # opens the CPU profile in the browser
task profile:view FILE=tmp/profile/mem.out ARGS=-alloc_space
```

**2) Profile the running server under real load**, generated from a second terminal:

```bash
task run:profiling                    # foreground server, RESP + live pprof on :6060
```

```bash
# in a second terminal, generate load, e.g.:
task bench:resp:pipeline
# or: ./bin/benchmark -addr 127.0.0.1:19988 -c 32 -n 1000000 -read-ratio 0.95 -skew 1.5
```

```bash
# while load is running, pull a profile and open it in the browser:
task profile:live                     # CPU, 30s sample (default)
task profile:live KIND=heap
task profile:live KIND=mutex
task profile:live KIND=block
task profile:live:trace               # execution trace, opened via `go tool trace`
```

`go tool pprof -http` starts a local web server and opens your default browser automatically.
On a headless/remote host, set `PORT=<port>` and open `http://<host>:<port>` yourself (e.g. via
an SSH tunnel), or browse the raw index at `http://127.0.0.1:6060/debug/pprof/` directly.

---

## Milestones

**Phase 1 — Core Engine (done):** sharded in‑memory engine with TTL eviction, binary TCP
protocol, Redis‑compatible RESP listener (GET/SET/PING/DEL), at‑rest encryption, Prometheus
metrics and OpenTelemetry tracing.

**Phase 1.5 — Persistence (done):** per-shard write-ahead log with zero-allocation hot path,
TTL-aware replay, and platform-specific default directories.

**Phase 2 — Protocol & Integration (future):** RESP3 compatibility, Memcached protocol
support, official client SDKs (Go, Python, Node.js), and write-through / write-behind
persistence to external databases (PostgreSQL, MariaDB, MSSQL, etc.) — using Tellstone as
a high-speed in-memory buffer store in front of durable backends.

## Vision

Tellstone aims to be the go‑to **in‑cluster accelerator** for cloud‑native applications —
reducing latency and off‑loading traffic from downstream databases.

## Contributing

Contributions are welcome — especially around networking, replication, persistence, and RESP
command coverage. Open an issue or start a discussion to share ideas.

---

*“A contest of focus. Keep yours made of steel.”* — **Tellstone**
