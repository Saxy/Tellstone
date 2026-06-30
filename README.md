# Tellstone

`Tellstone` is an ultra‑high‑performance, cloud‑native **in‑memory key/value store** written
entirely in **Go**. It speaks two protocols over TCP — a compact custom **binary protocol** and
a **Redis‑compatible (RESP2)** protocol — on top of a **sharded, low‑contention storage engine**
with optional TTL eviction and at‑rest encryption.

```
       +---------------------------------------------+
       |             Your K8s Cluster                |
       |                                             |
       |  [App Pod] --( binary :9988 / RESP :6379 )->|
       |                                             |
       |     +---------------------------------+     |
       |     |        TELLSTONE CORE           |     |
       |     |  (256 Sharded In‑Memory Buckets)|     |
       |     +---------------------------------+     |
       +---------------------------------------------+
```

## ⚡ Why Tellstone?

Many managed databases (PostgreSQL, MySQL, …) become bottlenecks under high‑frequency
workloads. Tellstone offers a **lean, modern, memory‑efficient buffer** that:

* **Zero‑Copy Binary Protocol** – Direct binary messages avoid text parsing / Protobuf overhead.
* **Redis‑Compatible** – An optional RESP2 listener lets you drive Tellstone with `redis-cli`,
  `redis-benchmark`, `memtier_benchmark`, and existing Redis client libraries (GET/SET/PING/DEL).
* **Sharded, Low‑Contention Engine** – 256 buckets indexed by key‑hash, each guarded by its own
  `RWMutex`, for near‑linear scaling across CPU cores.
* **Configurable TTL Eviction** – An active timing‑wheel (chronometer) evicts expired keys in
  O(1); lazy eviction on read backs it up.
* **Optional At‑Rest Encryption** – ChaCha20‑Poly1305, off by default.
* **Metrics & Tracing** – Built‑in Prometheus exporter and optional OpenTelemetry tracing.

### Core Architecture

| Layer | Package | Notes |
|---|---|---|
| Binary protocol | `internal/network` | `MsgRequest`/`MsgResponse` frames (`GET`/`SET`/`DEL`, TTL, key, value) |
| RESP2 protocol | `internal/resp` | Redis‑compatible listener reusing the same engine |
| Storage engine | `internal/storage` | 256 sharded buckets, per‑shard `RWMutex`, timing‑wheel eviction |
| Crypto | `internal/crypto` | Optional ChaCha20‑Poly1305 |
| Metrics / tracing | `internal/metrics`, `internal/trace` | Prometheus text exporter, OTLP/gRPC tracing |

---

## 🚀 Getting Started

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

### Configuration

Every option is available as a flag and an environment variable.

| Flag | Env | Default | Description |
|---|---|---|---|
| `--addr` | `TSD_ADDR` | `127.0.0.1:9988` | Binary‑protocol listen address |
| `--enable-resp` | `TSD_ENABLE_RESP` | `false` | Enable the Redis‑compatible RESP listener |
| `--resp-addr` | `TSD_RESP_ADDR` | `127.0.0.1:6379` | RESP listen address |
| `--max-msg-size` | `TSD_MAX_MSG_SIZE` | `16MiB` | Per‑message size limit |
| `--max-mem-bytes` | `TSD_MAX_MEM_BYTES` | `0` (unlimited) | Total engine memory ceiling |
| `--evict-interval` | `TSD_EVICT_INTERVAL` | `1s` | Chronometer tick interval (`0` disables active eviction) |
| `--evict-slots` | `TSD_EVICT_SLOTS` | `256` | Timing‑wheel slot count |
| `--enable-encryption` | `TSD_ENABLE_ENCRYPTION` | `false` | Enable ChaCha20‑Poly1305 at‑rest encryption |
| `--encryption-key` | `TSD_ENCRYPTION_KEY` | _(none)_ | 32‑byte key (required when encryption is on) |
| `--enable-metrics` | `TSD_ENABLE_METRICS` | `false` | Enable the Prometheus exporter |
| `--metrics-addr` | `TSD_METRICS_ADDR` | `:9100` | Prometheus exporter address (`/metrics`) |
| `--trace-ratio` | `TSD_TRACE_RATIO` | `0.0` | OpenTelemetry sample ratio (`0` disables) |

Runtime tuning (environment only): `TSD_GC_PERCENT` (default `-1`, GC off for a zero‑GC hot
path), `TSD_MEM_LIMIT_BYTES` (soft heap ceiling), `TSD_ENABLE_PROFILING` (serves `pprof` on
`127.0.0.1:6060`).

---

## 🔌 Using Tellstone

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

## 📊 Benchmarks

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

Single host (32‑core, WSL2), server and `memtier_benchmark` pinned to disjoint cores,
`--ratio=1:10`, 100 connections:

| Mode | Throughput | p50 | p99 | p99.9 |
|---|---|---|---|---|
| RESP, pipeline 1 | ~192k ops/s | 0.56ms | **1.04ms** | 1.19ms |
| RESP, pipeline 16 | ~2.2M ops/s | 0.68ms | 1.25ms | 1.61ms |

> Numbers are environment‑specific; reproduce with the tasks above. The high GET miss rate
> memtier reports under a random keyspace is a load‑shape artifact (use `bench:resp:hits` for a
> realistic hit rate), not a server behavior.

---

## 🧪 Development

```bash
task test           # go test ./...
task test:race      # go test -race ./...
task vet            # go vet ./...
task check          # vet + race tests (run before committing)
task fmt            # format
```

### Observability
* **Metrics:** `task run:resp` with `--enable-metrics` exposes Prometheus text at
  `http://<metrics-addr>/metrics` (default `:9100`).
* **Profiling:** set `TSD_ENABLE_PROFILING=1` to serve `pprof` on `127.0.0.1:6060`
  (e.g. `go tool pprof http://127.0.0.1:6060/debug/pprof/profile`).

---

## 🛠️ Milestones

**Phase 1 — Core Engine (done):** sharded in‑memory engine with TTL eviction, binary TCP
protocol, Redis‑compatible RESP listener (GET/SET/PING/DEL), at‑rest encryption, Prometheus
metrics and OpenTelemetry tracing.

**Phase 2 — Distributed (future):** event‑driven replication (e.g. NATS JetStream),
write‑through / write‑behind persistence, official client SDKs, and a broader RESP command set
(RESP3, INCR, EXPIRE, MULTI/EXEC).

## 🚀 Vision

Tellstone aims to be the go‑to **in‑cluster accelerator** for cloud‑native applications —
reducing latency and off‑loading traffic from downstream databases.

## 🤝 Contributing

Contributions are welcome — especially around networking, replication, persistence, and RESP
command coverage. Open an issue or start a discussion to share ideas.

---

*“A contest of focus. Keep yours made of steel.”* — **Tellstone**
