# Phase 3 Baseline: Network and Protocol

Taken on the code state after Phase 2 (lock-free engine reads, actor opt-in, LockFreeMap resize). No Phase 3 change is in this measurement.

Machine: AMD Ryzen 9950X, 16 cores / 32 threads, Linux. The server ran with `--shards 4`, no persistence, plaintext. Load generator: `cmd/benchmark` (synchronous request/response per connection, 95% GET / 5% SET, Zipf keys) on the same machine, so its CPU use competes with the server.

## 1. Load generator, binary protocol, end to end

| Connections | Requests | Throughput | p50 | p95 | p99 | p99.9 | Failed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 100 000 | 12 118 RPS | 78 µs | — | — | 270 µs | 0 |
| 16 | 200 000 | 60 095 RPS | 130 µs | 510 µs | 4.3 ms | 9.5 ms | 0 |
| 200 | 1 000 000 | 528 574 RPS | 110 µs | 1.3 ms | 2.9 ms | 7.3 ms | 0 |
| 1 000 | 1 000 000 | 1 118 272 RPS | 400 µs | 2.1 ms | 3.6 ms | 8.1 ms | 0 |
| 10 000 | 1 000 000 | 626 655 RPS | 11 ms | 14 ms | 74 ms | 98 ms | 0 |

Phase 3's exit criterion is "p50/p99 improved under 10k concurrent connections". The 10k row is the reference for that.

## 2. Server-side allocations under load (200 connections)

Measured from the runtime's `Mallocs` and `TotalAlloc` counters around a 3 000 000-request run.

| Metric | Per request |
|---|---:|
| Heap objects allocated | **2.47** |
| Heap bytes allocated | **727 B** |

Raw counters: `docs/perf/phase3_baseline_raw/server_heap_{pre,post}_c200.txt`.

Go-level confirmation (`BenchmarkGnetPlaintext`, round-trip ping):

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| GnetPlaintext | 70 000 | 672 | 2 |
| GnetPlaintextParallel | 6 600 – 6 900 | 672 | 2 |
| GnetTLS | 81 000 | 696 | 3 |
| GnetTLSParallel | 7 550 | 709–712 | 3 |
| GnetServerHandlerParallel | 6 600 | 672 | 2 |
| ReadMessageZeroAlloc | 2.5 | 0 | 0 |

Source of the response allocation: `internal/network/protocol.go` `Write` declares its frame buffer on the stack, then passes it through the `io.Writer` interface. The escape analysis moves it to the heap, so every response allocates.

## 3. Where the server spends its time (CPU profile, 200 connections, 5 s)

| Share | Function |
|---:|---|
| 62% | syscalls (`linux.Syscall6`) |
| 42% | `unix.write`, one write syscall per response, via `gnet.conn.write` |
| 12% | `EpollWait` |
| 9–11% | `Shard.Execute` / `Router.Dispatch` / `command.Execute` (the engine is about 3%) |
| 7% | `binary.PutUint32` inside `network.Write`, mostly first-touch of the freshly allocated frame |
| 4% | `mallocgc` |

Full top-60 list: `docs/perf/phase3_baseline_raw/cpu_c200_top.txt`.

The engine is not the bottleneck. The syscall cost per response is.

## 4. Protocol parsing (already allocation-free)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| ParseSQL_Select_Sequential | 26 | 0 | 0 |
| ParseSQL_Insert_Sequential | 74 | 0 | 0 |

## 5. Postgres wire path

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| PGWireSimpleSelectPK (new) | 25 000 | 5 048 | 112 | socket round-trip, client and server in one process, in-memory `testStore` |
| PGSimpleQueryPK, 1 shard | 901 | 1 232 | 20 | executor only, no socket |
| PGSimpleQueryPK, 4 shards | 2 268 | 3 500 | 25 | executor only |
| PGSimpleQueryPK, 16 shards | 7 058 | 12 256 | 38 | executor only; the row read scatters over all shards |

The wire benchmark counts client allocations too, so it is an upper bound for the server. The executor-only numbers show that most of the 25 µs is the socket and protocol round-trip, not query execution.

## 6. Known gaps in the current network layer

- The README says responses leave through `net.Buffers` and `writev`. The code calls `w.Write` once per response.
- Each response is its own write syscall, so pipelined clients get no batching.
- No io_uring backend exists, and `go.mod` contains no io_uring library.
- Frame buffers are allocated per response (section 2).

## Reproduce

```sh
go build -o /tmp/tellstone ./cmd/tellstone
go build -o /tmp/loadgen ./cmd/benchmark
/tmp/tellstone --addr 127.0.0.1:19990 --shards 4 --max-mem-bytes 4GiB &
/tmp/loadgen -addr 127.0.0.1:19990 -c 200 -n 1000000

# server allocations: start with TSD_ENABLE_PROFILING=1 (pprof on 127.0.0.1:6060)
curl -s 'http://127.0.0.1:6060/debug/pprof/heap?debug=1' | grep -E '^# (Mallocs|TotalAlloc)'

go test -run='^$' -bench='Gnet|ReadMessage' -benchmem ./internal/network/
go test -run='^$' -bench='ParseSQL_(Select|Insert)_Sequential' -benchmem ./internal/protocol/ ./internal/sql/
go test -run='^$' -bench='PGSimpleQueryPK|PGWireSimpleSelectPK' -benchmem ./internal/sql/
```
