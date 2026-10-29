# Phase 3 Results

Baseline: `docs/perf/PHASE3_BASELINE.md`. Method is the same for every milestone.

## M1: Remove the per-response allocation — DONE

### Change
- `network.Write` left the server hot path. `runHandler` now builds each response
  frame into a per-connection buffer (`connState.writeBuf`) via a new
  `appendFrame` helper, so no frame buffer escapes to the heap.
- The plaintext read path now reads into a per-connection buffer
  (`connState.readBuf`) instead of gnet's `Peek`/`Discard`, which allocated on
  ring-buffer wraps.
- The decoded `Message` moved into `connState` so its address stops escaping
  through the `unsafe.Pointer` in `runHandler`.

### Go-level benchmarks (`-benchmem`, `-benchtime=2s`)

| Benchmark | Before | After |
|---|---:|---:|
| GnetPlaintext | 70 000 ns, 672 B, 2 allocs | 67 500 ns, **0 B, 0 allocs** |
| GnetPlaintextParallel | 6 600 ns, 672 B, 2 allocs | 6 300 ns, **0 B, 0 allocs** |
| GnetTLS | 81 000 ns, 696 B, 3 allocs | 82 000 ns, 24 B, 1 alloc |
| GnetTLSParallel | 7 550 ns, 710 B, 3 allocs | 7 290 ns, 36 B, 1 alloc |

### End-to-end server allocations (200 connections, 3 000 000 requests)

| Metric | Before | After |
|---|---:|---:|
| Heap objects per request | 2.47 | **0.59** |
| Heap bytes per request | 727 B | **70 B** |

The remaining ~0.6 allocs/request are in the command/audit layer and SET writes,
not the network frame path. The remaining TLS alloc is the TLS record buffer.

### Throughput (c=200, synchronous client)
596 000 RPS after vs 528 574 RPS baseline. The difference is within the noise of
a shared machine; latency p50 was ~100 µs in both.

## M3: Write batching across a read — DONE

### Change
- `handleFrames` (shared by the plaintext and TLS paths) now decodes every
  complete frame in the read buffer, executes each, and accumulates the
  responses into `connState.writeBuf`, then flushes them with one `Write`.
  A pipelined client's batch therefore costs one write syscall instead of one
  per response.
- `cmd/benchmark` gained `-pipeline N`: a worker writes N request frames in one
  `Write` and then reads N responses, exercising the batching path.
- `runHandler` split into `executeFrame` (no I/O) so execution and flush are
  separate.

### Throughput, c=200, in-memory (higher is better)

| Pipeline depth | Throughput |
|---:|---:|
| 1 (synchronous) | 541 000 RPS |
| 4 | 1 582 000 RPS |
| 8 | 2 197 000 RPS |
| 16 | 2 840 000 RPS |

### Syscall cost (CPU profile, c=200, pipeline 16)

| Metric | Baseline (sync) | Pipelined |
|---|---:|---:|
| Syscall time (`linux.Syscall6`) | 62% | **32%** |
| `unix.write` | 42% | no longer in top 20 |
| Engine (`itemTable.load`) | 3% | ~10% |

Batching cuts the syscall share in half; the engine now shows up as the next
cost. This is what M5 (io_uring) and the remaining engine work target.

## M4: Measurement hygiene (core pinning) — DONE

Server pinned to cores 0–15, load generator to cores 16–31, so the two no
longer compete for CPU. In-memory, c=200:

| Pipeline | Shared cores (before) | Pinned cores (after) |
|---:|---:|---:|
| 1 | 541 000 RPS | 954 000 RPS |
| 16 | 2 840 000 RPS | 2 660 000 RPS |

The synchronous number nearly doubles once the client stops competing. At 10 000
connections, synchronous:

| Metric | Shared (before) | Pinned (after) |
|---|---:|---:|
| Throughput | 627 000 RPS | 832 000 RPS |
| p50 | 11 ms | 10 ms |
| p99 | 74 ms | **14 ms** |
| p99.9 | 98 ms | **25 ms** |

The big p99 drop is mostly core isolation rather than a network change: the
client no longer inflates the tail. All later milestones use pinned cores.

## M5: io_uring backend — foundation only

`x/sys/unix` exposes the io_uring syscall numbers but not the ring structures,
so the SQE/CQE layouts and opcodes are defined by hand. A minimal, correct
io_uring echo server now lives in `internal/network/iouring` behind
`//go:build linux && io_uring`, with a `!linux || !io_uring` stub so the default
build (gnet) is unaffected.

- Ring setup, mmap, submission and completion validated with a raw NOP
  round-trip, then a real accept/recv/send echo loop.
- `TestEchoRoundTrip` passes: a client writes bytes and reads them back.
- `BenchmarkEcho` (1 connection, synchronous): ~78 µs/op, 0 allocs. Comparable
  to `GnetPlaintext`'s ~67 µs/op for a single connection; the io_uring win
  shows only with many concurrent operations, which needs the full integration.

Full integration into `internal/network.Server` (a transport abstraction plus
protocol handling) is the remaining work. gnet remains the production backend.

## M6: Postgres wire fast path — wire part done, parse/plan dominates

Profiling `PGWireSimpleSelectPK` shows the response encoder is already direct:
the dominant allocations are SQL parse and planning, not the wire.

| Source | Share of allocs |
|---|---:|
| `pg_query_go` Parse (cgo/protobuf, `reflect.unsafe_New`) | 40% |
| TranslateAll / translateSelect | ~56% cum |
| Catalog.Get | 15% cum |
| Wire encoding (`msgBuilder`) | ~14% |

The wire encoder reuses a per-connection buffer now (`pgConn.scratch` for
RowDescription and DataRow), which cut the benchmark from 112 to 106 allocs/op
(5048 → 4936 B/op). The big remaining cost — SQL parse and plan — is Phase 5
(plan cache, KV bypass), not Phase 3.

## M7: Backend parity — scaffolding only

The `internal/network/iouring` package builds and passes `go vet` both with and
without `-tags io_uring`. Running and benchmarking both backends end-to-end is
blocked on the M5 integration described above.
