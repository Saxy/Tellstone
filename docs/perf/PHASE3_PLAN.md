# Phase 3 Plan: Network Zero-Copy + Fast Protocol

Baseline: `docs/perf/PHASE3_BASELINE.md`. Every step below names the baseline number it targets. A step ships only if its number moves; otherwise it is reverted, following the roadmap's ground rule.

## Where the time goes (from the baseline)

| Finding | Evidence | Share / size |
|---|---|---|
| Responses are written one syscall at a time | CPU profile: `unix.write` | 42% of server time |
| Epoll wakeups | CPU profile: `EpollWait` | 12% |
| Response frames escape to the heap | `network.Write` allocates per response | 2.47 allocs and 727 B per request |
| Engine is not the bottleneck | `engine.Get` | 3% |
| Parsing is already zero-alloc | `ParseSQL_*` | 0 allocs |
| Postgres wire adds most of the cost of a PK read | wire 25 µs vs executor 0.9 µs | 25 000 vs 901 ns |

Syscalls and allocations are the targets. Engine work is not.

## Targets

| Metric | Baseline | Phase 3 target |
|---|---:|---:|
| Server allocs per binary-protocol request (c=200) | 2.47 | ≤ 0.1 |
| Server bytes per request (c=200) | 727 B | ≤ 64 B |
| `GnetPlaintext` allocs/op | 2 | 0 |
| Write syscalls per response, pipelined clients (depth 16) | 1 | ≤ 1/8 |
| p50 / p99 at 10k connections | 11 ms / 74 ms | better than baseline, measured |
| `PGWireSimpleSelectPK` allocs/op | 112 | ≤ 40 (client and server combined) |
| Build and test on both backends | gnet only | gnet and io_uring, identical results |

## Milestones (in order)

### M1. Remove the per-response allocation — DONE

See `docs/perf/PHASE3_RESULTS.md`. Plaintext is 0 allocs/op; server allocs/request 2.47 → 0.59.
- `network.Write` builds the frame in a per-connection buffer instead of a stack array passed through `io.Writer`. This stops the escape.
- Add a `Write` variant that appends into a caller-owned `[]byte`.
- Measure: `GnetPlaintext` allocs (2 → 0), server allocs/request at c=200.
- Risk: low. Correctness is covered by `internal/network` tests.

### M2. Buffer rings and pre-encoded frames — DONE

Per-connection read/write buffers landed in M1; response payloads were already static `[]byte` constants, and full frames are built into the reusable write buffer. Cross-connection `sync.Pool` deferred: connection buffers are allocated once per connection, so pooling them is a churn-only micro-opt.
- Per-connection read and write buffers come from a `sync.Pool` with a size cap (same rule as `walBufMaxCap`). Oversized buffers are dropped, not pooled.
- Pre-encode the common error and status frames once at startup.
- Measure: server bytes per request (727 B → ≤ 64 B), allocs/request.

### M3. Write batching across a read — DONE

`handleFrames` batches responses per read; `-pipeline` added to the load generator. Pipelined c=200 throughput 541k → 2.84M RPS at depth 16; write syscall share 42% → ~0%.
- In `OnTraffic`, accumulate the responses for every complete frame in the read buffer into one output buffer, then issue one `AsyncWrite`/`Write` for the connection.
- Use `net.Buffers` (writev) where the output is a list of slices, which fixes the README/code mismatch.
- Add `-pipeline <depth>` to `cmd/benchmark` so the batching path is exercised. The current load generator is synchronous, so batching cannot show up with it.
- Measure: write syscalls per response at pipeline depth 16, and throughput.

### M4. Measurement hygiene — DONE

Server on cores 0–15, loadgen on 16–31. Clean numbers recorded in `PHASE3_RESULTS.md`.
- Pin the server and the load generator to disjoint core sets (`taskset`, 0–15 and 16–31). The current runs share the cores, which distorts both sides.
- Keep the same procedure for every milestone so the numbers are comparable.

### M5. io_uring backend (plaintext only) — FOUNDATION

`internal/network/iouring` behind `//go:build linux && io_uring`: raw ring over `x/sys/unix` syscalls (the structures are hand-defined, since x/sys lacks them), with a working accept/recv/send echo loop, round-trip test and a gnet fallback stub. Full integration into `network.Server` is the next step.
- Build tag `//go:build linux && io_uring`. The gnet backend stays the default and the fallback.
- Use a raw `io_uring` implementation over `x/sys/unix` (no new dependency), with one ring per event loop, registered buffers, and multishot receive where the kernel supports it. Check the kernel version at startup and fall back to gnet if it is too old.
- TLS stays on gnet in this milestone. It is the riskiest part and needs its own design.
- Go/no-go: proceed only if M1–M3 leave write syscalls as the dominant cost. Measure with the profile from M4.
- Measure: p50/p99 at 1k and 10k connections, syscalls per request, CPU per request.

### M6. Postgres wire fast path — WIRE PART DONE

The response encoder already used direct buffer appends; the dominant wire-path cost is SQL parse/plan (`pg_query_go` 40%, TranslateAll ~56%, Catalog 15%), which is Phase 5. Per-connection frame-buffer reuse cut `PGWireSimpleSelectPK` 112 → 106 allocs/op.
- Profile `PGWireSimpleSelectPK` server side (the same heap-counter method as section 2).
- Replace `fmt`/reflection on hot message types (RowDescription, DataRow, CommandComplete, ReadyForQuery) with direct encoding into a reusable buffer.
- Batch the rows of one result into one flush, not one write per message.
- Measure: `PGWireSimpleSelectPK` allocs/op and ns/op; pgwire integration tests must still pass.

### M7. Backend parity — SCAFFOLDING

`iouring` builds and vets with and without the tag; end-to-end parity awaits the M5 integration.
- CI runs the network and sql tests with and without `-tags io_uring`, and the benchmarks on both backends. Results must match.

## Exit criteria (from the roadmap)
- Network allocs down significantly: M1, M2, M6 (targets above).
- p50/p99 improved under 10k concurrent connections: M3, M4, M5 measured against the baseline row.
- Benchmarks pass on both backends: M7.

## Measurement procedure (every milestone)
1. Build the server and the load generator from the same commit. Record the commit hash.
2. Run `cmd/benchmark` at c=1, 16, 200, 1000, 10000 with the same `-n` as the baseline, and save the output under `docs/perf/phase3/<milestone>.txt`.
3. Read the server heap counters before and after a 3 000 000-request run at c=200.
4. Take a 5 s CPU profile during the c=200 run.
5. Run the Go benchmarks in `docs/perf/PHASE3_BASELINE.md` and save the output.
6. Record the deltas against the baseline in `docs/perf/PHASE3_RESULTS.md`.

## Risks
- **io_uring availability.** Containers and older kernels may lack the required features. Mitigation: runtime detection and the gnet fallback.
- **TLS on io_uring.** Mitigation: keep TLS on gnet in Phase 3.
- **Pipelining changes latency for sync clients.** Mitigation: batching only coalesces responses that are already ready in one read, so it adds no wait.
- **Measurement noise on a shared machine.** Mitigation: M4, and repeated runs.
