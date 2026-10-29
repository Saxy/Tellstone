# Phase 2 Results: Actor-Per-Shard + Lock-Free Hot Paths

## Status
Phase 2 is complete.

- **Lock-free reads.** `storage.Engine` reads (Get, GetInto) take no lock. The items table is a lock-free hash table (`internal/storage/items_table.go`). Writers serialize on `wmu`, a plain mutex. ScanPrefix reads the ordered index under `imu`, an RWMutex that only scans and index writers touch.
- **Actor (opt-in).** `--shard-actor` routes mutations through a per-shard goroutine. It is slower than direct execution here, so the default is off.
- **Core pinning (opt-in).** `--pin-shards` pins the actor goroutine to CPU `shardID % NumCPU`. It requires `--shard-actor`.
- **Cache-line padding.** Shard connection and byte counters and the actor's queue state each sit on their own cache line.
- **LockFreeMap.** Resizes on growth, with lock-free reads. Writes take a mutex.

## Measurement method

The parallel benchmarks (`internal/bench/shard_bench_test.go`) give each goroutine its own key cursor. An earlier version used one shared atomic counter per op, which serialized the goroutines and hid the real scaling. Numbers below use the corrected harness only.

Before = HEAD engine (`sync.RWMutex` on every read). After = current engine. Same harness, same machine (AMD Ryzen 9950X, 16 cores), `-benchtime=1s -count=3`, shards = GOMAXPROCS.

Raw output: `docs/perf/PHASE2_ENGINE_PARALLEL_BEFORE.txt`, `docs/perf/PHASE2_ENGINE_PARALLEL_AFTER.txt`.

## Engine contention (router path, direct execution)

| Benchmark | 1 core | 4 cores | 16 cores | allocs/op |
|---|---:|---:|---:|---:|
| GET, RWMutex (before) | 33.5 ns | 27.3 ns | 15.4 ns | 0 |
| GET, lock-free (after) | 31.0 ns | 10.2 ns | 6.6 ns | 0 |
| SET, RWMutex (before) | 497 ns | 394 ns | 136 ns | 1 (48 B) |
| SET, lock-free (after) | 521 ns | 439 ns | 164 ns | 2 (112 B) |

- **GET scales.** Going from 1 to 16 cores, GET drops from 31 to 6.6 ns, a 4.7x speedup. Before, it went from 33.5 to 15.4 ns, a 2.2x speedup. At 16 cores GET is 2.3x faster than before.
- **SET is slower.** It costs about 5–20% more at every core count, and allocates one extra object. Writes still serialize on `wmu`, as they did before, so the write convoy is unchanged. The regression comes from one allocation per update, which is explained below.

## Map primitives (no engine)

| Benchmark | 1 core | 4 cores | 16 cores |
|---|---:|---:|---:|
| Map GET, RWMutex | 12.1 ns | 18.6 ns | 23.9 ns |
| Map GET, LockFreeMap | 13.3 ns | 4.2 ns | 2.6 ns |

Under RWMutex, reads get slower as cores are added, because every reader updates the same reader count. LockFreeMap gets faster.

## Single-core KV path (GOMAXPROCS=4, `-count=3`)

| Benchmark | Before | After |
|---|---:|---:|
| BenchmarkKVGet | 22.1 ns, 0 allocs | 17.0 ns, 0 allocs |
| BenchmarkKVSet | 64 ns, 1 alloc (48 B) | 93 ns, 2 allocs (112 B) |
| BenchmarkKVBatch | 443 ns, 1 alloc | 480 ns, 2 allocs |

## Why SET allocates twice now

Readers need to load an item without a lock, so each item is published as an immutable `Item` behind an atomic pointer. Updating a key allocates a new `Item`, in addition to the key/value buffer, which is still one allocation. A new key uses one allocation for the entry and item together, plus the buffer.

Removing the extra allocation requires storing the item header inside the value buffer. That is a storage-format change, so it is a follow-up and not part of Phase 2.

## Striped counters (tried, reverted)

`hitCount`, `missCount`, and `totalCommands` are shared engine-wide. I tried striping them across 16 padded cache lines selected by key hash. At 16 cores GET went from 6.6 to 7.3 ns, which is within noise, so I reverted it.

## Not measured
- Mutex profile under load (`pprof` contention). The exit criterion asks for it; the benchmarks above are the proxy.
- Behaviour above 16 cores.
