# Performance Baseline (Phase 0)

Date: 2025-10-27

## Microbenchmarks
See `docs/perf/BASELINE_MICRO.txt` for full output.

Key observations from storage:
- `BenchmarkEngineGetNoAlloc` ~22.28 ns/op, 0 B/op, 0 allocs/op
- `BenchmarkEngineGetWithEncryptionNoAlloc` ~155.2 ns/op, 0 B/op, 0 allocs/op
- `BenchmarkEngineScanPrefixRow` 0 allocs
- `BenchmarkEngineSetNoTTL` ~427.6 ns/op, 32 B/op, 1 alloc/op (room to improve)

## Escape Analysis
See `docs/perf/ESCAPES.txt` (~41k lines, ~5.7k "escapes to heap"). Many escapes come from stdlib; focus on internal packages.

## Hot paths identified
- `internal/storage/engine.go` - Get/Set/Scan hot
- `internal/shard/*` - routing/execution
- `internal/network/*` - protocol/wire
- `internal/persistence/*` - WAL/group-commit

## Next steps (Phase 1)
- Eliminate allocs on Set hot path
- Pool buffers/objects
- Fix escapes in internal hot funcs
- Add targeted KV microbenches
