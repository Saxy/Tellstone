# Performance Benchmarking

This directory contains performance benchmarks and baseline results for the Tellstone database system.

## Benchmarking Process

The benchmarking process is designed to capture "before" and "after" numbers for each phase of optimization. The process is as follows:

1. **Capture "before" numbers**: Run the benchmarks before any optimizations are applied to establish a baseline.
2. **Apply optimizations**: Implement the optimizations for the current phase.
3. **Capture "after" numbers**: Run the benchmarks again to measure the impact of the optimizations.
4. **Compare results**: Analyze the "before" and "after" numbers to quantify the performance improvements.

## Running Benchmarks

To run all Phase 1 benchmarks and capture the "before" numbers, use the following command:

```bash
./scripts/capture_before_numbers.sh
```

To run all Phase 1 benchmarks and capture the "after" numbers, use the following command:

```bash
./scripts/capture_after_numbers.sh
```

To compare the "before" and "after" numbers, use the following command:

```bash
./scripts/compare_benchmarks.sh
```

## Benchmark Files

The benchmark files are located in the `internal/bench/` directory:

- `persistence_bench_test.go`: Benchmarks for the persistence layer (WAL write path)
- `router_bench_test.go`: Benchmarks for the router layer (router rewrite)
- `sql_bench_test.go`: Benchmarks for the SQL layer
- `phase1_bench_test.go`: Comprehensive benchmark suite for Phase 1 optimizations

## Baseline Results

The baseline results are stored in the following files:

- `BASELINE_PHASE1_BEFORE.txt`: "Before" numbers for Phase 1 optimizations
- `BASELINE_PHASE1_AFTER.txt`: "After" numbers for Phase 1 optimizations (to be created after optimizations are applied)

## Performance Roadmap

For more information on the performance optimization roadmap, see `docs/PERFORMANCE_ROADMAP.md`.