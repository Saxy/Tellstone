#!/bin/bash

set -euo pipefail

# Script to run all Phase 1 benchmarks and capture "before" numbers

# Set up the environment
export GOMAXPROCS=4

# Run all benchmarks
echo "Running Phase 1 benchmarks..."

# Run persistence benchmarks
echo "\n=== Persistence Benchmarks ==="
go test -bench=BenchmarkWriteSequential -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkWriteBatch -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkReadSequential -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkReadBatch -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkScanPrefix -benchmem -count=5 ./internal/bench/

# Run router benchmarks
echo "\n=== Router Benchmarks ==="
go test -bench=BenchmarkRouterScanPrefix -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkRouterGet -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkRouterSet -benchmem -count=5 ./internal/bench/

# Run SQL benchmarks
echo "\n=== SQL Benchmarks ==="
go test -bench=BenchmarkSQLSelectPK -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkSQLInsert -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkSQLUpdate -benchmem -count=5 ./internal/bench/

go test -bench=BenchmarkSQLDelete -benchmem -count=5 ./internal/bench/

# Run comprehensive Phase 1 benchmark suite
echo "\n=== Comprehensive Phase 1 Benchmark Suite ==="
go test -bench=BenchmarkPhase1All -benchmem -count=5 ./internal/bench/

echo "\nAll benchmarks completed."

# Save the results to a file
echo "Saving results to docs/perf/BASELINE_PHASE1_BEFORE.txt..."
go test -bench=. -benchmem -count=5 ./internal/bench/ > docs/perf/BASELINE_PHASE1_BEFORE.txt

echo "Results saved to docs/perf/BASELINE_PHASE1_BEFORE.txt"