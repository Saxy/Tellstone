#!/bin/bash

set -euo pipefail

# Script to capture "before" numbers for Phase 1 optimizations

# Set up the environment
export GOMAXPROCS=4
mkdir -p docs/perf

# Run comprehensive Phase 1 benchmark suite
echo "Running comprehensive Phase 1 benchmark suite..."
go test -bench=BenchmarkPhase1All -benchmem -count=5 ./internal/bench/ > docs/perf/BASELINE_PHASE1_BEFORE.txt

echo "\nResults saved to docs/perf/BASELINE_PHASE1_BEFORE.txt"