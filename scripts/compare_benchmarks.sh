#!/bin/bash

# Script to compare "before" and "after" numbers for Phase 1 optimizations

# Set up the environment
export GOMAXPROCS=4

# Check if both files exist
echo "Checking if both baseline files exist..."
if [ ! -f "docs/perf/BASELINE_PHASE1_BEFORE.txt" ]; then
	echo "Error: docs/perf/BASELINE_PHASE1_BEFORE.txt not found"
	exit 1
fi

if [ ! -f "docs/perf/BASELINE_PHASE1_AFTER.txt" ]; then
	echo "Error: docs/perf/BASELINE_PHASE1_AFTER.txt not found"
	exit 1
fi

# Compare the files
echo "\nComparing "before" and "after" numbers for Phase 1 optimizations..."
diff -u docs/perf/BASELINE_PHASE1_BEFORE.txt docs/perf/BASELINE_PHASE1_AFTER.txt

echo "\nComparison complete."