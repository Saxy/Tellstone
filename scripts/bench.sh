#!/usr/bin/env bash
#
# bench.sh — isolated local latency benchmark for Tellstone.
#
# Why this script exists:
#   A naive local benchmark runs the load generator on the same cores as the
#   server's gnet event loops (one loop per CPU). The two processes then fight
#   the OS scheduler, which dominates the tail latency — co-located runs measured
#   ~8-20ms p99.9 that was pure CPU contention, not the server. Pinning the
#   server and the client to DISJOINT core sets removes that artifact and reveals
#   the server's real numbers (~1-2ms p99.9, ~500k RPS on a 32-core box).
#
# In production the client is on a different machine, so this isolation matches
# reality. For an even cleaner measurement, run the load generator on a separate
# host entirely.
#
# Usage:
#   scripts/bench.sh [-- <extra args forwarded to cmd/benchmark>]
# Examples:
#   scripts/bench.sh
#   scripts/bench.sh -- -n 3000000 -c 32 -read-ratio 0.95
#
# Requires: taskset (util-linux). Falls back to a co-located run with a warning
# if taskset is unavailable.
set -euo pipefail

cd "$(dirname "$0")/.."

ADDR="127.0.0.1:9988"
BENCH_ARGS=( -n 1000000 -c 32 )
# Anything after `--` overrides the default benchmark args.
if [[ "${1:-}" == "--" ]]; then
  shift
  BENCH_ARGS=( "$@" )
fi

echo ">> building binaries..."
go build -o /tmp/tellstone-bench ./cmd/tellstone
go build -o /tmp/tsbench ./cmd/benchmark

NCPU="$(nproc)"
HALF=$(( NCPU / 2 ))
if (( HALF < 1 )); then HALF=1; fi

SRV_PREFIX=()
BENCH_PREFIX=()
if command -v taskset >/dev/null 2>&1 && (( NCPU >= 2 )); then
  SRV_CPUS="0-$(( HALF - 1 ))"
  BENCH_CPUS="${HALF}-$(( NCPU - 1 ))"
  echo ">> isolating: server cpu ${SRV_CPUS} (GOMAXPROCS=${HALF}), client cpu ${BENCH_CPUS}"
  SRV_PREFIX=( taskset -c "${SRV_CPUS}" env "GOMAXPROCS=${HALF}" )
  BENCH_PREFIX=( taskset -c "${BENCH_CPUS}" )
else
  echo ">> WARNING: taskset unavailable or single-core host; running co-located."
  echo ">> Tail latency will include scheduler contention between client and server."
fi

echo ">> starting server..."
"${SRV_PREFIX[@]}" env TSD_LOG_LEVEL=error /tmp/tellstone-bench >/tmp/tellstone-bench.log 2>&1 &
SRV_PID=$!
trap 'kill "${SRV_PID}" 2>/dev/null || true' EXIT
sleep 1.5

echo ">> running benchmark: ${BENCH_ARGS[*]}"
"${BENCH_PREFIX[@]}" /tmp/tsbench -addr "${ADDR}" "${BENCH_ARGS[@]}"
