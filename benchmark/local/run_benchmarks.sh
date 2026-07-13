#!/bin/bash

set -e

RESULTS_DIR="/home/debian/bench_results"
TELLSTONE_BIN="/home/debian/Tellstone/bin/tellstone"
PORT=6379
HOST="127.0.0.1"
DATA_SIZE=256
PIPELINE=10
REQUESTS=500000
RATIO="1:10"

CPU_COUNTS="4 16 32"
ENGINES="redis valkey dragonfly tellstone"

mkdir -p "$RESULTS_DIR"

stop_servers() {
    pkill -f "redis-server.*$PORT" 2>/dev/null || true
    pkill -f "valkey-server.*$PORT" 2>/dev/null || true
    pkill -f "dragonfly.*$PORT" 2>/dev/null || true
    pkill -f "tellstone.*resp-addr" 2>/dev/null || true
    sleep 2
}

wait_for_port() {
    local max_wait=30
    local waited=0
    while ! nc -z "$HOST" "$PORT" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$max_wait" ]; then
            echo "TIMEOUT waiting for $1 on port $PORT"
            return 1
        fi
    done
    echo "  $1 is ready on port $PORT (waited ${waited}s)"
}

start_server() {
    local engine=$1
    local cpus=$2
    local last_cpu=$((cpus - 1))
    local cpuset="0-$last_cpu"

    stop_servers

    echo "Starting $engine with taskset -c $cpuset ($cpus CPUs)..."
    case "$engine" in
        redis)
            taskset -c "$cpuset" redis-server \
                --port "$PORT" \
                --daemonize yes \
                --loglevel warning \
                --save "" \
                --maxmemory 80gb \
                --maxmemory-policy noeviction
            wait_for_port "redis"
            ;;
        valkey)
            taskset -c "$cpuset" valkey-server \
                --port "$PORT" \
                --daemonize yes \
                --loglevel warning \
                --save "" \
                --maxmemory 80gb \
                --maxmemory-policy noeviction
            wait_for_port "valkey"
            ;;
        dragonfly)
            taskset -c "$cpuset" dragonfly \
                --port "$PORT" \
                --proactor_threads "$cpus" \
                --maxmemory 80gb &
            wait_for_port "dragonfly"
            ;;
        tellstone)
            taskset -c "$cpuset" "$TELLSTONE_BIN" \
                -enable-resp \
                -resp-addr "$HOST:$PORT" \
                -shards "$cpus" \
                -max-mem-bytes 80GiB \
                -log-level warn &
            wait_for_port "tellstone"
            ;;
    esac
}

run_benchmark() {
    local engine=$1
    local cpus=$2
    local threads=$cpus
    local clients=4
    local outfile="${RESULTS_DIR}/${engine}_${cpus}c_bench.json"

    echo "  Running memtier: $threads threads, $clients clients/thread -> $outfile"

    memtier_benchmark \
        -s "$HOST" \
        -p "$PORT" \
        -c "$clients" \
        -t "$threads" \
        -n "$REQUESTS" \
        -d "$DATA_SIZE" \
        --ratio="$RATIO" \
        --pipeline="$PIPELINE" \
        --json-out-file="$outfile" \
        2>&1 | tail -20

    echo "  Benchmark complete: $outfile"
}

echo "========================================================"
echo "Benchmark Suite: Redis vs Valkey vs Dragonfly vs Tellstone"
echo "Server Hardware: 56 CPUs, 118 GB RAM"
echo "CPU configs: $CPU_COUNTS"
echo "Results dir: $RESULTS_DIR"
echo "========================================================"

for cpus in $CPU_COUNTS; do
    echo ""
    echo "=== CPU Config: $cpus ==="
    for engine in $ENGINES; do
        outfile="${RESULTS_DIR}/${engine}_${cpus}c_bench.json"
        if [ -f "$outfile" ]; then
            echo "  [SKIP] $outfile already exists"
            continue
        fi
        echo ""
        echo "--- $engine ($cpus CPUs) ---"
        start_server "$engine" "$cpus"
        run_benchmark "$engine" "$cpus"
        stop_servers
        sleep 2
    done
done

echo ""
echo "========================================================"
echo "All benchmarks complete!"
echo "Results saved in $RESULTS_DIR"
ls -la "$RESULTS_DIR"/*.json
echo "========================================================"
