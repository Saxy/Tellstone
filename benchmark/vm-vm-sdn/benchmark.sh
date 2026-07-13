#!/bin/bash

# Configuration
DEFAULT_IP="127.0.0.1"
PORT="6379"

# Static Parameters
REQUESTS=100000
DATA_SIZE=256
PIPELINE=10

# Initialize variables
TARGET=""
THREADS=""
CLIENTS=""
TARGET_IP="$DEFAULT_IP"

# Parse arguments cleanly (handles flags in any order)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --redis|--valkey|--dragonfly|--tellstone)
      TARGET="$1"
      shift
      ;;
    --ip)
      if [[ -n "$2" && "$2" != -* ]]; then
        TARGET_IP="$2"
        shift 2
      else
        echo "Error: --ip requires an argument."
        exit 1
      fi
      ;;
    *)
      # Assign positional arguments (threads and clients)
      if [ -z "$THREADS" ]; then
        THREADS="$1"
      elif [ -z "$CLIENTS" ]; then
        CLIENTS="$1"
      else
        echo "Error: Unexpected argument: $1"
        exit 1
      fi
      shift
      ;;
  esac
done

# Map target to name
case "$TARGET" in
  --redis) NAME="redis" ;;
  --valkey) NAME="valkey" ;;
  --dragonfly) NAME="dragonfly" ;;
  --tellstone) NAME="tellstone" ;;
  *)
    echo "Usage: $0 [--redis | --valkey | --dragonfly | --tellstone] [--ip target_ip] [threads] [clients]"
    echo "Example: $0 --redis --ip 192.168.1.50 4 16"
    echo "Default IP: $DEFAULT_IP"
    exit 1
    ;;
esac

# Validate thread/client inputs
if [ -z "$THREADS" ] || [ -z "$CLIENTS" ]; then
    echo "Error: Missing threads or clients configuration."
    echo "Example: $0 $TARGET --ip $TARGET_IP 4 16"
    exit 1
fi

OUT_FILE="${NAME}_${THREADS}t_${CLIENTS}c.json"

echo "--------------------------------------------------------"
echo "Running benchmark against $NAME on $TARGET_IP:$PORT"
echo "Config: $THREADS threads, $CLIENTS clients per thread"
echo "Output: $OUT_FILE"
echo "--------------------------------------------------------"

memtier_benchmark \
  -s "$TARGET_IP" \
  -p "$PORT" \
  -c "$CLIENTS" \
  -t "$THREADS" \
  -n "$REQUESTS" \
  -d "$DATA_SIZE" \
  ---pipeline="$PIPELINE" \
  --json-out-file="$OUT_FILE"

echo "Done."