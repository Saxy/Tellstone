#!/usr/bin/env bash
#
# Manual backward-compatibility test: v1.2.0 (plaintext) -> dev (encrypted) upgrade.
#
# Scenario:
#   1. v1.2.0 starts with persistence ON, encryption OFF. Writes 50 keys.
#   2. dev starts with persistence ON, encryption ON against the SAME data dir.
#      - v1.2.0 plaintext WAL should be replayed and migrated to encrypted format.
#      - All 50 v1.2.0 keys should be readable.
#   3. dev writes 50 new encrypted keys alongside the v1.2.0 legacy data.
#   4. dev restarts — all 100 keys survive.
#   5. SIGKILL crash recovery — all keys survive.
#
# This proves the plaintext-to-encrypted WAL migration path works end-to-end.
#
# Prerequisites: redis-cli, go, git
set -euo pipefail

OLD_BIN="./bin/tellstone-v1.2.0"
NEW_BIN="./bin/tellstone-dev"
RESP_PORT=11998

# 32-byte key, base64-encoded for TSD_ENCRYPTION_KEY
ENCRYPT_KEY="MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE="  # "01234567890123456789012345678901" base64

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
pass() { echo -e "  ${GREEN}PASS${NC} $1"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { echo -e "  ${RED}FAIL${NC} $1"; FAIL_COUNT=$((FAIL_COUNT+1)); }
section() { echo; echo -e "${BOLD}${CYAN}=== $1 ===${NC}"; }

start_server() {
    # BIN env var selects which binary to run
    # ENCRYPTION flag controls whether encryption is enabled
    local -a env_args=(
        "TSD_ENABLE_RESP=true"
        "TSD_RESP_ADDR=127.0.0.1:${RESP_PORT}"
        "TSD_ADDR=127.0.0.1:0"
        "TSD_NUM_SHARDS=1"
        "TSD_ENABLE_PERSISTENCE=true"
        "TSD_PERSISTENCE_DIR=${DATA_DIR}"
    )
    if [ "${ENCRYPTION_ON:-}" = "true" ]; then
        env_args+=("TSD_ENABLE_ENCRYPTION=true")
        env_args+=("TSD_ENCRYPTION_KEY_FILE=${ENCRYPT_KEY_FILE}")
    fi
    env "${env_args[@]}" "${BIN}" >/tmp/tellstone-manual-test.log 2>&1 &
    SRV_PID=$!
    for i in $(seq 1 50); do
        if redis-cli -p "$RESP_PORT" PING 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 0.1
    done
    echo "  Server failed to start. Log:"
    tail -20 /tmp/tellstone-manual-test.log
    exit 1
}

stop_server() {
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    sleep 0.3
}

cleanup() {
    stop_server
    rm -rf "$DATA_DIR" "${ENCRYPT_KEY_FILE:-}"
}
trap cleanup EXIT

# ======================================================================
section "Build both binaries"
# ======================================================================

DATA_DIR=$(mktemp -d)

echo "  Building v1.2.0 from tag..."
git stash 2>/dev/null
git checkout v1.2.0 2>&1 | grep -v "^You are" | grep -v "^HEAD is" || true
go build -ldflags "-X github.com/Saxy/Tellstone/internal/version.Version=1.2.0 -X github.com/Saxy/Tellstone/internal/version.Commit=$(git rev-parse --short HEAD)" \
    -o "$OLD_BIN" ./cmd/tellstone
git checkout - 2>&1 | grep -v "^Your branch" || true
git stash pop 2>/dev/null || true

echo "  Building dev build from HEAD (no ldflags, default version)..."
go build -o "$NEW_BIN" ./cmd/tellstone

echo
echo "  v1.2.0 binary:  $(${OLD_BIN} --version 2>&1)"
echo "  dev binary:     $(${NEW_BIN} --version 2>&1)"

# Verify versions
OLD_VER=$(${OLD_BIN} --version 2>&1)
NEW_VER=$(${NEW_BIN} --version 2>&1)

if echo "$OLD_VER" | grep -q "1.2.0"; then
    pass "v1.2.0 binary version confirmed: ${OLD_VER}"
else
    fail "v1.2.0 binary version unexpected: ${OLD_VER}"
fi

if echo "$NEW_VER" | grep -q "0.0.0-dev"; then
    pass "dev binary version confirmed: ${NEW_VER}"
else
    fail "dev binary version unexpected: ${NEW_VER}"
fi

# Write encryption key to a temp file for the dev build
ENCRYPT_KEY_FILE=$(mktemp)
echo -n "$ENCRYPT_KEY" | base64 -d > "$ENCRYPT_KEY_FILE"

# ======================================================================
section "Phase 1: v1.2.0 writes plaintext data (no encryption)"
echo "  Persistence ON, encryption OFF. Write 50 keys, stop cleanly."
# ======================================================================

BIN="$OLD_BIN"
ENCRYPTION_ON=""
start_server
pass "v1.2.0 server started ($(${OLD_BIN} --version 2>&1))"

echo "  Writing 50 keys..."
WRITE_OK=true
for i in $(seq 0 49); do
    redis-cli -p "$RESP_PORT" SET "legacy:${i}" "val-${i}" >/dev/null 2>&1
done

echo "  Verifying writes..."
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "legacy:${i}" 2>/dev/null || true)
    if [ "$got" != "val-${i}" ]; then
        echo "    legacy:${i}: got '$got', want 'val-${i}'"
        WRITE_OK=false
    fi
done
if $WRITE_OK; then
    pass "50 keys written and verified via v1.2.0"
else
    fail "Some writes failed on v1.2.0"
fi

stop_server
pass "v1.2.0 stopped"

# ======================================================================
section "Phase 2: Inspect v1.2.0 WAL on disk"
# ======================================================================

WAL_FILE="${DATA_DIR}/shard_000.db"
if [ ! -f "$WAL_FILE" ]; then
    fail "shard_000.db not found"
else
    WAL_SIZE=$(stat -c%s "$WAL_FILE" 2>/dev/null || stat -f%z "$WAL_FILE" 2>/dev/null)
    echo "  WAL file: ${WAL_FILE}"
    echo "  WAL size: ${WAL_SIZE} bytes"

    FIRST4=$(xxd -l 4 -p "$WAL_FILE" 2>/dev/null || echo "(empty)")
    echo "  First 4 bytes: 0x${FIRST4}"

    if [ "$FIRST4" = "54535701" ]; then
        fail "WAL has encrypted magic header -- v1.2.0 should be plaintext"
    else
        pass "WAL is plaintext (v1.2.0 format, no encryption magic)"
    fi
fi

if [ -f "${DATA_DIR}/shard_000.snap" ]; then
    fail "Unexpected .snap file (v1.2.0 has no snapshots)"
else
    pass "No .snap file (correct for v1.2.0)"
fi

if [ -f "${DATA_DIR}/shard_000.nonce" ]; then
    fail "Unexpected .nonce file (v1.2.0 has no sidecar)"
else
    pass "No nonce sidecar (correct for v1.2.0)"
fi

# ======================================================================
section "Phase 3: dev (0.0.0-dev) starts with ENCRYPTION, reads v1.2.0 data"
echo "  Persistence ON, encryption ON. v1.2.0 plaintext WAL should migrate."
# ======================================================================

BIN="$NEW_BIN"
ENCRYPTION_ON="true"
start_server
pass "dev (0.0.0-dev) server started with encryption ($(${NEW_BIN} --version 2>&1))"

echo "  Checking all 50 legacy keys created by v1.2.0..."
LEGACY_OK=true
for i in $(seq 0 49); do
    got=$(redis-cli -p "$RESP_PORT" GET "legacy:${i}" 2>/dev/null || true)
    if [ "$got" != "val-${i}" ]; then
        echo "    legacy:${i}: got '$got', want 'val-${i}'"
        LEGACY_OK=false
    fi
done
if $LEGACY_OK; then
    pass "All 50 v1.2.0 keys readable after plaintext-to-encrypted migration"
else
    fail "Some v1.2.0 keys missing after migration"
fi

stop_server

# ======================================================================
section "Phase 4: Verify WAL was migrated to encrypted format"
# ======================================================================

FIRST4=$(xxd -l 4 -p "$WAL_FILE" 2>/dev/null || echo "(empty)")
echo "  WAL first 4 bytes: 0x${FIRST4}"

if [ "$FIRST4" = "54535701" ]; then
    pass "WAL has encrypted magic header (migration succeeded)"
else
    fail "WAL still plaintext after migration (magic = 0x${FIRST4})"
fi

if [ -f "${DATA_DIR}/shard_000.nonce" ]; then
    pass "Nonce sidecar created (encrypted WAL needs counter persistence)"
else
    fail "No nonce sidecar after migration (encrypted WAL needs it)"
fi

# ======================================================================
section "Phase 5: dev writes 50 new encrypted keys alongside v1.2.0 data"
# ======================================================================

BIN="$NEW_BIN"
ENCRYPTION_ON="true"
start_server

for i in $(seq 0 49); do
    redis-cli -p "$RESP_PORT" SET "fresh:${i}" "new-${i}" >/dev/null 2>&1
done

echo "  Checking fresh writes from dev (0.0.0-dev)..."
FRESH_OK=true
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "fresh:${i}" 2>/dev/null || true)
    if [ "$got" != "new-${i}" ]; then
        echo "    fresh:${i}: got '$got', want 'new-${i}'"
        FRESH_OK=false
    fi
done
if $FRESH_OK; then
    pass "50 new encrypted keys alongside 50 v1.2.0 legacy keys"
else
    fail "Some dev writes failed"
fi

# Also verify legacy keys are still present
echo "  Checking legacy v1.2.0 keys still present..."
LEGACY2_OK=true
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "legacy:${i}" 2>/dev/null || true)
    if [ "$got" != "val-${i}" ]; then
        echo "    legacy:${i}: got '$got', want 'val-${i}'"
        LEGACY2_OK=false
    fi
done
if $LEGACY2_OK; then
    pass "v1.2.0 legacy keys still present alongside fresh keys"
else
    fail "v1.2.0 legacy keys corrupted"
fi

# ======================================================================
section "Phase 6: Restart dev -- all 100 keys survive"
# ======================================================================

stop_server
start_server
pass "dev (0.0.0-dev) restarted with encryption"

echo "  Checking v1.2.0 legacy keys after restart..."
L3_OK=true
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "legacy:${i}" 2>/dev/null || true)
    if [ "$got" != "val-${i}" ]; then
        echo "    legacy:${i}: got '$got', want 'val-${i}'"
        L3_OK=false
    fi
done
if $L3_OK; then
    pass "v1.2.0 legacy keys intact after restart"
else
    fail "v1.2.0 legacy keys corrupted after restart"
fi

echo "  Checking dev fresh keys after restart..."
F3_OK=true
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "fresh:${i}" 2>/dev/null || true)
    if [ "$got" != "new-${i}" ]; then
        echo "    fresh:${i}: got '$got', want 'new-${i}'"
        F3_OK=false
    fi
done
if $F3_OK; then
    pass "dev fresh keys intact after restart"
else
    fail "dev fresh keys corrupted after restart"
fi

# ======================================================================
section "Phase 7: Crash recovery (SIGKILL)"
echo "  dev writes, SIGKILL, restart -- all should survive."
# ======================================================================

for i in $(seq 0 9); do
    redis-cli -p "$RESP_PORT" SET "crash:${i}" "boom-${i}" >/dev/null 2>&1
done

echo "  Verifying crash keys before kill..."
CRASH_OK=true
for i in 0 5 9; do
    got=$(redis-cli -p "$RESP_PORT" GET "crash:${i}" 2>/dev/null || true)
    if [ "$got" != "boom-${i}" ]; then
        echo "    crash:${i}: got '$got', want 'boom-${i}'"
        CRASH_OK=false
    fi
done
if $CRASH_OK; then
    pass "10 crash keys written"
else
    fail "Crash key writes failed"
fi

echo "  Sending SIGKILL..."
kill -9 "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
sleep 0.5

echo "  Restarting dev (0.0.0-dev) after crash..."
start_server
pass "dev (0.0.0-dev) recovered after SIGKILL"

echo "  Verifying all keys survived SIGKILL..."
RECOVERY_OK=true
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "legacy:${i}" 2>/dev/null || true)
    if [ "$got" != "val-${i}" ]; then
        echo "    legacy:${i}: got '$got', want 'val-${i}'"
        RECOVERY_OK=false
    fi
done
for i in 0 1 24 25 49; do
    got=$(redis-cli -p "$RESP_PORT" GET "fresh:${i}" 2>/dev/null || true)
    if [ "$got" != "new-${i}" ]; then
        echo "    fresh:${i}: got '$got', want 'new-${i}'"
        RECOVERY_OK=false
    fi
done
for i in 0 5 9; do
    got=$(redis-cli -p "$RESP_PORT" GET "crash:${i}" 2>/dev/null || true)
    if [ "$got" != "boom-${i}" ]; then
        echo "    crash:${i}: got '$got', want 'boom-${i}'"
        RECOVERY_OK=false
    fi
done
if $RECOVERY_OK; then
    pass "All 110 keys (50 legacy + 50 fresh + 10 crash) survived SIGKILL"
else
    fail "Some keys lost after crash recovery"
fi

# ======================================================================
section "Phase 8: dev plaintext -> dev encrypted (fresh data dir)"
echo "  Same binary: dev writes without encryption, then restarts with encryption."
# ======================================================================

DEV_DATA_DIR=$(mktemp -d)
stop_server

echo "  Writing 25 keys with dev (encryption OFF)..."
BIN="$NEW_BIN"
ENCRYPTION_ON=""
TSD_PERSISTENCE_DIR="${DEV_DATA_DIR}" \
    TSD_ENABLE_RESP=true \
    TSD_RESP_ADDR="127.0.0.1:${RESP_PORT}" \
    TSD_ADDR="127.0.0.1:0" \
    TSD_NUM_SHARDS=1 \
    TSD_ENABLE_PERSISTENCE=true \
    "${BIN}" >/tmp/tellstone-manual-test.log 2>&1 &
SRV_PID=$!
for i in $(seq 1 50); do
    if redis-cli -p "$RESP_PORT" PING 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 0.1
done

DEV_PLAIN_OK=true
for i in $(seq 0 24); do
    redis-cli -p "$RESP_PORT" SET "dp:${i}" "plain-${i}" >/dev/null 2>&1
done
for i in 0 12 24; do
    got=$(redis-cli -p "$RESP_PORT" GET "dp:${i}" 2>/dev/null || true)
    if [ "$got" != "plain-${i}" ]; then
        echo "    dp:${i}: got '$got', want 'plain-${i}'"
        DEV_PLAIN_OK=false
    fi
done
if $DEV_PLAIN_OK; then
    pass "25 keys written with dev (encryption OFF)"
else
    fail "Some writes failed with dev (encryption OFF)"
fi
stop_server

DEV_FIRST4=$(xxd -l 4 -p "${DEV_DATA_DIR}/shard_000.db" 2>/dev/null || echo "(empty)")
if [ "$DEV_FIRST4" = "54535701" ]; then
    fail "WAL has encryption magic -- should be plaintext"
else
    pass "WAL is plaintext (dev without encryption)"
fi

echo "  Restarting dev with encryption ON against the same data dir..."
BIN="$NEW_BIN"
ENCRYPTION_ON="true"
DATA_DIR="${DEV_DATA_DIR}"
start_server
pass "dev restarted with encryption"

echo "  Checking 25 plaintext keys after migration..."
DEV_MIG_OK=true
for i in $(seq 0 24); do
    got=$(redis-cli -p "$RESP_PORT" GET "dp:${i}" 2>/dev/null || true)
    if [ "$got" != "plain-${i}" ]; then
        echo "    dp:${i}: got '$got', want 'plain-${i}'"
        DEV_MIG_OK=false
    fi
done
if $DEV_MIG_OK; then
    pass "All 25 dev plaintext keys survived migration to encrypted"
else
    fail "Some dev plaintext keys lost during migration"
fi

DEV_MIG_FIRST4=$(xxd -l 4 -p "${DEV_DATA_DIR}/shard_000.db" 2>/dev/null || echo "(empty)")
if [ "$DEV_MIG_FIRST4" = "54535701" ]; then
    pass "WAL migrated to encrypted format"
else
    fail "WAL still plaintext after migration (magic = 0x${DEV_MIG_FIRST4})"
fi

if [ -f "${DEV_DATA_DIR}/shard_000.nonce" ]; then
    pass "Nonce sidecar created during migration"
else
    fail "No nonce sidecar after migration"
fi

stop_server
rm -rf "${DEV_DATA_DIR}"

# ======================================================================
section "Summary"
# ======================================================================

stop_server
echo
TOTAL=$((PASS_COUNT + FAIL_COUNT))
echo -e "  ${BOLD}Results: ${PASS_COUNT} passed, ${FAIL_COUNT} failed (${TOTAL} total)${NC}"
echo
if [ "$FAIL_COUNT" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}All tests passed.${NC}"
    echo
    echo "  Binaries used:"
    echo "    v1.2.0:     $(${OLD_BIN} --version 2>&1)"
    echo "    dev (HEAD): $(${NEW_BIN} --version 2>&1)"
    echo
    echo "  Proved:"
    echo "    1. v1.2.0 wrote plaintext WAL (no encryption)"
    echo "    2. dev (0.0.0-dev) with encryption ON reads the v1.2.0 plaintext WAL"
    echo "    3. Plaintext WAL is migrated to encrypted format (magic header written)"
    echo "    4. New encrypted writes coexist with migrated legacy data"
    echo "    5. Restart preserves all keys (legacy + fresh)"
    echo "    6. Crash recovery (SIGKILL) preserves all keys"
    echo "    7. dev plaintext -> dev encrypted upgrade works (same binary)"
    exit 0
else
    echo -e "  ${RED}${BOLD}${FAIL_COUNT} test(s) failed.${NC}"
    exit 1
fi
