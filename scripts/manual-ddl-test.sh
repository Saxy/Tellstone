#!/usr/bin/env bash
#
# Manual Phase 9 DDL test: two tables, written and read through SQL, on a real
# 3-node --cluster-mode Tellstone cluster (Raft per region + PD/TSO).
#
# Scenario:
#   1. Start 3 nodes, wait for Raft quorum.
#   2. CREATE TABLE meta and CREATE TABLE address (the catalog is a Raft
#      ReplicatedLog, so schema exists on every node).
#   3. INSERT the same "user" into both tables on node 1.
#   4. Read them back from node 1, node 2, and node 3 — a row that is only
#      visible on the writer is not replicated, so reading from a non-leader
#      is the part that actually proves Raft is doing its job.
#   5. Refuse a duplicate CREATE, and refuse a DROP of a table that still has
#      rows; then empty the table and DROP succeeds.
#
# Usage:
#   scripts/manual-ddl-test.sh
#
# Requires: psql (postgresql-client) on PATH. Builds ./cmd/tellstone unless
# TELLSTONE_BIN points at an existing binary. Exits non-zero on any failed check.

set -u

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || dirname "$script_dir")

bin="${TELLSTONE_BIN:-}"
work=$(mktemp -d /tmp/tellstone-ddl-test.XXXXXX)
nodes=()
base_pg=47700   # PostgreSQL wire ports, one per node
base_bin=19600  # data ports (PD client/peer derive +10000/+20000)
pass=0
fail=0

log() { printf '\n\033[1m-- %s\033[0m\n' "$*"; }
ok()  { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }

# check <label> <expected-substring> <sql>
check() {
        local label="$1" expect="$2" sql="$3"
        local out shown
        out=$(psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t -c "$sql" 2>&1)
        shown=$(printf '%s' "$out" | head -c 160 | tr '\n' ' ')
        if [ -z "$expect" ]; then
                [ -z "$out" ] && ok "$label (no output)" || bad "$label: expected no output, got: $shown"
        elif printf '%s' "$out" | grep -qF "$expect"; then
                ok "$label (got: $shown)"
        else
                bad "$label: wanted '$expect', got: $shown"
        fi
}

# expect_err <label> <expected-substring> <sql>
expect_err() {
        local label="$1" expect="$2" sql="$3"
        local out shown
        out=$(psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t -c "$sql" 2>&1)
        shown=$(printf '%s' "$out" | head -c 160 | tr '\n' ' ')
        if printf '%s' "$out" | grep -qiE "^ERROR" && printf '%s' "$out" | grep -qF "$expect"; then
                ok "$label (refused: $shown)"
        else
                bad "$label: wanted error '$expect', got: $shown"
        fi
}

wait_pg() {
        local port="$1" n=0
        while ! timeout 3 psql -h 127.0.0.1 -p "$port" -U default -d test -X -t \
                -c "SELECT key FROM tellstone WHERE key = 'ddl-wait'" >/dev/null 2>&1; do
                n=$((n+1))
                [ "$n" -ge 60 ] && { bad "postgres frontend on :$port did not respond"; return 1; }
                sleep 0.5
        done
        return 0
}

cleanup() {
        if [ ${#nodes[@]} -gt 0 ]; then
                for p in "${nodes[@]}"; do kill "$p" 2>/dev/null; done
                wait 2>/dev/null
        fi
        [ "${KEEP_LOGS:-0}" = "1" ] || rm -rf "$work"
        printf '\n%d pass, %d fail\n' "$pass" "$fail"
        [ "$fail" -gt 0 ] && exit 1
}

command -v psql >/dev/null 2>&1 || {
        echo "manual-ddl-test: psql is required (postgresql-client)" >&2
        exit 1
}

[ -n "$bin" ] && [ -x "$bin" ] || {
        bin="$work/tellstone"
        log "building ./cmd/tellstone"
        (cd "$repo" && go build -o "$bin" ./cmd/tellstone) || { echo "build failed" >&2; exit 1; }
}

trap cleanup EXIT

# ------------------------------------------------------------- 3-node cluster
log "starting 3-node cluster (Raft per region + PD/TSO)"
peers=() pd=()
for i in 1 2 3; do
        peers+=("$i@127.0.0.1:$((base_bin + 9 + i * 10))")
        pd+=("$i@127.0.0.1:$((base_bin + i * 10))")
done
peer_str=$(IFS=,; echo "${peers[*]}")
pd_str=$(IFS=,; echo "${pd[*]}")
for i in 1 2 3; do
        mkdir -p "$work/pd-$i"
        "$bin" --cluster-mode --node-role hybrid --node-id "$i" \
                --peer-addr "127.0.0.1:$((base_bin + 9 + i * 10))" --peers "$peer_str" \
                --pd-members "$pd_str" --pd-dir "$work/pd-$i" \
                --addr "127.0.0.1:$((base_bin + i * 10))" \
                --pg-addr "127.0.0.1:$((base_pg + i))" --shards 4 \
                >"$work/cluster-$i.log" 2>&1 &
        nodes+=($!)
done

for i in 1 2 3; do
        wait_pg "$((base_pg + i))" || {
                bad "node-$i never became ready; logs kept in $work"
                exit 1
        }
done
ok "all 3 nodes accepting SQL"

# ------------------------------------------------------------------- DDL
port=$((base_pg + 1))
log "CREATE TABLE on node-1 (pg:$port)"
check "create meta"    "CREATE TABLE" "CREATE TABLE meta (id int PRIMARY KEY, username text, email text)"
check "create address" "CREATE TABLE" "CREATE TABLE address (id int PRIMARY KEY, user_id int, street text, city text)"

log "schema is replicated: re-create from node-2 must be refused as duplicate"
port=$((base_pg + 2))
expect_err "node-2 sees meta already exists" "already exists" \
        "CREATE TABLE meta (id int PRIMARY KEY, username text, email text)"

log "INSERT the same user into both tables (on node-1)"
port=$((base_pg + 1))
check "insert into meta"    "INSERT 0 1" "INSERT INTO meta (id, username, email) VALUES (1, 'ada', 'ada@tellstone.dev')"
check "insert into address" "INSERT 0 1" "INSERT INTO address (id, user_id, street, city) VALUES (1, 1, '12 Analytical Way', 'London')"

log "read back from EVERY node (only the writer seeing it would mean no replication)"
for i in 1 2 3; do
        port=$((base_pg + i))
        check "node-$i meta"    "ada|ada@tellstone.dev"          "SELECT username, email FROM meta WHERE id = 1"
        check "node-$i address" "12 Analytical Way|London"      "SELECT street, city FROM address WHERE id = 1"
done

log "row id is the key: a second user in the same table"
check "insert second user" "INSERT 0 1" "INSERT INTO meta (id, username, email) VALUES (2, 'grace', 'grace@tellstone.dev')"
check "read back user 2"  "grace|grace@tellstone.dev"      "SELECT username, email FROM meta WHERE id = 2"
check "user 1 unchanged"  "ada|ada@tellstone.dev"          "SELECT * FROM meta WHERE id = 1"

log "DROP TABLE guard: a table that still has rows is refused"
expect_err "drop non-empty meta" "table is not empty" "DROP TABLE meta"

log "empty the table, then DROP succeeds"
check "delete user 1"    "DELETE 1" "DELETE FROM meta WHERE id = 1"
check "delete user 2"    "DELETE 1" "DELETE FROM meta WHERE id = 2"
check "drop now allowed" "DROP TABLE" "DROP TABLE meta"

log "the dropped table is gone from every node"
for i in 1 2 3; do
        port=$((base_pg + i))
        expect_err "node-$i meta is gone" "does not exist" "SELECT username FROM meta WHERE id = 1"
done
port=$((base_pg + 2))
check "address untouched by meta's drop" "12 Analytical Way" "SELECT street FROM address WHERE id = 1"

if [ "$fail" -gt 0 ]; then
        log "FAILURES — node logs kept in $work"
        KEEP_LOGS=1
fi
exit 0