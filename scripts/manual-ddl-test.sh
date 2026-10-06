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
#   6. Phase 10: EXPLAIN the plan for a point lookup, a range and a full scan;
#      confirm EXPLAIN does not execute; confirm ANALYZE counts rows and is
#      node-local; confirm a planned-but-unexecutable plan refuses with 0A000.
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

# run_timeout <seconds> <cmd...> runs cmd and kills it after <seconds>. GNU
# coreutils `timeout` does not ship on macOS, so this is the portable form.
run_timeout() {
        local sec="$1"; shift
        local pid killer rc
        "$@" & pid=$!
        { sleep "$sec"; kill "$pid" 2>/dev/null; } & killer=$!
        wait "$pid"; rc=$?
        kill "$killer" 2>/dev/null
        return "$rc"
}

# check <label> <expected-substring> <sql>
check() {
        local label="$1" expect="$2" sql="$3"
        local out shown
        out=$(psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t -c "$sql" 2>&1)
        shown=$(printf '%s' "$out" | head -c 160 | tr '\n' ' ')
        if [ -z "$expect" ]; then
                [ -z "$out" ] && ok "$label (no output)" || bad "$label: expected no output, got: $shown"
        elif printf '%s' "$out" | grep -qF -- "$expect"; then
                ok "$label (got: $shown)"
        else
                bad "$label: wanted '$expect', got: $shown"
        fi
}

# check_not <label> <forbidden-substring> <sql>
#
# The inverse of check. Needed for the two properties Phase 10 has that Phase 9
# did not: EXPLAIN must not return the row it describes, and a node's ANALYZE
# count must not appear on a node that never ran it.
check_not() {
        local label="$1" forbid="$2" sql="$3"
        local out shown
        out=$(psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t -c "$sql" 2>&1)
        shown=$(printf '%s' "$out" | head -c 160 | tr '\n' ' ')
        if printf '%s' "$out" | grep -qF -- "$forbid"; then
                bad "$label: output should not contain '$forbid', got: $shown"
        else
                ok "$label (no '$forbid')"
        fi
}

# expect_err <label> <expected-substring> <sql>
expect_err() {
        local label="$1" expect="$2" sql="$3"
        local out shown
        out=$(psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t -c "$sql" 2>&1)
        shown=$(printf '%s' "$out" | head -c 160 | tr '\n' ' ')
        if printf '%s' "$out" | grep -qiE "^ERROR" && printf '%s' "$out" | grep -qF -- "$expect"; then
                ok "$label (refused: $shown)"
        else
                bad "$label: wanted error '$expect', got: $shown"
        fi
}

wait_pg() {
        local port="$1" n=0
        while ! run_timeout 3 psql -h 127.0.0.1 -p "$port" -U default -d test -X -t \
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

# ------------------------------------------------------- Phase 10: planner
#
# The Phase 9 checks above all exercise one access path: a primary-key equality.
# Phase 10's observable behaviour is *which* path was chosen, which is only
# visible through EXPLAIN, plus the refusal of the paths that are planned but
# cannot be executed yet. Everything below is checked on a live cluster rather
# than in-process, because the planner reads the same replicated catalog the
# DDL tests above just wrote.

log "Phase 10: create a table to plan against"
port=$((base_pg + 1))
check "create metrics" "CREATE TABLE" "CREATE TABLE metrics (id bigint PRIMARY KEY, label text, score int)"

log "seed 12 rows, including the int64 extremes and a negative id"
for i in $(seq 1 10); do
        check "insert id=$i" "INSERT 0 1" \
                "INSERT INTO metrics (id, label, score) VALUES ($i, 'label-$i', $((i * 10)))"
done
check "insert MaxInt64"  "INSERT 0 1" "INSERT INTO metrics (id, label, score) VALUES (9223372036854775807, 'top', 1)"
check "insert negative"  "INSERT 0 1" "INSERT INTO metrics (id, label, score) VALUES (-5, 'sub-zero', -1)"

log "row ids are hex: a point lookup must still return the integer, not the key bytes"
check "MaxInt64 reads back as an integer" "9223372036854775807" \
        "SELECT id FROM metrics WHERE id = 9223372036854775807"
check "negative id reads back"  "-5"       "SELECT id FROM metrics WHERE id = -5"
check "id=9 is not confused with id=10" "9|90" \
        "SELECT id, score FROM metrics WHERE id = 9"

log "EXPLAIN picks the access method; a client can see the plan, not the data"
check "equality on the key is an index scan" "Index Scan on metrics" \
        "EXPLAIN SELECT label FROM metrics WHERE id = 4"
check "bounded inequality is a range scan"   "Range Scan on metrics" \
        "EXPLAIN SELECT label FROM metrics WHERE id >= 2 AND id <= 8"
check "BETWEEN is desugared into a range"    "Range Scan on metrics" \
        "EXPLAIN SELECT label FROM metrics WHERE id BETWEEN 2 AND 8"
check "non-key filter falls back to a scan"  "Seq Scan on metrics" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"

log "a TEXT primary key cannot be ranged, so it must NOT claim a range scan"
check "create notes" "CREATE TABLE" "CREATE TABLE notes (slug text PRIMARY KEY, body text)"
check "insert into notes" "INSERT 0 1" "INSERT INTO notes (slug, body) VALUES ('a', 'first note')"
check "text key point lookup works" "first note" "SELECT body FROM notes WHERE slug = 'a'"
check "text key equality is an index scan" "Index Scan on notes" \
        "EXPLAIN SELECT body FROM notes WHERE slug = 'a'"
check "text key inequality is NOT a range scan" "Seq Scan on notes" \
        "EXPLAIN SELECT body FROM notes WHERE slug > 'a'"
expect_err "drop non-empty notes" "table is not empty" "DROP TABLE notes"
check "delete the note" "DELETE 1" "DELETE FROM notes WHERE slug = 'a'"
check "drop now allowed" "DROP TABLE" "DROP TABLE notes"

log "EXPLAIN must not execute: it describes the query, it does not run it"
check_not "EXPLAIN returns no row data" "label-4" \
        "EXPLAIN SELECT label, score FROM metrics WHERE id = 4"
log "before ANALYZE the cost model admits it is guessing"
check "un-analyzed table says so" "never analyzed" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"

log "ANALYZE counts rows, not keys (12 rows x 3 columns would be 36 keys)"
check "analyze metrics" "ANALYZE 1" "ANALYZE metrics"
check "count is 12 rows, not 36 keys" "12 rows" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"
check_not "the guess caveat is gone after ANALYZE" "never analyzed" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"
check "bare ANALYZE covers every catalog table" "ANALYZE 13" "ANALYZE"

log "statistics are node-local by design (ADR-014 decision 8)"
port=$((base_pg + 1))
check "node-1 has the measurement" "12 rows" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"
port=$((base_pg + 3))
check_not "node-3 never ran ANALYZE, so it must not claim the number" "12 rows" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"
check "node-3 still reports the estimate" "never analyzed" \
        "EXPLAIN SELECT label FROM metrics WHERE label = 'label-4'"

log "a planned but unexecutable plan refuses with 0A000, naming plan and phase"
port=$((base_pg + 1))
expect_err "range scan refused" "RangeScan" \
        "SELECT label FROM metrics WHERE id >= 2 AND id <= 8"
expect_err "range refusal names the phase that will run it" "Phase 11" \
        "SELECT label FROM metrics WHERE id >= 2 AND id <= 8"
expect_err "full scan refused" "FullScan" \
        "SELECT label FROM metrics WHERE label = 'label-4'"
expect_err "unfiltered query refused" "FullScan" \
        "SELECT label FROM metrics"
expect_err "multi-row UPDATE refused" "Phase 11" \
        "UPDATE metrics SET score = 1 WHERE score > 0"
expect_err "multi-row DELETE refused" "Phase 11" \
        "DELETE FROM metrics WHERE id >= 2"
check "point UPDATE still works" "UPDATE 1" "UPDATE metrics SET score = 99 WHERE id = 4"
check "point DELETE still works" "DELETE 1" "DELETE FROM metrics WHERE id = 10"

log "statements that would require executing under EXPLAIN are refused, not faked"
expect_err "EXPLAIN ANALYZE refused" "EXPLAIN ANALYZE" \
        "EXPLAIN ANALYZE SELECT label FROM metrics WHERE id = 4"
expect_err "VACUUM refused" "VACUUM" "VACUUM"
expect_err "EXPLAIN of DDL refused" "syntax error" \
        "EXPLAIN CREATE TABLE nope (id bigint PRIMARY KEY)"
expect_err "EXPLAIN of a transaction refused" "syntax error" "EXPLAIN BEGIN"

log "Phase 10 leaves the cluster clean"
for i in 1 2 3 4 5 6 7 8 9; do
        check "delete id=$i" "DELETE 1" "DELETE FROM metrics WHERE id = $i"
done
check "delete MaxInt64" "DELETE 1" "DELETE FROM metrics WHERE id = 9223372036854775807"
check "delete negative" "DELETE 1" "DELETE FROM metrics WHERE id = -5"
check "drop metrics"   "DROP TABLE" "DROP TABLE metrics"
expect_err "metrics is gone" "does not exist" "SELECT label FROM metrics WHERE id = 1"
port=$((base_pg + 1))
check "address survived the whole phase" "12 Analytical Way" \
        "SELECT street FROM address WHERE id = 1"

if [ "$fail" -gt 0 ]; then
        log "FAILURES — node logs kept in $work"
        KEEP_LOGS=1
fi
exit 0