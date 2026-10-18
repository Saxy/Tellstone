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

# check_txn <label> <expected-substring> <statement>...
#
# Runs the statements over ONE psql session (a single connection), so BEGIN,
# COMMIT and ROLLBACK share transaction state, and checks the combined output
# contains <expected-substring>. Piping to psql sends each statement as its own
# simple Query on the one connection, which is what keeps the block open across
# statements.
check_txn() {
        local label="$1" expect="$2"; shift 2
        local out shown
        out=$({ for s in "$@"; do printf '%s;\n' "$s"; done; } | \
                psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t 2>&1)
        shown=$(printf '%s' "$out" | head -c 200 | tr '\n' ' ')
        if printf '%s' "$out" | grep -qF -- "$expect"; then
                ok "$label (got: $shown)"
        else
                bad "$label: wanted '$expect', got: $shown"
        fi
}

# check_snapshot <label> <port> <snapshot-sql> <concurrent-sql> <seen> <absent>
#
# Proves snapshot isolation across TWO live connections, which a single psql
# session cannot. One session is held open on a FIFO: it runs BEGIN and reads,
# fixing its snapshot. A second connection then runs <concurrent-sql> and
# commits a newer value. The held session reads again, and its combined output
# must contain <seen> (the value as of its snapshot) and must not contain
# <absent> (the value committed after it began).
check_snapshot() {
        local label="$1" sport="$2" snap_sql="$3" other_sql="$4" seen="$5" absent="$6"
        local fifo="$work/snap.fifo" splog="$work/snap.log" pid out
        rm -f "$fifo" "$splog"; mkfifo "$fifo"
        psql -h 127.0.0.1 -p "$sport" -U default -d test -X -At -t < "$fifo" > "$splog" 2>&1 &
        pid=$!
        exec 9> "$fifo"
        printf 'BEGIN;\n%s;\n' "$snap_sql" >&9
        sleep 0.4
        psql -h 127.0.0.1 -p "$sport" -U default -d test -X -At -t -c "$other_sql" >/dev/null 2>&1
        printf '%s;\nROLLBACK;\n' "$snap_sql" >&9
        sleep 0.4
        exec 9>&-
        wait "$pid" 2>/dev/null
        out=$(tr '\n' ' ' < "$splog")
        if printf '%s' "$out" | grep -qF -- "$absent"; then
                bad "$label: snapshot saw the concurrently committed '$absent' (got: $out)"
        elif printf '%s' "$out" | grep -qF -- "$seen"; then
                ok "$label (saw $seen, not $absent)"
        else
                bad "$label: expected '$seen' in the snapshot reads, got: $out"
        fi
        rm -f "$fifo"
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
port=$((base_pg + 1))

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
port=$((base_pg + 1))
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
check "range scan executes" "label-2" \
        "SELECT label FROM metrics WHERE id >= 2 AND id <= 8"
check "full scan executes for non-key filter" "label-4" \
        "SELECT label FROM metrics WHERE label = 'label-4'"
check "unfiltered query executes (multi-row)" "label-1" \
        "SELECT label FROM metrics"
check "multi-row UPDATE executes" "UPDATE 11" "UPDATE metrics SET score = 1 WHERE score > 0"
check "multi-row DELETE executes" "DELETE 10" "DELETE FROM metrics WHERE id >= 2"
check "point UPDATE still works" "UPDATE 1" "UPDATE metrics SET score = 99 WHERE id = 1"
check "point SELECT sees the updated row" "99" "SELECT score FROM metrics WHERE id = 1"
check "point DELETE still works" "DELETE 1" "DELETE FROM metrics WHERE id = 1"

log "statements that would require executing under EXPLAIN are refused, not faked"
expect_err "EXPLAIN ANALYZE refused" "EXPLAIN ANALYZE" \
        "EXPLAIN ANALYZE SELECT label FROM metrics WHERE id = 4"
expect_err "VACUUM refused" "VACUUM" "VACUUM"
expect_err "EXPLAIN of DDL refused" "syntax error" \
        "EXPLAIN CREATE TABLE nope (id bigint PRIMARY KEY)"
expect_err "EXPLAIN of a transaction refused" "syntax error" "EXPLAIN BEGIN"

log "Phase 12: a transaction block buffers its writes and COMMIT/ROLLBACK decides their fate"
port=$((base_pg + 1))
check_txn "a committed transaction persists its write" "txn-commit" \
        "BEGIN" \
        "INSERT INTO tellstone (key, value) VALUES ('txn-commit', 'yes')" \
        "COMMIT" \
        "SELECT key FROM tellstone WHERE key = 'txn-commit'"
check "the committed row is visible to a fresh session" "txn-commit" \
        "SELECT key FROM tellstone WHERE key = 'txn-commit'"
check_txn "ROLLBACK discards the buffered write" "ROLLBACK" \
        "BEGIN" \
        "INSERT INTO tellstone (key, value) VALUES ('txn-rollback', 'no')" \
        "ROLLBACK"
check_not "the rolled-back row never landed" "txn-rollback" \
        "SELECT key FROM tellstone WHERE key = 'txn-rollback'"
check_txn "a schema change inside a block is refused" "not supported inside a transaction block" \
        "BEGIN" \
        "CREATE TABLE txn_nope (id bigint PRIMARY KEY)" \
        "ROLLBACK"

log "Phase 12 MVCC: a catalog row is versioned, so a block reads its own write and ROLLBACK restores the old version"
check "create acct" "CREATE TABLE" "CREATE TABLE acct (id bigint PRIMARY KEY, balance int)"
check "insert acct" "INSERT 0 1" "INSERT INTO acct (id, balance) VALUES (1, 100)"
check_txn "a committed catalog update persists" "150" \
	"BEGIN" \
	"UPDATE acct SET balance = 150 WHERE id = 1" \
	"COMMIT" \
	"SELECT balance FROM acct WHERE id = 1"
check_txn "read-your-writes: the block sees its buffered update" "250" \
	"BEGIN" \
	"UPDATE acct SET balance = 250 WHERE id = 1" \
	"SELECT balance FROM acct WHERE id = 1" \
	"ROLLBACK"
check "the rolled-back update left the prior version" "150" \
	"SELECT balance FROM acct WHERE id = 1"
check_snapshot "a block reads its snapshot, not a later commit" "$port" \
	"SELECT balance FROM acct WHERE id = 1" \
	"UPDATE acct SET balance = 300 WHERE id = 1" \
	"150" "300"
check "a fresh session sees the committed newer value" "300" \
	"SELECT balance FROM acct WHERE id = 1"

# cross-region transaction: the two UPDATEs address keys in different Raft
# regions, so one COMMIT must land in more than one region. Keys hash by
# FNV-1a over the full key, and for row id 8000000000000001 the columns map to
# distinct shards (acct/id=1, acct/balance=0, txn2/id=2, txn2/balance=1), so the
# blocked write set spans four regions. COMMIT applies them under one timestamp;
# ROLLBACK must leave every region untouched.
check "create txn2" "CREATE TABLE" "CREATE TABLE txn2 (id bigint PRIMARY KEY, balance int)"
check "seed txn2" "INSERT 0 1" "INSERT INTO txn2 (id, balance) VALUES (1, 100)"
check_txn "cross-region transaction: both updates commit" "COMMIT" \
	"BEGIN" \
	"UPDATE acct SET balance = 75 WHERE id = 1" \
	"UPDATE txn2 SET balance = 200 WHERE id = 1" \
	"COMMIT"
check "cross-region commit landed in region A" "75" \
	"SELECT balance FROM acct WHERE id = 1"
check "cross-region commit landed in region B" "200" \
	"SELECT balance FROM txn2 WHERE id = 1"
check_txn "cross-region transaction: both roll back on abort" "ROLLBACK" \
	"BEGIN" \
	"UPDATE acct SET balance = 10000 WHERE id = 1" \
	"UPDATE txn2 SET balance = 10000 WHERE id = 1" \
	"ROLLBACK"
check "cross-region rollback left region A" "75" \
	"SELECT balance FROM acct WHERE id = 1"
check "cross-region rollback left region B" "200" \
	"SELECT balance FROM txn2 WHERE id = 1"

check "delete acct row" "DELETE 1" "DELETE FROM acct WHERE id = 1"
check "delete txn2 row" "DELETE 1" "DELETE FROM txn2 WHERE id = 1"
check "drop acct over its tombstones" "DROP TABLE" "DROP TABLE acct"
check "drop txn2 over its tombstones" "DROP TABLE" "DROP TABLE txn2"

log "Phase 12 MVCC ordering: a later write from a different node must beat an earlier one"
# The regression this guards against: each SQL node pre-allocates timestamps, so
# a delete on node-3 could be stamped below an insert node-1 issued earlier,
# leaving the live version winning resolution and the row undeletable. Every
# catalog write now takes its timestamp from the etcd watermark, whose CAS is the
# cluster's single ordering point, so alternating nodes cannot invert the order.
# Both directions are exercised: insert A then delete B, and insert B then
# delete A.
check "create hop" "CREATE TABLE" "CREATE TABLE hop (id bigint PRIMARY KEY, note text)"
for i in 1 2 3 4; do
	port=$((base_pg + 1))
	check "hop $i inserted on node-1" "INSERT 0 1" "INSERT INTO hop (id, note) VALUES ($i, 'n$i')"
	port=$((base_pg + 3))
	check "hop $i deleted on node-3" "DELETE 1" "DELETE FROM hop WHERE id = $i"
	port=$((base_pg + 2))
	check "hop $i gone on node-2" "" "SELECT note FROM hop WHERE id = $i"

	port=$((base_pg + 3))
	check "hop $i+ inserted on node-3" "INSERT 0 1" "INSERT INTO hop (id, note) VALUES ($((i + 100)), 'r$i')"
	port=$((base_pg + 1))
	check "hop $i+ deleted on node-1" "DELETE 1" "DELETE FROM hop WHERE id = $((i + 100))"
	port=$((base_pg + 2))
	check "hop $i+ gone on node-2" "" "SELECT note FROM hop WHERE id = $((i + 100))"
done
port=$((base_pg + 1))
check "drop hop over its tombstones" "DROP TABLE" "DROP TABLE hop"

log "Phase 10 leaves the cluster clean"
# The multi-row DELETE above took ids 2..10 and the int64 max, and the point
# DELETE took id=1, so the negative id is the only live row left. The tombstones
# those deletes left behind must not make the table look non-empty and block the
# drop.
check "delete the last row" "DELETE 1" "DELETE FROM metrics WHERE id = -5"
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