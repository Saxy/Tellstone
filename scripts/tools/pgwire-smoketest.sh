#!/usr/bin/env bash
# Manual PostgreSQL wire (Phase 8 / ADR-012) smoke test.
#
# Drives the SQL frontend with real psql statements (INSERT/SELECT/UPDATE/
# DELETE plus bytea hex round-trips), in single-node mode and, when --cluster
# is given, a 3-node --cluster-mode Tellstone cluster.
#
# Usage:
#   scripts/tools/pgwire-smoketest.sh            # single node only
#   scripts/tools/pgwire-smoketest.sh --cluster  # single + 3-node cluster
#
# Requirements: psql + pg_isready on PATH. Builds ./cmd/tellstone unless
# TELLSTONE_BIN is set to an existing binary. Exits non-zero on any failed
# check.

set -u

mode="${1:-single}"
bin="${TELLSTONE_BIN:-}"
work=$(mktemp -d /tmp/pgwire-smoketest.XXXXXX)
pids=()
base_pg=47000   # PostgreSQL wire ports
base_bin=19000  # binary protocol / data ports (PD client/peer derive +10000/+20000)
pass=0
fail=0

log()  { printf '\n\033[1m-- %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }

# check <label> <expected-substring> <psql args, e.g. -c "SQL" ...>
check() {
	local label="$1" expect="$2"; shift 2
	local out
	out=$(psql -h 127.0.0.1 -p "$port" -U default -d test -X -At -t "$@" 2>&1)
	local shown
	shown=$(printf '%s' "$out" | head -c 160 | tr '\n' ' ')
	if [ -z "$expect" ]; then
		[ -z "$out" ] && ok "$label (no output)" || bad "$label: expected no output, got: $shown"
	elif printf '%s' "$out" | grep -qF "$expect"; then
		ok "$label (got: $shown)"
	else
		bad "$label: wanted '$expect', got: $shown"
	fi
}

# wait_pg <port> retries until the frontend answers a query that succeeds on
# Tellstone's SQL subset (a WHERE-key SELECT returning zero rows "succeeds").
wait_pg() {
	local port="$1" n=0
	while ! timeout 2 psql -h 127.0.0.1 -p "$port" -U default -d test -X -t -c "SELECT key FROM tellstone WHERE key = 'pgwire-wait'" >/dev/null 2>&1; do
		n=$((n+1))
		[ "$n" -ge 40 ] && { bad "postgres frontend on :$port did not respond"; return 1; }
		sleep 0.5
	done
	return 0
}

cleanup() {
	for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null; done
	[ -n "${ts_nodes:-}" ] && for p in $ts_nodes; do kill "$p" 2>/dev/null; done
	wait 2>/dev/null
	rm -rf "$work"
	printf '\n%d pass, %d fail\n' "$pass" "$fail"
	[ "$fail" -eq 0 ]
}

if ! command -v psql >/dev/null 2>&1 || ! command -v pg_isready >/dev/null 2>&1; then
	echo "pgwire-smoketest: psql and pg_isready are required (postgresql-client)" >&2
	exit 1
fi

[ -n "$bin" ] && [ -x "$bin" ] || {
	bin="$work/tellstone"
	log "building ./cmd/tellstone"
	go build -o "$bin" ./cmd/tellstone || { echo "build failed" >&2; exit 1; }
}

trap cleanup EXIT

# ---------------------------------------------------------------- single node
port=$base_pg
log "single-node trust mode (pg:$port)"
"$bin" --addr "127.0.0.1:$((base_bin+10))" --pg-addr "127.0.0.1:$port" --shards 4 >"$work/single.log" 2>&1 &
pids+=($!)
wait_pg "$port" || exit 1

export port
check "INSERT" "INSERT 0 1" -c "INSERT INTO tellstone (key, value) VALUES ('foo', 'bar')"
check "SELECT key/value" "foo|\x626172" -c "SELECT key, value FROM tellstone WHERE key = 'foo'"
check "bytea literal insert" "INSERT 0 1" -c "INSERT INTO tellstone (key, value) VALUES ('bin', '\x0102')"
check "bytea literal round-trip" "\x0102" -c "SELECT value FROM tellstone WHERE key = 'bin'"
check "UPDATE" "UPDATE 1" -c "UPDATE tellstone SET value = 'baz' WHERE key = 'foo'"
check "SELECT after update" "foo|\x62617a" -c "SELECT key, value FROM tellstone WHERE key = 'foo'"
check "DELETE" "DELETE 1" -c "DELETE FROM tellstone WHERE key = 'bin'"
check "missing key returns no rows" "" -c "SELECT key FROM tellstone WHERE key = 'nope'"
check "no-WHERE-select rejected" "requires a WHERE key" -c "SELECT key FROM tellstone"

for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null; done
pids=()
sleep 0.3

# ------------------------------------------------------------------- cluster
if [ "$mode" = "--cluster" ]; then
	log "3-node cluster mode (raft + PD/TSO)"
	peers=() pd=()
	for i in 1 2 3; do
		# port layout mirrors manual_test.go: binary = base_bin + i*10,
		# raft = base_bin + 9 + i*10, PD client/peer derive +10000/+20000.
		peers+=("$i@127.0.0.1:$((base_bin + 9 + i * 10))")
		pd+=("$i@127.0.0.1:$((base_bin + i * 10))")
	done
	peer_str=$(IFS=,; echo "${peers[*]}")
	pd_str=$(IFS=,; echo "${pd[*]}")
	ts_nodes=""
	for i in 1 2 3; do
		mkdir -p "$work/pd-$i"
		"$bin" --cluster-mode --node-role hybrid --node-id "$i" \
			--peer-addr "127.0.0.1:$((base_bin + 9 + i * 10))" --peers "$peer_str" \
			--pd-members "$pd_str" --pd-dir "$work/pd-$i" \
			--addr "127.0.0.1:$((base_bin + i * 10))" \
			--pg-addr "127.0.0.1:$((base_pg + i))" --shards 4 \
			>"$work/cluster-$i.log" 2>&1 &
		ts_nodes="$ts_nodes $!"
	done

	port=$((base_pg+1))
	wait_pg "$port" || exit 1
	log "   quorum up; psql on node-1 (pg:$port)"
	check "cluster INSERT via psql" "INSERT 0 1" -c "INSERT INTO tellstone (key, value) VALUES ('c1', 'raft')"
	check "cluster SELECT same node" "c1|\x72616674" -c "SELECT key, value FROM tellstone WHERE key = 'c1'"

	port=$((base_pg+2))
	log "   read-your-own-write on node-2 (pg:$port)"
	check "cluster SELECT across nodes" "c1|\x72616674" -c "SELECT key, value FROM tellstone WHERE key = 'c1'"
	check "cluster SELECT missing key" "" -c "SELECT key FROM tellstone WHERE key = 'ghost'"
	psql -h 127.0.0.1 -p "$((base_pg+1))" -U default -d test -X -At -c "DELETE FROM tellstone WHERE key = 'c1'" >/dev/null 2>&1
	port=$((base_pg+3))
	check "cluster DELETE replicated" "" -c "SELECT key FROM tellstone WHERE key = 'c1'"

	for p in $ts_nodes; do kill "$p" 2>/dev/null; done
	ts_nodes=""
	sleep 0.3
fi

exit 0