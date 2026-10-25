#!/usr/bin/env bash
#
# Run the pgbench read-only / read-write workload against any PostgreSQL-wire
# target: Tellstone, PostgreSQL, CockroachDB, ...
#
# The workload is deliberately transport-neutral. Tables are expected to be
# already loaded (setup-tellstone.sh for Tellstone, "pgbench -i" for
# PostgreSQL, "cockroach workload init" for CockroachDB); the random key
# ranges below must match the loaded row counts.
#
# Environment:
#   HOST        target host                    (default: 127.0.0.1)
#   PORT        target PostgreSQL wire port    (default: 47101)
#   DBUSER      database user                  (default: admin)
#   DB          database                       (default: test)
#   PASSWORD    password                       (default: admin)
#   SSLMODE     client SSL mode                (default: require)
#   PGBENCH     pgbench binary                 (default: auto-detect)
#   ACCOUNTS    pgbench_accounts row count     (default: 10000)
#   BRANCHES    pgbench_branches row count     (default: 1000)
#   TELLERS     pgbench_tellers row count      (default: 1000)
#   MODE        ro | rw | both                 (default: both)
#   CLIENTS     space-separated client counts  (default: "1 8 32")
#   THREADS     max pgbench threads            (default: 4)
#   DURATION    seconds per run                (default: 15)
#   RETRIES     >0 enables pgbench retries on
#               serialization failures         (default: 0)
#   EXTRA       extra pgbench arguments        (default: empty)
#
set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-47101}
DBUSER=${DBUSER:-${PGUSER:-admin}}
DB=${DB:-${PGDATABASE:-test}}
PASSWORD=${PASSWORD:-admin}
SSLMODE=${SSLMODE:-require}
ACCOUNTS=${ACCOUNTS:-10000}
BRANCHES=${BRANCHES:-1000}
TELLERS=${TELLERS:-1000}
MODE=${MODE:-both}
CLIENTS=${CLIENTS:-"1 8 32"}
THREADS=${THREADS:-4}
DURATION=${DURATION:-15}
RETRIES=${RETRIES:-0}
EXTRA=${EXTRA:-}

die() {
	echo "run-pgbench: $*" >&2
	exit 1
}

if [ -z "${PGBENCH:-}" ]; then
	for candidate in /usr/lib/postgresql/*/bin/pgbench; do
		[ -x "$candidate" ] && PGBENCH=$candidate
	done
	PGBENCH=${PGBENCH:-$(command -v pgbench || true)}
fi
[ -n "${PGBENCH:-}" ] && [ -x "$PGBENCH" ] || die "pgbench not found; set PGBENCH"

export PGPASSWORD="$PASSWORD" PGSSLMODE="$SSLMODE"

work=$(mktemp -d "${TMPDIR:-/tmp}/pgbench-work.XXXXXX") || die "mktemp failed"
trap 'rm -rf "$work"' EXIT

cat >"$work/read-only.sql" <<SQL
\set aid random(1, $ACCOUNTS)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
SQL

cat >"$work/read-write.sql" <<SQL
\set aid random(1, $ACCOUNTS)
\set bid random(1, $BRANCHES)
\set tid random(1, $TELLERS)
\set delta random(-5000, 5000)

BEGIN;
UPDATE pgbench_accounts SET abalance = :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = :delta WHERE bid = :bid;
COMMIT;
SQL

pgbench_args=(-h "$HOST" -p "$PORT" -U "$DBUSER" -d "$DB" -n --no-vacuum
	-T "$DURATION" -P 5)
# shellcheck disable=SC2206
[ -n "$EXTRA" ] && pgbench_args+=($EXTRA)
[ "$RETRIES" -gt 0 ] && pgbench_args+=(--max-tries="$RETRIES")

run_mode() {
	local name=$1 script=$2 failed=0
	local out
	printf '\n\033[1m== %s (%s) ==\033[0m\n' "$name" "$script"
	printf '%-8s %10s %14s %10s\n' clients tps "latency(ms)" failed
	local c
	for c in $CLIENTS; do
		local j=$c
		[ "$j" -gt "$THREADS" ] && j=$THREADS
		out=$("$PGBENCH" "${pgbench_args[@]}" -c "$c" -j "$j" -f "$work/$script" 2>&1) || true
		local tps lat fails fcount
		tps=$(sed -n 's/^tps = \([0-9.]*\).*/\1/p' <<<"$out" | tail -1)
		lat=$(sed -n 's/^latency average = \([0-9.]*\) ms.*/\1/p' <<<"$out" | tail -1)
		fails=$(sed -n 's/^number of failed transactions: [0-9]* (\([0-9.]*\)%).*/\1/p' <<<"$out" | tail -1)
		fcount=$(sed -n 's/^number of failed transactions: \([0-9]*\).*/\1/p' <<<"$out" | tail -1)
		[ -z "$tps" ] && { printf '%s\n' "$out" >&2; die "pgbench run failed"; }
		if [ -n "${fcount:-}" ] && [ "$fcount" -gt 0 ] 2>/dev/null; then
			failed=1
		fi
		printf '%-8s %10s %14s %9s%%\n' "$c" "$tps" "$lat" "${fails:-0}"
	done
	return "$failed"
}

status=0
case "$MODE" in
ro) run_mode "read-only" read-only.sql || status=$? ;;
rw) run_mode "read-write" read-write.sql || status=$? ;;
both)
	run_mode "read-only" read-only.sql || status=$?
	run_mode "read-write" read-write.sql || status=$?
	;;
*) die "MODE must be ro, rw or both" ;;
esac

printf '\n'
if [ "$status" -ne 0 ]; then
	echo "note: at least one run had failed transactions (see table above)"
fi
