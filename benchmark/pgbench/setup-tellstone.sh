#!/usr/bin/env bash
#
# Start a Tellstone instance and load the pgbench schema.
#
# Modes:
#   cluster (default) - 3-node cluster via scripts/tools/local-cluster.sh (TLS + RBAC)
#   single            - single node with TLS + RBAC (same auth as cluster)
#
# Environment:
#   MODE       cluster | single           (default: cluster)
#   DATA_DIR   data directory             (default: ~/.tellstone/benchmark or ~/.tellstone/benchmark-single)
#   PG_BASE    PostgreSQL wire port       (default: 47101)
#   BIN_BASE   internal port base         (default: 19101)
#   ACCOUNTS   pgbench_accounts rows      (default: 10000)
#   BRANCHES   pgbench_branches rows      (default: 1000)
#   TELLERS    pgbench_tellers rows       (default: 1000)
#   SHARDS     shards per node            (default: 4)
#   SKIP_UP    set to 1 to use already running instance (default: 0)
#   TLS        1 enable TLS + RBAC        (default: 1)
#   RBAC       1 enable RBAC              (default: 1)
#   SSLMODE    client SSL mode            (default: require)
#   DBUSER     user                      (default: admin)
#   PASSWORD   password                   (default: admin)
#   DB         database                  (default: test)
#   MEM_LIMIT  per-node memory ceiling    (default: 10GiB)
#

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || (cd "$script_dir/../.." && pwd))

MODE=${MODE:-cluster}

if [ "$MODE" = "single" ]; then
	DATA_DIR=${DATA_DIR:-$HOME/.tellstone/benchmark-single}
	PG_BASE=${PG_BASE:-47101}
	BIN_BASE=${BIN_BASE:-19101}
else
	DATA_DIR=${DATA_DIR:-$HOME/.tellstone/benchmark}
	PG_BASE=${PG_BASE:-47101}
	BIN_BASE=${BIN_BASE:-19101}
fi

ACCOUNTS=${ACCOUNTS:-10000}
BRANCHES=${BRANCHES:-1000}
TELLERS=${TELLERS:-1000}
SHARDS=${SHARDS:-4}
SKIP_UP=${SKIP_UP:-0}
TLS=${TLS:-1}
RBAC=${RBAC:-1}
SSLMODE=${SSLMODE:-require}
DBUSER=${DBUSER:-admin}
PASSWORD=${PASSWORD:-admin}
DB=${DB:-test}
MEM_LIMIT=${MEM_LIMIT:-10GiB}
PORT=$PG_BASE

case "$MEM_LIMIT" in
*[Gg][Ii][Bb]) MEM_BYTES=$(( ${MEM_LIMIT%???} * 1024 * 1024 * 1024 )) ;;
*[Mm][Ii][Bb]) MEM_BYTES=$(( ${MEM_LIMIT%???} * 1024 * 1024 )) ;;
*) MEM_BYTES="$MEM_LIMIT" ;;
esac

die() {
	echo "setup-tellstone: $*" >&2
	exit 1
}

log() {
	printf '\033[1m-- %s\033[0m\n' "$*"
}

command -v psql >/dev/null 2>&1 || die "psql is required"

psql_cmd() {
	if [ "$TLS" = "1" ]; then
		PGPASSWORD="$PASSWORD" PGSSLMODE="${SSLMODE:-require}" \
			psql -h 127.0.0.1 -p "$PORT" -U "$DBUSER" -d "$DB" -X "$@"
	else
		PGSSLMODE="${SSLMODE:-disable}" \
			psql -h 127.0.0.1 -p "$PORT" -U "$DBUSER" -d "$DB" -X "$@"
	fi
}

wait_ready() {
	local i
	for i in $(seq 1 90); do
		if psql_cmd -tAq -c "SELECT key FROM tellstone WHERE key = 'pgbench-wait'" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	die "instance on 127.0.0.1:$PORT did not become ready"
}

filler() {
	local s="filler-$1"
	while [ "${#s}" -lt "$2" ]; do
		s=$s.
	done
	printf '%s' "${s:0:$2}"
}

table_state() {
	psql_cmd -tAq -c "SELECT count(*), max($2) FROM $1" | tr '|' ' '
}

ensure_tls() {
	[ "$TLS" = "1" ] || return 0
	command -v openssl >/dev/null 2>&1 || die "openssl is required to create the development TLS certificate"
	mkdir -p "$tls_dir"
	[ -s "$cert" ] && [ -s "$key" ] && return 0
	log "creating self-signed TLS certificate in $tls_dir"
	openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 \
		-subj "/CN=localhost" -keyout "$key" -out "$cert" >/dev/null 2>&1 || die "openssl failed to create the certificate"
	chmod 600 "$key"
}

ensure_policy() {
	[ "$TLS" = "1" ] && [ "$RBAC" = "1" ] || return 0
	[ -s "$policy" ] && return 0
	mkdir -p "$DATA_DIR"
	cat >"$policy" <<'YAML'
# Tellstone dev RBAC policy
roles:
  - name: admin
    rules: ["+@all", "~*"]
  - name: reader
    rules: ["+@read", "~*"]
  - name: test
    rules: ["+@read", "~tellstone/test/"]
users:
  - name: admin
    role: admin
    password: "$2b$10$jL.1I2iKAQGlvG5ujYFi4u77VqWSEWOc47GS7DfFjY/pO2A.gtsTa"   # admin
  - name: reader
    role: reader
    password: "$2b$10$LbC/fnhg5Ob3GgvnNUhUBeASrjWztXKnBx/fXTNpFHCNYw.Rynopu"   # reader
  - name: test
    role: test
    password: "$2b$10$jDmHEd1ocxHnCW8LXtBc6OQqo54pvEo7VPUKFvxnSMOKwcSfZxngq"   # test
YAML
	chmod 600 "$policy"
}

start_single() {
	local bin="$DATA_DIR/tellstone"
	local pidfile="$DATA_DIR/single.pid"
	local logfile="$DATA_DIR/single.log"
	local addr_port=$BIN_BASE

	tls_dir="$DATA_DIR/tls"
	cert="$tls_dir/cert.pem"
	key="$tls_dir/key.pem"
	policy="$DATA_DIR/rbac.yaml"

	if [ -f "$pidfile" ]; then
		local pid
		pid=$(cat "$pidfile" 2>/dev/null || true)
		if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
			log "single node already running (pid $pid)"
			return 0
		fi
		rm -f "$pidfile"
	fi

	mkdir -p "$DATA_DIR" "$DATA_DIR/logs"

	if [ -z "${TELLSTONE_BIN:-}" ]; then
		if [ ! -x "$bin" ]; then
			log "building ./cmd/tellstone"
			(cd "$repo" && go build -o "$bin" ./cmd/tellstone) || die "build failed"
		fi
	else
		bin="$TELLSTONE_BIN"
	fi
	[ -x "$bin" ] || die "no tellstone binary at $bin"

	auth_flags=()
	if [ "$TLS" = "1" ]; then
		ensure_tls
		auth_flags+=(--pg-tls --tls-cert "$cert" --tls-key "$key")
		if [ "$RBAC" = "1" ]; then
			ensure_policy
			auth_flags+=(--rbac-config "$policy")
			log "authentication: TLS + RBAC (policy: $policy)"
		else
			log "authentication: TLS only"
		fi
	else
		log "authentication: plaintext (trust)"
	fi

	log "starting single node (pg:$PORT, addr: 127.0.0.1:$addr_port, data dir: $DATA_DIR)"
	persist_flags=()
	if [ "${PERSIST:-0}" = "1" ]; then
		mkdir -p "$DATA_DIR/wal" "$DATA_DIR/snapshots"
		persist_flags=(--enable-persistence --persistence-dir "$DATA_DIR/wal")
		log "persistence: enabled (dir: $DATA_DIR/wal)"
	else
		log "persistence: disabled"
	fi
	TSD_MEM_LIMIT_BYTES="$MEM_BYTES" nohup "$bin" \
		--addr "127.0.0.1:$addr_port" \
		--pg-addr "127.0.0.1:$PORT" \
		--shards "$SHARDS" \
		--max-mem-bytes "$MEM_LIMIT" \
		${persist_flags[@]+"${persist_flags[@]}"} \
		${auth_flags[@]+"${auth_flags[@]}"} \
		>"$logfile" 2>&1 &
	echo $! >"$pidfile"
	sleep 2
}

if [ "$MODE" = "single" ]; then
	[ "$SKIP_UP" != 1 ] && start_single
else
	if [ "$SKIP_UP" != 1 ]; then
		log "starting cluster (data dir: $DATA_DIR, pg port: $PG_BASE)"
		if [ "$TLS" = "1" ]; then
			RBAC=1 DATA_DIR="$DATA_DIR" PG_BASE="$PG_BASE" "$repo/scripts/tools/local-cluster.sh" up
		else
			RBAC=0 DATA_DIR="$DATA_DIR" PG_BASE="$PG_BASE" "$repo/scripts/tools/local-cluster.sh" up
		fi
	fi
fi

wait_ready

log "creating pgbench tables"
psql_cmd -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS pgbench_accounts (
	aid      integer PRIMARY KEY,
	bid      integer,
	abalance integer,
	filler   varchar(84)
);
CREATE TABLE IF NOT EXISTS pgbench_tellers (
	tid      integer PRIMARY KEY,
	bid      integer,
	tbalance integer,
	filler   varchar(84)
);
CREATE TABLE IF NOT EXISTS pgbench_branches (
	bid      integer PRIMARY KEY,
	bbalance integer,
	filler   varchar(88)
);
SQL

sql_file=$(mktemp "${TMPDIR:-/tmp}/pgbench-fill.XXXXXX") || die "mktemp failed"
trap 'rm -f "$sql_file"' EXIT

fill_table() {
	local table=$1 key=$2 target=$3 gen=$4
	local count max
	read -r count max < <(table_state "$table" "$key")
	max=${max:-0}
	if [ "$count" -ge "$target" ]; then
		log "$table: $count rows, already loaded"
		return 0
	fi
	local from=$((max + 1))
	[ "$from" -lt 1 ] && from=1
	log "$table: filling $from..$target ($count rows present)"
	{
		local i
		for ((i = from; i <= target; i++)); do
			"$gen" "$i"
			if ((i % 1000 == 0)); then
				printf '\\echo "  %s: %d/%d"\n' "$table" "$i" "$target"
			fi
		done
	} >"$sql_file"
	psql_cmd -q -v ON_ERROR_STOP=1 -f "$sql_file"
}

gen_account() {
	printf "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES (%d, %d, 0, '%s');\n" \
		"$1" "$((1 + $1 % BRANCHES))" "$(filler "account-$1" 84)"
}

gen_teller() {
	printf "INSERT INTO pgbench_tellers (tid, bid, tbalance, filler) VALUES (%d, %d, 0, '%s');\n" \
		"$1" "$((1 + $1 % BRANCHES))" "$(filler "teller-$1" 84)"
}

gen_branch() {
	printf "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES (%d, 0, '%s');\n" \
		"$1" "$(filler "branch-$1" 88)"
}

fill_table pgbench_branches bid "$BRANCHES" gen_branch
fill_table pgbench_tellers tid "$TELLERS" gen_teller
fill_table pgbench_accounts aid "$ACCOUNTS" gen_account

printf '\n'
printf 'accounts  %s\n' "$(psql_cmd -tAq -c 'SELECT count(*) FROM pgbench_accounts')"
printf 'tellers   %s\n' "$(psql_cmd -tAq -c 'SELECT count(*) FROM pgbench_tellers')"
printf 'branches  %s\n' "$(psql_cmd -tAq -c 'SELECT count(*) FROM pgbench_branches')"

printf '\n'
log "ready; run the workload with:"
if [ "$MODE" = "single" ]; then
	printf '  ACCOUNTS=%d BRANCHES=%d TELLERS=%d MODE=ro CLIENTS="8 32 64" DURATION=30 %s/run-pgbench.sh\n' \
		"$ACCOUNTS" "$BRANCHES" "$TELLERS" "$script_dir"
	printf '\n  stop with: kill $(cat %s/single.pid 2>/dev/null)\n' "$DATA_DIR"
	printf '  wipe:      rm -rf %s\n' "$DATA_DIR"
else
	printf '  ACCOUNTS=%d BRANCHES=%d TELLERS=%d PORT=%d %s/run-pgbench.sh\n' \
		"$ACCOUNTS" "$BRANCHES" "$TELLERS" "$PORT" "$script_dir"
	printf '\n  stop cluster: DATA_DIR=%s %s/../../scripts/tools/local-cluster.sh down\n' "$DATA_DIR" "$script_dir"
fi
