#!/usr/bin/env bash
#
# Local 3-node Tellstone cluster for manual client testing.
#
# Runs a real --cluster-mode cluster: Raft consensus per region, embedded
# PD/TSO on every node (hybrid role), and WAL persistence enabled with one
# data directory per node. Data survives "down" and "up"; "reset" wipes it.
#
# This is a development convenience, not a hardened deployment: plaintext
# PostgreSQL wire, trust authentication (any user/database/password is
# accepted), and default timeouts. Point DataGrip (or psql, or pgJDBC) at any
# one of the three PostgreSQL ports.
#
# Usage:
#   scripts/tools/local-cluster.sh [up|run|down|restart|status|logs [-f] [node]|reload|reset]
#
#   up        build (when needed) and start the cluster detached; prints the
#             DataGrip connection details. The binary is rebuilt from the
#             working tree on every start, so the cluster never runs a
#             stale build. The nodes keep running after this command returns.
#   run       start (if needed), stream all three nodes' logs to this terminal,
#             and stop the cluster when you press Ctrl-C. Nothing is left
#             running in the background.
#   down      stop the cluster; data is kept for the next "up".
#   restart   down, then up.
#   status    show each node's pid, PostgreSQL port, and readiness.
#   logs [-f] [n]
#             tail node n's log (n = 1..3), or all three; -f follows live.
#   reload    SIGHUP every node so it re-reads the RBAC policy file.
#   reset     stop the cluster and delete its data directory.
#
# Authentication (RBAC=1, the default):
#   The cluster runs with --pg-tls and an RBAC policy generated once at
#   <data>/rbac.yaml. Passwords are cleartext credentials that may only cross
#   TLS, so clients must connect with SSL (psql: sslmode=require; DataGrip:
#   SSL mode "require"). Users:
#     admin  / admin   everything (DDL, reads, writes, role management)
#     reader / reader  read-only on every table
#     test   / test    read-only on the table named "test" and nothing else
#   Edit <data>/rbac.yaml and run "reload" to apply password/role changes;
#   bcrypt hashes are required (the file comments show how to make one).
#
# Environment:
#   DATA_DIR           data directory (default: ~/.tellstone/local-cluster)
#   PG_BASE            first PostgreSQL wire port (default: 47101, nodes 1..3)
#   BIN_BASE           first data/peer port (default: 19101; node i uses
#                      BIN_BASE+(i-1)*10; Raft peer +9; PD client/peer derive
#                      +10000/+20000)
#   MEM_LIMIT          per-node memory ceiling, e.g. 10GiB (default: 10GiB);
#                      becomes both the Go runtime soft limit
#                      (TSD_MEM_LIMIT_BYTES) and the engine's in-memory
#                      ceiling (--max-mem-bytes)
#   SHARDS             shards per node (default: 4)
#   SNAPSHOT_INTERVAL  e.g. 30s; 0 disables periodic snapshots (default: 0,
#                      WAL replay alone provides durability)
#   RBAC               1 (default) enables TLS + the RBAC policy above; 0 runs
#                      the old plaintext, trust-everything dev cluster
#   TELLSTONE_BIN      use an existing binary instead of building one
#
# Requires: go (for building) and, for readiness checks, psql from
# postgresql-client. Without psql the script falls back to a TCP probe.
# With RBAC=1, openssl is required once to mint a self-signed certificate.

set -u

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || dirname "$script_dir")

cmd="${1:-up}"
arg="${2:-}"

: "${HOME:?local-cluster: HOME must be set}"
data_dir="${DATA_DIR:-$HOME/.tellstone/local-cluster}"
pg_base="${PG_BASE:-47101}"
bin_base="${BIN_BASE:-19101}"
mem_limit="${MEM_LIMIT:-10GiB}"
case "$mem_limit" in
*[Gg][Ii][Bb]) mem_bytes=$(( ${mem_limit%???} * 1024 * 1024 * 1024 )) ;;
*[Mm][Ii][Bb]) mem_bytes=$(( ${mem_limit%???} * 1024 * 1024 )) ;;
*) mem_bytes="$mem_limit" ;;
esac
shards="${SHARDS:-4}"
snapshot_interval="${SNAPSHOT_INTERVAL:-0}"
rbac="${RBAC:-1}"
bin="${TELLSTONE_BIN:-$data_dir/tellstone}"

nodes=(1 2 3)
logs_dir="$data_dir/logs"
tls_dir="$data_dir/tls"
cert="$tls_dir/cert.pem"
key="$tls_dir/key.pem"
policy="$data_dir/rbac.yaml"

log()  { printf '\033[1m-- %s\033[0m\n' "$*"; }
die()  { printf 'local-cluster: %s\n' "$*" >&2; exit 1; }

node_pg_port()   { echo $((pg_base + $1 - 1)); }
node_addr_port() { echo $((bin_base + ($1 - 1) * 10)); }
node_peer_port() { echo $((bin_base + 9 + ($1 - 1) * 10)); }
node_dir()       { echo "$data_dir/node-$1"; }
node_pidfile()   { echo "$data_dir/node-$1.pid"; }
node_log()       { echo "$logs_dir/node-$1.log"; }

peer_str() {
	local out="" i
	for i in "${nodes[@]}"; do
		[ -n "$out" ] && out+=","
		out+="$i@127.0.0.1:$(node_peer_port "$i")"
	done
	echo "$out"
}

pd_str() {
	local out="" i
	for i in "${nodes[@]}"; do
		[ -n "$out" ] && out+=","
		out+="$i@127.0.0.1:$(node_addr_port "$i")"
	done
	echo "$out"
}

pid_alive() {
	local pid="$1"
	[ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

node_running() {
	local pf pid
	pf=$(node_pidfile "$1")
	[ -f "$pf" ] || return 1
	pid=$(cat "$pf" 2>/dev/null || true)
	pid_alive "$pid"
}

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

# tcp_open <port> reports whether something accepts a connection on loopback.
tcp_open() {
	(exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

# psql_admin <port> [psql args...] connects as the admin user. With RBAC the
# connection must be TLS (cleartext passwords never cross a plaintext socket)
# and carries the dev password; without it the listener is trust-only.
psql_admin() {
	local port="$1"; shift
	if [ "$rbac" = "1" ]; then
		PGPASSWORD=admin PGSSLMODE=require psql -h 127.0.0.1 -p "$port" -U admin -d test -X "$@"
	else
		psql -h 127.0.0.1 -p "$port" -U default -d test -X "$@"
	fi
}

# ensure_tls mints a self-signed certificate once, when RBAC is enabled.
# Clients use sslmode=require, which encrypts without validating the chain, so
# a throwaway cert is enough for local testing.
ensure_tls() {
	[ "$rbac" = "1" ] || return 0
	command -v openssl >/dev/null 2>&1 || 		die "openssl is required to create the development TLS certificate (or set RBAC=0)"
	mkdir -p "$tls_dir"
	[ -s "$cert" ] && [ -s "$key" ] && return 0
	log "creating self-signed TLS certificate in $tls_dir"
	openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 		-subj "/CN=localhost" -keyout "$key" -out "$cert" >/dev/null 2>&1 || 		die "openssl failed to create the certificate"
	chmod 600 "$key"
}

# ensure_policy writes the development RBAC policy once. The bcrypt hashes are
# for the fixed dev passwords printed by "up"; edit the file and run "reload"
# to change them. It is never overwritten, so local edits survive restarts.
ensure_policy() {
	[ "$rbac" = "1" ] || return 0
	[ -s "$policy" ] && return 0
	mkdir -p "$data_dir"
	cat >"$policy" <<'YAML'
# Tellstone local-cluster RBAC policy
#
# Roles are Redis-ACL-shaped: +@category / +cmd grant commands, ~prefix grants
# a key namespace. Data keys look like tellstone/<table>/<row>/<column>.
#
# To change a password, replace its bcrypt hash. Generate one with:
#   python3 -c 'import bcrypt,sys;print(bcrypt.hashpw(sys.argv[1].encode(),bcrypt.gensalt()).decode())' NEWPASSWORD
# then run: scripts/tools/local-cluster.sh reload
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

# wait_ready <node> retries until the PostgreSQL frontend answers. A WHERE-key
# SELECT against the implicit tellstone table is the probe the other cluster
# scripts use: it succeeds on Tellstone's SQL subset and returns zero rows.
wait_ready() {
	local i="$1" port n=0
	local user="default"
	local -a creds=()
	port=$(node_pg_port "$i")
	if [ "$rbac" = "1" ]; then
		user="admin"
		creds=(env PGPASSWORD=admin PGSSLMODE=require)
	fi
	if command -v psql >/dev/null 2>&1; then
		if [ ${#creds[@]} -gt 0 ]; then
			while ! run_timeout 3 "${creds[@]}" psql -h 127.0.0.1 -p "$port" -U "$user" -d test -X -t \
				-c "SELECT key FROM tellstone WHERE key = 'local-cluster-wait'" >/dev/null 2>&1; do
				n=$((n + 1))
				[ "$n" -ge 60 ] && return 1
				sleep 0.5
			done
		else
			while ! run_timeout 3 psql -h 127.0.0.1 -p "$port" -U "$user" -d test -X -t \
				-c "SELECT key FROM tellstone WHERE key = 'local-cluster-wait'" >/dev/null 2>&1; do
				n=$((n + 1))
				[ "$n" -ge 60 ] && return 1
				sleep 0.5
			done
		fi
		return 0
	fi
	while ! tcp_open "$port"; do
		n=$((n + 1))
		[ "$n" -ge 120 ] && return 1
		sleep 0.5
	done
	return 0
}

cleanup_pidfiles() {
	local i pf
	for i in "${nodes[@]}"; do
		pf=$(node_pidfile "$i")
		rm -f "$pf"
	done
}

do_status() {
	local i pf pid port state
	for i in "${nodes[@]}"; do
		pf=$(node_pidfile "$i")
		port=$(node_pg_port "$i")
		state="stopped"
		if [ -f "$pf" ]; then
			pid=$(cat "$pf" 2>/dev/null || true)
			if pid_alive "$pid"; then
				state="running (pid $pid)"
				tcp_open "$port" && state+=", pg ready" || state+=", pg not ready"
			else
				state="stale pidfile ($pf)"
			fi
		fi
		printf 'node-%s  pg:%-6s %s\n' "$i" "$port" "$state"
	done
}

do_up() {
	local i port addr peer pf
	if ! command -v go >/dev/null 2>&1 && [ ! -x "$bin" ]; then
		die "go is required to build the server (or set TELLSTONE_BIN)"
	fi

	if node_running 1 || node_running 2 || node_running 3; then
		log "cluster already running"
		do_status
		printf '\n  Follow logs: %s logs -f    Foreground run: %s run\n' "$0" "$0"
		return 0
	fi

	mkdir -p "$logs_dir"
	ensure_tls
	ensure_policy

	if [ -z "${TELLSTONE_BIN:-}" ]; then
		# The build is cheap against the Go build cache, and always building
		# means the cluster never runs a binary older than the working tree.
		log "building ./cmd/tellstone"
		(cd "$repo" && go build -o "$bin" ./cmd/tellstone) || die "build failed"
	fi
	[ -x "$bin" ] || die "no tellstone binary at $bin"

	cleanup_pidfiles

	log "starting 3-node cluster (Raft + PD/TSO + WAL persistence)"
	log "memory ceiling: $mem_limit per node (runtime + engine)"
	log "data directory: $data_dir"
	auth_flags=()
	if [ "$rbac" = "1" ]; then
		log "authentication: TLS + RBAC (policy: $policy)"
		auth_flags=(--pg-tls --tls-cert "$cert" --tls-key "$key" --rbac-config "$policy")
	else
		log "authentication: trust (RBAC=0; plaintext)"
	fi
	for i in "${nodes[@]}"; do
		port=$(node_pg_port "$i")
		addr=$(node_addr_port "$i")
		peer=$(node_peer_port "$i")
		mkdir -p "$(node_dir "$i")/pd" "$(node_dir "$i")/data"
		pf=$(node_pidfile "$i")
		cd "$(node_dir "$i")" || die "cannot enter $(node_dir "$i")"
		TSD_MEM_LIMIT_BYTES="$mem_bytes" nohup "$bin" \
			--cluster-mode --node-role hybrid --node-id "$i" \
			--addr "127.0.0.1:$addr" \
			--peer-addr "127.0.0.1:$peer" \
			--peers "$(peer_str)" \
			--pd-members "$(pd_str)" \
			--pd-dir "$(node_dir "$i")/pd" \
			--pg-addr "127.0.0.1:$port" \
			--shards "$shards" \
			--enable-persistence \
			--persistence-dir "$(node_dir "$i")/data" \
			--snapshot-interval "$snapshot_interval" \
			--max-mem-bytes "$mem_limit" \
			${auth_flags[@]+"${auth_flags[@]}"} \
			>"$(node_log "$i")" 2>&1 &
		echo $! >"$pf"
		cd "$repo" || true
	done

	for i in "${nodes[@]}"; do
		if ! wait_ready "$i"; then
			log "node-$i did not become ready; see $(node_log "$i")"
			do_status
			return 1
		fi
	done

	log "all 3 nodes accepting SQL"

	# A table for the restricted user to read: "test" is the only table the
	# "test" user may touch, so seed one row (tolerating a replay on restart).
	if command -v psql >/dev/null 2>&1; then
		psql_admin "$(node_pg_port 1)" -q \
			-c "CREATE TABLE IF NOT EXISTS test (id bigint PRIMARY KEY, name text)" >/dev/null 2>&1
		psql_admin "$(node_pg_port 1)" -q \
			-c "INSERT INTO test (id, name) VALUES (1, 'hello')" >/dev/null 2>&1
	fi

	printf '\n'
	printf '  PostgreSQL wire (DataGrip / psql / pgJDBC):\n'
	for i in "${nodes[@]}"; do
		printf '    node-%s  127.0.0.1:%s\n' "$i" "$(node_pg_port "$i")"
	done
	printf '\n'
	printf '  Database: test\n'
	if [ "$rbac" = "1" ]; then
		printf '  SSL mode: require (self-signed development certificate)\n'
		printf '\n'
		printf '  admin  / admin    everything, including DDL and role management\n'
		printf '  reader / reader   read-only on every table\n'
		printf '  test   / test     read-only on the table named "test" only\n'
		printf '\n'
		printf '  psql "host=127.0.0.1 port=%s user=admin dbname=test sslmode=require"\n' "$(node_pg_port 1)"
		printf '  psql "host=127.0.0.1 port=%s user=test  dbname=test sslmode=require"\n' "$(node_pg_port 1)"
		printf '\n'
		printf '  Policy (edit + "%s reload"): %s\n' "$0" "$policy"
	else
		printf '  User: default    Password: (any; trust auth)\n'
		printf '  TLS: disabled -- set the client SSL mode to "disable" if it refuses\n'
		printf '\n'
		printf '  psql -h 127.0.0.1 -p %s -U default -d test\n' "$(node_pg_port 1)"
	fi
	printf '  Data: %s (kept across down/up)\n' "$data_dir"
	printf '  Logs: %s\n' "$logs_dir"
	printf '\n'
	printf '  Stop: %s down    Wipe data: %s reset\n' "$0" "$0"
}

do_down() {
	local i pf pid n
	local any=0
	for i in "${nodes[@]}"; do
		pf=$(node_pidfile "$i")
		[ -f "$pf" ] || continue
		any=1
		pid=$(cat "$pf" 2>/dev/null || true)
		pid_alive "$pid" && kill "$pid" 2>/dev/null
	done
	if [ "$any" = 0 ]; then
		log "cluster is not running"
		return 0
	fi
	for i in "${nodes[@]}"; do
		pf=$(node_pidfile "$i")
		[ -f "$pf" ] || continue
		pid=$(cat "$pf" 2>/dev/null || true)
		n=0
		while pid_alive "$pid"; do
			n=$((n + 1))
			if [ "$n" -ge 100 ]; then
				kill -9 "$pid" 2>/dev/null
				break
			fi
			sleep 0.1
		done
	done
	cleanup_pidfiles
	log "cluster stopped (data kept in $data_dir)"
}

do_logs() {
	local follow=0 which="" a i
	for a in "$@"; do
		case "$a" in
		-f | --follow) follow=1 ;;
		1 | 2 | 3) which="$a" ;;
		"") ;;
		*) die "logs takes -f and/or a node number (1..3)" ;;
		esac
	done
	if [ -n "$which" ]; then
		if [ "$follow" = 1 ]; then
			exec tail -n 80 -F "$(node_log "$which")"
		fi
		tail -n 80 "$(node_log "$which")"
		return 0
	fi
	if [ "$follow" = 1 ]; then
		exec tail -n 40 -F "$(node_log 1)" "$(node_log 2)" "$(node_log 3)"
	fi
	for i in "${nodes[@]}"; do
		printf '\033[1m-- node-%s (%s) --\033[0m\n' "$i" "$(node_log "$i")"
		tail -n 40 "$(node_log "$i")" 2>/dev/null
	done
}

# do_run is the "no background jobs" mode: start the cluster if it is not
# already up, stream every node's log to the terminal, and stop the cluster on
# Ctrl-C (or on any signal that ends the script). A terminal Ctrl-C reaches the
# whole foreground process group, so tail and this shell both get it and the
# trap runs; killing the tail alone also falls through to the stop below.
do_run() {
	do_up "" || return 1
	log "following node logs; Ctrl-C stops the cluster"
	trap 'do_down; exit 0' INT TERM HUP
	tail -n 20 -F "$(node_log 1)" "$(node_log 2)" "$(node_log 3)"
	trap - INT TERM HUP
	do_down
}

do_reload() {
	local i pf pid any=0
	for i in "${nodes[@]}"; do
		pf=$(node_pidfile "$i")
		[ -f "$pf" ] || continue
		pid=$(cat "$pf" 2>/dev/null || true)
		pid_alive "$pid" || continue
		any=1
		kill -HUP "$pid" 2>/dev/null
	done
	if [ "$any" = 0 ]; then
		log "cluster is not running"
		return 0
	fi
	log "sent SIGHUP to all nodes (RBAC policy and TLS files re-read)"
}

do_reset() {
	case "$data_dir" in
	"" | "/" | "$HOME" | "$HOME/")
		die "refusing to delete $data_dir"
		;;
	esac
	do_down
	rm -rf "$data_dir"
	log "data directory removed: $data_dir"
}

case "$cmd" in
up) do_up "$arg" ;;
run | foreground) do_run ;;
down) do_down ;;
restart)
	do_down
	do_up ""
	;;
status) do_status ;;
logs) shift; do_logs "$@" ;;
reload) do_reload ;;
reset) do_reset ;;
-h | --help | help)
	awk 'NR == 1 { next } /^set -u/ { exit } { print }' "${BASH_SOURCE[0]}" |
		sed 's/^# \{0,1\}//'
	;;
*)
	die "unknown command: $cmd (try: up, run, down, restart, status, logs, reload, reset)"
	;;
esac
