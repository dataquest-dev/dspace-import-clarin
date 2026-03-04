#!/bin/bash
set -euo pipefail

ts() {
	date +"%Y-%m-%d %H:%M:%S"
}

echo "Starting postgres"
/usr/local/bin/docker-entrypoint.sh postgres &> ./__postgres.log &
PID=$!

tail_postgres_log() {
	echo "Last postgres log lines:"
	tail -n 120 ./__postgres.log || true
}

assert_server_process_alive() {
	if ! kill -0 "$PID" >/dev/null 2>&1; then
		echo "Postgres entrypoint exited early."
		tail_postgres_log
		exit 1
	fi
}

wait_for_ready() {
	local timeout_seconds="${1:-180}"
	local ready=0

	echo "Waiting for postgres to accept connections..."
	for i in $(seq 1 "$timeout_seconds"); do
		if pg_isready -U postgres >/dev/null 2>&1; then
			ready=1
			break
		fi

		assert_server_process_alive
		sleep 1
	done

	if [ "$ready" -ne 1 ]; then
		echo "Timed out waiting for postgres startup."
		tail_postgres_log
		exit 1
	fi
}

run_with_retry() {
	local label="$1"
	shift

	local retries=20
	for i in $(seq 1 "$retries"); do
		assert_server_process_alive
		if "$@"; then
			return 0
		fi

		echo "Retry [$i/$retries] failed for: $label"
		wait_for_ready 30
		sleep 1
	done

	echo "Failed after retries: $label"
	tail_postgres_log
	exit 1
}

ensure_pv_available() {
	if command -v pv >/dev/null 2>&1; then
		return 0
	fi

	echo "[$(ts)] 'pv' not found; attempting to install for progress display"

	if command -v apt-get >/dev/null 2>&1; then
		DEBIAN_FRONTEND=noninteractive apt-get update >/dev/null 2>&1 || true
		DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends pv >/dev/null 2>&1 || true
	elif command -v apk >/dev/null 2>&1; then
		apk add --no-cache pv >/dev/null 2>&1 || true
	fi

	if command -v pv >/dev/null 2>&1; then
		echo "[$(ts)] Installed 'pv' successfully"
	else
		echo "[$(ts)] Could not install 'pv'; continuing with elapsed-time heartbeat progress"
	fi
}

import_dump_with_progress() {
	local db_name="$1"
	local dump_file="$2"
	local log_file="$3"
	local dump_path="../dump/$dump_file"
	local started_at
	local finished_at
	local elapsed

	if [ ! -f "$dump_path" ]; then
		echo "Missing dump file: $dump_path"
		exit 1
	fi

	echo "[$(ts)] Importing $db_name from $dump_file"
	started_at=$(date +%s)

	if command -v pv >/dev/null 2>&1; then
		run_with_retry "psql import $db_name (pv)" bash -lc "pv \"$dump_path\" | psql -v ON_ERROR_STOP=1 -U postgres \"$db_name\" > \"$log_file\" 2>&1"
	else
		run_with_retry "psql import $db_name" bash -lc '
			set -euo pipefail
			dump_path="$1"
			db_name="$2"
			log_file="$3"
			start_epoch=$(date +%s)
			(
				while true; do
					sleep 15
					now_epoch=$(date +%s)
					echo "[$(date +"%Y-%m-%d %H:%M:%S")] importing $db_name... $((now_epoch - start_epoch))s elapsed"
				done
			) &
			heartbeat_pid=$!
			trap "kill $heartbeat_pid >/dev/null 2>&1 || true" EXIT
			psql -v ON_ERROR_STOP=1 -U postgres "$db_name" < "$dump_path" > "$log_file" 2>&1
			kill $heartbeat_pid >/dev/null 2>&1 || true
			wait $heartbeat_pid 2>/dev/null || true
		' _ "$dump_path" "$db_name" "$log_file"
	fi

	finished_at=$(date +%s)
	elapsed=$((finished_at - started_at))
	echo "[$(ts)] Finished import for $db_name in ${elapsed}s (log: $log_file)"
}

wait_for_ready 180
ensure_pv_available

run_with_retry "createuser dspace" createuser --username=postgres dspace

echo "[$(ts)] Preparing clarin-dspace"
run_with_retry "createdb clarin-dspace" createdb --username=postgres --owner=dspace --encoding=UNICODE clarin-dspace
import_dump_with_progress "clarin-dspace" "clarin-dspace.sql" "./__clarin-dspace.log"

echo "[$(ts)] Preparing clarin-utilities"
run_with_retry "createdb clarin-utilities" createdb --username=postgres --encoding=UNICODE clarin-utilities
import_dump_with_progress "clarin-utilities" "clarin-utilities.sql" "./__clarin-utilities.log"

echo "Done, starting psql"

# psql -U postgres
echo "Waiting for PID:$PID /usr/local/bin/docker-entrypoint.sh"
wait $PID