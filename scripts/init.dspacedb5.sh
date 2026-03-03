#!/bin/bash
set -euo pipefail

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

wait_for_ready 180

run_with_retry "createuser dspace" createuser --username=postgres dspace

echo "Importing clarin-dspace"
run_with_retry "createdb clarin-dspace" createdb --username=postgres --owner=dspace --encoding=UNICODE clarin-dspace
run_with_retry "psql import clarin-dspace" bash -lc "psql -U postgres clarin-dspace < ../dump/clarin-dspace.sql > /dev/null 2>&1"
run_with_retry "psql import clarin-dspace log" bash -lc "psql -U postgres clarin-dspace < ../dump/clarin-dspace.sql > ./__clarin-dspace.log 2>&1"

echo "Importing clarin-utilities"
run_with_retry "createdb clarin-utilities" createdb --username=postgres --encoding=UNICODE clarin-utilities
run_with_retry "psql import clarin-utilities" bash -lc "psql -U postgres clarin-utilities < ../dump/clarin-utilities.sql > /dev/null 2>&1"
run_with_retry "psql import clarin-utilities log" bash -lc "psql -U postgres clarin-utilities < ../dump/clarin-utilities.sql > ./__clarin-utilities.log 2>&1"

echo "Done, starting psql"

# psql -U postgres
echo "Waiting for PID:$PID /usr/local/bin/docker-entrypoint.sh"
wait $PID