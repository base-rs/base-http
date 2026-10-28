#!/bin/bash
# Released under the MIT License.
# Copyright, 2026, by Samuel Williams.

# Measure user-space instructions, cycles and CPU time per request on Linux.
# The server is pinned to SERVER_CPUS and wrk to CLIENT_CPUS. Each repeat warms
# up for one second, then counts the server process during a three second run.
#
# Usage: instructions.sh BINARY BACKEND CONNECTIONS [REPEATS]
set -euo pipefail

binary=$1
backend=$2
connections=$3
repeats=${4:-3}
wrk=${WRK:-wrk}
server_cpus=${SERVER_CPUS:-0}
client_cpus=${CLIENT_CPUS:-1-3,5-7}
threads=$((connections < 4 ? connections : 4))

directory=$(mktemp -d)
trap 'rm -rf "$directory"' EXIT

for _ in $(seq "$repeats"); do
	rm -f "$directory/address"
	taskset -c "$server_cpus" "$binary" "$backend" >"$directory/address" 2>/dev/null &
	server=$!
	while [ ! -s "$directory/address" ]; do sleep 0.05; done
	url="http://$(head -1 "$directory/address")/"

	taskset -c "$client_cpus" "$wrk" -t"$threads" -c"$connections" -d1s "$url" >/dev/null
	perf stat -x, -e instructions:u,cycles:u,task-clock -p "$server" -o "$directory/stat" -- sleep 3.2 &
	counter=$!
	requests=$(taskset -c "$client_cpus" "$wrk" -t"$threads" -c"$connections" -d3s "$url" | awk '/requests in/ {print $1}')
	wait "$counter"
	kill "$server"
	wait "$server" 2>/dev/null || true

	awk -F, -v requests="$requests" -v backend="$backend" -v connections="$connections" '
		/instructions/ {instructions = $1}
		/cycles/ {cycles = $1}
		/task-clock/ {clock = $1}
		END {
			printf "{\"backend\":\"%s\",\"connections\":%d,\"requests_per_second\":%.0f,\"instructions_per_request\":%.0f,\"cycles_per_request\":%.0f,\"cpu_microseconds_per_request\":%.3f}\n",
				backend, connections, requests / 3, instructions / requests, cycles / requests, clock * 1000 / requests
		}' "$directory/stat"
done
