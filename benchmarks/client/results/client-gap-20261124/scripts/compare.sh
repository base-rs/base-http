#!/bin/bash
# Interleaved perf stat comparison of variants given as NAME=CHECKOUT:IMPLEMENTATION.
# The server always runs from main's binary.
set -e
cd ~/client-gap
SERVER=main/benchmarks/client/target/release/base-http-client-benchmark
OUT=${OUT:?}
ROUNDS=${ROUNDS:-3}
SECONDS_=${SECONDS_:-3}
EXTRA=${EXTRA:-}
EV=${EV:-task-clock,instructions:u,cycles:u,L1-dcache-load-misses:u,L1-icache-load-misses:u,branch-misses:u}
mkdir -p $OUT
for r in $(seq $ROUNDS); do
  for v in "$@"; do
    name=${v%%=*}; rest=${v#*=}; checkout=${rest%%:*}; rest=${rest#*:}; impl=${rest%%:*}
    envs=(); if [[ $rest == *:* ]]; then IFS=, read -ra kv <<< "${rest#*:}"; for e in "${kv[@]}"; do envs+=(--env "$e"); done; fi
    L=$name-r$r
    python3 diag.py $SERVER $impl --client-binary $checkout/benchmarks/client/target/release/base-http-client-benchmark \
      --out $OUT --seconds $SECONDS_ --label $L $EXTRA "${envs[@]}" \
      --client-wrap "perf stat -x , -o $OUT/$L.client.perf -e $EV" \
      --server-wrap "perf stat -x , -o $OUT/$L.server.perf -e task-clock,instructions:u,cycles:u"
  done
done
python3 perfsum.py $OUT
