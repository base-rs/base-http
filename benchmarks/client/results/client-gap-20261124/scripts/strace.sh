#!/bin/bash
# Syscall counts for client and server (diagnostic only; strace perturbs timing).
set -e
cd ~/client-gap
B=${B:-main/benchmarks/client/target/release/base-http-client-benchmark}
OUT=${OUT:-out/strace}
IMPLS=${IMPLS:-"base-open hyper"}
EXTRA=${EXTRA:-}
mkdir -p $OUT
for i in $IMPLS; do
  L=$i$(echo $EXTRA | tr -d ' -')
  python3 diag.py $B $i --out $OUT --seconds 2 --warmup 0.5 --label $L $EXTRA \
    --client-wrap "strace -f -c -o $OUT/$L.client.strace" \
    --server-wrap "strace -f -c -o $OUT/$L.server.strace"
done
for f in $OUT/*.json; do
  L=$(basename $f .json)
  N=$(python3 -c "import json;print(json.load(open('$f'))['server']['requests'])")
  for side in client server; do
    echo "== $L $side per request (N=$N)"
    awk -v n=$N 'NR>2 && $1 ~ /^[0-9.]+$/ { calls=$4; name=$NF; if ($5 ~ /^[0-9]+$/) {errors=$5} else {errors=0}; if (calls/n > 0.001) printf "  %-16s %8.4f calls/req %8.4f errors/req\n", name, calls/n, errors/n }' $OUT/$L.$side.strace
  done
done
