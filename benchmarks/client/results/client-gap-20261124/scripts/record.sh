#!/bin/bash
# perf record the client (frame-pointer build) for each implementation.
set -e
cd ~/client-gap
B=${B:-main/benchmarks/client/target/fp/release/base-http-client-benchmark}
OUT=${OUT:-out/record}
IMPLS=${IMPLS:-"base-open hyper"}
EV=${EV:-cycles:u}
EXTRA=${EXTRA:-}
mkdir -p $OUT
for i in $IMPLS; do
  L=$i$(echo $EXTRA | tr -d ' -')
  python3 diag.py $B $i --out $OUT --seconds 4 --label $L $EXTRA \
    --client-wrap "perf record -q -e $EV -c 20011 --call-graph fp --user-callchains -o $OUT/$L.data"
done
