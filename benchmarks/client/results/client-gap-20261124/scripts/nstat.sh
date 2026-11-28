#!/bin/bash
# TCP segment counters per request (host-wide counters; the host is otherwise quiet on loopback).
set -e
cd ~/client-gap
B=${B:-main/benchmarks/client/target/release/base-http-client-benchmark}
OUT=${OUT:-out/nstat}
IMPLS=${IMPLS:-"base-open hyper"}
ROUNDS=${ROUNDS:-2}
EXTRA=${EXTRA:-}
mkdir -p $OUT
for r in $(seq $ROUNDS); do
  for i in $IMPLS; do
    L=$i-r$r$(echo $EXTRA | tr -d ' -')
    nstat -n
    python3 diag.py $B $i --out $OUT --seconds 3 --label $L $EXTRA
    nstat -z > $OUT/$L.nstat
    nstat -n
  done
done
python3 - $OUT <<'EOF'
import sys, glob, json, os, collections
out = sys.argv[1]
keys = ["TcpInSegs", "TcpOutSegs", "TcpExtDelayedACKs", "TcpExtTCPPureAcks", "TcpExtTCPHPAcks", "TcpExtTCPHPHits",
        "TcpExtTCPBacklogCoalesce", "TcpExtTCPAutoCorking", "TcpExtTCPOrigDataSent", "IpExtInOctets"]
for f in sorted(glob.glob(f"{out}/*.nstat")):
    label = os.path.basename(f)[:-6]
    n = json.load(open(f"{out}/{label}.json"))["server"]["requests"]
    vals = {}
    for line in open(f):
        parts = line.split()
        if len(parts) >= 2 and parts[0] in keys:
            vals[parts[0]] = int(parts[1]) / n
    print(label, " ".join(f"{k}={vals.get(k, 0):.3f}" for k in keys))
EOF
