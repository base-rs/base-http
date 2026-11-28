#!/bin/bash
# Regenerate the syscall and TCP segment summaries from their raw records.
cd ~/client-gap/out
if [ "$1" = strace ]; then
  cd ${2:-strace}
  for f in *.json; do
    L=${f%.json}
    N=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['server']['requests'])" $f)
    for side in client server; do
      echo "== $L $side, calls per server-counted request (N=$N)"
      awk -v n=$N 'NR>2 && $1 ~ /^[0-9.]+$/ { calls=$4; name=$NF; if ($5 ~ /^[0-9]+$/) {errors=$5} else {errors=0}; if (calls/n > 0.001) printf "  %-16s %8.4f calls %8.4f errors\n", name, calls/n, errors/n }' $L.$side.strace
    done
  done
else
  python3 - nstat <<'EOF'
import sys, glob, json, os
out = sys.argv[1]
keys = ["TcpInSegs", "TcpOutSegs", "TcpExtDelayedACKs", "TcpExtTCPPureAcks", "TcpExtTCPOrigDataSent", "IpExtInOctets"]
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
fi
