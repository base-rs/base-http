#!/bin/bash
# usage: cperf.sh BINARY IMPL CONCURRENCY [EVENTS] [tls]
B=$1; IMPL=$2; C=$3; EV=${4:-instructions:u,cycles:u}; TLS=${5:+--tls}
ID=/tmp/claude-client-identity
[ -e $ID ] || $B identity --identity $ID >/dev/null
coproc S { exec taskset -c 2,3,6,7 $B server --identity $ID $TLS; }
read -r READY <&${S[0]}; ADDR=${READY#READY }
perf stat -x, -e $EV -o /tmp/cperf.perf taskset -c 1 $B client --implementation $IMPL --address $ADDR --identity $ID --concurrency $C --seconds 4 --warmup 1 $TLS > /tmp/cperf.json
kill $S_PID; wait $S_PID 2>/dev/null
python3 "$(dirname "$0")/cperf.py" "$IMPL" "$C"
