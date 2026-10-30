# usage: cmp.sh ROUNDS IMPL BIN...  — interleaved, prints per-binary lines
R=$1; IMPL=$2; shift 2
for r in $(seq $R); do for b in "$@"; do
  echo -n "$(basename $(dirname $(dirname $b))) "; timeout 30 bash "$(dirname "$0")/cperf.sh" $b $IMPL 32 instructions:u,cycles:u,L1-dcache-load-misses:u,task-clock | cut -c1-140
done; done
