set -euo pipefail
cd /tmp/base-http-receive-lifecycle-20261028
cat /proc/loadavg > timing-initial-load.txt
ps -eo pid,pcpu,args --sort=-pcpu > timing-initial-processes.txt
python3 http-run.py --variants owned:control:base-iouring-owned-connections-raw poll-first:poll-first:base-iouring-owned-connections-raw tokio:control:tokio-connections-raw --output unpaced --repetitions 5 --duration 4 --concurrency 1 64 > unpaced.log 2>&1
python3 fixed-run.py --variants owned:binaries/control:base-iouring-owned-connections-raw poll-first:binaries/poll-first:base-iouring-owned-connections-raw tokio:binaries/control:tokio-connections-raw --client binaries/client --output fixed --rates 200000 240000 --repetitions 5 --warmup 2 --seconds 6 > fixed.log 2>&1
sha256sum binaries/* wrk/wrk > final-binaries.sha256
