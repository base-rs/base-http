set -euo pipefail
cd /tmp/base-http-owned-receive-20261028
sha256sum --check expected-binaries.sha256 > binary-verification.log
date -Ins > started.txt
uptime > initial-load.txt
mpstat -P ALL 1 3 > initial-cpu.txt
ps -eo pid,pcpu,comm,args --sort=-pcpu > initial-processes.txt
ss -s > initial-sockets.txt
taskset -c 1 python3 http-run.py --variants borrowed:candidate:base-iouring-raw owned:candidate:base-iouring-owned-raw frozen:frozen:base-iouring-raw tokio:candidate:tokio-task-raw --output comparison --concurrency 1 16 64 --repetitions 5 --duration 4 > comparison.log 2>&1
sha256sum --check expected-binaries.sha256 > final-binary-verification.log
uptime > final-load.txt
ss -s > final-sockets.txt
date -Ins > finished.txt
