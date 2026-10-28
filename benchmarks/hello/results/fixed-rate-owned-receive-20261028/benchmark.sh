set -euo pipefail
cd /tmp/base-http-fixed-rate-20261028
sha256sum --check expected-binaries.sha256 > binary-verification.log
mpstat -P ALL 1 3 > comparison-initial-cpu.txt
ps -eo pid,pcpu,comm,args --sort=-pcpu > comparison-initial-processes.txt
uptime > comparison-initial-load.txt
ss -s > initial-sockets.txt
date -Ins > started.txt
taskset -c 1 python3 run.py --server binaries/server --client binaries/client --output comparison --rates 200000 220000 240000 --repetitions 5 --warmup 2 --seconds 8 > comparison.log 2>&1
sha256sum --check expected-binaries.sha256 > final-binary-verification.log
uptime > final-load.txt
ss -s > final-sockets.txt
date -Ins > finished.txt
