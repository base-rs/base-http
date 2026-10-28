set -euo pipefail
cd /tmp/base-http-execution-model-20261028
ps -eo pid,pcpu,comm --sort=-pcpu | head -15 > initial-processes.txt
cat /proc/loadavg > initial-load.txt
python3 fixed-run.py --variants before:binaries/baseline:base-iouring-owned-raw after:binaries/candidate:base-iouring-owned-raw borrowed:binaries/baseline:base-iouring-raw tokio:binaries/baseline:tokio-task-raw --client binaries/client --output fixed-240k --rates 240000 --repetitions 5 > fixed-240k.log 2>&1
python3 http-run.py --variants before:baseline:base-iouring-owned-raw after:candidate:base-iouring-owned-raw borrowed:baseline:base-iouring-raw tokio:baseline:tokio-task-raw --output unpaced --repetitions 5 --duration 4 --concurrency 1 64 > unpaced.log 2>&1
sha256sum binaries/* wrk/wrk > final-binaries.sha256
