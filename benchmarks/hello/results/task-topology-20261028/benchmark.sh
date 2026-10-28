set -euo pipefail
cd /tmp/base-http-task-topology-20261028
cat /proc/loadavg > initial-load.txt
ps -eo pid,pcpu,args --sort=-pcpu > initial-processes.txt
python3 http-run.py --variants owned-supervisor:topology:base-iouring-owned-raw owned-tasks:topology:base-iouring-owned-connections-raw borrowed-supervisor:topology:base-iouring-raw borrowed-tasks:topology:base-iouring-connections-raw tokio-supervisor:topology:tokio-task-raw tokio-tasks:topology:tokio-connections-raw --output unpaced --repetitions 5 --duration 4 --concurrency 1 64 > unpaced.log 2>&1
python3 fixed-run.py --variants owned-supervisor:binaries/topology:base-iouring-owned-raw owned-tasks:binaries/topology:base-iouring-owned-connections-raw borrowed-supervisor:binaries/topology:base-iouring-raw borrowed-tasks:binaries/topology:base-iouring-connections-raw tokio-supervisor:binaries/topology:tokio-task-raw tokio-tasks:binaries/topology:tokio-connections-raw --client binaries/client --output fixed-240k --rates 240000 --repetitions 5 > fixed-240k.log 2>&1
sha256sum binaries/* wrk/wrk > final-binaries.sha256
