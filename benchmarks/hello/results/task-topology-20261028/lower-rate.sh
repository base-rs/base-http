set -euo pipefail
cd /tmp/base-http-task-topology-20261028
python3 fixed-run.py --variants owned-supervisor:binaries/topology:base-iouring-owned-raw owned-tasks:binaries/topology:base-iouring-owned-connections-raw borrowed-supervisor:binaries/topology:base-iouring-raw borrowed-tasks:binaries/topology:base-iouring-connections-raw tokio-supervisor:binaries/topology:tokio-task-raw tokio-tasks:binaries/topology:tokio-connections-raw --client binaries/client --output fixed-200k --rates 200000 --repetitions 5 > fixed-200k.log 2>&1
sha256sum binaries/* wrk/wrk > final-binaries.sha256
