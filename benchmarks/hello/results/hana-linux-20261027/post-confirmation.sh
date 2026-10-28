#!/bin/bash
set -euo pipefail
cd /tmp/base-http-hana-epoll-uring-7nxlaica
taskset -c 1 python3 profile.py --binary baseline --backends base-iouring --kind syscalls --concurrency 1 --output results/accept-before > accept-before.log 2>&1
taskset -c 1 python3 profile.py --binary final --backends base-iouring --kind syscalls --concurrency 1 --output results/accept-after > accept-after.log 2>&1
taskset -c 1 python3 profile.py --binary final --backends base-native-inq base-iouring-inq --kind syscalls --concurrency 64 --output results/hints-counters-after > hints-counters-after.log 2>&1
taskset -c 1 python3 profile.py --binary final --backends base-iouring --kind perf --concurrency 1 --output results/profile-one-after > profile-one-after.log 2>&1
taskset -c 1 python3 profile.py --binary final --backends base-native-inq base-iouring-inq --kind perf --concurrency 64 --output results/profile-hints-after > profile-hints-after.log 2>&1
