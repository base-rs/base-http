#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
taskset -c 1 python3 benchmark.py --variants old-epoll:baseline:base-native new-epoll:updated:base-native old-iouring:baseline:base-iouring new-iouring:updated:base-iouring hyper:updated:hyper-tokio --output results/confirmation --repetitions 5 --duration 6 --concurrency 1 16 64
