#!/usr/bin/env bash
set -euo pipefail
cd /tmp/base-http-ring-overhead-repeat-20261028
sha256sum --check expected-binaries.sha256 > binary-verification.log
date -Ins > started.txt
uptime > initial-load.txt
mpstat -P ALL 1 3 > initial-cpu.txt
ss -s > initial-sockets.txt
printf "Phase 1 of 4: concurrent echo\n"
taskset -c 1 python3 probe/run.py --binary /tmp/base-http-uring-readiness-20261027/binaries/after-final --candidate /tmp/base-http-ring-overhead-20261027/deferred-final-echo --minimal /tmp/base-http-ring-overhead-20261027/minimal-final --baseline /tmp/base-http-uring-readiness-20261027/binaries/before --output confirmation --repetitions 5 --warmup 1.0 --seconds 4.0 --connections 64 --bytes 64 4096 --backends mini-epoll mini-uring mini-uring-defer native-poll iouring-poll iouring-deferred tokio iouring iouring-owned-deferred --client-workers 4 --client-cpus 2 3 6 7 > confirmation.log 2>&1
printf "Phase 2 of 4: single-connection echo\n"
taskset -c 1 python3 probe/run.py --binary /tmp/base-http-uring-readiness-20261027/binaries/after-final --candidate /tmp/base-http-ring-overhead-20261027/deferred-final-echo --minimal /tmp/base-http-ring-overhead-20261027/minimal-final --baseline /tmp/base-http-uring-readiness-20261027/binaries/before --output single --repetitions 3 --warmup 1.0 --seconds 4.0 --connections 1 --bytes 64 4096 --backends iouring-poll iouring-deferred native-poll tokio iouring iouring-owned-deferred --client-workers 4 --client-cpus 2 3 6 7 > single.log 2>&1
printf "Phase 3 of 4: full HTTP\n"
taskset -c 1 python3 http-run.py --variants before:http-before:base-iouring after:http-after:base-iouring epoll:http-before:base-native hyper:http-before:hyper-tokio --output http --concurrency 64 --repetitions 5 --duration 4 > http.log 2>&1
printf "Phase 4 of 4: raw HTTP\n"
taskset -c 1 python3 http-run.py --variants before:http-before:base-iouring-raw after:http-after:base-iouring-raw epoll:http-before:base-native-raw tokio:http-before:tokio-task-raw --output http-raw --concurrency 64 --repetitions 5 --duration 4 > http-raw.log 2>&1
python3 probe/summarize.py . --http-cohorts http http-raw > validation.log
sha256sum --check expected-binaries.sha256 > final-binary-verification.log
uptime > final-load.txt
ss -s > final-sockets.txt
date -Ins > finished.txt
printf "Repeat complete\n"
