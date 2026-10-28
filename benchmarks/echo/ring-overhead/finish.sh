#!/usr/bin/env bash
set -euo pipefail
export LLVM_COV=/usr/bin/llvm-cov LLVM_PROFDATA=/usr/bin/llvm-profdata
export PATH=/tmp/base-http-hana-epoll-uring-7nxlaica/tools/bin:$PATH
root=/tmp/base-http-ring-overhead-20261027
cd "$root/scheduler-deferred"
printf 'Checking final dependency build and lint\n'
CARGO_TARGET_DIR="$root/candidate-target" cargo build --release --offline -p base-scheduler-echo-benchmark > "$root/dependency-build.log" 2>&1
python3 "$root/probe/compare_elf.py" "$root/deferred-final-echo" "$root/candidate-target/release/base-scheduler-echo-benchmark" --output "$root/dependency-rebuild-sections.json"
CARGO_TARGET_DIR="$root/candidate-target" cargo clippy --offline -p base-scheduler-iouring -p base-scheduler-echo-benchmark --all-targets -- -D warnings > "$root/clippy.log" 2>&1
printf 'Running shared production coverage gate\n'
CARGO_TARGET_DIR="$root/coverage-target" CARGO_NET_OFFLINE=true LLVM_COV_FLAGS='--ignore-filename-regex=(/usr/src/debug/rust|/tests/|/tests.rs|_tests.rs)' /tmp/base-http-uring-readiness-20261027/tool-target/debug/base-scheduler-bake test:coverage --package base-scheduler-iouring > "$root/coverage.log" 2>&1
cd "$root"
printf 'Tracing final and minimal ring policies\n'
client=/tmp/base-http-uring-readiness-20261027/binaries/before
library=/tmp/base-http-layers-20261027/raw-followup-tools/counts.so
python3 probe/diagnose.py --server deferred-final-echo --client "$client" --backend iouring-poll --output trace-deferred --library "$library" > trace-deferred.log 2>&1
python3 probe/diagnose.py --server minimal-final --client "$client" --backend uring --output trace-mini-coop --library "$library" > trace-mini-coop.log 2>&1
python3 probe/diagnose.py --server minimal-final --client "$client" --backend uring-defer --output trace-mini-defer --library "$library" > trace-mini-defer.log 2>&1
python3 probe/diagnose.py --server /tmp/base-http-uring-readiness-20261027/binaries/after-final --client "$client" --backend iouring --output trace-owned --library "$library" > trace-owned.log 2>&1
python3 probe/diagnose.py --server deferred-final-echo --client "$client" --backend iouring --output trace-owned-deferred --library "$library" > trace-owned-deferred.log 2>&1
for name in trace-base-64 trace-base-4096 trace-deferred trace-mini-coop trace-mini-defer trace-owned trace-owned-deferred; do
 taskset -c 5 python3 probe/summarize_trace.py "$name" > "$name/summary.log"
done
printf 'Profiling candidate and owned receive paths\n'
python3 probe/diagnose.py --server deferred-final-echo --client "$client" --backend iouring-poll --output profile-deferred --kind record --seconds 6 > profile-deferred.log 2>&1
python3 probe/diagnose.py --server deferred-final-echo --client "$client" --backend iouring --output profile-owned-deferred --kind record --seconds 6 > profile-owned-deferred.log 2>&1
printf 'Building identical HTTP workload with final scheduler\n'
rsync -a --exclude target /tmp/base-http-uring-readiness-20261027/http-after/ http-deferred/
python3 - <<'PYREMOTE'
from pathlib import Path
p=Path('http-deferred/base-http/benchmarks/hello/Cargo.toml')
s=p.read_text();s=s.replace('/tmp/base-http-uring-readiness-20261027/scheduler','/tmp/base-http-ring-overhead-20261027/scheduler-deferred');p.write_text(s)
PYREMOTE
CARGO_TARGET_DIR="$root/http-target" cargo build --manifest-path http-deferred/base-http/benchmarks/hello/Cargo.toml --release --offline > http-build.log 2>&1
mkdir binaries wrk
ln -s /tmp/base-http-uring-readiness-20261027/binaries/http-after binaries/http-before
cp http-target/release/base-http-hello-benchmark binaries/http-after
ln -s /tmp/base-http-uring-readiness-20261027/wrk/wrk wrk/wrk
printf 'Measuring raw and full HTTP confirmation\n'
taskset -c 1 python3 probe/../http-run.py --variants before:http-before:base-iouring after:http-after:base-iouring epoll:http-before:base-native hyper:http-before:hyper-tokio --output http --concurrency 64 --repetitions 5 --duration 4 > http.log 2>&1
taskset -c 1 python3 probe/../http-run.py --variants before:http-before:base-iouring-raw after:http-after:base-iouring-raw epoll:http-before:base-native-raw tokio:http-before:tokio-task-raw --output http-raw --concurrency 64 --repetitions 5 --duration 4 > http-raw.log 2>&1
printf 'Preparing off-host benchmark binaries\n'
rsync -a --exclude target --exclude .git /tmp/base-http-uring-readiness-20261027/scheduler/ scheduler-network-before/
python3 - <<'PYREMOTE'
from pathlib import Path
for root in ['scheduler-network-before','scheduler-deferred']:
 p=Path(root)/'benchmarks/echo/src/server.rs'
 s=p.read_text();old='TcpListener::bind("127.0.0.1:0")';new='TcpListener::bind(std::env::var("BASE_HTTP_LISTEN").unwrap_or_else(|_| "127.0.0.1:0".into()))';assert old in s;s=s.replace(old,new);p.write_text(s)
PYREMOTE
CARGO_TARGET_DIR="$root/network-target" cargo build --manifest-path scheduler-network-before/Cargo.toml --release --offline -p base-scheduler-echo-benchmark > network-before-build.log 2>&1
cp network-target/release/base-scheduler-echo-benchmark network-before
CARGO_TARGET_DIR="$root/candidate-target" cargo build --manifest-path scheduler-deferred/Cargo.toml --release --offline -p base-scheduler-echo-benchmark > network-after-build.log 2>&1
cp candidate-target/release/base-scheduler-echo-benchmark network-after
mkdir network-usage
printf 'Remote phases complete\n'
