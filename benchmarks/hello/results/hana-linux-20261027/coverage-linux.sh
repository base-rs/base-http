#!/bin/bash
set -euo pipefail
cd /tmp/base-http-hana-epoll-uring-7nxlaica
CARGO_BUILD_JOBS=3 cargo install cargo-llvm-cov --version 0.9.1 --locked --root "$PWD/tools" --target-dir "$PWD/tool-target" > coverage-tool-build.log 2>&1
export PATH="$PWD/tools/bin:$PATH"
export LLVM_COV=/usr/bin/llvm-cov LLVM_PROFDATA=/usr/bin/llvm-profdata
export CARGO_TARGET_DIR="$PWD/target-coverage" CARGO_BUILD_JOBS=3 RUST_TEST_THREADS=2
cargo llvm-cov --release --locked --json --output-path "$PWD/results/linux-coverage.json" --manifest-path base-scheduler/Cargo.toml -p base-scheduler-selector -p base-scheduler-iouring -p base-scheduler-native > coverage-linux.log 2>&1
