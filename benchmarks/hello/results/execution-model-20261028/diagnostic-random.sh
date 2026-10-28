set -euo pipefail
cd /tmp/base-http-execution-model-20261028
cp scheduler/crates/iouring/src/io_uring_selector.rs final-production.rs
cp scheduler/crates/iouring/src/io_uring_selector/socket_read.rs final-production-socket-read.rs
trap 'cp final-production.rs scheduler/crates/iouring/src/io_uring_selector.rs; cp final-production-socket-read.rs scheduler/crates/iouring/src/io_uring_selector/socket_read.rs' EXIT
cp diagnostic-random-baseline.rs scheduler/crates/iouring/src/io_uring_selector.rs
cp diagnostic-socket-read.rs scheduler/crates/iouring/src/io_uring_selector/socket_read.rs
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > diagnostic-random-build.log 2>&1
cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" binaries/diagnostic-random-baseline
python3 diagnostic-random-run.py > diagnostic-random-summary.log
sha256sum binaries/* wrk/wrk > final-binaries.sha256
