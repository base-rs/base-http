set -euo pipefail
cd /tmp/base-http-task-topology-20261028
mkdir -p production-sources
cp http/base-http/benchmarks/hello/src/raw.rs production-sources/raw.rs
cp http/base-http/benchmarks/hello/src/main.rs production-sources/main.rs
cp scheduler/crates/iouring/src/io_uring_selector.rs production-sources/io_uring_selector.rs
cp scheduler/crates/iouring/src/io_uring_selector/socket_read.rs production-sources/socket_read.rs
cp scheduler/crates/iouring/src/io_uring_worker.rs production-sources/io_uring_worker.rs
cp scheduler/crates/iouring/src/lib.rs production-sources/iouring-lib.rs
python3 make-diagnostics.py .
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > diagnostic-build.log 2>&1
cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" binaries/diagnostic
sha256sum binaries/* wrk/wrk > diagnostic-binaries.sha256
python3 diagnostic-run.py > diagnostic-run.log 2>&1
