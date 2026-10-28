set -euo pipefail
cd /tmp/base-http-execution-model-20261028
cp scheduler/crates/iouring/src/io_uring_selector.rs production.rs
cp scheduler/crates/iouring/src/io_uring_selector/socket_read.rs production-socket-read.rs
trap 'cp production.rs scheduler/crates/iouring/src/io_uring_selector.rs; cp production-socket-read.rs scheduler/crates/iouring/src/io_uring_selector/socket_read.rs' EXIT
cp diagnostic-socket-read.rs scheduler/crates/iouring/src/io_uring_selector/socket_read.rs
for variant in baseline candidate; do
  cp "diagnostic-$variant.rs" scheduler/crates/iouring/src/io_uring_selector.rs
  cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > "diagnostic-build-$variant.log" 2>&1
  cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" "binaries/diagnostic-$variant"
done
python3 diagnostic-run.py > diagnostic-summary.log
