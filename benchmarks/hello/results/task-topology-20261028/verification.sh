set -euo pipefail
cd /tmp/base-http-task-topology-20261028
cp production-sources/io_uring_selector.rs scheduler/crates/iouring/src/io_uring_selector.rs
cp production-sources/socket_read.rs scheduler/crates/iouring/src/io_uring_selector/socket_read.rs
cp production-sources/io_uring_worker.rs scheduler/crates/iouring/src/io_uring_worker.rs
cp production-sources/iouring-lib.rs scheduler/crates/iouring/src/lib.rs
cp final-main.rs http/base-http/benchmarks/hello/src/main.rs
cp final-raw.rs http/base-http/benchmarks/hello/src/raw.rs
cargo clippy --offline --locked --manifest-path http/base-http/benchmarks/hello/Cargo.toml --bin base-http-hello-benchmark --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" -- -D warnings > clippy-linux.log 2>&1
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --bin base-http-hello-benchmark --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > verification-build.log 2>&1
cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" binaries/verification
python3 validate_raw.py --binary binaries/verification --output verification-smoke.json --backends base-iouring-owned-raw base-iouring-owned-connections-raw base-iouring-raw base-iouring-connections-raw tokio-task-raw tokio-connections-raw > verification-smoke.log
sha256sum binaries/* wrk/wrk > verification-binaries.sha256
