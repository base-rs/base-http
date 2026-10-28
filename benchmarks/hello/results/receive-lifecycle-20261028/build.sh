# Reproduce the two completed builds. Run from this experiment root.
set -euo pipefail
cp socket_read.before.rs candidate/scheduler/crates/iouring/src/io_uring_selector/socket_read.rs
cargo build --offline --locked --release --manifest-path candidate/http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-receive-lifecycle/http-target" > build-baseline.log 2>&1
cp "$HOME/.cache/base-http-receive-lifecycle/http-target/release/base-http-hello-benchmark" binaries/control
patch -p1 --directory=candidate/scheduler < poll-first.patch
cargo build --offline --locked --release --manifest-path candidate/http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-receive-lifecycle/http-target" > build-poll-first.log 2>&1
cp "$HOME/.cache/base-http-receive-lifecycle/http-target/release/base-http-hello-benchmark" binaries/poll-first
sha256sum binaries/* > experiment-binaries.sha256
python3 validate_raw.py --binary binaries/poll-first --output smoke-poll-first.json --backends base-iouring-owned-connections-raw base-iouring-connections-raw tokio-connections-raw
