set -euo pipefail
cd /tmp/base-http-post-submit-20261028
cp scheduler/crates/iouring/src/io_uring_selector.rs production.rs
trap 'cp production.rs scheduler/crates/iouring/src/io_uring_selector.rs' EXIT
for variant in before after; do
  cp diagnostic-$variant.rs scheduler/crates/iouring/src/io_uring_selector.rs
  cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir /tmp/base-http-ring-overhead-20261027/http-target > diagnostic-build-$variant.log 2>&1
  cp /tmp/base-http-ring-overhead-20261027/http-target/release/base-http-hello-benchmark binaries/diagnostic-$variant
done
