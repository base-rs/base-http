set -euo pipefail
cd /tmp/base-http-owned-receive-20261028
cargo build --offline --locked --release --manifest-path source/http/base-http/benchmarks/hello/Cargo.toml --target-dir /tmp/base-http-ring-overhead-20261027/http-target > build.log 2>&1
cp /tmp/base-http-ring-overhead-20261027/http-target/release/base-http-hello-benchmark binaries/candidate
sha256sum binaries/* wrk/wrk > expected-binaries.sha256
python3 source/http/base-http/benchmarks/hello/diagnostics/validate_raw.py --binary binaries/candidate --backends base-iouring-raw base-iouring-owned-raw base-native-raw tokio-task-raw --output validation.json > validation.log
