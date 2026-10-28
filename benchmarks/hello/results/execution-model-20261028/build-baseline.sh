set -euo pipefail
cd /tmp/base-http-execution-model-20261028
mkdir -p "$HOME/.cache/base-http-execution-model-20261028"
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > build-baseline.log 2>&1
cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" binaries/baseline
sha256sum binaries/* wrk/wrk > baseline-binaries.sha256
