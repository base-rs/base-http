set -euo pipefail
cd /tmp/base-http-execution-model-20261028
mkdir -p "$HOME/.cache/base-http-execution-model-20261028"
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > build-candidate.log 2>&1
cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" binaries/candidate
sha256sum binaries/* wrk/wrk > candidate-binaries.sha256
export CARGO_TARGET_DIR="$HOME/.cache/base-http-execution-model-20261028/scheduler-target"
cargo test --offline --locked --manifest-path scheduler/Cargo.toml -p base-scheduler-iouring > regression-after.log 2>&1
python3 http/base-http/benchmarks/hello/diagnostics/validate_raw.py --binary binaries/candidate --backends base-iouring-raw base-iouring-owned-raw base-native-raw tokio-task-raw --output validation.json > validation.log
