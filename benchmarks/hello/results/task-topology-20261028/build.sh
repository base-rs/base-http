set -euo pipefail
cd /tmp/base-http-task-topology-20261028
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir "$HOME/.cache/base-http-execution-model-20261028/http-target" > build.log 2>&1
cp "$HOME/.cache/base-http-execution-model-20261028/http-target/release/base-http-hello-benchmark" binaries/topology
sha256sum binaries/* wrk/wrk > binaries.sha256
python3 validate_raw.py --binary binaries/topology --output smoke.json --backends base-iouring-owned-raw base-iouring-owned-connections-raw base-iouring-raw base-iouring-connections-raw tokio-task-raw tokio-connections-raw
