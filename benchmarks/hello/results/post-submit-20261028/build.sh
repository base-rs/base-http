set -euo pipefail
cd /tmp/base-http-post-submit-20261028
python3 - <<'SCRIPT'
from pathlib import Path
p=Path('http/base-http/benchmarks/hello/Cargo.toml')
s=p.read_text().replace('/tmp/base-http-ring-overhead-20261027/scheduler-deferred', '/tmp/base-http-post-submit-20261028/scheduler')
p.write_text(s)
SCRIPT
cargo build --offline --locked --release --manifest-path http/base-http/benchmarks/hello/Cargo.toml --target-dir /tmp/base-http-ring-overhead-20261027/http-target > build.log 2>&1
cp /tmp/base-http-ring-overhead-20261027/http-target/release/base-http-hello-benchmark binaries/candidate
sha256sum binaries/* wrk/wrk > binaries.sha256
python3 http/base-http/benchmarks/hello/diagnostics/validate_raw.py --binary binaries/candidate --backends base-iouring-raw base-iouring-owned-raw base-native-raw tokio-task-raw --output validation.json > validation.log
