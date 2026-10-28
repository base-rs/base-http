set -euo pipefail
cd /tmp/base-http-execution-model-20261028/scheduler
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/.cache/base-http-execution-model-20261028/scheduler-target"
export LLVM_COV=/usr/bin/llvm-cov LLVM_PROFDATA=/usr/bin/llvm-profdata
cargo build --offline --locked -p base-scheduler-bake > ../bake-build.log 2>&1
cargo clippy --offline --locked -p base-scheduler-iouring --all-targets --all-features -- -D warnings > ../clippy.log 2>&1
mkdir -p ../coverage-tools
cat > ../coverage-tools/cargo <<'WRAPPER'
#!/bin/bash
if [[ "${1:-}" == llvm-cov ]]; then
  exec /usr/bin/cargo "$@" --ignore-filename-regex '^/usr/src/debug/rust/'
fi
exec /usr/bin/cargo "$@"
WRAPPER
chmod +x ../coverage-tools/cargo
export PATH="/tmp/base-http-execution-model-20261028/coverage-tools:$PATH"
export CARGO="/tmp/base-http-execution-model-20261028/coverage-tools/cargo"
"$CARGO_TARGET_DIR/debug/base-scheduler-bake" test:coverage --package base-scheduler-iouring --all-targets true --all-features true > ../coverage-final.log 2>&1
