set -euo pipefail
cd /tmp/base-http-post-submit-20261028/scheduler
export PATH="$HOME/.cargo/bin:$PATH"
export LLVM_COV=/usr/bin/llvm-cov LLVM_PROFDATA=/usr/bin/llvm-profdata
# Arch embeds std source mappings at /usr/src/debug/rust; the source package is
# absent. Exclude only these external compiler paths from the crate report.
mkdir -p ../coverage-tools
cat > ../coverage-tools/cargo <<'WRAPPER'
#!/bin/bash
if [[ "${1:-}" == llvm-cov ]]; then
  exec /usr/bin/cargo "$@" --ignore-filename-regex '^/usr/src/debug/rust/'
fi
exec /usr/bin/cargo "$@"
WRAPPER
chmod +x ../coverage-tools/cargo
export PATH="/tmp/base-http-post-submit-20261028/coverage-tools:$PATH"
export CARGO="/tmp/base-http-post-submit-20261028/coverage-tools/cargo"
./target/debug/base-scheduler-bake test:coverage --package base-scheduler-iouring --all-targets true --all-features true > ../coverage-final.log 2>&1
