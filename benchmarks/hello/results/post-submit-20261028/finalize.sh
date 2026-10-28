set -euo pipefail
cd /tmp/base-http-post-submit-20261028
sha256sum --check binaries.sha256 > binary-verification.log
bash coverage.sh
cargo clippy --offline --locked --manifest-path scheduler/Cargo.toml -p base-scheduler-iouring --all-targets --all-features -- -D warnings > clippy.log 2>&1
mkdir -p retained-binaries
gzip -c binaries/candidate > retained-binaries/candidate.gz
tar --exclude=target --exclude=.git --exclude=.agents -czf experiment-source.tar.gz scheduler http fixed-run.py http-run.py diagnostic-before.rs diagnostic-after.rs diagnostic-run.py diagnostics.sh build.sh benchmark.sh coverage.sh
sha256sum experiment-source.tar.gz retained-binaries/candidate.gz > archive-sha256.txt
cat /proc/loadavg > final-load.txt
