set -euo pipefail
cd /tmp/base-http-execution-model-20261028/scheduler
export CARGO_TARGET_DIR="$HOME/.cache/base-http-execution-model-20261028/scheduler-target"
set +e
cargo test --offline --locked -p base-scheduler-iouring nonblocking_owned_reads_attempt_submission_without_an_extra_turn > ../regression-before.log 2>&1
status=$?
set -e
test "$status" -eq 101
grep -q 'assertion failed: FAILURES.with' ../regression-before.log
