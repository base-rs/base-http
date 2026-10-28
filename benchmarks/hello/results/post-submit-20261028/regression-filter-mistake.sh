set -euo pipefail
cd /tmp/base-http-post-submit-20261028
tar -xzf scheduler.tar.gz -C scheduler
cd scheduler
set +e
cargo test --offline --locked -p base-scheduler-iouring nonblocking_submission_publishes_immediate_completions_before_returning -- --exact --nocapture > ../regression-before.log 2>&1
result=$?
set -e
printf '%s\n' "$result" > ../regression-before-status.txt
cat ../regression-before.log
