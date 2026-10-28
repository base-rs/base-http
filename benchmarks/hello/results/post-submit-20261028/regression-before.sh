set -euo pipefail
cd /tmp/base-http-post-submit-20261028
mkdir scheduler-before-test
tar -xzf scheduler.tar.gz -C scheduler-before-test
cd scheduler-before-test
set +e
cargo test --offline --locked -p base-scheduler-iouring nonblocking_submission_publishes_immediate_completions_before_returning -- --nocapture > ../regression-before.log 2>&1
result=$?
set -e
printf '%s\n' "$result" > ../regression-before-status.txt
cat ../regression-before.log
test "$result" -eq 101
