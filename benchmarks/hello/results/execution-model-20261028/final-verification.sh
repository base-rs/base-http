set -euo pipefail
cd /tmp/base-http-execution-model-20261028
cp coverage-final.log coverage-candidate.log
cp clippy.log clippy-candidate.log
python3 diagnostic-repeat.py > diagnostic-repeat-summary.log
bash verification.sh
