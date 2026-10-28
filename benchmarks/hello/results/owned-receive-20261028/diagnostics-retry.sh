set -euo pipefail
cd /tmp/base-http-owned-receive-20261028
mv trace-borrowed trace-initial-worker-name
mv trace-borrowed.log trace-initial-worker-name.log
python3 diagnose.py --backend base-iouring-raw --output trace-borrowed > trace-borrowed.log 2>&1
python3 diagnose.py --backend base-iouring-owned-raw --output trace-owned > trace-owned.log 2>&1
python3 diagnose.py --kind profile --backend base-iouring-raw --output profile-borrowed > profile-borrowed.log 2>&1
python3 diagnose.py --kind profile --backend base-iouring-owned-raw --output profile-owned > profile-owned.log 2>&1
sha256sum --check expected-binaries.sha256 > diagnostics-binary-verification.log
date -Ins > diagnostics-finished.txt
