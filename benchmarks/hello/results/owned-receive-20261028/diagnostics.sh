set -euo pipefail
cd /tmp/base-http-owned-receive-20261028
python3 - <<'REMOTE'
from pathlib import Path
import time
until=time.monotonic()+600
while not Path('finished.txt').exists():
    assert time.monotonic()<until, 'throughput comparison did not finish'
    time.sleep(1)
REMOTE
python3 diagnose.py --backend base-iouring-raw --output trace-borrowed > trace-borrowed.log 2>&1
python3 diagnose.py --backend base-iouring-owned-raw --output trace-owned > trace-owned.log 2>&1
python3 diagnose.py --kind profile --backend base-iouring-raw --output profile-borrowed > profile-borrowed.log 2>&1
python3 diagnose.py --kind profile --backend base-iouring-owned-raw --output profile-owned > profile-owned.log 2>&1
sha256sum --check expected-binaries.sha256 > diagnostics-binary-verification.log
date -Ins > diagnostics-finished.txt
