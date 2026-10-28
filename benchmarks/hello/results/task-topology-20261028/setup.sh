set -euo pipefail
root=/tmp/base-http-task-topology-20261028
mkdir "$root"
mkdir "$root/scheduler" "$root/binaries"
cp -a /tmp/base-http-execution-model-20261028/http "$root/http"
cp -a /tmp/base-http-execution-model-20261028/wrk "$root/wrk"
cp /tmp/base-http-execution-model-20261028/binaries/client "$root/binaries/client"
python3 - <<'SCRIPT'
from pathlib import Path
p=Path('/tmp/base-http-task-topology-20261028/http/base-http/benchmarks/hello/Cargo.toml')
p.write_text(p.read_text().replace('/tmp/base-http-execution-model-20261028/scheduler','/tmp/base-http-task-topology-20261028/scheduler'))
SCRIPT
