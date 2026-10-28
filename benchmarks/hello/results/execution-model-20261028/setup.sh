set -euo pipefail
root=/tmp/base-http-execution-model-20261028
mkdir "$root"
mkdir "$root/scheduler" "$root/binaries"
cp -a /tmp/base-http-post-submit-20261028/http "$root/http"
cp -a /tmp/base-http-post-submit-20261028/wrk "$root/wrk"
cp /tmp/base-http-post-submit-20261028/binaries/client "$root/binaries/client"
cp /tmp/base-http-post-submit-20261028/fixed-run.py "$root/fixed-run.py"
cp /tmp/base-http-post-submit-20261028/http-run.py "$root/http-run.py"
cd "$root"
cat /proc/loadavg > initial-load.txt
ps -eo pid,pcpu,args --sort=-pcpu > initial-processes.txt
python3 - <<'SCRIPT'
from pathlib import Path
p=Path('http/base-http/benchmarks/hello/Cargo.toml')
p.write_text(p.read_text().replace('/tmp/base-http-post-submit-20261028/scheduler','/tmp/base-http-execution-model-20261028/scheduler'))
SCRIPT
