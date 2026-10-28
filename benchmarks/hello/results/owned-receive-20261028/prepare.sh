set -euo pipefail
root=/tmp/base-http-owned-receive-20261028
mkdir -p "$root/source/http/base-http/benchmarks/hello/diagnostics"
python3 - <<'REMOTE'
from pathlib import Path
import json,hashlib
root=Path('/tmp/base-http-owned-receive-20261028')
p=root/'http-run.py'
s=p.read_text().replace("sys.path.insert(0, '/tmp/base-http-uring-readiness-20261027/http-after/base-http/benchmarks/hello')", "sys.path.insert(0, str(ROOT / 'source/http/base-http/benchmarks/hello'))")
s=s.replace("'server_cpu_percent_one_core']", "'server_cpu_percent_one_core','server_rss_kib_after']")
p.write_text(s)
expected=json.loads(Path('/tmp/base-http-ring-overhead-20261027/expected-scheduler-sha256.json').read_text())['files']
scheduler=Path('/tmp/base-http-ring-overhead-20261027/scheduler-deferred')
actual={name:hashlib.sha256((scheduler/name).read_bytes()).hexdigest() for name in expected}
assert actual==expected, [(k,v,actual[k]) for k,v in expected.items() if actual[k]!=v]
(root/'verified-scheduler-sha256.json').write_text(json.dumps(actual,indent=2)+'\n')
print('Verified scheduler source:',len(actual),'files')
REMOTE
