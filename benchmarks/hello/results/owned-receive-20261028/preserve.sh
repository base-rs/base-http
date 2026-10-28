set -euo pipefail
cd /tmp/base-http-owned-receive-20261028
cp /tmp/base-http-ring-overhead-20261027/retained/counts.c counts.c
cp /tmp/base-http-ring-overhead-20261027/retained/counts.so counts.so
python3 - <<'REMOTE'
from pathlib import Path
import difflib,json,hashlib
root=Path('/tmp/base-http-owned-receive-20261028')
patch=[]
for name in ('src/main.rs','src/raw.rs'):
    before=(root/'source/before'/name).read_text().splitlines(True)
    after=(root/'source/http/base-http/benchmarks/hello'/name).read_text().splitlines(True)
    patch.extend(difflib.unified_diff(before,after,fromfile='a/'+name,tofile='b/'+name))
(root/'benchmark.patch').write_text(''.join(patch))
source=root/'source'
files={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file()}
(root/'source-sha256.json').write_text(json.dumps(files,indent=2)+'\n')
REMOTE
