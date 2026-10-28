set -euo pipefail
cd /tmp/base-http-execution-model-20261028
cat /proc/loadavg > final-load.txt
sha256sum binaries/* wrk/wrk > final-binaries.sha256
python3 - <<'SCRIPT'
import gzip,hashlib,json,shutil,tarfile
from pathlib import Path
r=Path.cwd()
b=r/'retained-binaries';b.mkdir(exist_ok=True)
for name in ('baseline','candidate','client'):
 with (r/'binaries'/name).open('rb') as src,(b/(name+'.gz')).open('wb') as out:
  with gzip.GzipFile(filename='',mode='wb',fileobj=out,mtime=0) as dst:shutil.copyfileobj(src,dst)
with tarfile.open('http-source.tar.gz','w:gz') as archive:archive.add('http')
with tarfile.open('results.tar.gz','w:gz') as archive:
 for path in sorted(r.iterdir()):
  if path.name in ('scheduler','http','binaries','wrk','results.tar.gz'):continue
  archive.add(path,arcname=path.name)
SCRIPT
