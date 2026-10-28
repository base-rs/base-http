#!/usr/bin/env python3
"""Freeze identical churn fixtures against preserved baseline and pointer routing."""
from pathlib import Path
import hashlib,json,shutil,subprocess
here=Path(__file__).resolve().parent
scheduler=here.parents[6]/'base-scheduler'
source=scheduler/'crates/selector/src'
paths=[source/'readiness_driver.rs',source/'readiness_driver/tests.rs',source/'readiness_driver/kqueue.rs',source/'readiness_driver/kqueue/tests.rs',source/'readiness_socket_stream_writer/tests.rs']
original={p:p.read_text() for p in paths}
root=here.parent
fixture=(root/'churn/fixture.rs').read_text()
baseline={paths[0]:(root/'cow-routing/before-readiness_driver.rs').read_text(),paths[1]:(root/'cow-routing/before-tests.rs').read_text()+fixture,paths[2]:(root/'pointer-routing/before-readiness_driver-kqueue.rs').read_text(),paths[3]:original[paths[3]].split('\n#[test]\nfn receipt_errors_distinguish_closed_additions')[0],paths[4]:original[paths[4]].split('\n#[cfg(target_os = \"macos\")]\n#[test]\nfn blocked_writes_report_failed_registration')[0]}
candidate=dict(original);candidate[paths[1]]+=fixture
metadata={}
try:
 for name,revision in [('before',baseline),('pointer',candidate)]:
  for path,value in revision.items():path.write_text(value)
  r=subprocess.run(['cargo','test','--release','--locked','-p','base-scheduler-selector','--lib','--no-run','--message-format=json'],cwd=scheduler,capture_output=True,text=True)
  (here/f'{name}-build.log').write_text(r.stderr);r.check_returncode()
  rows=[json.loads(l) for l in r.stdout.splitlines() if l.startswith('{')]
  exe=next(Path(row['executable']) for row in rows if row.get('reason')=='compiler-artifact' and row.get('executable') and row['target']['name']=='base_scheduler_selector')
  target=here/f'{name}-tests';shutil.copyfile(exe,target);target.chmod(0o755)
  metadata[name]={'binary_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'sources':{str(p.relative_to(source)):hashlib.sha256(v.encode()).hexdigest() for p,v in revision.items()}}
  for path,value in revision.items():(here/(name+'-'+str(path.relative_to(source)).replace('/','-'))).write_text(value)
finally:
 for path,value in original.items():path.write_text(value)
(here/'provenance.json').write_text(json.dumps(metadata,indent=2)+'\n')
