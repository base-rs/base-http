#!/usr/bin/env python3
"""Validate retained fields against the staged Linux scheduler, restoring locks."""
from pathlib import Path
import os,subprocess
ROOT=Path(__file__).resolve().parents[1]
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/base-http-hana-epoll-uring-7nxlaica/target')
config=[]
for name,relative in [('base-scheduler','.'),('base-scheduler-direct','crates/direct'),('base-scheduler-selector','crates/selector'),('base-scheduler-native','crates/native'),('base-scheduler-tokio','crates/tokio'),('base-scheduler-timers','crates/timers'),('base-scheduler-iouring','crates/iouring')]:
 config.extend(['--config',f'patch.crates-io.{name}.path="{ROOT/"base-scheduler"/relative}"'])
roots=[ROOT/'sources'/name for name in ['base-http','base-http1','base-http-server']]
locks={root/'Cargo.lock':(root/'Cargo.lock').read_bytes() if (root/'Cargo.lock').exists() else None for root in roots}
try:
 for root in roots:
  for name,args in [('lock',['generate-lockfile','--offline']),('test',['test','--locked']),('clippy',['clippy','--all-targets','--locked','--','-D','warnings'])]:
   command=['cargo',args[0],*config,*args[1:]]
   with (ROOT/'diagnostics'/f'fields-{root.name}-linux-{name}.log').open('w') as log:
    subprocess.run(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
   print(root.name,name,'passed',flush=True)
finally:
 for path,data in locks.items():
  if data is None:path.unlink(missing_ok=True)
  else:path.write_bytes(data)
