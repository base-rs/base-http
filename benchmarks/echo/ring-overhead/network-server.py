#!/usr/bin/env python3
"""Remote child wrapper: forward readiness and retain whole-process CPU accounting."""
import argparse,json,os,resource,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--backend',required=True);p.add_argument('--workers',type=int,default=1);p.add_argument('--cpus',default='0');p.add_argument('--connections',type=int,required=True);p.add_argument('--address',required=True);p.add_argument('--usage',type=Path,required=True);a=p.parse_args()
env=dict(os.environ,BASE_HTTP_LISTEN=a.address)
command=['taskset','-c',a.cpus,a.binary,'server',a.backend,str(a.workers),str(a.connections)]
started=time.monotonic()
with a.usage.with_suffix('.stderr').open('w') as errors:
 child=subprocess.Popen(command,env=env,stdout=subprocess.PIPE,stderr=errors,text=True)
 try:
  print(child.stdout.readline().strip(),flush=True)
  code=child.wait(timeout=60)
 finally:
  if child.poll() is None:child.kill();child.wait()
  child.stdout.close()
u=resource.getrusage(resource.RUSAGE_CHILDREN)
a.usage.write_text(json.dumps({'command':command,'returncode':code,'user_seconds':u.ru_utime,'system_seconds':u.ru_stime,'wall_seconds':time.monotonic()-started,'scope':'whole server lifetime including warmup/setup/cleanup'})+'\n')
raise SystemExit(code)
