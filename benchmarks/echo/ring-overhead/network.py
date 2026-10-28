#!/usr/bin/env python3
"""Interleaved off-host echo checks; same validating client logic, no SSH payload forwarding."""
import argparse,hashlib,itertools,json,random,resource,select,shlex,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--client',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--remote',default='hana.local');p.add_argument('--root',default='/tmp/base-http-ring-overhead-20261027');p.add_argument('--bytes',type=int,default=64);p.add_argument('--workers',type=int,nargs='+',default=[1,4]);p.add_argument('--connections',type=int,nargs='+',default=[64,256]);p.add_argument('--repetitions',type=int,default=3);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
variants={'epoll':('network-before','native-poll'),'before':('network-before','iouring-poll'),'after':('network-after','iouring-poll'),'tokio':('network-before','tokio')}
meta={'client_sha256':hashlib.sha256(a.client.read_bytes()).hexdigest(),'configuration':{k:str(v) if isinstance(v,Path) else v for k,v in vars(a).items()},'variants':variants,'network_address':'192.168.1.80','client_source':'exact canonical echo client.rs; standalone Tokio 1.54.1 executable','cpu_accounting_scope':'whole server and client lifetimes, including setup/warmup/cleanup'}
meta['server_sha256']=subprocess.check_output(['ssh',a.remote,'sha256sum '+shlex.quote(a.root+'/network-before')+' '+shlex.quote(a.root+'/network-after')],text=True)
meta['local_uname']=subprocess.check_output(['uname','-a'],text=True)
(a.output/'environment.json').write_text(json.dumps(meta,indent=2)+'\n')
rng=random.Random(20261027)
with (a.output/'samples.jsonl').open('w') as journal:
 for rep in range(1,a.repetitions+1):
  cases=list(itertools.product(a.workers,a.connections));rng.shuffle(cases)
  for workers,c in cases:
   names=list(variants);rng.shuffle(names)
   for name in names:
    binary,backend=variants[name];stem=f'{name}-w{workers}-c{c}-b{a.bytes}-r{rep}'
    cpus=','.join(str(i) for i in range(workers));usage=f'{a.root}/network-usage/{stem}.json'
    args=['taskset','-c','5','python3',f'{a.root}/probe/network-server.py','--binary',f'{a.root}/{binary}','--backend',backend,'--workers',str(workers),'--cpus',cpus,'--connections',str(c),'--address','192.168.1.80:0','--usage',usage]
    with (a.output/(stem+'.ssh.stderr')).open('w') as errors:
     server=subprocess.Popen(['ssh',a.remote,shlex.join(args)],stdout=subprocess.PIPE,stderr=errors,text=True)
     try:
      assert select.select([server.stdout],[],[],30)[0]
      address=server.stdout.readline().strip();assert address.startswith('192.168.1.80:'),address
      before=resource.getrusage(resource.RUSAGE_CHILDREN);started=time.monotonic()
      client=subprocess.run([str(a.client.resolve()),'client',address,'4',str(c),str(a.bytes),'1','3'],capture_output=True,text=True,timeout=40,check=True)
      elapsed=time.monotonic()-started;after=resource.getrusage(resource.RUSAGE_CHILDREN)
      (a.output/(stem+'.client.json')).write_text(client.stdout);(a.output/(stem+'.client.stderr')).write_text(client.stderr);assert not client.stderr
      assert server.wait(timeout=30)==0
      row=json.loads(client.stdout);row.update(variant=name,server_workers=workers,repetition=rep,client_user_seconds=after.ru_utime-before.ru_utime,client_system_seconds=after.ru_stime-before.ru_stime,client_wall_seconds=elapsed)
      row['server_usage']=json.loads(subprocess.check_output(['ssh',a.remote,'cat '+shlex.quote(usage)],text=True))
      stderr=subprocess.check_output(['ssh',a.remote,'cat '+shlex.quote(usage.removesuffix('.json')+'.stderr')],text=True);assert not stderr,stderr
      journal.write(json.dumps(row)+'\n');journal.flush();print(stem,round(row['requests_per_second']),flush=True)
     finally:
      if server.poll() is None:server.terminate();server.wait(timeout=10)
      server.stdout.close()
