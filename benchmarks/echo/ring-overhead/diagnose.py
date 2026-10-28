#!/usr/bin/env python3
"""Separate trace/profile diagnostics against frozen payload-validating echo workloads."""
import argparse, datetime, json, os, select, struct, subprocess
from pathlib import Path

OPS = ['read','readv','recv','recvfrom','recvmsg','write','writev','send','sendto','sendmsg','epoll_wait','epoll_pwait','epoll_pwait2','epoll_ctl','accept','accept4']
METRICS = ['calls','positive','would_block','errors','bytes_or_events','zero']
EVENTS = ['io_uring:io_uring_create','io_uring:io_uring_submit_req','io_uring:io_uring_complete','io_uring:io_uring_poll_arm','io_uring:io_uring_task_add','io_uring:io_uring_task_work_run','io_uring:io_uring_local_work_run','io_uring:io_uring_cqe_overflow','syscalls:sys_enter_io_uring_enter','syscalls:sys_enter_epoll_ctl','syscalls:sys_enter_epoll_wait','syscalls:sys_enter_epoll_pwait']
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--server',type=Path,required=True)
p.add_argument('--client',type=Path,required=True)
p.add_argument('--backend',required=True)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--kind',choices=['trace','record','stat'],default='trace')
p.add_argument('--library',type=Path)
p.add_argument('--connections',type=int,default=64)
p.add_argument('--bytes',type=int,default=64)
p.add_argument('--seconds',type=float,default=0.4)
a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
base=['sudo','-n','perf',a.kind,'-o',str(a.output.resolve()/('perf.data' if a.kind!='stat' else 'perf.stat'))]
if a.kind=='trace':
    base[3]='record'
    base+=['-a','-m','1024']
    for event in EVENTS:base+=['-e',event]
elif a.kind=='record':base+=['-F','999','-g','--call-graph','dwarf,8192']
else:base+=['-e','task-clock,cycles,instructions,context-switches,cpu-migrations,cache-misses,syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_epoll_wait,syscalls:sys_enter_epoll_pwait']
server=['taskset','-c','0']
if a.library:server+=['env','LD_PRELOAD='+str(a.library.resolve()),'BASE_HTTP_SYSCALL_COUNTS='+str((a.output/'counts.bin').resolve())]
server+=[str(a.server.resolve()),'server',a.backend,'1',str(a.connections)]
command=[*base,'--',*server]
(a.output/'command.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'command':command,'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(a).items()}},indent=2)+'\n')
with (a.output/'server-perf.stderr').open('w') as errors:
    proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=errors,text=True)
    try:
        assert select.select([proc.stdout],[],[],30)[0], 'readiness timeout'
        address=proc.stdout.readline().strip();assert address.startswith('127.0.0.1:'),address
        client=subprocess.run(['taskset','-c','2,3,6,7',str(a.client.resolve()),'client',address,'4',str(a.connections),str(a.bytes),'0.1',str(a.seconds)],capture_output=True,text=True,timeout=a.seconds+40,check=True)
        (a.output/'client.json').write_text(client.stdout)
        (a.output/'client.stderr').write_text(client.stderr)
        assert not client.stderr
        assert proc.wait(timeout=30)==0,(a.output/'server-perf.stderr').read_text()
    finally:
        if proc.poll() is None:
            subprocess.run(['sudo','-n','pkill','-TERM','-P',str(proc.pid)],check=False)
            proc.terminate();proc.wait(timeout=30)
        proc.stdout.close()
subprocess.run(['sudo','-n','chown','-R',f'{os.getuid()}:{os.getgid()}',str(a.output)],check=True)
if a.library:
    values=struct.unpack('<'+'Q'*(len(OPS)*len(METRICS)),(a.output/'counts.bin').read_bytes())
    counts={op:{key:values[i*len(METRICS)+j] for j,key in enumerate(METRICS)} for i,op in enumerate(OPS)}
    (a.output/'counts.json').write_text(json.dumps(counts,indent=2)+'\n')
if a.kind=='trace':
    with (a.output/'trace.txt').open('w') as output:
        subprocess.run(['sudo','-n','perf','script','-f','-i',str(a.output/'perf.data')],stdout=output,check=True)
elif a.kind=='record':
    for mode,flags in [('self',['--no-children']),('tree',[])]:
        with (a.output/(mode+'.txt')).open('w') as output:
            subprocess.run(['sudo','-n','perf','report','-f','--stdio','-i',str(a.output/'perf.data'),*flags,'--percent-limit','0.25'],stdout=output,check=True)
print(a.backend,a.kind,'complete',flush=True)
