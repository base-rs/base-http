"""Separate FIFO-synchronized perf counters/profiles of frozen raw servers."""
import argparse,datetime,hashlib,json,os,random,select,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
VARIANTS={'owned':'base-iouring-owned-connections-raw','borrowed':'base-iouring-connections-raw','tokio':'tokio-connections-raw'}
EVENTS=[
 
 ('tcp_recv_calls','base_http_lifecycle:tcp_recvmsg',None),
 ('tcp_recv_eagain','base_http_lifecycle:tcp_recvmsg_return32__return','result==-11'),
]
def usage(pid):
 fields=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split();ticks=os.sysconf('SC_CLK_TCK')
 return dict(user_seconds=int(fields[11])/ticks,system_seconds=int(fields[12])/ticks)
def control(ctl,ack,command,perf,log):
 os.write(ctl,(command+'\n').encode());deadline=time.monotonic()+10
 while time.monotonic()<deadline:
  if select.select([ack],[],[],.1)[0]:
   data=os.read(ack,4096);assert data.rstrip(b'\0')==b'ack\n',(command,data);return
  assert perf.poll() is None,(command,perf.returncode,log.read_text())
 raise RuntimeError('perf control timeout: '+command)
def stop(perf,pidfile):
 if perf.poll() is None and pidfile.exists():
  subprocess.run(['sudo','-n','kill','-INT',pidfile.read_text().strip()],capture_output=True)
 try:perf.wait(timeout=15)
 except subprocess.TimeoutExpired:
  subprocess.run(['sudo','-n','kill','-KILL',pidfile.read_text().strip()],capture_output=True);perf.wait(timeout=5)
def run(mode,name,connections,rate,rep):
 stem=f'confirm-{mode}-{name}-c{connections}-{rate}-r{rep}';out=ROOT/stem;out.mkdir()
 ctlpath=out/'control';ackpath=out/'ack';pidfile=out/'perf.pid'
 os.mkfifo(ctlpath);os.mkfifo(ackpath)
 ctl=os.open(ctlpath,os.O_RDWR|os.O_NONBLOCK);ack=os.open(ackpath,os.O_RDWR|os.O_NONBLOCK)
 with (out/'server.stderr').open('w') as errors,(out/'perf.stderr').open('w') as perferr:
  server=subprocess.Popen(['taskset','-c','0',str(ROOT/'binaries/baseline'),VARIANTS[name]],stdout=subprocess.PIPE,stderr=errors,text=True)
  perf=None
  try:
   assert select.select([server.stdout],[],[],10)[0];address=server.stdout.readline().strip();assert address.startswith('127.0.0.1:')
   events=[]
   if mode=='counts':
    args=['perf','stat','--no-big-num','--no-scale','-x','\t','-o',str(out/'counts.tsv')]
    for label,event,filter_ in EVENTS:
     args+=['-e',event]
     if filter_:args+=['--filter',filter_]
   else:args=['perf','record','-o',str(out/'perf.data'),'-e','cpu-clock','-F','997','--call-graph','dwarf,4096']
   args+=['-p',str(server.pid),'--delay=-1','--control',f'fifo:{ctlpath},{ackpath}']
   command=['sudo','-n','bash','-c','echo $$ > "$1"; shift; exec "$@"','profile',str(pidfile),*args]
   (out/'command.json').write_text(json.dumps(command,indent=2)+'\n')
   perf=subprocess.Popen(command,stdout=perferr,stderr=perferr)
   control(ctl,ack,'enable',perf,out/'perf.stderr')
   (out/'threads.txt').write_text(subprocess.check_output(['ps','-T','-p',str(server.pid),'-o','pid,tid,psr,comm'],text=True))
   (out/'affinity.txt').write_text(subprocess.check_output(['taskset','-apc',str(server.pid)],text=True))
   before=usage(server.pid)
   seconds=4 if mode=='counts' else 6
   cpus='2' if connections==1 else '2,3,6,7'
   client=subprocess.run(['taskset','-c','1',str(ROOT/'binaries/client'),address,str(connections),str(rate),'1',str(seconds),cpus],capture_output=True,text=True,timeout=seconds+40)
   (out/'client.json').write_text(client.stdout);(out/'client.stderr').write_text(client.stderr)
   after=usage(server.pid)
   control(ctl,ack,'disable',perf,out/'perf.stderr')
   stop(perf,pidfile)
   assert client.returncode==0 and not client.stderr,(client.returncode,client.stderr)
   data=json.loads(client.stdout);assert data['total_responses_validated']==rate*(1+seconds);assert data['measured_responses_validated']==rate*seconds;assert data['payload_validation_errors']==0
   row=dict(mode=mode,variant=name,backend=VARIANTS[name],connections=connections,rate=rate,repetition=rep,responses=data['total_responses_validated'],achieved_rate=data['achieved_rate'],drain_ms=data['drain_ms'],server_cpu={k:after[k]-before[k] for k in before},perf_returncode=perf.returncode)
   assert perf.returncode in (0,130,-2),row
   if mode=='counts':
    lines=[x.split('\t') for x in (out/'counts.tsv').read_text().splitlines() if x and not x.startswith('#')]
    rows=[x for x in lines if len(x)>=3]
    assert len(rows)==len(EVENTS),(rows,len(EVENTS))
    assert all(fields[2]==event and float(fields[4])==100.0 for (_,event,_),fields in zip(EVENTS,rows)),rows
    row['counts']={label:dict(value=float(fields[0]),unit=fields[1],event=fields[2],raw=fields) for (label,event,filter_),fields in zip(EVENTS,rows)}
   else:
    for report,flags in [('self',['--no-children','-g','none']),('callgraph',['--children'])]:
     with (out/f'report-{report}.txt').open('w') as reportfile:
      subprocess.run(['sudo','-n','perf','report','--stdio','-i',str(out/'perf.data'),'--percent-limit','0.1',*flags],stdout=reportfile,stderr=reportfile,check=True)
   (out/'summary.json').write_text(json.dumps(row,indent=2)+'\n')
   print(stem,'responses',row['responses'],'CPU',row['server_cpu'],'achieved',round(row['achieved_rate']),'drain',round(row['drain_ms'],3),flush=True)
   return row
  finally:
   if perf is not None:stop(perf,pidfile)
   server.terminate();server.wait(timeout=10);server.stdout.close();os.close(ctl);os.close(ack);ctlpath.unlink();ackpath.unlink()
def main():
 p=argparse.ArgumentParser();p.add_argument('--mode',choices=['counts','profile'],required=True);p.add_argument('--repetitions',type=int,default=2);p.add_argument('--variants',nargs='+',default=list(VARIANTS));a=p.parse_args()
 cases=[(1,50000),(64,200000),(64,240000)] if a.mode=='counts' else [(1,50000),(64,240000)]
 jobs=[(name,c,rate,rep) for rep in range(1,a.repetitions+1) for c,rate in cases for name in a.variants];random.Random(20261028).shuffle(jobs)
 metadata=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),events=EVENTS,binaries={n:hashlib.sha256((ROOT/'binaries'/n).read_bytes()).hexdigest() for n in ('baseline','client')},uname=list(os.uname()),initial_load=Path('/proc/loadavg').read_text(),cpu=subprocess.check_output(['lscpu'],text=True))
 (ROOT/f'confirm-{a.mode}-environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
 with (ROOT/f'confirm-{a.mode}-samples.jsonl').open('w') as journal:
  for job in jobs:
   row=run(a.mode,*job);journal.write(json.dumps(row)+'\n');journal.flush()
if __name__=='__main__':main()
