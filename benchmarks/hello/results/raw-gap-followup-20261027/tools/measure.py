#!/usr/bin/env python3
"""CPU diagnostics on the frozen uninstrumented raw controls."""
import argparse, hashlib, json, os, random, re, select, signal, subprocess, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));from benchmark import runner,usage

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--kind',choices=['stat','perf'],default='stat');p.add_argument('--rate',type=int,default=0);p.add_argument('--repetitions',type=int,default=3);p.add_argument('--concurrency',type=int,nargs='+',default=[16,64]);p.add_argument('--duration',type=int,default=8);a=p.parse_args()
 a.output.mkdir(parents=True,exist_ok=False);rows=[];rng=random.Random(20261027)
 (a.output/'configuration.json').write_text(json.dumps({'args':vars(a)|{'output':str(a.output)},'binary_sha256':hashlib.sha256((ROOT/'binaries/oob-default').read_bytes()).hexdigest(),'loadavg':Path('/proc/loadavg').read_text()},indent=2)+'\n')
 for rep in range(1,a.repetitions+1):
  for c in a.concurrency:
   backends=['base-native-raw','tokio-task-raw'];rng.shuffle(backends)
   for backend in backends:
    prefix=a.output/f'{backend}-c{c}-r{rep}';profiler=None
    with Path(str(prefix)+'.server.stderr').open('w') as errors:
     server=subprocess.Popen([str(ROOT/'binaries/oob-default'),backend],stdout=subprocess.PIPE,stderr=errors,text=True)
     try:
      assert select.select([server.stdout],[],[],10)[0];address=server.stdout.readline().strip();validation=runner.validate(address,raw=True)
      wrk=[str(ROOT/'wrk/wrk'),'-t4',f'-c{c}','--latency','--timeout','2s']
      warmup=subprocess.run([*wrk,'-d1s',f'http://{address}/'],capture_output=True,text=True,check=True);runner.parse_wrk(warmup.stdout)
      time.sleep(.2)
      if a.kind=='stat':
       command=['sudo','-n','perf','stat','-x',';','-p',str(server.pid),'-e','task-clock,cycles:u,cycles:k,instructions:u,instructions:k,context-switches,cpu-migrations','-o',str(prefix)+'.stat']
      else:
       command=['sudo','-n','perf','record','-e','cycles:u','-F','999','-g','--call-graph','dwarf,8192','-p',str(server.pid),'-o',str(prefix)+'.data']
      with Path(str(prefix)+'.profiler.log').open('w') as log:
       profiler=subprocess.Popen(command,stdout=log,stderr=log,start_new_session=True);time.sleep(.3)
       before=usage(server.pid)
       if a.rate:load=[str(ROOT/'raw-followup-tools/fixed_load'),address.rsplit(':',1)[1],str(c),str(a.rate),str(a.duration)]
       else:load=[*wrk,f'-d{a.duration}s',f'http://{address}/']
       measured=subprocess.run(load,capture_output=True,text=True,check=True,timeout=a.duration+15)
       after=usage(server.pid);time.sleep(.1)
       os.killpg(profiler.pid,signal.SIGINT);profiler.wait(timeout=15)
      Path(str(prefix)+'.load.txt').write_text(measured.stdout+measured.stderr)
      if a.rate:
       result=json.loads(measured.stdout);assert result['requests']==result['sent'];assert result['requests']>=a.rate*a.duration*.99,result
       n=result['requests']
      else:
       result=runner.parse_wrk(measured.stdout);n=int(re.search(r'(\d+) requests in',measured.stdout)[1])
      row={'backend':backend,'concurrency':c,'repetition':rep,'requests':n,'load':result,'validation':validation,'cpu_us_per_request':{key:(after[key]-before[key])*1e6/n for key in before}}
      if a.kind=='stat':
       metrics={}
       for line in Path(str(prefix)+'.stat').read_text().splitlines():
        fields=line.split(';')
        if len(fields)<3 or fields[0].startswith('#') or not fields[0].strip():continue
        try:value=float(fields[0].strip())
        except ValueError:continue
        metrics[fields[2]]=value
       row['perf']=metrics;row['perf_per_response']={key:value/n for key,value in metrics.items()}
      else:
       for mode,extra in [('self',['--no-children']),('tree',[])]:
        report=subprocess.run(['sudo','-n','perf','report','--stdio','-i',str(prefix)+'.data',*extra,'--percent-limit','0.2'],capture_output=True,text=True,check=True)
        Path(str(prefix)+'.'+mode+'.txt').write_text(report.stdout+report.stderr)
      rows.append(row);(a.output/'samples.json').write_text(json.dumps(rows,indent=2)+'\n')
      print(backend,c,rep,json.dumps(row.get('perf_per_response',{})),flush=True)
     finally:
      if profiler and profiler.poll() is None:os.killpg(profiler.pid,signal.SIGINT);profiler.wait(timeout=15)
      server.terminate();server.wait(timeout=10);server.stdout.close()
if __name__=='__main__':main()
