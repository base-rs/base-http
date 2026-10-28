#!/usr/bin/env python3
"""Interleaved Linux HTTP samples; variants are NAME:BINARY:BACKEND."""
import argparse, datetime, hashlib, json, os, random, statistics, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, '/tmp/base-http-uring-readiness-20261027/http-after/base-http/benchmarks/hello')
import run as runner

TICKS = os.sysconf('SC_CLK_TCK')
def usage(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return {'user_seconds': int(fields[11])/TICKS, 'system_seconds': int(fields[12])/TICKS}
def cpu_rss(pid):
    return sum(usage(pid).values()), int(Path(f'/proc/{pid}/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')/1024
runner.cpu_usage = usage
runner.cpu_and_rss = cpu_rss
original_popen = subprocess.Popen
def pinned_popen(command, *args, **kwargs):
    if isinstance(command, list) and command:
        if str(command[0]).startswith(str(ROOT / 'binaries')):
            command = ['taskset', '-c', '0', *command]
        elif str(command[0]) == str(ROOT / 'wrk/wrk'):
            command = ['taskset', '-c', '2,3,6,7', *command]
    return original_popen(command, *args, **kwargs)
runner.subprocess.Popen = pinned_popen

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--variants', nargs='+', default=['epoll:baseline:base-native','iouring:baseline:base-iouring','hyper:baseline:hyper-tokio'])
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repetitions',type=int,default=3)
    p.add_argument('--duration',type=int,default=4)
    p.add_argument('--concurrency',type=int,nargs='+',default=[1,16,64])
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    variants={name:(ROOT/'binaries'/binary,backend) for name,binary,backend in (v.split(':') for v in a.variants)}
    meta={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'uname':list(os.uname()),'rustc':subprocess.check_output(['rustc','-Vv'],text=True),
          'cpu':subprocess.check_output(['lscpu'],text=True),'configuration':{k:str(v) if isinstance(v,Path) else v for k,v in vars(a).items()},
          'affinity':{'server_all_threads':[0],'load':[2,3,6,7],'controller':[1]},'cpu_ticks_per_second':TICKS,
          'binary_sha256':{name:hashlib.sha256(binary.read_bytes()).hexdigest() for name,(binary,_) in variants.items()},
          'initial_load':Path('/proc/loadavg').read_text(),'initial_proc_stat':Path('/proc/stat').read_text()}
    (a.output/'environment.json').write_text(json.dumps(meta,indent=2)+'\n')
    rng=random.Random(20261027);rows=[]
    with (a.output/'samples.jsonl').open('w') as journal:
        for rep in range(1,a.repetitions+1):
            loads=a.concurrency.copy();rng.shuffle(loads)
            for c in loads:
                names=list(variants);rng.shuffle(names)
                for name in names:
                    binary,backend=variants[name];out=a.output/name;out.mkdir(exist_ok=True)
                    args=argparse.Namespace(binary=binary,wrk=ROOT/'wrk/wrk',output=out,threads=4,warmup=1,duration=a.duration)
                    print(f'{name} c={c} r={rep}',flush=True)
                    row=runner.sample(args,backend,c,rep);row['variant']=name
                    row['total_cpu_us_per_request']=row['user_cpu_us_per_request']+row['system_cpu_us_per_request']
                    rows.append(row);journal.write(json.dumps(row)+'\n');journal.flush()
    metrics=['requests_per_second','user_cpu_us_per_request','system_cpu_us_per_request','total_cpu_us_per_request','latency_p99_ms','server_cpu_percent_one_core']
    summary=[]
    for c in a.concurrency:
        groups={name:sorted((r for r in rows if r['concurrency']==c and r['variant']==name),key=lambda r:r['repetition']) for name in variants}
        item={'concurrency':c,'medians':{name:{metric:statistics.median(r[metric] for r in g) for metric in metrics} for name,g in groups.items()},'paired':{}}
        for name,g in groups.items():
            for other,h in groups.items():
                if name==other:continue
                item['paired'][name+'_vs_'+other]={}
                for metric in metrics:
                    values=[100*(x[metric]/y[metric]-1) for x,y in zip(g,h)]
                    item['paired'][name+'_vs_'+other][metric]={'median_percent':statistics.median(values),'min_percent':min(values),'max_percent':max(values),'values_percent':values}
        summary.append(item)
    (a.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)

if __name__=='__main__':main()
