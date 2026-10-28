#!/usr/bin/env python3
"""Interleaved fixed-arrival comparison of frozen raw response servers on Linux."""
import argparse,datetime,hashlib,json,os,random,select,statistics,subprocess,time
from pathlib import Path


def usage(pid):
    fields=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
    ticks=os.sysconf('SC_CLK_TCK')
    return {'user_seconds':int(fields[11])/ticks,'system_seconds':int(fields[12])/ticks}


def sample(a,name,backend,rate,rep):
    stem=f'{name}-{rate}-r{rep}'
    with (a.output/f'{stem}.server.stderr').open('w') as errors:
        server=subprocess.Popen(['taskset','-c','0',str(a.variant_binaries[name]),backend],stdout=subprocess.PIPE,stderr=errors,text=True)
        try:
            assert select.select([server.stdout],[],[],10)[0],'server readiness timeout'
            address=server.stdout.readline().strip();assert address.startswith('127.0.0.1:'),address
            before=usage(server.pid)
            command=['taskset','-c','1',str(a.client),address,str(a.connections),str(rate),str(a.warmup),str(a.seconds),a.client_cpus]
            started=time.monotonic()
            client=subprocess.run(command,capture_output=True,text=True,timeout=a.warmup+a.seconds+25)
            elapsed=time.monotonic()-started
            after=usage(server.pid)
            (a.output/f'{stem}.client.json').write_text(client.stdout)
            (a.output/f'{stem}.client.stderr').write_text(client.stderr)
            assert client.returncode==0,(client.returncode,client.stderr)
            assert not client.stderr,client.stderr
            row=json.loads(client.stdout)
            assert row['total_responses_validated']==rate*(a.warmup+a.seconds)
            assert row['measured_responses_validated']==rate*a.seconds
            assert row['payload_validation_errors']==0
            row.update(variant=name,backend=backend,repetition=rep,client_elapsed_seconds=elapsed,
                       whole_run_server_cpu_seconds={k:after[k]-before[k] for k in before},
                       target_delivery_fraction=row['achieved_rate']/rate,
                       target_delivery_within_half_percent=abs(row['achieved_rate']/rate-1)<=0.005)
            print(f"{name:8} {rate}/s r{rep}: achieved {row['achieved_rate']:.0f}/s; scheduled p99 {row['scheduled_latency']['p99_us']:.2f} us; RTT p99 {row['wire_rtt']['p99_us']:.2f} us; dispatch p99 {row['dispatch_lag']['p99_us']:.2f} us; drain {row['drain_ms']:.2f} ms",flush=True)
            return row
        finally:
            server.terminate()
            try:server.wait(timeout=10)
            except subprocess.TimeoutExpired:server.kill();server.wait()
            server.stdout.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--variants',nargs='+',required=True)
    p.add_argument('--client',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--rates',type=int,nargs='+',default=[200000,220000,240000])
    p.add_argument('--repetitions',type=int,default=5)
    p.add_argument('--warmup',type=int,default=2)
    p.add_argument('--seconds',type=int,default=8)
    p.add_argument('--connections',type=int,default=64)
    p.add_argument('--client-cpus',default='2,3,6,7')
    a=p.parse_args();a.client=a.client.resolve()
    a.output.mkdir(parents=True,exist_ok=False)
    specs=[v.split(':') for v in a.variants]
    variants={name:backend for name,binary,backend in specs}
    a.variant_binaries={name:Path(binary).resolve() for name,binary,backend in specs}
    metadata={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'configuration':{k:str(v) if isinstance(v,Path) else v for k,v in vars(a).items() if k!='variant_binaries'},'binaries':{k:{'path':str(v),'sha256':hashlib.sha256(v.read_bytes()).hexdigest()} for k,v in list(a.variant_binaries.items())+[('client',a.client)]},'uname':list(os.uname()),'cpu':subprocess.check_output(['lscpu'],text=True),'rustc':subprocess.check_output(['rustc','-Vv'],text=True),'initial_load':Path('/proc/loadavg').read_text(),'initial_proc_stat':Path('/proc/stat').read_text()}
    (a.output/'environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
    rows=[];rng=random.Random(20261028)
    with (a.output/'samples.jsonl').open('w') as journal:
        for rep in range(1,a.repetitions+1):
            rates=a.rates.copy();rng.shuffle(rates)
            for rate in rates:
                names=list(variants);rng.shuffle(names)
                for name in names:
                    row=sample(a,name,variants[name],rate,rep)
                    rows.append(row);journal.write(json.dumps(row)+'\n');journal.flush()
    summary=[]
    for rate in a.rates:
        groups={name:sorted([r for r in rows if r['rate']==rate and r['variant']==name],key=lambda r:r['repetition']) for name in variants}
        item={'rate':rate,'medians':{},'paired':{}}
        for name,g in groups.items():
            item['medians'][name]={
                'achieved_rate':statistics.median(r['achieved_rate'] for r in g),
                'target_delivery_min':min(r['target_delivery_fraction'] for r in g),
                'target_delivery_max':max(r['target_delivery_fraction'] for r in g),
                'drain_ms':statistics.median(r['drain_ms'] for r in g),
                **{key:{metric:statistics.median(r[key][metric] for r in g) for metric in ('mean_us','p50_us','p95_us','p99_us','p999_us','max_us')} for key in ('scheduled_latency','wire_rtt','send_delay','dispatch_lag')}
            }
        for baseline in variants:
            if baseline=='after': continue
            values=[100*(x['scheduled_latency']['p99_us']/y['scheduled_latency']['p99_us']-1) for x,y in zip(groups['after'],groups[baseline])]
            item['paired']['after_vs_'+baseline]={'scheduled_p99_percent':{'median':statistics.median(values),'min':min(values),'max':max(values),'values':values}}
        summary.append(item)
    (a.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)

if __name__=='__main__':main()
