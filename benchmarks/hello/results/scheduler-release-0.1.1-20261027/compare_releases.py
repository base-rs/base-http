#!/usr/bin/env python3
"""Matched triples: released Base scheduler 0.1.0, 0.1.1, and Hyper/Tokio."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'after/base-http/benchmarks/hello'))
import run as runner

VARIANTS = {'before': ('before', 'base-native'), 'after': ('after', 'base-native'), 'hyper': ('after', 'hyper-tokio')}
PLATFORM = 'macos' if sys.platform == 'darwin' else 'linux'
if PLATFORM == 'linux':
    TICKS = os.sysconf('SC_CLK_TCK')
    PAGE_KIB = os.sysconf('SC_PAGE_SIZE') / 1024
    def usage(pid):
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return {'user_seconds': int(fields[11]) / TICKS, 'system_seconds': int(fields[12]) / TICKS}
    def cpu_rss(pid):
        cpu = usage(pid)
        return sum(cpu.values()), int(Path(f'/proc/{pid}/statm').read_text().split()[1]) * PAGE_KIB
    runner.cpu_usage = usage
    runner.cpu_and_rss = cpu_rss
    original_popen = subprocess.Popen
    def pinned_popen(command, *args, **kwargs):
        if isinstance(command, list) and command:
            if str(command[0]).startswith(str(ROOT / 'binaries/linux')):
                command = ['taskset', '-c', '0', *command]
            elif str(command[0]) == '/usr/bin/wrk':
                command = ['taskset', '-c', '2-5', *command]
        return original_popen(command, *args, **kwargs)
    runner.subprocess.Popen = pinned_popen
else:
    def cpu_rss(pid):
        cpu = runner.cpu_usage(pid)
        rss = int(subprocess.check_output(['ps', '-p', str(pid), '-o', 'rss='], text=True))
        return cpu['user_seconds'] + cpu['system_seconds'], rss
    runner.cpu_and_rss = cpu_rss

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--wrk', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repetitions', type=int, default=5)
    p.add_argument('--duration', type=int, default=8)
    p.add_argument('--concurrency', type=int, nargs='+', default=[1, 16, 64])
    options = p.parse_args()
    options.output.mkdir(parents=True, exist_ok=False)
    source_hashes = {str(path.relative_to(ROOT)): digest(path) for variant in ['before','after'] for project in ['base-http','base-http1','base-http-server'] for path in (ROOT/variant/project).rglob('*') if path.is_file() and (path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock'])}
    metadata = {
        'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'platform': PLATFORM,
        'uname': list(os.uname()), 'compiler': subprocess.check_output(['rustc','-Vv'],text=True),
        'configuration': {key:str(value) if isinstance(value,Path) else value for key,value in vars(options).items()},
        'variants': VARIANTS, 'scheduler_versions': {'before':'0.1.0','after':'0.1.1'},
        'scheduler_commits': {'before':'d68cea1b5f264e08a68025a426147446c635fe33','after':'3f628896d84a226e53e285fc0f07b7fc25f95531'},
        'binary_sha256': {name:digest(ROOT/'binaries'/PLATFORM/name) for name in ['before','after']},
        'source_sha256':source_hashes,
        'wrk_version':subprocess.run([str(options.wrk),'--version'],capture_output=True,text=True).stdout,
        'affinity':{'server':[0],'load':[2,3,4,5],'controller':[1]} if PLATFORM=='linux' else 'uncontrolled',
    }
    if PLATFORM=='linux':
        metadata.update(lscpu=subprocess.check_output(['lscpu'],text=True),cpu_ticks_per_second=TICKS,initial_load=Path('/proc/loadavg').read_text(),initial_proc_stat=Path('/proc/stat').read_text())
    else:
        metadata.update(hardware=subprocess.check_output(['sysctl','hw.physicalcpu','hw.logicalcpu','hw.memsize','machdep.cpu.brand_string'],text=True),initial_load=subprocess.check_output(['uptime'],text=True))
    (options.output/'environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
    randomizer = random.Random(20261027)
    rows=[]
    with (options.output/'samples.jsonl').open('w') as journal:
        for repetition in range(1,options.repetitions+1):
            conditions=options.concurrency.copy()
            randomizer.shuffle(conditions)
            for concurrency in conditions:
                variants=list(VARIANTS)
                randomizer.shuffle(variants)
                for variant in variants:
                    binary,backend=VARIANTS[variant]
                    out=options.output/variant
                    out.mkdir(exist_ok=True)
                    args=argparse.Namespace(binary=ROOT/'binaries'/PLATFORM/binary,wrk=options.wrk,output=out,threads=4,warmup=1,duration=options.duration)
                    print(f'{PLATFORM} {variant} c={concurrency} r={repetition}',flush=True)
                    row=runner.sample(args,backend,concurrency,repetition)
                    row.update(variant=variant,platform=PLATFORM)
                    row['total_cpu_us_per_request']=row['user_cpu_us_per_request']+row['system_cpu_us_per_request']
                    journal.write(json.dumps(row)+'\n'); journal.flush(); rows.append(row)
    metrics=['requests_per_second','user_cpu_us_per_request','system_cpu_us_per_request','total_cpu_us_per_request','latency_p99_ms']
    summary=[]
    for concurrency in options.concurrency:
        groups={variant:sorted([row for row in rows if row['variant']==variant and row['concurrency']==concurrency],key=lambda row:row['repetition']) for variant in VARIANTS}
        item={'concurrency':concurrency,'platform':PLATFORM,'medians':{variant:{metric:statistics.median(row[metric] for row in group) for metric in metrics} for variant,group in groups.items()}}
        for numerator,denominator in [('after','before'),('after','hyper'),('before','hyper')]:
            item[numerator+'_vs_'+denominator]={}
            for metric in metrics:
                values=[100*(a[metric]/b[metric]-1) for a,b in zip(groups[numerator],groups[denominator])]
                item[numerator+'_vs_'+denominator][metric]={'median_percent':statistics.median(values),'min_percent':min(values),'max_percent':max(values),'values_percent':values}
        summary.append(item)
    (options.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)

if __name__=='__main__':
    main()
