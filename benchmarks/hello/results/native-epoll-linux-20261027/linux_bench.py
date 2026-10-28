#!/usr/bin/env python3
"""Paired Linux polling/native epoll benchmark with per-process CPU accounting."""
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
sys.path.insert(0, str(ROOT / 'base-http/benchmarks/hello'))
import run as runner

TICKS = os.sysconf('SC_CLK_TCK')
PAGE_KIB = os.sysconf('SC_PAGE_SIZE') / 1024

def usage(pid):
    # Fields after comm begin at field 3; utime/stime are fields 14/15.
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return {'user_seconds': int(fields[11]) / TICKS,
            'system_seconds': int(fields[12]) / TICKS}

def cpu_rss(pid):
    cpu = usage(pid)
    resident = int(Path(f'/proc/{pid}/statm').read_text().split()[1])
    return cpu['user_seconds'] + cpu['system_seconds'], resident * PAGE_KIB

runner.cpu_usage = usage
runner.cpu_and_rss = cpu_rss
original_popen = subprocess.Popen

def pinned_popen(command, *args, **kwargs):
    if isinstance(command, list) and command:
        if str(command[0]).startswith(str(ROOT / 'binaries')):
            command = ['taskset', '-c', '0', *command]
        elif str(command[0]) == '/usr/bin/wrk':
            command = ['taskset', '-c', '2-5', *command]
    return original_popen(command, *args, **kwargs)

runner.subprocess.Popen = pinned_popen

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--duration', type=int, default=8)
    parser.add_argument('--concurrency', nargs='+', type=int, default=[1, 16, 64])
    parser.add_argument('--workloads', nargs='+', default=['raw', 'http'])
    options = parser.parse_args()
    options.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'uname': list(os.uname()),
        'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
        'lscpu': subprocess.check_output(['lscpu'], text=True),
        'wrk': subprocess.run(['/usr/bin/wrk', '--version'], capture_output=True, text=True).stdout,
        'server_cpus': [0], 'client_cpus': [2,3,4,5], 'controller_cpus': [1],
        'cpu_ticks_per_second': TICKS,
        'configuration': {key: str(value) if isinstance(value, Path) else value for key,value in vars(options).items()},
        'source_baseline': '549603509de7577f76ccce9a8a261be743d07af4',
        'binary_sha256': {name: digest(ROOT/'binaries'/name) for name in ['polling','native']},
        'before_sources': json.loads((ROOT/'state.json').read_text())['sources'],
        'after_sources': {str(p.relative_to(ROOT)): digest(p) for directory in ['base-scheduler','base-http','base-http1','base-http-server']
                          for p in (ROOT/directory).rglob('*.rs')},
        'initial_vm_load': Path('/proc/loadavg').read_text(),
        'initial_proc_stat': Path('/proc/stat').read_text(),
    }
    (options.output/'environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
    randomizer = random.Random(20261027)
    rows = []
    with (options.output/'samples.jsonl').open('w') as journal:
        for repetition in range(1,options.repetitions+1):
            conditions = [(workload, concurrency) for workload in options.workloads for concurrency in options.concurrency]
            randomizer.shuffle(conditions)
            for workload, concurrency in conditions:
                variants = ['polling','native']
                randomizer.shuffle(variants)
                for variant in variants:
                    backend = 'base-native-raw' if workload == 'raw' else 'base-native'
                    output = options.output/variant/workload
                    output.mkdir(parents=True,exist_ok=True)
                    args = argparse.Namespace(binary=ROOT/'binaries'/variant,wrk=Path('/usr/bin/wrk'),
                                              output=output,threads=4,warmup=1,duration=options.duration)
                    print(f'{variant} {workload} repetition={repetition}',flush=True)
                    row = runner.sample(args,backend,concurrency,repetition)
                    row.update(variant=variant,workload=workload)
                    row['total_cpu_us_per_request'] = row['user_cpu_us_per_request'] + row['system_cpu_us_per_request']
                    row['vm_load_after'] = Path('/proc/loadavg').read_text()
                    journal.write(json.dumps(row)+'\n')
                    journal.flush()
                    rows.append(row)
    summary=[]
    for workload in options.workloads:
        for concurrency in options.concurrency:
            item={'workload':workload,'concurrency':concurrency}
            groups={name:[r for r in rows if r['variant']==name and r['workload']==workload and r['concurrency']==concurrency]
                    for name in ['polling','native']}
            metrics=['requests_per_second','user_cpu_us_per_request','system_cpu_us_per_request','total_cpu_us_per_request','latency_p99_ms']
            for name, group in groups.items():
                item[name]={metric:statistics.median(r[metric] for r in group) for metric in metrics}
                item[name]['rps_min']=min(r['requests_per_second'] for r in group)
                item[name]['rps_max']=max(r['requests_per_second'] for r in group)
            for metric in metrics:
                paired=[100*(after[metric]/before[metric]-1) for before,after in zip(groups['polling'],groups['native'])]
                item['paired_'+metric+'_change_percent']={'median':statistics.median(paired),'min':min(paired),'max':max(paired),'values':paired}
            summary.append(item)
    (options.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    (options.output/'final-proc-stat.txt').write_text(Path('/proc/stat').read_text())
    print(json.dumps(summary,indent=2),flush=True)

if __name__ == '__main__':
    main()
