#!/usr/bin/env python3
"""Separate traced diagnostic runs; their throughput is not benchmark evidence."""
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'base-http/benchmarks/hello'))
from run import validate,parse_wrk

out=ROOT/'results/syscalls'
out.mkdir(exist_ok=False)
for variant in ['polling','native']:
    summary=out/f'{variant}.strace.txt'
    with (out/f'{variant}.stderr').open('w') as errors:
        command=['taskset','-c','0','strace','-f','-c','-o',str(summary),
                 '-e','trace=epoll_ctl,epoll_wait,epoll_pwait,epoll_pwait2,timerfd_settime,read,write,eventfd2',
                 str(ROOT/'binaries'/variant),'base-native-raw']
        server=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=errors,text=True,start_new_session=True)
        try:
            assert select.select([server.stdout],[],[],10)[0], 'server readiness timeout'
            address=server.stdout.readline().strip()
            assert address
            validation=validate(address,raw=True)
            load=subprocess.run(['taskset','-c','2','/usr/bin/wrk','-t1','-c1','-d2s','--latency','--timeout','2s',f'http://{address}/'],
                                capture_output=True,text=True,check=True,timeout=20)
            parse_wrk(load.stdout)
            (out/f'{variant}.wrk.txt').write_text(load.stdout+load.stderr)
            (out/f'{variant}.validation.json').write_text(json.dumps(validation,indent=2)+'\n')
        finally:
            os.killpg(server.pid,signal.SIGINT)
            try:
                server.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid,signal.SIGKILL)
                server.communicate(timeout=5)
    print(variant,summary.read_text(),flush=True)
