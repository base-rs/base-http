#!/usr/bin/env python3
"""Separate perf/strace diagnostics; these rates are never benchmark evidence."""
import argparse,json,os,select,signal,subprocess,time
from pathlib import Path
from benchmark import ROOT,runner

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',default='baseline')
    p.add_argument('--backends',nargs='+',default=['base-native','base-iouring','hyper-tokio'])
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--kind',choices=['perf','strace','stat','syscalls'],default='perf')
    p.add_argument('--concurrency',type=int,default=64)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    for backend in a.backends:
        with (a.output/(backend+'.server.stderr')).open('w') as errors:
            server=subprocess.Popen([str(ROOT/'binaries'/a.binary),backend],stdout=subprocess.PIPE,stderr=errors,text=True)
            profiler=None
            try:
                assert select.select([server.stdout],[],[],10)[0]
                address=server.stdout.readline().strip();runner.validate(address,raw=backend.endswith('-raw'))
                wrk=[str(ROOT/'wrk/wrk'),'-t1' if a.concurrency == 1 else '-t4',f'-c{a.concurrency}','--latency',f'http://{address}/']
                subprocess.run([*wrk[:-1],'-d1s',wrk[-1]],capture_output=True,check=True)
                prefix=a.output/backend
                if a.kind=='perf':
                    command=['sudo','-n','perf','record','-F','499','-g','--call-graph','dwarf,8192','-p',str(server.pid),'-o',str(prefix)+'.data']
                elif a.kind=='strace':
                    command=['sudo','-n','strace','-f','-c','-p',str(server.pid),'-o',str(prefix)+'.strace']
                elif a.kind=='stat':
                    command=['sudo','-n','perf','stat','-p',str(server.pid),'-e','task-clock,cycles,instructions,context-switches,cpu-migrations,cache-misses','-o',str(prefix)+'.stat']
                else:
                    command=['sudo','-n','perf','stat','-p',str(server.pid),'-e','syscalls:sys_enter_recvfrom',
                             '-e','syscalls:sys_exit_recvfrom','--filter','ret == -11',
                             '-e','syscalls:sys_enter_recvmsg','-e','syscalls:sys_exit_recvmsg','--filter','ret == -11',
                             '-e','syscalls:sys_enter_sendto,syscalls:sys_enter_writev,syscalls:sys_enter_epoll_pwait,syscalls:sys_enter_epoll_wait,syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_accept4,syscalls:sys_enter_epoll_ctl',
                             '-o',str(prefix)+'.syscalls']
                with Path(str(prefix)+'.profiler.log').open('w') as log:
                    profiler=subprocess.Popen(command,stdout=log,stderr=log,start_new_session=True)
                    time.sleep(.3)
                    measured=subprocess.run([*wrk[:-1],'-d8s' if a.kind=='perf' else '-d4s',wrk[-1]],capture_output=True,text=True,check=True)
                    Path(str(prefix)+'.wrk').write_text(measured.stdout+measured.stderr)
                    os.killpg(profiler.pid,signal.SIGINT)
                    profiler.wait(timeout=15)
                if a.kind=='perf':
                    for mode,extra in [('self',['--no-children']),('tree',[])]:
                        report=subprocess.run(['sudo','-n','perf','report','--stdio','-i',str(prefix)+'.data',*extra,'--percent-limit','0.5'],capture_output=True,text=True,check=True)
                        Path(str(prefix)+'.'+mode+'.txt').write_text(report.stdout+report.stderr)
                print(backend,a.kind,'completed',flush=True)
            finally:
                if profiler and profiler.poll() is None:
                    os.killpg(profiler.pid,signal.SIGINT);profiler.wait(timeout=15)
                server.terminate();server.wait(timeout=10);server.stdout.close()

if __name__=='__main__':main()
