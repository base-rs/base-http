#!/usr/bin/env python3
"""Separate operation-count diagnostics, never throughput evidence."""
import argparse, json, os, select, struct, subprocess, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT)); from benchmark import runner
OPS=['read','readv','recv','recvfrom','recvmsg','write','writev','send','sendto','sendmsg','epoll_wait','epoll_pwait','epoll_pwait2','epoll_ctl','accept','accept4']
METRICS=['calls','positive','would_block','errors','bytes_or_events','zero']
POLLS=['supervisor_polls','supervisor_pending','supervisor_wakes','unused0','connection_polls','connection_pending','connection_wakes','unused1']
def snapshot(path):
    data=path.read_bytes(); return list(struct.unpack('<'+'Q'*(len(data)//8),data))
def main():
    p=argparse.ArgumentParser();p.add_argument('--binary',default='oob-default');p.add_argument('--backends',nargs='+',default=['base-native-raw','tokio-task-raw']);p.add_argument('--output',type=Path,required=True);p.add_argument('--concurrency',type=int,nargs='+',default=[16,64]);p.add_argument('--polls',action='store_true');a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False); rows=[]
    for c in a.concurrency:
      for backend in a.backends:
        prefix=a.output/f'{backend}-c{c}'
        calls=Path(str(prefix)+'.calls.bin');polls=Path(str(prefix)+'.polls.bin')
        env=dict(os.environ,LD_PRELOAD=str(ROOT/'raw-followup-tools/counts.so'),BASE_HTTP_SYSCALL_COUNTS=str(calls))
        if a.polls:env['BASE_HTTP_POLL_COUNTS']=str(polls)
        with Path(str(prefix)+'.server.stderr').open('w') as errors:
          server=subprocess.Popen([str(ROOT/'binaries'/a.binary),backend],stdout=subprocess.PIPE,stderr=errors,text=True,env=env)
          try:
            assert select.select([server.stdout],[],[],10)[0]
            address=server.stdout.readline().strip();validation=runner.validate(address,raw=backend.endswith('-raw'))
            command=[str(ROOT/'wrk/wrk'),f'-t{min(4,c)}',f'-c{c}','--latency','--timeout','2s']
            warmup=subprocess.run([*command,'-d1s',f'http://{address}/'],capture_output=True,text=True,check=True);runner.parse_wrk(warmup.stdout)
            time.sleep(.2)
            before=snapshot(calls);before_p=snapshot(polls) if a.polls else []
            measured=subprocess.run([*command,'-d4s',f'http://{address}/'],capture_output=True,text=True,check=True);runner.parse_wrk(measured.stdout)
            time.sleep(.2)
            after=snapshot(calls);after_p=snapshot(polls) if a.polls else []
            import re
            n=int(re.search(r'(\d+) requests in',measured.stdout)[1]);delta=[b-a for a,b in zip(before,after)]
            row={'binary':a.binary,'backend':backend,'concurrency':c,'requests':n,'validation':validation,'operations':{op:{metric:delta[i*6+j] for j,metric in enumerate(METRICS)} for i,op in enumerate(OPS)},'polls':{name:y-x for name,x,y in zip(POLLS,before_p,after_p)}}
            row['per_response']={op:{metric:value/n for metric,value in metrics.items()} for op,metrics in row['operations'].items()}
            row['polls_per_response']={name:value/n for name,value in row['polls'].items()}
            Path(str(prefix)+'.wrk').write_text(measured.stdout+measured.stderr)
            rows.append(row);(a.output/'counts.json').write_text(json.dumps(rows,indent=2)+'\n')
            print(backend,c,json.dumps({'recv':sum(row['per_response'][op]['calls'] for op in OPS[:5]),'send':sum(row['per_response'][op]['calls'] for op in OPS[5:10]),'epoll_calls':sum(row['per_response'][op]['calls'] for op in OPS[10:13]),'epoll_events':sum(row['per_response'][op]['bytes_or_events'] for op in OPS[10:13]),'polls':row['polls_per_response']}),flush=True)
          finally:
            server.terminate();server.wait(timeout=10);server.stdout.close()
if __name__=='__main__':main()
