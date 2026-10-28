#!/usr/bin/env python3
"""Separate HTTP receive traces; instrumented rates are excluded from comparisons."""
import argparse, collections, hashlib, json, os, re, select, struct, subprocess, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
EVENTS=['io_uring:io_uring_create','io_uring:io_uring_submit_req','io_uring:io_uring_complete','io_uring:io_uring_poll_arm','io_uring:io_uring_local_work_run','io_uring:io_uring_cqe_overflow','syscalls:sys_enter_io_uring_enter']
OPS=['read','readv','recv','recvfrom','recvmsg','write','writev','send','sendto','sendmsg','epoll_wait','epoll_pwait','epoll_pwait2','epoll_ctl','accept','accept4']
METRICS=['calls','positive','would_block','errors','bytes_or_events','zero']
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--backend',required=True)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--child',action='store_true')
p.add_argument('--kind',choices=['trace','profile'],default='trace')
a=p.parse_args();a.output=a.output.resolve()
if a.child:
    env=os.environ.copy()
    if a.kind=='trace':
        env.update(LD_PRELOAD=str(ROOT/'counts.so'),BASE_HTTP_SYSCALL_COUNTS=str(a.output/'counts.bin'))
    with (a.output/'server.stderr').open('w') as errors:
        server=subprocess.Popen(['taskset','-c','0',str(ROOT/'binaries/candidate'),a.backend],stdout=subprocess.PIPE,stderr=errors,text=True,env=env)
        try:
            assert select.select([server.stdout],[],[],10)[0], 'readiness timeout'
            address=server.stdout.readline().strip();assert address.startswith('127.0.0.1:'),address
            (a.output/'server.json').write_text(json.dumps({'pid':server.pid,'threads':{str(t.name):(t/'comm').read_text().strip() for t in Path(f'/proc/{server.pid}/task').iterdir()},'address':address,'binary_sha256':hashlib.sha256((ROOT/'binaries/candidate').read_bytes()).hexdigest()})+'\n')
            client=subprocess.run(['taskset','-c','2,3,6,7',str(ROOT/'wrk/wrk'),'-t4','-c64','-d1s','--latency','--timeout','2s',f'http://{address}/'],capture_output=True,text=True,check=True,timeout=20)
            (a.output/'wrk.txt').write_text(client.stdout+client.stderr)
            assert not client.stderr,client.stderr
            assert 'Socket errors' not in client.stdout and 'Non-2xx' not in client.stdout,client.stdout
        finally:
            server.terminate();server.wait(timeout=10);server.stdout.close()
else:
    a.output.mkdir(parents=True,exist_ok=False)
    command=['sudo','-n','perf','record','-o',str(a.output/'perf.data')]
    if a.kind=='trace':
        command+=['-a','-m','1024']
        for event in EVENTS:command+=['-e',event]
    else:
        # CPU 0 contains the server worker; exclude the load generator CPUs.
        command+=['-C','0','-F','999','-g','--call-graph','dwarf,8192']
    command+=['--','taskset','-c','1','python3',str(Path(__file__).resolve()),'--child','--backend',a.backend,'--kind',a.kind,'--output',str(a.output)]
    (a.output/'command.json').write_text(json.dumps(command,indent=2)+'\n')
    with (a.output/'perf.stderr').open('w') as errors:
        subprocess.run(command,stderr=errors,check=True,timeout=45)
    subprocess.run(['sudo','-n','chown','-R',f'{os.getuid()}:{os.getgid()}',str(a.output)],check=True)
    if a.kind=='profile':
        for name,extra in [('self',['--no-children']),('tree',[])]:
            with (a.output/(name+'.txt')).open('w') as report:
                subprocess.run(['sudo','-n','perf','report','--stdio','-f','-i',str(a.output/'perf.data'),'--percent-limit','0.5','--comms',','.join(set(json.loads((a.output/'server.json').read_text())['threads'].values())),*extra],stdout=report,check=True)
    else:
        with (a.output/'trace.txt').open('w') as trace, (a.output/'script.stderr').open('w') as errors:
            subprocess.run(['sudo','-n','perf','script','-f','-i',str(a.output/'perf.data')],stdout=trace,stderr=errors,check=True)
        values=struct.unpack('<'+'Q'*(len(OPS)*len(METRICS)),(a.output/'counts.bin').read_bytes())
        counts={op:{key:values[i*len(METRICS)+j] for j,key in enumerate(METRICS)} for i,op in enumerate(OPS)}
        (a.output/'counts.json').write_text(json.dumps(counts,indent=2)+'\n')
        pattern=re.compile(r'^\s*(.*?)\s+(\d+)\s+\[\d+\]\s+[\d.]+:\s+(\S+):\s+(.*)$')
        events=[]
        for line in (a.output/'trace.txt').read_text().splitlines():
            assert 'LOST' not in line,line
            match=pattern.match(line)
            if match:events.append((match[1],int(match[2]),match[3],match[4]))
        creates=[{'comm':comm,'tid':tid,'ring':re.search(r'ring (0x\w+)',body)[1],'flags':int(re.search(r'flags (0x\w+)',body)[1],16)} for comm,tid,event,body in events if event=='io_uring:io_uring_create']
        server_info=json.loads((a.output/'server.json').read_text());pid=server_info['pid']
        assert creates and all(str(c['tid']) in server_info['threads'] for c in creates),creates
        # All server threads belong to this process, and it is the only candidate.
        rings={c['ring'] for c in creates};tids={c['tid'] for c in creates}
        ring_events=collections.Counter();submissions=collections.Counter();completions=collections.Counter();syscalls=collections.Counter();operations={};payload=collections.Counter();work=collections.Counter()
        for comm,tid,event,body in events:
            if event.startswith('syscalls:') and tid in tids:syscalls[event]+=1
            ring=re.search(r'ring (0x\w+)',body)
            if not ring or ring[1] not in rings:continue
            ring_events[event]+=1
            if event=='io_uring:io_uring_submit_req':
                token=int(re.search(r'user_data (0x\w+)',body)[1],16);opcode=re.search(r'opcode (\w+)',body)[1]
                operations[(ring[1],token)]=opcode;submissions[opcode]+=1
            elif event=='io_uring:io_uring_complete':
                token=int(re.search(r'user_data (0x\w+)',body)[1],16);result=int(re.search(r'result (-?\d+)',body)[1]);opcode=operations.get((ring[1],token),'UNKNOWN')
                completions[(opcode,'positive' if result>0 else str(result))]+=1
                if result>0 and opcode in ('RECV','SEND'):payload[opcode]+=result
            elif event=='io_uring:io_uring_local_work_run':
                work['batches']+=1;work['tasks']+=int(re.search(r'count (\d+)',body)[1])
        receive=[counts[k] for k in OPS[:5]];send=[counts[k] for k in OPS[5:10]]
        responses=sum(x['bytes_or_events'] for x in send)/78
        assert responses.is_integer() and responses>0,responses
        report={'instrumented_rates_excluded':True,'server_pid':pid,'ring_creation':creates,'ring_events':dict(ring_events),'submissions':dict(submissions),'completions':{str(k):v for k,v in completions.items()},'ring_payload_bytes':dict(payload),'creator_thread_syscalls':dict(syscalls),'local_work':dict(work),'complete_server_writes':int(responses),'receive_syscalls_per_response':sum(x['calls'] for x in receive)/responses,'receive_eagain_per_response':sum(x['would_block'] for x in receive)/responses,'send_syscalls_per_response':sum(x['calls'] for x in send)/responses,'submissions_per_response':{k:v/responses for k,v in submissions.items()},'completions_per_response':{str(k):v/responses for k,v in completions.items()},'ring_events_per_response':{k:v/responses for k,v in ring_events.items()}}
        assert not ring_events['io_uring:io_uring_cqe_overflow'],report
        (a.output/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2))
