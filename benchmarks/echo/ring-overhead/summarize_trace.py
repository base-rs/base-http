#!/usr/bin/env python3
"""Summarize system-wide ring traces by ring identity; exclude other processes."""
import argparse, collections, json, re, struct
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args()
lines=(a.directory/'trace.txt').read_text().splitlines()
pattern=re.compile(r'^\s*(.*?)\s+(\d+)\s+\[\d+\]\s+[\d.]+:\s+(\S+):\s+(.*)$')
events=[]
task_work_counts=collections.Counter()
local_work_counts=collections.Counter()
for line in lines:
    m=pattern.match(line)
    if m: events.append((m[1],int(m[2]),m[3],m[4]))
    else: assert not line.strip() or 'LOST' not in line, line
creates=[{'comm':comm,'tid':tid,'ring':re.search(r'ring (0x\w+)',body)[1],'flags':int(re.search(r'flags (0x\w+)',body)[1],16)} for comm,tid,event,body in events if event=='io_uring:io_uring_create']
assert creates
# The only ring-creating program during these isolated diagnostics must be the selected server. Tokio clients do not create rings. Preserve identities so this assumption can be audited.
assert all(c['comm'] in ['after-final','base-native-tas','base-http-ring-','minimal-final','deferred-final-'] for c in creates), creates
rings={c['ring'] for c in creates};tids={c['tid'] for c in creates}
submissions=collections.Counter();completions=collections.Counter();ring_events=collections.Counter();syscalls=collections.Counter();tokens=collections.defaultdict(collections.Counter)
mini=all(c['comm'] in ['base-http-ring-','minimal-final'] for c in creates)
operations={};ring_bytes=collections.Counter()
def category(token):
    if mini:return 'write_poll' if token&(1<<32) else 'read_poll'
    if token==2**64-1:return 'notifier'
    if token&(1<<63):return 'cancel'
    return 'monitor' if token&1 else 'operation'
for comm,tid,event,body in events:
    if event=='io_uring:io_uring_task_work_run' and tid in tids:
        task_work_counts['batches']+=1
        task_work_counts['tasks']+=int(re.search(r'count (\d+)',body)[1])
    if event.startswith('syscalls:') and tid in tids:
        syscalls[event]+=1
    m=re.search(r'ring (0x\w+)',body)
    if not m or m[1] not in rings: continue
    ring_events[event]+=1
    if event=='io_uring:io_uring_local_work_run':
        local_work_counts['batches']+=1
        local_work_counts['tasks']+=int(re.search(r'count (\d+)',body)[1])
        local_work_counts['loops']+=int(re.search(r'loops (\d+)',body)[1])
    if event=='io_uring:io_uring_submit_req':
        token=int(re.search(r'user_data (0x\w+)',body)[1],16)
        op=re.search(r'opcode (\w+)',body)[1]
        operations[(m[1],token)]=op
        submissions[(op,category(token))]+=1
        tokens[op][str(token)]+=1
    elif event=='io_uring:io_uring_complete':
        token=int(re.search(r'user_data (0x\w+)',body)[1],16)
        result=int(re.search(r'result (-?\d+)',body)[1])
        flags=int(re.search(r'cflags (0x\w+)',body)[1],16)
        completions[(category(token),result,flags)]+=1
        if result>0:
            op=operations.get((m[1],token))
            if op in ['RECV','SEND']:ring_bytes[op]+=result
report={'ring_creation':creates,'ring_events':dict(ring_events),'creator_thread_syscalls':dict(syscalls),'creator_thread_task_work':dict(task_work_counts),'ring_local_work':dict(local_work_counts),'submissions':[{'opcode':op,'category':category,'count':n} for (op,category),n in sorted(submissions.items())], 'completions':[{'category':category,'result':result,'flags':flags,'more':bool(flags&2),'count':n} for (category,result,flags),n in sorted(completions.items())], 'submission_counts_by_token':{op:dict(v) for op,v in tokens.items()},'instrumented_throughput_excluded':True}
report['scalar_ring_payload_bytes']=dict(ring_bytes)
countfile=a.directory/'counts.json'
if not countfile.exists() and (a.directory/'counts.bin').exists():
    ops=['read','readv','recv','recvfrom','recvmsg','write','writev','send','sendto','sendmsg','epoll_wait','epoll_pwait','epoll_pwait2','epoll_ctl','accept','accept4']
    metrics=['calls','positive','would_block','errors','bytes_or_events','zero']
    values=struct.unpack('<'+'Q'*(len(ops)*len(metrics)),(a.directory/'counts.bin').read_bytes())
    countfile.write_text(json.dumps({op:{key:values[i*len(metrics)+j] for j,key in enumerate(metrics)} for i,op in enumerate(ops)},indent=2)+'\n')
if countfile.exists():
    calls=json.loads(countfile.read_text());receive=[calls[k] for k in ['read','readv','recv','recvfrom','recvmsg']];send=[calls[k] for k in ['write','writev','send','sendto','sendmsg']]
    received=sum(x['bytes_or_events'] for x in receive)+ring_bytes['RECV']
    sent=sum(x['bytes_or_events'] for x in send)+ring_bytes['SEND']
    assert received==sent,(received,sent)
    payload=json.loads((a.directory/'client.json').read_text())['bytes']
    total=received;assert total%payload==0
    echoes=total//payload
    report['echoes_including_warmup_and_boundaries']=echoes
    report['receive_calls_per_echo']=sum(x['calls'] for x in receive)/echoes
    report['send_calls_per_echo']=sum(x['calls'] for x in send)/echoes
    report['ring_events_per_echo']={k:v/echoes for k,v in ring_events.items()}
(a.directory/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ['submission_counts_by_token']},indent=2))
