#!/usr/bin/env python3
"""Validate complete echo/HTTP cohorts and compare each pair within rounds."""
import argparse, itertools, json, statistics
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('root',type=Path)
p.add_argument('--echo-cohorts',nargs='+',default=['confirmation','single'])
p.add_argument('--http-cohorts',nargs='*',default=[])
a=p.parse_args();report={}
def change(x,y):
    values=[100*(u/v-1) for u,v in zip(x,y)]
    return {'median_percent':statistics.median(values),'min_percent':min(values),'max_percent':max(values),'values_percent':values}
for name in a.echo_cohorts:
    directory=a.root/name
    meta=json.loads((directory/'environment.json').read_text());config=meta['configuration']
    rows=[json.loads(l) for l in (directory/'samples.jsonl').read_text().splitlines()]
    seen=[(r['backend'],r['connections'],r['bytes'],r['repetition']) for r in rows]
    expected=set(itertools.product(config['backends'],config['connections'],config['bytes'],range(1,config['repetitions']+1)))
    assert len(seen)==len(set(seen)) and set(seen)==expected,name
    assert not meta['instrumented'],name
    assert all(r['requests']>0 for r in rows),name
    assert all(not f.read_text() for f in directory.glob('*.stderr')),name
    for r in rows:
        client=json.loads((directory/f"{r['backend']}-c{r['connections']}-b{r['bytes']}-r{r['repetition']}.client.json").read_text())
        assert all(client[k]==r[k] for k in ['requests','connections','bytes','requests_per_second']),name
    results=[]
    for c,b in itertools.product(config['connections'],config['bytes']):
        groups={k:sorted((r for r in rows if (r['backend'],r['connections'],r['bytes'])==(k,c,b)),key=lambda r:r['repetition']) for k in config['backends']}
        metrics=['requests_per_second','rtt_mean_us','rtt_p50_us','rtt_p99_us','server_process_cpu_percent']
        item={'connections':c,'bytes':b,'medians':{k:{m:statistics.median(r[m] for r in g) for m in metrics} for k,g in groups.items()},'paired':{}}
        for k,ref in itertools.permutations(groups,2):
            item['paired'][k+'_vs_'+ref]={m:change([r[m] for r in groups[k]],[r[m] for r in groups[ref]]) for m in metrics if m!='server_process_cpu_percent'}
        results.append(item)
    report[name]={'samples':len(rows),'complete_matrix':True,'all_payload_validating_clients_succeeded':True,'all_stderr_empty':True,'instrumented':False,'results':results}
for name in a.http_cohorts:
    directory=a.root/name
    meta=json.loads((directory/'environment.json').read_text());config=meta['configuration']
    rows=[json.loads(l) for l in (directory/'samples.jsonl').read_text().splitlines()]
    variants={v.split(':')[0] for v in config['variants']}
    seen=[(r['variant'],r['concurrency'],r['repetition']) for r in rows]
    assert len(seen)==len(set(seen)) and set(seen)==set(itertools.product(variants,config['concurrency'],range(1,config['repetitions']+1)))
    for f in directory.glob('*/*.txt'):
        text=f.read_text()
        assert 'Socket errors:' not in text and 'Non-2xx or 3xx responses:' not in text,f
    resets={};tails={};reset='connection: Connection reset by peer (os error 104)'
    for f in directory.glob('*/*.server.stderr'):
        text=f.read_text();lines=text.splitlines()
        if lines and not text.endswith('\n'):
            tail=lines.pop();assert reset.startswith(tail),f;tails[str(f.relative_to(directory))]=tail
        assert all(l==reset for l in lines),f
        resets[str(f.relative_to(directory))]=len(lines)
    assert all(r['errors']==0 and r['requests']>0 and len(r['validation'])==3 for r in rows)
    report[name]={'samples':len(rows),'complete_matrix':True,'wrk_socket_or_status_errors':False,'server_peer_reset_reports':resets,'unterminated_reset_log_tails':tails,'results':json.loads((directory/'summary.json').read_text())}
(a.root/'validated-summary.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v['samples'] for k,v in report.items()}))
