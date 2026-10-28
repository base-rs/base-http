#!/usr/bin/env python3
"""Validate off-host matrices and calculate paired throughput/latency changes."""
import argparse,itertools,json,statistics
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args()
meta=json.loads((a.directory/'environment.json').read_text());config=meta['configuration'];variants=list(meta['variants'])
rows=[json.loads(l) for l in (a.directory/'samples.jsonl').read_text().splitlines()]
expected=set(itertools.product(variants,config['workers'],config['connections'],range(1,config['repetitions']+1)))
seen=[(r['variant'],r['server_workers'],r['connections'],r['repetition']) for r in rows]
assert len(seen)==len(set(seen)) and set(seen)==expected
assert all(not p.read_text() for p in a.directory.glob('*.stderr'))
for r in rows:
 stem=f"{r['variant']}-w{r['server_workers']}-c{r['connections']}-b{r['bytes']}-r{r['repetition']}"
 raw=json.loads((a.directory/(stem+'.client.json')).read_text())
 assert r['requests']>0 and r['bytes']==config['bytes'] and r['server_usage']['returncode']==0
 assert all(r[k]==raw[k] for k in raw)
 u=r['server_usage'];r['server_cpu_equivalent_cores_whole_lifetime']=(u['user_seconds']+u['system_seconds'])/u['wall_seconds']
 r['client_cpu_equivalent_cores_whole_lifetime']=(r['client_user_seconds']+r['client_system_seconds'])/r['client_wall_seconds']
metrics=['requests_per_second','rtt_mean_us','rtt_p99_us','server_cpu_equivalent_cores_whole_lifetime','client_cpu_equivalent_cores_whole_lifetime']
results=[]
for workers,c in itertools.product(config['workers'],config['connections']):
 groups={v:sorted((r for r in rows if (r['variant'],r['server_workers'],r['connections'])==(v,workers,c)),key=lambda r:r['repetition']) for v in variants}
 item={'workers':workers,'connections':c,'medians':{v:{m:statistics.median(r[m] for r in g) for m in metrics} for v,g in groups.items()},'paired':{}}
 for v,ref in itertools.permutations(variants,2):
  paired={}
  for m in metrics[:3]:
   values=[100*(x[m]/y[m]-1) for x,y in zip(groups[v],groups[ref])]
   paired[m]={'median_percent':statistics.median(values),'values_percent':values}
  item['paired'][v+'_vs_'+ref]=paired
 results.append(item)
report={'samples':len(rows),'complete_matrix':True,'payload_validation_succeeded':True,'stderr_empty':True,'results':results,'scope':'Whole-lifetime CPU estimates; direct network can limit throughput independently of server.'}
(a.directory/'validated-summary.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
