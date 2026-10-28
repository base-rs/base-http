"""Recompute paired comparisons and validation without merging load levels."""
import json, statistics
from pathlib import Path
root=Path(__file__).resolve().parent
report={}
for directory in ('unpaced','fixed-240k','fixed-200k'):
 path=root/directory/'samples.jsonl'
 if not path.exists():continue
 rows=[json.loads(x) for x in path.read_text().splitlines()]
 names=sorted({r['variant'] for r in rows})
 load_key='concurrency' if directory=='unpaced' else 'rate'
 by_load=[]
 for load in sorted({r[load_key] for r in rows}):
  groups={name:sorted((r for r in rows if r['variant']==name and r[load_key]==load),key=lambda r:r['repetition']) for name in names}
  assert all([r['repetition'] for r in group]==list(range(1,6)) for group in groups.values()),[(n,len(g)) for n,g in groups.items()]
  if directory=='unpaced':
   metrics=('requests_per_second','total_cpu_us_per_request')
   value=lambda r,key:r[key]
  else:
   metrics=tuple(f'{k}.{m}' for k in ('wire_rtt','scheduled_latency') for m in ('mean_us','p50_us','p99_us'))
   value=lambda r,key:r[key.split('.')[0]][key.split('.')[1]]
  item={load_key:load,'medians':{},'paired':{}}
  for name,group in groups.items():item['medians'][name]={key:statistics.median(value(r,key) for r in group) for key in metrics}
  pairs=[(f'{mode}-tasks',f'{mode}-supervisor') for mode in ('owned','borrowed','tokio')]
  pairs += [('owned-supervisor','tokio-supervisor'),('owned-tasks','tokio-tasks')]
  for name,baseline in pairs:
   item['paired'][name+'_vs_'+baseline]={}
   for metric in metrics:
    ratios=[100*(value(x,metric)/value(y,metric)-1) for x,y in zip(groups[name],groups[baseline])]
    item['paired'][name+'_vs_'+baseline][metric]={'median_percent':statistics.median(ratios),'range_percent':[min(ratios),max(ratios)],'values_percent':ratios}
  by_load.append(item)
 if directory!='unpaced':
  validation=dict(samples=len(rows),measured_responses=sum(r['measured_responses_validated'] for r in rows),all_responses=sum(r['total_responses_validated'] for r in rows),payload_errors=sum(r['payload_validation_errors'] for r in rows),max_drain_ms=max(r['drain_ms'] for r in rows),max_target_deviation_percent=max(abs(r['target_delivery_fraction']-1)*100 for r in rows))
  assert validation['payload_errors']==0
  assert all(r['measured_responses_validated']==r['rate']*r['seconds'] for r in rows)
  report[directory]=dict(loads=by_load,validation=validation)
 else:report[directory]=dict(loads=by_load,samples=len(rows))
(root/'analysis.json').write_text(json.dumps(report,indent=2)+'\n')
for cohort,data in report.items():
 for item in data['loads']:
  print(cohort,item.get('concurrency',item.get('rate')))
  for name,medians in item['medians'].items():print(name,{k:round(v,3) for k,v in medians.items()})
  for pair,metrics in item['paired'].items():print(pair,{k:round(v['median_percent'],2) for k,v in metrics.items()})
 if 'validation' in data:print('validation',data['validation'])
