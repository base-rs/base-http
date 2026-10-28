import json, statistics
from pathlib import Path
root=Path(__file__).resolve().parent
report={}
p=root/'fixed-240k/samples.jsonl'
if p.exists():
 rows=[json.loads(x) for x in p.read_text().splitlines()]
 names=sorted({r['variant'] for r in rows})
 fixed={}
 for name in names:
  group=[r for r in rows if r['variant']==name]
  fixed[name]={'samples':len(group),'medians':{f'{key}.{metric}':statistics.median(r[key][metric] for r in group) for key in ('wire_rtt','scheduled_latency','send_delay','dispatch_lag') for metric in ('mean_us','p50_us','p99_us','p999_us')},'p99_ranges':{key:[min(r[key]['p99_us'] for r in group),max(r[key]['p99_us'] for r in group)] for key in ('wire_rtt','scheduled_latency')}}
 paired={}
 for baseline in ('before','tokio'):
  comparisons={}
  pairs=[(a,next(b for b in rows if b['variant']==baseline and b['repetition']==a['repetition'])) for a in rows if a['variant']=='after']
  for key in ('wire_rtt','scheduled_latency'):
   for metric in ('mean_us','p50_us','p99_us'):
    ratios=[100*(a[key][metric]/b[key][metric]-1) for a,b in pairs]
    comparisons[f'{key}.{metric}']={'median_percent':statistics.median(ratios),'range_percent':[min(ratios),max(ratios)],'values_percent':ratios}
  paired['after_vs_'+baseline]=comparisons
 report['fixed']={'medians':fixed,'paired':paired,'validation':{'samples':len(rows),'measured_responses':sum(r['measured_responses_validated'] for r in rows),'all_responses':sum(r['total_responses_validated'] for r in rows),'payload_errors':sum(r['payload_validation_errors'] for r in rows),'max_drain_ms':max(r['drain_ms'] for r in rows),'max_target_deviation_percent':max(abs(r['target_delivery_fraction']-1)*100 for r in rows)}}
p=root/'unpaced/summary.json'
if p.exists(): report['unpaced']=json.loads(p.read_text())
(root/'analysis.json').write_text(json.dumps(report,indent=2)+'\n')
if 'fixed' in report:
 for name,entry in report['fixed']['medians'].items():
  m=entry['medians'];print(name, {k:round(v,3) for k,v in m.items() if '.p50' in k or '.p99_' in k})
 print('paired',json.dumps(report['fixed']['paired'],indent=2))
if 'unpaced' in report:
 for c in report['unpaced']:
  print('unpaced',c['concurrency'])
  for name,m in c['medians'].items(): print(name,{k:round(m[k],3) for k in ('requests_per_second','total_cpu_us_per_request','latency_p99_ms')})
  print('after/before', c['paired']['after_vs_before']['requests_per_second'])
