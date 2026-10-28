import json, statistics
from pathlib import Path
root=Path(__file__).resolve().parent
names=['publication_to_cq_end','cq_end_to_latest_driver_return','latest_driver_return_to_task_poll','task_poll_to_connection_poll','connection_poll_to_consumption','consumption_to_read_return','read_return_to_response_written','publication_to_response_written']
def describe(values):
 values=sorted(values);n=len(values)
 return dict(samples=n,mean_ns=statistics.mean(values),p50_ns=values[(n-1)//2],p99_ns=values[((n*99+99)//100)-1],max_ns=values[-1])
report=[]
for path in sorted(root.glob('diagnostic-*-r*.stderr')):
 if path.name.endswith('.client.stderr'):continue
 lines=path.read_text().splitlines();assert all(x.startswith('TRACE ') for x in lines),path
 rows=[row for line in lines for row in json.loads(line.removeprefix('TRACE '))]
 assert rows,path
 for row in rows:assert len(row)==10 and sum(row[:7])==row[7] and all(x>=0 for x in row)
 report.append(dict(run=path.stem,samples=len(rows),stages={name:describe([r[i] for r in rows]) for i,name in enumerate(names)},inner_poll_index=dict(mean=statistics.mean(r[8] for r in rows),max=max(r[8] for r in rows)),cq_batch_len=dict(mean=statistics.mean(r[9] for r in rows),max=max(r[9] for r in rows))))
(root/'diagnostic-analysis.json').write_text(json.dumps(report,indent=2)+'\n')
for r in report:
 print(r['run'],r['samples'],{k:round(v['mean_ns']/1000,3) for k,v in r['stages'].items()},r['inner_poll_index'],r['cq_batch_len'])
