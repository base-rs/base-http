"""Summarize retained cumulative diagnostic prefixes, never production timings."""
import json
from pathlib import Path
r=Path(__file__).resolve().parent
limits=[1,2,4,8,16,32,64,128,256,512,1024,2048,None]
def distribution(row,count,total,maximum,buckets):
 n=row[count]
 return {'samples':n,'mean_us':row[total]/n/1000 if n else None,
         'max_us':row[maximum]/1000,'buckets_le_us':list(zip(limits,row[buckets]))}
report={}
for p in sorted(r.glob('diagnostic-*.stderr')):
 lines=p.read_text().splitlines()
 def last(prefix):
  values=[json.loads(x[len(prefix):]) for x in lines if x.startswith(prefix)]
  return values[-1] if values else None
 d=last('DIAG ');poll=last('POLL ');queue=last('QUEUE ')
 if d is None:continue
 report[p.stem]={'raw':{'counter':d,'poll':poll,'queue':queue},
   'deferred_batches_percent':100*d['deferred_batches']/d['payload_batches'] if d['payload_batches'] else None,
   'deferred_requests_percent':100*d['deferred_requests']/d['payload_requests'] if d['payload_requests'] else None,
   'stage_to_entry_attempt':distribution(d,'delays','delay_sum_ns','max_delay_ns','delay_buckets_le_us'),
   'publication_to_consumption':distribution(poll,'samples','sum_ns','max_ns','buckets_le_us'),
   'queue_to_preparation':distribution(queue,'samples','sum_ns','max_ns','buckets_le_us')}
(r/'diagnostic-analysis.json').write_text(json.dumps(report,indent=2)+'\n')
for name,row in report.items():
 if row['deferred_requests_percent'] is None:continue
 print(name, {key:row[key]['mean_us'] for key in ('stage_to_entry_attempt','queue_to_preparation','publication_to_consumption')})
