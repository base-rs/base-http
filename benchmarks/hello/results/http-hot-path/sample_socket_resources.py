from pathlib import Path
from collections import Counter
import json,datetime,resource
rows=[line.split() for line in Path('/tmp/base-http-tcp-resource-snapshot.txt').read_text().splitlines() if line.lstrip().startswith('tcp')]
out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'tcp_entries':len(rows),'states':dict(Counter(row[5] for row in rows)),'tcp_local_ports':len({row[3].rsplit('.',1)[-1] for row in rows}),'maximum_receive_queue':max((int(row[1]) for row in rows),default=0),'maximum_send_queue':max((int(row[2]) for row in rows),default=0),'system_files':Path('/tmp/base-http-file-resource-snapshot.txt').read_text(),'mbufs':Path('/tmp/base-http-mbuf-resource-snapshot.txt').read_text(),'inherited_rlimit_nofile':resource.getrlimit(resource.RLIMIT_NOFILE)}
with (Path(__file__).parent/'timeout-diagnostic-resources.jsonl').open('a') as f:f.write(json.dumps(out)+'\n')
print({key:out[key] for key in ['utc','tcp_entries','states']})
