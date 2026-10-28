from pathlib import Path
import sys,json,datetime
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from run import sample
root=Path(__file__).resolve().parent
out=root/'timeout-diagnostic'
out.mkdir(exist_ok=True)
args=SimpleNamespace(binary=root/'before-server',wrk=Path((root/'diagnostic-wrk-path.txt').read_text().strip())/'wrk',output=out,threads=4,warmup=1,duration=8)
for repetition in range(1,13):
 print('diagnostic repetition',repetition,flush=True)
 try:
  result=sample(args,'base-native',64,repetition)
 except Exception as error:
  (out/'failure.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'repetition':repetition,'exception':repr(error)},indent=2)+'\n')
  print(error,flush=True)
  raise SystemExit(1)
 else:
  with (out/'samples.jsonl').open('a') as f:f.write(json.dumps(result)+'\n')
