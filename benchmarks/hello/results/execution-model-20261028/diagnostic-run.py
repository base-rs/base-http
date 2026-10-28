import json, subprocess, select
from pathlib import Path
root=Path(__file__).resolve().parent
for variant in ('baseline','candidate'):
 for backend in ('base-iouring-owned-raw','base-iouring-raw'):
  stem=f'diagnostic-{variant}-{backend}'
  with (root/f'{stem}.stderr').open('w') as err:
   server=subprocess.Popen(['taskset','-c','0',str(root/'binaries'/f'diagnostic-{variant}'),backend],stdout=subprocess.PIPE,stderr=err,text=True)
   try:
    assert select.select([server.stdout],[],[],10)[0]
    address=server.stdout.readline().strip()
    client=subprocess.run(['taskset','-c','1',str(root/'binaries/client'),address,'64','240000','1','4','2,3,6,7'],capture_output=True,text=True,timeout=35)
    assert client.returncode==0,(client.returncode,client.stderr)
    (root/f'{stem}.json').write_text(client.stdout)
   finally:
    server.terminate();server.wait(timeout=10);server.stdout.close()
  lines=(root/f'{stem}.stderr').read_text().splitlines()
  diag=[json.loads(x.removeprefix('DIAG ')) for x in lines if x.startswith('DIAG ')]
  assert diag,lines
  poll=[json.loads(x.removeprefix('POLL ')) for x in lines if x.startswith('POLL ')]
  queue=[json.loads(x.removeprefix('QUEUE ')) for x in lines if x.startswith('QUEUE ')]
  print(stem,diag[-1],{'poll':poll[-1] if poll else None,'queue':queue[-1] if queue else None},flush=True)
