"""Separate single-worker traces; every sample begins after a successful receive."""
import json, select, subprocess, time
from pathlib import Path
root=Path(__file__).resolve().parent
random=0x4d595df4d0f33173;expected=0
for _ in range(1000000):
 random=(random*6364136223846793005+1442695040888963407)&((1<<64)-1)
 expected+=random>>57==0
validations=[]
for repetition in range(1,4):
 for topology,backend in [('supervisor','base-iouring-owned-raw'),('tasks','base-iouring-owned-connections-raw')]:
  stem=f'diagnostic-low-{topology}-r{repetition}'
  with (root/f'{stem}.stderr').open('w') as errors:
   server=subprocess.Popen(['taskset','-c','0',str(root/'binaries/diagnostic'),backend],stdout=subprocess.PIPE,stderr=errors,text=True)
   try:
    assert select.select([server.stdout],[],[],10)[0]
    address=server.stdout.readline().strip();assert address.startswith('127.0.0.1:')
    client=subprocess.run(['taskset','-c','1',str(root/'binaries/client'),address,'64','200000','1','4','2,3,6,7'],capture_output=True,text=True,timeout=35)
    (root/f'{stem}.client.json').write_text(client.stdout)
    (root/f'{stem}.client.stderr').write_text(client.stderr)
    assert client.returncode==0,(client.returncode,client.stderr)
    data=json.loads(client.stdout)
    assert data['payload_validation_errors']==0
    assert data['total_responses_validated']==1000000
    assert data['measured_responses_validated']==800000
    # Wait for EOF to flush the final partial block. Exact sample count follows
    # the deterministic sampler and the byte-validated successful-read count.
    deadline=time.monotonic()+5
    while True:
     lines=[x.rstrip('\n') for x in (root/f'{stem}.stderr').read_text().splitlines(keepends=True) if x.endswith('\n')]
     assert all(x.startswith('TRACE ') for x in lines),lines[-3:]
     observed=sum(len(json.loads(x.removeprefix('TRACE '))) for x in lines)
     if observed==expected:break
     assert observed<expected,(observed,expected)
     assert time.monotonic()<deadline,(observed,expected)
     time.sleep(0.01)
    validations.append(dict(run=stem,responses=data['total_responses_validated'],measured_responses=data['measured_responses_validated'],payload_errors=data['payload_validation_errors'],expected_samples=expected,observed_samples=observed))
    print(stem,'responses',data['total_responses_validated'],'samples',observed,flush=True)
   finally:
    server.terminate();server.wait(timeout=10);server.stdout.close()
  lines=(root/f'{stem}.stderr').read_text().splitlines()
  assert lines and all(x.startswith('TRACE ') for x in lines),lines[-3:]

(root/'diagnostic-low-validation.json').write_text(json.dumps(validations,indent=2)+'\n')
