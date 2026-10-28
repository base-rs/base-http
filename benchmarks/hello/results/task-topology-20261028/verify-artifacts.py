"""Replay summaries, verify frozen binaries and reconstruct exact probe sources."""
import argparse, gzip, hashlib, io, json, subprocess, tarfile, tempfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--scheduler',type=Path,required=True);a=p.parse_args()
root=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
expected={}
for name in ('production-binaries.sha256','diagnostic-binaries.sha256'):
 for line in (root/name).read_text().splitlines():
  digest,path=line.split();key=Path(path).name
  if key in expected:assert expected[key]==digest
  expected[key]=digest
binaries={name:sha(gzip.decompress((root/'retained-binaries'/f'{name}.gz').read_bytes())) for name in ('topology','client','diagnostic','wrk')}
assert binaries==expected
for cohort in ('fixed-200k','fixed-240k'):
 env=json.loads((root/cohort/'environment.json').read_text())
 assert all(v['sha256']==binaries['topology'] for k,v in env['binaries'].items() if k!='client')
 assert env['binaries']['client']['sha256']==binaries['client']
env=json.loads((root/'unpaced/environment.json').read_text())
assert all(v==binaries['topology'] for v in env['binary_sha256'].values())
production=tarfile.open(root/'production-source.tar.gz')
diagnostic=tarfile.open(root/'diagnostic-source.tar.gz')
production_bytes={m.name:production.extractfile(m).read() for m in production.getmembers() if m.isfile()}
diagnostic_bytes={m.name:diagnostic.extractfile(m).read() for m in diagnostic.getmembers() if m.isfile()}
pairs={'io_uring_selector.rs':'scheduler/crates/iouring/src/io_uring_selector.rs','socket_read.rs':'scheduler/crates/iouring/src/io_uring_selector/socket_read.rs','io_uring_worker.rs':'scheduler/crates/iouring/src/io_uring_worker.rs','iouring-lib.rs':'scheduler/crates/iouring/src/lib.rs','raw.rs':'http/base-http/benchmarks/hello/src/raw.rs','main.rs':'http/base-http/benchmarks/hello/src/main.rs'}
for name,path in pairs.items():assert (root/'production-sources'/name).read_bytes()==production_bytes[path]
baseline='5cc179985803e1da812353df73868db668234202'
git_tar=subprocess.check_output(['git','-C',str(a.scheduler),'archive',baseline])
tracked=tarfile.open(fileobj=io.BytesIO(git_tar));verified=0
for member in tracked.getmembers():
 if member.isfile():
  assert tracked.extractfile(member).read()==production_bytes['scheduler/'+member.name],member.name
  verified+=1
with tempfile.TemporaryDirectory(prefix='base-http-topology-artifact-') as d:
 dest=Path(d);production.extractall(dest,filter='data')
 patch=subprocess.run(['patch','--dry-run','-p1','-i',str(root/'diagnostic.patch')],cwd=dest,capture_output=True,text=True)
 (root/'diagnostic-patch-verification.log').write_text(patch.stdout+patch.stderr);patch.check_returncode()
 (dest/'diagnostic.rs').write_bytes((root/'diagnostic.rs').read_bytes())
 subprocess.run(['python3',str(root/'make-diagnostics.py'),str(dest)],check=True)
 for path in [*pairs.values(),'scheduler/crates/iouring/src/diagnostic.rs']:
  assert (dest/path).read_bytes()==diagnostic_bytes[path],path
for script,output in [('summarize.py','analysis.json'),('summarize-diagnostics.py','diagnostic-analysis.json')]:
 before=(root/output).read_bytes()
 run=subprocess.run(['python3',str(root/script)],capture_output=True,text=True);run.check_returncode()
 (root/(script.removesuffix('.py')+'-replay.log')).write_text(run.stdout+run.stderr)
 assert (root/output).read_bytes()==before,output
analysis=json.loads((root/'analysis.json').read_text());traces=json.loads((root/'diagnostic-analysis.json').read_text())
validations=json.loads((root/'diagnostic-validation.json').read_text())+json.loads((root/'diagnostic-low-validation.json').read_text())
assert len(traces)==len(validations)==12
for row in validations:
 trace=next(t for t in traces if t['run']==row['run'])
 assert trace['samples']==row['observed_samples']==row['expected_samples']
 assert row['payload_errors']==0
for cohort in ('fixed-200k','fixed-240k'):
 assert all(not path.read_bytes() for path in (root/cohort).glob('*.stderr'))
rows=[json.loads(x) for x in (root/'unpaced/samples.jsonl').read_text().splitlines()]
assert len(rows)==60 and all(row['errors']==0 for row in rows)
result=dict(baseline=baseline,verified_scheduler_files=verified,binaries=binaries,diagnostic_source_files_reconstructed=7,summaries_replay_byte_for_byte=True,unpaced_samples=len(rows),unpaced_reported_wrk_errors=0,fixed_samples=sum(analysis[c]['validation']['samples'] for c in ('fixed-200k','fixed-240k')),fixed_responses=sum(analysis[c]['validation']['all_responses'] for c in ('fixed-200k','fixed-240k')),diagnostic_runs=len(validations),diagnostic_samples=sum(t['samples'] for t in traces),diagnostic_responses=sum(v['responses'] for v in validations))
(root/'artifact-verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
