#!/usr/bin/env python3
"""Collect this investigation after all timing, including compressed perf data."""
from pathlib import Path
import hashlib,json,os,shutil,tarfile
ROOT=Path(__file__).resolve().parents[1];OUT=ROOT/'export-raw-gap-followup'
OUT.mkdir(exist_ok=True);data=[]
def copy_tree(source,destination):
 destination.mkdir(parents=True,exist_ok=True)
 for path in source.rglob('*'):
  if not path.is_file():continue
  if path.suffix=='.data':data.append(path);continue
  target=destination/path.relative_to(source);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
for name in ['oob-raw-perf-c64','oob-cpu-stat-c64','oob-socket-counts','oob-poll-counts','oob-fixed-stat','oob-fixed-stat-50k','oob-user-perf','oob-codec-perf']:
 copy_tree(ROOT/'diagnostics'/name,OUT/'diagnostics'/name)
for name in ['oob-raw-screen','oob-http-context','oob-inline-screen','oob-inline-confirmation','oob-fields-http','oob-fields-confirmation']:
 copy_tree(ROOT/'results'/name,OUT/'benchmark'/name)
 for line in (ROOT/'results'/name/'samples.jsonl').read_text().splitlines():
  row=json.loads(line);assert row['errors']==0
  assert len(row['validation'])==3 and all(x['status']==200 and x['body']=='Hello, world!' for x in row['validation'])
for path in (ROOT/'diagnostics').glob('*'):
 if path.is_file() and (path.name.startswith(('raw-followup-','fields-')) or path.name in ['oob-codec-comparison.json','oob-codec-phases.json','oob-fields-codec.json'] or path.name.startswith(('oob-raw-screen.','oob-http-context.','oob-inline-screen.','oob-inline-confirmation.','oob-fields-http.','oob-fields-confirmation.','oob-fixed-stat.','oob-fixed-stat-50k.','oob-user-perf.'))):
  shutil.copy2(path,OUT/'diagnostics'/path.name)
copy_tree(ROOT/'raw-followup-tools',OUT/'tools')
for name in ['benchmark.py','profile.py']:shutil.copy2(ROOT/name,OUT/name)
for name in ['base-http','base-http1','base-http-server']:
 source=ROOT/'sources'/name
 copy_tree(source/'src',OUT/'sources'/name/'src');copy_tree(source/'tests',OUT/'sources'/name/'tests')
 for filename in ['Cargo.toml','Cargo.lock']:
  if (source/filename).exists():shutil.copy2(source/filename,OUT/'sources'/name/filename)
copy_tree(ROOT/'sources/base-http/benchmarks/hello/src',OUT/'sources/base-http/benchmarks/hello/src')
for name in ['Cargo.toml','Cargo.lock','run.py']:shutil.copy2(ROOT/'sources/base-http/benchmarks/hello'/name,OUT/'sources/base-http/benchmarks/hello'/name)
binary_hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'binaries').iterdir() if p.is_file()}
names=['oob-default','raw-followup-before-fields','raw-followup-retained','codec-probe-before-fields','codec-probe-fields']
if (OUT/'perf-and-binaries.tar.gz').exists():
 previous=json.loads((OUT/'binary-sha256.json').read_text())
 assert all(previous[name]==binary_hashes[name] for name in names)
else:
 with tarfile.open(OUT/'perf-and-binaries.tar.gz','w:gz') as archive:
  for path in data:archive.add(path,arcname=str(path.relative_to(ROOT)))
  for name in names:archive.add(ROOT/'binaries'/name,arcname='binaries/'+name)
(OUT/'binary-sha256.json').write_text(json.dumps(binary_hashes,indent=2)+'\n')
uid=ROOT.stat().st_uid;gid=ROOT.stat().st_gid
for path in [OUT,*OUT.rglob('*')]:os.chown(path,uid,gid)
print('collected',len(data),'perf recordings',sum(p.stat().st_size for p in OUT.rglob('*') if p.is_file()),'bytes',flush=True)
