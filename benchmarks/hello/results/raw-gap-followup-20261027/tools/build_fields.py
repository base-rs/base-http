#!/usr/bin/env python3
"""Freeze field-helper inlining candidate; always restore staged source/binary."""
from pathlib import Path
import difflib,hashlib,json,os,shutil,subprocess
ROOT=Path(__file__).resolve().parents[1];HERE=Path(__file__).resolve().parent;SNAPSHOT=HERE/'fields';SNAPSHOT.mkdir(exist_ok=True)
SYNTAX=ROOT/'sources/base-http/src/syntax.rs';FIELDS=ROOT/'sources/base-http/src/fields.rs'
original={p:p.read_bytes() for p in [SYNTAX,FIELDS]}
binary=ROOT/'binaries/codec-probe';saved=ROOT/'binaries/codec-probe-before-fields';shutil.copyfile(binary,saved);saved.chmod(0o755)
server=ROOT/'binaries/raw-followup-parity';saved_server=ROOT/'binaries/raw-followup-before-fields';shutil.copyfile(server,saved_server);saved_server.chmod(0o755)
server_provenance=(HERE/'parity/provenance.json').read_bytes()
provenance=(HERE/'codec-provenance.json').read_bytes();(SNAPSHOT/'before-codec-provenance.json').write_bytes(provenance)
try:
    syntax=original[SYNTAX].decode();assert syntax.count('pub(crate) fn is_token(')==1
    SYNTAX.write_text(syntax.replace('pub(crate) fn is_token(','#[inline]\npub(crate) fn is_token('))
    fields=original[FIELDS].decode()
    for old,new in [
        ('impl NameStorage {\n    fn new(', 'impl NameStorage {\n    #[inline]\n    fn new('),
        ('impl ValueStorage {\n    fn new(', 'impl ValueStorage {\n    #[inline]\n    fn new('),
        ('    pub fn new(name: impl AsRef<str>, value: impl AsRef<[u8]>)', '    #[inline]\n    pub fn new(name: impl AsRef<str>, value: impl AsRef<[u8]>)'),
        ('    pub fn new() -> Self {\n        Self::default()', '    #[inline]\n    pub fn new() -> Self {\n        Self::default()'),
        ('    pub fn add(\n', '    #[inline]\n    pub fn add(\n'),
        ('    pub fn append(&mut self, field: Field)', '    #[inline]\n    pub fn append(&mut self, field: Field)'),
    ]:
        assert fields.count(old)==1,old;fields=fields.replace(old,new)
    FIELDS.write_text(fields)
    changes=[]
    for path,before in original.items():
        (SNAPSHOT/path.name).write_bytes(path.read_bytes())
        changes.extend(difflib.unified_diff(before.decode().splitlines(keepends=True),path.read_text().splitlines(keepends=True),fromfile='before/'+path.name,tofile='fields/'+path.name))
    (SNAPSHOT/'change.diff').write_text(''.join(changes))
    subprocess.run(['python3',str(HERE/'build_codec.py')],check=True)
    shutil.copyfile(binary,ROOT/'binaries/codec-probe-fields');(ROOT/'binaries/codec-probe-fields').chmod(0o755)
    (SNAPSHOT/'after-codec-provenance.json').write_bytes((HERE/'codec-provenance.json').read_bytes())
    subprocess.run(['python3',str(HERE/'build.py'),'parity'],check=True)
    shutil.copyfile(ROOT/'binaries/raw-followup-parity',ROOT/'binaries/raw-followup-fields');(ROOT/'binaries/raw-followup-fields').chmod(0o755)
    (SNAPSHOT/'provenance.json').write_text(json.dumps({'source_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in original},'codec_sha256':hashlib.sha256((ROOT/'binaries/codec-probe-fields').read_bytes()).hexdigest(),'server_sha256':hashlib.sha256((ROOT/'binaries/raw-followup-fields').read_bytes()).hexdigest()},indent=2)+'\n')
finally:
    for path,data in original.items():path.write_bytes(data)
    shutil.copyfile(saved,binary);(HERE/'codec-provenance.json').write_bytes(provenance)
    shutil.copyfile(saved_server,server);(HERE/'parity/provenance.json').write_bytes(server_provenance)
