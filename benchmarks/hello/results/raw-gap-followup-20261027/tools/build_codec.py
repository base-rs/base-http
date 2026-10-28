#!/usr/bin/env python3
"""Build an in-memory codec diagnostic using the existing allocation probe I/O."""
from pathlib import Path
import hashlib,json,os,shutil,subprocess
ROOT=Path(__file__).resolve().parents[1];HERE=Path(__file__).resolve().parent;HELLO=ROOT/'sources/base-http/benchmarks/hello';TARGET=Path('/tmp/base-http-hana-epoll-uring-7nxlaica/target')
source=(HELLO/'src/bin/allocation_probe.rs').read_text().split('fn main() {')[0]
source=source.replace('struct Output(Arc<AtomicUsize>);','#[derive(Default)]\nstruct Stats { writes: AtomicUsize, bytes: AtomicUsize }\nstruct Output(Arc<Stats>);')
source=source.replace('self.0.fetch_add(1, Ordering::Relaxed);','self.0.writes.fetch_add(1, Ordering::Relaxed);\n        self.0.bytes.fetch_add(bytes.len(), Ordering::Relaxed);\n        std::hint::black_box(bytes);')
source+=(HERE/'codec_main.rs').read_text()
path=HELLO/'src/bin/codec_probe.rs';assert not path.exists();(HERE/'codec_probe.rs').write_text(source)
try:
 path.write_text(source)
 subprocess.run(['cargo','build','--release','--locked','--features','profile-tools','--manifest-path',str(HELLO/'Cargo.toml'),'--bin','codec_probe'],env=dict(os.environ,CARGO_TARGET_DIR=str(TARGET)),check=True)
 binary=ROOT/'binaries/codec-probe';shutil.copyfile(TARGET/'release/codec_probe',binary);binary.chmod(0o755)
 (HERE/'codec-provenance.json').write_text(json.dumps({'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()},indent=2)+'\n')
finally:path.unlink(missing_ok=True)
