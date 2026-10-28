"""Rerun frozen binaries, retaining their original measured source snapshot."""
from pathlib import Path
import hashlib
import importlib.util
import json
import shutil
import sys

ROOT = Path('/Users/samuel/Developer/base-rs/base-http')
HARNESS = ROOT / 'benchmarks/client/run.py'
BUILD = ROOT / 'benchmarks/client/results/orchestration-final-full-20261029'
metadata = json.loads((BUILD / 'metadata.json').read_text())
binary = Path('/tmp/base-http-orchestration-final-20261029')
assert hashlib.sha256(binary.read_bytes()).hexdigest() == metadata['binary_sha256']
baseline = Path('/tmp/base-http-exchange-reuse-final-20261029')
assert hashlib.sha256(baseline.read_bytes()).hexdigest() == 'd6e9658443587be61afa277065f39c962c23e28d5196581eb3d07fd091c42329'
spec = importlib.util.spec_from_file_location('harness', HARNESS)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

def frozen_sources(output, cargo_config=None):
    for name in ['sources.tar.gz', 'source-hashes.json']:
        shutil.copyfile(BUILD / name, output / name)
    measured = output / 'measured-build'
    measured.mkdir()
    shutil.copyfile(BUILD / 'metadata.json', measured / 'metadata.json')
    shutil.copyfile(HARNESS, output / 'harness.py')
    shutil.copyfile(__file__, output / 'rerun.py')
    (output / 'provenance.json').write_text(json.dumps({
        'measured_sources': str(BUILD / 'sources.tar.gz'),
        'source_archive_sha256': hashlib.sha256((BUILD / 'sources.tar.gz').read_bytes()).hexdigest(),
        'note': 'Frozen executables and their original measured sources; no rebuild or dependency override changes.',
    }, indent=2) + '\n')

harness.snapshot_sources = frozen_sources
harness.main()
