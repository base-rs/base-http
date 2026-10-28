"""Check retained source/binary identity and exact analysis replay."""
import difflib
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def members(path):
    with tarfile.open(path) as archive:
        return {member.name: archive.extractfile(member).read()
                for member in archive if member.isfile()}


baseline = members(ROOT / 'production-source.tar.gz')
candidate = members(ROOT / 'candidate-source.tar.gz')
assert baseline.keys() == candidate.keys(), (baseline.keys() - candidate.keys(), candidate.keys() - baseline.keys())
changed = [name for name in baseline if baseline[name] != candidate[name]]
assert sorted(changed) == ['http/base-http/benchmarks/hello/Cargo.toml',
                           'scheduler/crates/iouring/src/io_uring_selector/socket_read.rs'], changed
runtime_path = 'scheduler/crates/iouring/src/io_uring_selector/socket_read.rs'
before = baseline[runtime_path].decode()
after = candidate[runtime_path].decode()
assert (ROOT / 'socket_read.before.rs').read_text() == before
assert after == before.replace('''            self.length,
        )
        .build()''', '''            self.length,
        )
        // Experimental IORING_RECVSEND_POLL_FIRST; not yet a portable policy.
        .ioprio(1)
        .build()''')
patch = ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True),
                fromfile='a/' + runtime_path.removeprefix('scheduler/'),
                tofile='b/' + runtime_path.removeprefix('scheduler/')))
assert patch == (ROOT / 'poll-first.patch').read_text()
assert digest(ROOT / 'binaries/baseline') == '09ac9a5a25d4d2b97f652bc1a58cedd92c6dab3435d22631af89891befdb2859'
assert digest(ROOT / 'binaries/client') == '45ff7d7ef8a0bd4cdd2280aae1706506b1ac7c0d180ca036a7bdbf5df9ef19c5'
for name in ('fixed', 'unpaced'):
    environment = json.loads((ROOT / name / 'environment.json').read_text())
    if name == 'fixed':
        hashes = {key: value['sha256'] for key, value in environment['binaries'].items()}
    else:
        hashes = environment['binary_sha256']
    for variant, binary in (('owned', 'control'), ('poll-first', 'poll-first'), ('tokio', 'control')):
        assert hashes[variant] == digest(ROOT / 'binaries' / binary)
original = (ROOT / 'analysis.json').read_bytes()
subprocess.run(['python3', str(ROOT / 'analyze.py')], check=True)
assert original == (ROOT / 'analysis.json').read_bytes()
assert 'base_http_lifecycle' not in (ROOT / 'probes-after-cleanup.txt').read_text()
assert not (ROOT / 'kprobe-profile.txt').read_text().strip()
print(json.dumps(dict(source_changes=changed, frozen_binary_hashes=True,
                      timing_hashes=True, exact_analysis_replay=True,
                      probe_cleanup=True, kretprobe_misses='inventory unavailable'), indent=2))
