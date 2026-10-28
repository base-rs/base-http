#!/usr/bin/env python3
"""Freeze private test binaries for an isolated cross-thread routing diagnostic.

Temporarily installs the same fixture in each source revision, then restores all
working sources before returning. Run with no other build/test/benchmark active.
"""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

here = Path(__file__).resolve().parent
scheduler = here.parents[6] / 'base-scheduler'
driver = scheduler / 'crates/selector/src/readiness_driver.rs'
tests = scheduler / 'crates/selector/src/readiness_driver/tests.rs'
original = {p: p.read_text() for p in (driver, tests)}
base = here.parent / 'cow-routing'
base_driver = (base / 'before-readiness_driver.rs').read_text()
base_tests = (base / 'before-tests.rs').read_text()
old = '''    let retired = registration
        .owner
        .table
        .lock()
        .unwrap()
        .registrations
        .remove(&registration.key);'''
new = '''    let retired = Arc::make_mut(&mut registration.owner.table.lock().unwrap().registrations)
        .remove(&registration.key);'''
assert base_tests.count(old) == 1
fixture = (here / 'fixture.rs').read_text()
revisions = {
    'before': (base_driver, base_tests + fixture),
    'snapshot': (original[driver], base_tests.replace(old, new) + fixture),
}
metadata = {'rustc': subprocess.check_output(['rustc', '-Vv'], text=True), 'revisions': {}}
try:
    for name, (source, test_source) in revisions.items():
        driver.write_text(source)
        tests.write_text(test_source)
        command = ['cargo', 'test', '--release', '--locked', '-p', 'base-scheduler-selector',
                   '--lib', '--no-run', '--message-format=json']
        result = subprocess.run(command, cwd=scheduler, capture_output=True, text=True)
        (here / f'{name}-build.log').write_text(result.stderr)
        result.check_returncode()
        artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
        executable = next(Path(row['executable']) for row in artifacts
                          if row.get('reason') == 'compiler-artifact' and row.get('executable')
                          and row['target']['name'] == 'base_scheduler_selector')
        target = here / f'{name}-tests'
        shutil.copyfile(executable, target)
        target.chmod(0o755)
        (here / f'{name}-driver.rs').write_text(source)
        (here / f'{name}-tests.rs').write_text(test_source)
        metadata['revisions'][name] = {
            'binary_sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
            'driver_sha256': hashlib.sha256(source.encode()).hexdigest(),
            'tests_sha256': hashlib.sha256(test_source.encode()).hexdigest(),
        }
finally:
    for path, source in original.items():
        path.write_text(source)
(here / 'build-provenance.json').write_text(json.dumps(metadata, indent=2) + '\n')
