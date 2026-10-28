#!/usr/bin/env python3
"""Measure named candidate binaries in interleaved isolated wrk samples."""
import argparse
import datetime
import hashlib
import json
import os
import subprocess
from pathlib import Path
import random
import statistics
from types import SimpleNamespace
from run import sample


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wrk', type=Path, required=True)
    parser.add_argument('--variants', type=Path, required=True, help='JSON map of name to {binary, mode}')
    parser.add_argument('--concurrency', type=int, default=64)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--duration', type=int, default=8)
    options = parser.parse_args()
    options.output.mkdir(parents=True, exist_ok=True)
    configured = json.loads(options.variants.read_text())
    variants = {name: (Path(value['binary']).resolve(), value['mode'])
                for name, value in configured.items()}
    for name in variants:
        if not name or any(character not in 'abcdefghijklmnopqrstuvwxyz0123456789-_' for character in name):
            parser.error(f'invalid variant name: {name!r}')
    metadata = {
        'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'uname': list(os.uname()),
        'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
        'wrk': subprocess.run([str(options.wrk.resolve()), '--version'], capture_output=True, text=True).stdout,
        'scheduler_commit': subprocess.check_output(['git', '-C', '../../../base-scheduler', 'rev-parse', 'HEAD'], text=True).strip(),
        'manifests': {str(path): digest(path) for path in [Path('Cargo.toml'), Path('Cargo.lock'),
            Path('../../Cargo.toml'), Path('../../../base-http1/Cargo.toml'),
            Path('../../../base-http-server/Cargo.toml'), Path('../../../base-scheduler/Cargo.toml'),
            Path('../../../base-scheduler/Cargo.lock')]},
        'repetitions': options.repetitions, 'duration_seconds': options.duration,
        'concurrency': options.concurrency, 'warmup_seconds': 1, 'load_threads': min(4, options.concurrency),
        'variants': {name: {'binary': str(path), 'sha256': digest(path), 'mode': mode}
                     for name, (path, mode) in variants.items()},
        'sources': {str(path): digest(path) for root in [Path('../../src'), Path('../../../base-http1/src'),
            Path('../../../base-http-server/src'), Path('../../../base-scheduler/src'),
            Path('../../../base-scheduler/crates'), Path('src')]
            for path in root.rglob('*.rs') if not any(part in ('target', '.agents', 'external') for part in path.parts)},
    }
    (options.output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    rng = random.Random(20261026)
    jobs = []
    for repetition in range(1, options.repetitions + 1):
        batch = [(name, repetition) for name in variants]
        rng.shuffle(batch)
        jobs.extend(batch)
    results = []
    with (options.output / 'samples.jsonl').open('w') as journal:
        for name, repetition in jobs:
            path, mode = variants[name]
            output = options.output / name
            output.mkdir(exist_ok=True)
            args = SimpleNamespace(binary=path, wrk=options.wrk.resolve(), output=output,
                                   threads=4, warmup=1, duration=options.duration)
            print(name, flush=True)
            result = sample(args, mode, options.concurrency, repetition)
            result['variant'] = name
            results.append(result)
            journal.write(json.dumps(result) + '\n')
            journal.flush()
    summary = {}
    for name in variants:
        rows = [row for row in results if row['variant'] == name]
        values = [row['requests_per_second'] for row in rows]
        summary[name] = {
            'median_requests_per_second': statistics.median(values),
            'min_requests_per_second': min(values), 'max_requests_per_second': max(values),
            'median_p99_ms': statistics.median(row['latency_p99_ms'] for row in rows),
            'median_cpu_percent': statistics.median(row['server_cpu_percent_one_core'] for row in rows),
            'errors': sum(row['errors'] for row in rows),
        }
        for key in ('user_cpu_us_per_request', 'system_cpu_us_per_request'):
            if all(key in row for row in rows):
                summary[name]['median_' + key] = statistics.median(row[key] for row in rows)
    (options.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
