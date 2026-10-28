#!/usr/bin/env python3
"""Validate the before/after cohorts and compute paired throughput changes."""
import argparse
import itertools
import json
from pathlib import Path
import statistics
import subprocess
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('root', type=Path)
p.add_argument('--validator', type=Path, default=Path(__file__).resolve().parents[1] / 'summarize.py')
a = p.parse_args()
root = a.root.resolve()
report = {}
validator = a.validator
for name in ['screen', 'confirmation', 'headroom', 'counts', 'counts-final']:
    directory = root / name
    subprocess.run([sys.executable, str(validator), str(directory)], check=True, capture_output=True)
    rows = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
    results = []
    for connections, size in sorted({(r['connections'], r['bytes']) for r in rows}):
        groups = {backend: sorted([r for r in rows if (r['connections'], r['bytes'], r['backend']) == (connections, size, backend)], key=lambda r: r['repetition']) for backend in sorted({r['backend'] for r in rows})}
        paired = {}
        for backend, reference in [('iouring-poll', 'iouring-before'), ('iouring-poll', 'native-poll'), ('iouring-poll', 'tokio')]:
            values = [100 * (x['requests_per_second'] / y['requests_per_second'] - 1) for x, y in zip(groups[backend], groups[reference])]
            paired[backend + '_vs_' + reference] = {'median_percent': statistics.median(values), 'min_percent': min(values), 'max_percent': max(values), 'values_percent': values}
        results.append({'connections': connections, 'bytes': size, 'medians': {backend: statistics.median(r['requests_per_second'] for r in group) for backend, group in groups.items()}, 'paired': paired})
    report[name] = {'samples': len(rows), 'results': results}
for name in ['http-raw', 'http']:
    directory = root / name
    metadata = json.loads((directory / 'environment.json').read_text())
    rows = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
    config = metadata['configuration']
    variants = {v.split(':')[0] for v in config['variants']}
    seen = [(r['variant'], r['concurrency'], r['repetition']) for r in rows]
    assert len(seen) == len(set(seen)) and set(seen) == set(itertools.product(variants, config['concurrency'], range(1, config['repetitions'] + 1)))
    for file in directory.glob('*/*.wrk.txt'):
        text = file.read_text()
        assert 'Socket errors:' not in text and 'Non-2xx or 3xx responses:' not in text
    resets = {}
    tails = {}
    expected_reset = 'connection: Connection reset by peer (os error 104)'
    for file in directory.glob('*/*.server.stderr'):
        text = file.read_text()
        lines = text.splitlines()
        if lines and not text.endswith('\n'):
            tail = lines.pop()
            assert expected_reset.startswith(tail), str(file)
            tails[str(file.relative_to(directory))] = tail
        assert all(line == expected_reset for line in lines), str(file)
        resets[str(file.relative_to(directory))] = len(lines)
    report[name] = {'samples': len(rows), 'complete_matrix': True, 'server_peer_reset_reports': resets,
                    'unterminated_reset_log_tails': tails, 'other_complete_server_errors': False, 'wrk_socket_or_status_errors': False,
                    'results': json.loads((directory / 'summary.json').read_text())}
(root / 'validated-summary.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({name: value['samples'] for name, value in report.items()}))
