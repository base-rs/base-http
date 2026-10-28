#!/usr/bin/env python3
"""Validate an echo cohort and normalize separate server syscall counts."""
import argparse
import itertools
import json
from pathlib import Path
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('directory', type=Path)
a = p.parse_args()
meta = json.loads((a.directory / 'environment.json').read_text())
rows = [json.loads(line) for line in (a.directory / 'samples.jsonl').read_text().splitlines()]
config = meta['configuration']
expected = set(itertools.product(config['backends'], config['connections'], config['bytes'], range(1, config['repetitions'] + 1)))
seen = [(r['backend'], r['connections'], r['bytes'], r['repetition']) for r in rows]
assert len(seen) == len(set(seen)) and set(seen) == expected
assert all(row['requests'] > 0 for row in rows)
assert all(not path.read_text() for path in a.directory.glob('*.stderr'))
counts = []
for row in rows:
    if 'syscalls' not in row: continue
    calls = row['syscalls']
    def group(ops):
        return {key: sum(calls[op][key] for op in ops) for key in next(iter(calls.values()))}
    receive = group(['read', 'readv', 'recv', 'recvfrom', 'recvmsg'])
    send = group(['write', 'writev', 'send', 'sendto', 'sendmsg'])
    epoll = group(['epoll_wait', 'epoll_pwait', 'epoll_pwait2'])
    assert receive['bytes_or_events'] == send['bytes_or_events']
    assert receive['bytes_or_events'] % row['bytes'] == 0
    echoes = receive['bytes_or_events'] // row['bytes']
    assert echoes > 0
    counts.append({'backend': row['backend'], 'connections': row['connections'], 'bytes': row['bytes'],
                   'echoes_including_warmup_and_boundaries': echoes,
                   'receive_calls_per_echo': receive['calls'] / echoes,
                   'receive_would_block_per_echo': receive['would_block'] / echoes,
                   'send_calls_per_echo': send['calls'] / echoes,
                   'epoll_calls_per_echo': epoll['calls'] / echoes,
                   'empty_epoll_calls_per_echo': epoll['zero'] / echoes,
                   'epoll_events_per_echo': epoll['bytes_or_events'] / echoes,
                   'empty_fraction_of_epoll_calls': epoll['zero'] / epoll['calls'] if epoll['calls'] else None})
report = {'samples': len(rows), 'complete_matrix': True, 'all_payload_validating_clients_succeeded': True,
          'all_stderr_empty': True, 'instrumented': meta['instrumented'], 'syscall_counts': counts}
(a.directory / 'checks.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
