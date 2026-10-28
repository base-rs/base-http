#!/usr/bin/env python3
"""Interleave canonical echo workloads; keep syscall diagnostics separate."""
import argparse
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path
import random
import resource
import select
import statistics
import struct
import subprocess
import time

OPS = ['read', 'readv', 'recv', 'recvfrom', 'recvmsg', 'write', 'writev', 'send', 'sendto', 'sendmsg',
       'epoll_wait', 'epoll_pwait', 'epoll_pwait2', 'epoll_ctl', 'accept', 'accept4']
METRICS = ['calls', 'positive', 'would_block', 'errors', 'bytes_or_events', 'zero']
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--candidate', type=Path)
p.add_argument('--minimal', type=Path, required=True)
p.add_argument('--baseline', type=Path, help='frozen pre-change executable; adds iouring-before and keeps the client fixed')
p.add_argument('--output', type=Path, required=True)
p.add_argument('--repetitions', type=int, default=5)
p.add_argument('--warmup', type=float, default=1)
p.add_argument('--seconds', type=float, default=4)
p.add_argument('--connections', type=int, nargs='+', default=[1, 64])
p.add_argument('--bytes', type=int, nargs='+', default=[64, 4096])
p.add_argument('--backends', nargs='+', default=['native-poll', 'tokio', 'tokio-adapter-poll', 'iouring-poll'])
p.add_argument('--client-workers', type=int, default=4)
p.add_argument('--client-cpus', type=int, nargs='+', default=[2, 3, 6, 7])
p.add_argument('--library', type=Path, help='enable separate instrumented diagnostics, not throughput evidence')
a = p.parse_args()
assert a.repetitions > 0 and a.warmup >= 0 and a.seconds > 0 and a.client_workers > 0
assert a.client_cpus and 0 not in a.client_cpus and min(a.client_cpus) >= 0
assert min(a.connections + a.bytes) > 0 and max(a.bytes) <= 65536
a.binary = a.binary.resolve()
a.minimal = a.minimal.resolve()
if a.candidate: a.candidate = a.candidate.resolve()
if a.baseline:
    a.baseline = a.baseline.resolve()
a.output.mkdir(parents=True, exist_ok=False)
meta = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'configuration': {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()},
        'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest(),
        'minimal_sha256': hashlib.sha256(a.minimal.read_bytes()).hexdigest(),
        'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
        'cpu': subprocess.check_output(['lscpu'], text=True), 'uname': list(os.uname()),
        'affinity': {'server_all_threads': [0], 'client_all_threads': a.client_cpus, 'controller': sorted(os.sched_getaffinity(0))},
        'server_workers': 1, 'client_workers': a.client_workers, 'loadavg': Path('/proc/loadavg').read_text(),
        'cpu_accounting_scope': 'whole child lifetime, including warmup/setup/cleanup; not timed-window CPU per request',
        'instrumented': bool(a.library)}
if a.candidate:
    meta['candidate_sha256'] = hashlib.sha256(a.candidate.read_bytes()).hexdigest()
if a.baseline:
    meta['baseline_sha256'] = hashlib.sha256(a.baseline.read_bytes()).hexdigest()
if a.library:
    a.library = a.library.resolve()
    meta['library_sha256'] = hashlib.sha256(a.library.read_bytes()).hexdigest()
(a.output / 'environment.json').write_text(json.dumps(meta, indent=2) + '\n')

def usage():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return r.ru_utime, r.ru_stime

def sample(backend, connections, size, repetition):
    name = f'{backend}-c{connections}-b{size}-r{repetition}'
    prefix = a.output / name
    counts = a.output / (name + '.counts.bin')
    env = dict(os.environ)
    if a.library:
        env.update(LD_PRELOAD=str(a.library), BASE_HTTP_SYSCALL_COUNTS=str(counts.resolve()))
    executable, server_backend = a.binary, backend
    if backend == 'iouring-before':
        assert a.baseline, 'iouring-before requires --baseline'
        executable, server_backend = a.baseline, 'iouring-poll'
    if backend in ['iouring-deferred', 'iouring-owned-deferred']:
        assert a.candidate
        executable, server_backend = a.candidate, ('iouring-poll' if backend == 'iouring-deferred' else 'iouring')
    if backend.startswith('mini-'):
        executable, server_backend = a.minimal, backend.removeprefix('mini-')
    server_command = ['taskset', '-c', '0', str(executable), 'server', server_backend, '1', str(connections)]
    with Path(str(prefix) + '.server.stderr').open('w') as errors:
        before = usage()
        server_start = time.monotonic()
        server = subprocess.Popen(server_command, stdout=subprocess.PIPE, stderr=errors, text=True, env=env)
        try:
            assert select.select([server.stdout], [], [], 30)[0], 'server readiness timeout'
            address = server.stdout.readline().strip()
            assert address.startswith('127.0.0.1:'), address
            client_command = ['taskset', '-c', ','.join(map(str, a.client_cpus)), str(a.baseline or a.binary), 'client', address, str(a.client_workers), str(connections), str(size), str(a.warmup), str(a.seconds)]
            client_start = time.monotonic()
            client = subprocess.run(client_command, text=True, capture_output=True, timeout=a.warmup + a.seconds + 30)
            client_wall = time.monotonic() - client_start
            after_client = usage()
            Path(str(prefix) + '.client.json').write_text(client.stdout)
            Path(str(prefix) + '.client.stderr').write_text(client.stderr)
            assert client.returncode == 0, (name, client.stderr)
            assert server.wait(timeout=30) == 0, name
            server_wall = time.monotonic() - server_start
            after_server = usage()
            row = json.loads(client.stdout)
            assert row['requests'] > 0 and row['connections'] == connections and row['bytes'] == size
            row.update(backend=backend, repetition=repetition, server_workers=1, client_workers=a.client_workers,
                       server_process_wall_seconds=server_wall, client_process_wall_seconds=client_wall,
                       server_process_user_seconds=after_server[0] - after_client[0],
                       server_process_system_seconds=after_server[1] - after_client[1],
                       client_process_user_seconds=after_client[0] - before[0],
                       client_process_system_seconds=after_client[1] - before[1])
            for side in ['server', 'client']:
                row[side + '_process_cpu_percent'] = 100 * (row[side + '_process_user_seconds'] + row[side + '_process_system_seconds']) / row[side + '_process_wall_seconds']
            if a.library:
                values = struct.unpack('<' + 'Q' * (len(OPS) * len(METRICS)), counts.read_bytes())
                row['syscalls'] = {op: {metric: values[i * len(METRICS) + j] for j, metric in enumerate(METRICS)} for i, op in enumerate(OPS)}
            return row
        finally:
            if server.poll() is None:
                server.kill()
            server.wait()
            server.stdout.close()

rows = []
rng = random.Random(20261027)
with (a.output / 'samples.jsonl').open('w') as journal:
    for repetition in range(1, a.repetitions + 1):
        cases = list(itertools.product(a.connections, a.bytes))
        rng.shuffle(cases)
        for connections, size in cases:
            backends = a.backends.copy()
            rng.shuffle(backends)
            for backend in backends:
                row = sample(backend, connections, size, repetition)
                rows.append(row)
                journal.write(json.dumps(row) + '\n'); journal.flush()
                print(f'{backend} c={connections} b={size} r={repetition}: {row["requests_per_second"]:.0f} echoes/s, p99={row["rtt_p99_us"]:.2f} us, server CPU={row["server_process_cpu_percent"]:.1f}%, client CPU={row["client_process_cpu_percent"]:.1f}%', flush=True)

summary = []
for connections, size in itertools.product(a.connections, a.bytes):
    groups = {backend: sorted([r for r in rows if (r['connections'], r['bytes'], r['backend']) == (connections, size, backend)], key=lambda r: r['repetition']) for backend in a.backends}
    metrics = ['requests_per_second', 'rtt_p50_us', 'rtt_p99_us', 'server_process_cpu_percent', 'client_process_cpu_percent']
    item = {'connections': connections, 'bytes': size,
            'medians': {backend: {metric: statistics.median(r[metric] for r in group) for metric in metrics} for backend, group in groups.items()},
            'paired_vs_tokio': {}}
    if 'tokio' in groups:
        for backend, group in groups.items():
            item['paired_vs_tokio'][backend] = {}
            for metric in ['requests_per_second', 'rtt_p99_us']:
                ratios = [100 * (x[metric] / y[metric] - 1) for x, y in zip(group, groups['tokio'])]
                item['paired_vs_tokio'][backend][metric] = {'median_percent': statistics.median(ratios), 'min_percent': min(ratios), 'max_percent': max(ratios), 'values_percent': ratios}
    summary.append(item)
(a.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
