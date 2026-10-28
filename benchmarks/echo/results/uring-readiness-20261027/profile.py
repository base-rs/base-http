#!/usr/bin/env python3
"""Profile the frozen echo executables separately from throughput runs."""
import argparse
from pathlib import Path
import select
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('root', type=Path)
a = p.parse_args()
root = a.root.resolve()
output = root / 'profiles'
output.mkdir(exist_ok=False)
for name, binary, backend in [('before', 'before', 'iouring-poll'), ('after', 'after', 'iouring-poll'), ('epoll', 'after', 'native-poll')]:
    for kind in ['record', 'stat']:
        prefix = output / (name + '-' + kind)
        with prefix.with_suffix('.server.stderr').open('w') as errors:
            server = subprocess.Popen(['taskset', '-c', '0', str(root / 'binaries' / binary), 'server', backend, '1', '64'], stdout=subprocess.PIPE, stderr=errors, text=True)
            profiler = None
            try:
                assert select.select([server.stdout], [], [], 30)[0]
                address = server.stdout.readline().strip()
                assert address.startswith('127.0.0.1:')
                command = ['sudo', '-n', 'perf', kind, '-p', str(server.pid), '-o', str(prefix) + ('.data' if kind == 'record' else '.stat')]
                if kind == 'record':
                    command += ['-F', '999', '-g', '--call-graph', 'dwarf,8192']
                else:
                    command += ['-e', 'task-clock,cycles,instructions,context-switches,cpu-migrations,cache-misses,syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_epoll_wait,syscalls:sys_enter_epoll_pwait']
                with prefix.with_suffix('.perf.log').open('w') as log:
                    profiler = subprocess.Popen(command, stdout=log, stderr=log)
                    client = subprocess.run(['taskset', '-c', '2,3,6,7', str(root / 'binaries/before'), 'client', address, '4', '64', '64', '1', '8'], text=True, capture_output=True, timeout=40, check=True)
                    prefix.with_suffix('.client.json').write_text(client.stdout)
                    prefix.with_suffix('.client.stderr').write_text(client.stderr)
                    assert server.wait(timeout=15) == 0
                    assert profiler.wait(timeout=15) == 0
                if kind == 'record':
                    for mode, flags in [('self', ['--no-children']), ('tree', [])]:
                        report = subprocess.run(['sudo', '-n', 'perf', 'report', '--stdio', '-i', str(prefix) + '.data', *flags, '--percent-limit', '0.5'], text=True, capture_output=True, check=True)
                        prefix.with_suffix('.' + mode + '.txt').write_text(report.stdout + report.stderr)
                print(name, kind, 'complete', flush=True)
            finally:
                if server.poll() is None:
                    server.kill()
                server.wait()
                server.stdout.close()
                if profiler and profiler.poll() is None:
                    profiler.terminate()
                    profiler.wait(timeout=15)
