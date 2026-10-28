#!/usr/bin/env python3
"""Summarize LLVM regions in the disposable benchmark extension."""
import argparse
import json
from pathlib import Path
import subprocess
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('root', type=Path)
a = p.parse_args()
root = a.root.resolve()
objects = []
for line in (root / 'coverage-tests.jsonl').read_text().splitlines():
    try: row = json.loads(line)
    except ValueError: continue
    if row.get('reason') == 'compiler-artifact' and row.get('executable') and 'base-scheduler-echo-benchmark' in row['package_id']:
        objects.append(row['executable'])
objects = sorted(set(objects))
assert objects
profiles = sorted(str(p) for p in (root / 'coverage').glob('*.profraw'))
subprocess.run(['llvm-profdata', 'merge', '-sparse', *profiles, '-o', str(root / 'coverage/merged.profdata')], check=True)
source = root / 'scheduler/benchmarks/echo/src/server.rs'
command = ['llvm-cov', 'export', objects[0], *['-object=' + name for name in objects[1:]],
           '-instr-profile=' + str(root / 'coverage/merged.profdata'), str(source)]
result = subprocess.run(command, text=True, capture_output=True, check=True)
(root / 'coverage/export.json').write_text(result.stdout)
(root / 'coverage/export.stderr').write_text(result.stderr)
regions = {}
for datum in json.loads(result.stdout)['data']:
    for function in datum['functions']:
        for region in function['regions']:
            if region[7] != 0: continue
            name = function['filenames'][region[5]]
            if name != str(source): continue
            span = tuple(region[:4])
            regions[span] = max(regions.get(span, 0), region[4])
lines = source.read_text().splitlines()
start = next(i for i, line in enumerate(lines, 1) if '"native-poll" =>' in line)
end = next(i for i, line in enumerate(lines, 1) if i > start and '"iouring" |' in line) - 2
retirement = [i for i, line in enumerate(lines, 1) if 'retire stream reader' in line or 'retire stream writer' in line]
selected = {span: count for span, count in regions.items() if (span[0] >= start and span[2] <= end) or any(span[0] <= line <= span[2] and span[2] - span[0] < 3 for line in retirement)}
assert selected
summary = {'new_region_count': len(selected), 'new_uncovered_regions': [list(span) for span, count in selected.items() if not count],
           'all_server_region_count': len(regions), 'all_server_uncovered_region_count': sum(not value for value in regions.values()),
           'objects': objects, 'profile_count': len(profiles), 'new_native_branch_lines': [start, end], 'new_retirement_lines': retirement}
(root / 'coverage/summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary))
assert not summary['new_uncovered_regions']
