#!/usr/bin/env python3
"""Paired per-repetition ratios of run.py samples: candidate against baseline, within each case."""
import glob, json, os, statistics, sys

out = sys.argv[1]
pairs = [("base-open", "before-open"), ("base-call", "before-call"), ("base-open", "hyper")]
rows = {}
for path in glob.glob(f"{out}/*.json"):
    name = os.path.basename(path)
    if name in ("summary.json", "metadata.json", "source-hashes.json", "paired.json"):
        continue
    row = json.load(open(path))
    # Samples are named <transport>-<mode>-c<concurrency>-<implementation>-r<repetition>.json.
    repetition = int(name[:-5].rsplit("-r", 1)[1])
    rows[(row["tls"], row["concurrency"], row["implementation"], repetition)] = row
result = []
for tls in (False, True):
    for concurrency in (1, 32):
        for candidate, baseline in pairs:
            ratios, cpu = [], []
            for repetition in range(1, 100):
                a = rows.get((tls, concurrency, candidate, repetition))
                b = rows.get((tls, concurrency, baseline, repetition))
                if not a or not b:
                    continue
                ratios.append(a["requests_per_second"] / b["requests_per_second"] - 1)
                cpu.append(a["client_cpu_us_per_request"] / b["client_cpu_us_per_request"] - 1)
            if not ratios:
                continue
            entry = dict(tls=tls, concurrency=concurrency, candidate=candidate, baseline=baseline, pairs=len(ratios),
                         throughput_change=dict(median=statistics.median(ratios), min=min(ratios), max=max(ratios)),
                         cpu_change=dict(median=statistics.median(cpu), min=min(cpu), max=max(cpu)),
                         improving=sum(r > 0 for r in ratios))
            result.append(entry)
            print(f'{"tls" if tls else "tcp"} c{concurrency:<2} {candidate:>9} vs {baseline:<11} '
                  f'rps {100*entry["throughput_change"]["median"]:+6.1f}% [{100*min(ratios):+.1f}, {100*max(ratios):+.1f}] '
                  f'cpu {100*entry["cpu_change"]["median"]:+6.1f}% [{100*min(cpu):+.1f}, {100*max(cpu):+.1f}] '
                  f'improving {entry["improving"]}/{len(ratios)}')
json.dump(result, open(f"{out}/paired.json", "w"), indent=2)
