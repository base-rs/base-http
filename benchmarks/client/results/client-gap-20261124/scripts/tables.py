#!/usr/bin/env python3
"""Markdown tables of medians and ranges from counters/*.jsonl and formal/summary.json."""
import json, statistics, sys
from pathlib import Path

root = Path(sys.argv[1])
order = ["main", "head", "noprobe", "hyper"]
names = {"main": "main", "head": "candidate", "noprobe": "candidate, no probe (diagnostic)", "hyper": "Hyper"}
metrics = [
    ("requests_per_second", "Responses/s", "{:,.0f}"),
    ("client_instructions:u_per_request", "Instructions:u", "{:,.0f}"),
    ("client_cycles:u_per_request", "Cycles:u", "{:,.0f}"),
    ("client_task-clock:u_per_request", "Task clock µs", "{:.2f}"),
    ("client_L1-dcache-load-misses:u_per_request", "L1d misses", "{:.0f}"),
    ("client_L1-icache-load-misses:u_per_request", "L1i misses", "{:.0f}"),
    ("client_system_us_per_request", "System µs", "{:.2f}"),
    ("server_voluntary_switches_per_request", "Server sleeps", "{:.3f}"),
]


def cell(values, form):
    values = sorted(values)
    return f"{form.format(statistics.median(values))} ({form.format(values[0])}–{form.format(values[-1])})"


for case in ["c32", "c1", "tls-c32"]:
    rows = [json.loads(line) for line in open(root / "counters" / f"final-{case}.jsonl")]
    print(f"\n### {case}\n")
    print("| Variant | " + " | ".join(label for _, label, _ in metrics) + " |")
    print("| --- | " + " | ".join("---:" for _ in metrics) + " |")
    for variant in order:
        samples = [row for row in rows if row["variant"] == variant]
        print(f"| {names[variant]} | " + " | ".join(cell([s[key] for s in samples], form) for key, _, form in metrics) + " |")

print("\n### formal\n")
summary = json.load(open(root / "formal" / "summary.json"))
print("| Transport | Connections | Client | Median responses/s | Range | Median CPU µs/response | Range |")
print("| --- | ---: | --- | ---: | ---: | ---: | ---: |")
for row in summary:
    rps, cpu = row["requests_per_second"], row["client_cpu_us_per_request"]
    print(f'| {"HTTPS" if row["tls"] else "HTTP"} | {row["concurrency"]} | `{row["implementation"]}` | {rps["median"]:,.0f} | '
          f'{rps["min"]:,.0f}–{rps["max"]:,.0f} | {cpu["median"]:.2f} | {cpu["min"]:.2f}–{cpu["max"]:.2f} |')
print("\n### paired\n")
print("| Transport | Connections | Client | Against | Responses/s | CPU µs/response | Improving pairs |")
print("| --- | ---: | --- | --- | ---: | ---: | ---: |")
for row in json.load(open(root / "formal" / "paired.json")):
    t, c = row["throughput_change"], row["cpu_change"]
    print(f'| {"HTTPS" if row["tls"] else "HTTP"} | {row["concurrency"]} | `{row["candidate"]}` | `{row["baseline"]}` | '
          f'{100*t["median"]:+.1f}% ({100*t["min"]:+.1f}% to {100*t["max"]:+.1f}%) | '
          f'{100*c["median"]:+.1f}% ({100*c["min"]:+.1f}% to {100*c["max"]:+.1f}%) | {row["improving"]}/{row["pairs"]} |')
