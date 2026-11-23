#!/usr/bin/env python3
"""Write a base-benchmark campaign's pair comparisons for the key metrics as CSV: pairs.py SUMMARY.json > OUT.csv."""
import csv
import json
import sys

METRICS = [
    "wall_time",
    "server/cpu_time_per_operation",
    "server/user_cpu_time_per_operation",
    "server/system_cpu_time_per_operation",
    "service_time_p50",
    "service_time_p99",
]

summary = json.load(open(sys.argv[1]))
out = csv.writer(sys.stdout)
out.writerow(["kind", "baseline", "candidate", "metric", "median_change", "min_change", "max_change", "improved", "regressed", "rounds", "sign_test_p"])
for pair in summary["pairs"]:
    for metric in pair["metrics"]:
        if metric["name"] in METRICS and metric["change"]:
            change = metric["change"]
            out.writerow([pair["kind"], pair["baseline"], pair["candidate"], metric["name"],
                          f'{change["median"]:+.4f}', f'{change["minimum"]:+.4f}', f'{change["maximum"]:+.4f}',
                          metric["improved"], metric["regressed"], metric["rounds"], f'{metric["sign_test_p"]:.3f}'])
