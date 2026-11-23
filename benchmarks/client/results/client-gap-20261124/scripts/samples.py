#!/usr/bin/env python3
"""Collect run.py's per-sample records into one compact samples.jsonl."""
import glob, json, os, sys

out = sys.argv[1]
keys = ["implementation", "tls", "concurrency", "requests", "warmup_requests", "elapsed_seconds", "requests_per_second",
        "client_cpu_us_per_request", "client_user_us_per_request", "client_system_us_per_request",
        "client_voluntary_switches_per_request", "client_involuntary_switches_per_request", "latency_us",
        "server", "server_cpu_seconds_including_warmup"]
lines = []
for path in sorted(glob.glob(f"{out}/*-r*.json")):
    name = os.path.basename(path)[:-5]
    row = json.load(open(path))
    record = {"sample": name, "repetition": int(name.rsplit("-r", 1)[1])}
    record.update({key: row[key] for key in keys if key in row})
    lines.append(json.dumps(record, sort_keys=False))
open(f"{out}/samples.jsonl", "w").write("\n".join(lines) + "\n")
print(len(lines), "samples")
