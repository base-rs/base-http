#!/usr/bin/env python3
"""Collect compare.sh samples (client JSON plus perf stat CSVs) into one counters.jsonl, per server-counted request."""
import glob, json, os, sys

out, case = sys.argv[1], sys.argv[2]
lines = []
for path in sorted(glob.glob(f"{out}/*-r*.json")):
    label = os.path.basename(path)[:-5]
    variant, repetition = label.rsplit("-r", 1)
    result = json.load(open(path))
    requests = result["server"]["requests"]
    record = {"case": case, "variant": variant, "round": int(repetition), "server_requests": requests}
    for key in ["requests", "requests_per_second", "client_cpu_us_per_request", "client_user_us_per_request",
                "client_system_us_per_request", "client_voluntary_switches_per_request",
                "client_involuntary_switches_per_request", "server_voluntary_switches_per_request",
                "server_involuntary_switches_per_request"]:
        if key in result:
            record[key] = result[key]
    for side in ["client", "server"]:
        perf = f"{out}/{label}.{side}.perf"
        if not os.path.exists(perf):
            continue
        for line in open(perf):
            parts = line.strip().split(",")
            if len(parts) < 3 or not parts[0] or parts[0].startswith("<") or line.startswith("#"):
                continue
            value = float(parts[0])
            if parts[1] == "msec":
                value *= 1000
            record[f"{side}_{parts[2]}_per_request"] = value / requests
    lines.append(json.dumps(record))
sys.stdout.write("\n".join(lines) + "\n")
