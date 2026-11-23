#!/usr/bin/env python3
"""Summarize perf stat CSV files from stat.sh per server-counted request."""
import json, sys, glob, os, collections
out = sys.argv[1]
rows = collections.defaultdict(list)
for path in sorted(glob.glob(f"{out}/*.json")):
    label = os.path.basename(path)[:-5]
    result = json.load(open(path))
    n = result["server"]["requests"]
    row = {k: result[k] for k in ["requests_per_second", "client_cpu_us_per_request", "client_user_us_per_request",
           "client_system_us_per_request", "client_voluntary_switches_per_request", "client_involuntary_switches_per_request", "server_voluntary_switches_per_request", "server_involuntary_switches_per_request"] if k in result}
    for side in ["client", "server"]:
        f = f"{out}/{label}.{side}.perf"
        if not os.path.exists(f):
            continue
        for line in open(f):
            parts = line.strip().split(",")
            if len(parts) < 3 or not parts[0] or parts[0].startswith("<"):
                continue
            try:
                value = float(parts[0])
            except ValueError:
                continue
            event = parts[2]
            if parts[1] == "msec":
                value *= 1000  # ms -> us
            elif parts[1] == "ns":
                value /= 1000
            elif parts[1] == "s":
                value *= 1e6
            row[f"{side}:{event}"] = value / n
        t = f"{out}/{label}.{side}.time"
        if os.path.exists(t):
            u, s, c, w, e = open(t).read().strip().splitlines()[-1].split(",")
            row[f"{side}:user_us"] = float(u) * 1e6 / n
            row[f"{side}:sys_us"] = float(s) * 1e6 / n
            row[f"{side}:invol_cs"] = float(c) / n
            row[f"{side}:vol_cs"] = float(w) / n
    impl = label.rsplit("-r", 1)[0]
    rows[impl].append(row)
keys = []
for samples in rows.values():
    for s in samples:
        for k in s:
            if k not in keys:
                keys.append(k)
print(f"{'':30s}" + "".join(f"{impl[:24]:>26s}" for impl in rows))
for key in keys:
    line = f"{key:30s}"
    for impl, samples in rows.items():
        vals = sorted(s[key] for s in samples if key in s)
        if not vals:
            line += f"{'':>26s}"; continue
        med = vals[len(vals) // 2]
        line += f"{med:12.3f} [{vals[0]:.4g}-{vals[-1]:.4g}]".rjust(26)
    print(line)
