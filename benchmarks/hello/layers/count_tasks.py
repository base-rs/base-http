#!/usr/bin/env python3
"""Separate syscall diagnostics for scheduling probes; rates are not evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess

OPS = ["read", "readv", "recv", "recvfrom", "recvmsg", "write", "writev", "send", "sendto", "sendmsg",
       "epoll_wait", "epoll_pwait", "epoll_pwait2", "epoll_ctl", "accept", "accept4"]
METRICS = ["calls", "positive", "would_block", "errors", "bytes_or_events", "zero"]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--binary", type=Path, required=True)
p.add_argument("--library", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=False)
(a.output / "environment.json").write_text(json.dumps({
    "binary": str(a.binary), "binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(),
    "library": str(a.library), "library_sha256": hashlib.sha256(a.library.read_bytes()).hexdigest(),
    "scope": "whole process, including startup and shutdown; timing is instrumented",
}, indent=2) + "\n")
rows = []
for kind, width in [("tasks", 1), ("tasks", 64), ("supervisor", 64)]:
    for backend in ["base", "base-noio", "iouring", "tokio", "tokio-noio"]:
        name = f"{kind}-{backend}-w{width}"
        counts_path = (a.output / (name + ".calls.bin")).resolve()
        env = dict(os.environ, LD_PRELOAD=str(a.library.resolve()), BASE_HTTP_SYSCALL_COUNTS=str(counts_path))
        result = subprocess.run(["taskset", "-c", "0", str(a.binary), backend, kind, str(1_000_000 // width), str(width)],
                                text=True, capture_output=True, env=env, timeout=60)
        (a.output / (name + ".stdout.json")).write_text(result.stdout)
        (a.output / (name + ".stderr")).write_text(result.stderr)
        assert result.returncode == 0, (name, result.stderr)
        probe = json.loads(result.stdout)
        values = struct.unpack("<" + "Q" * (len(OPS) * len(METRICS)), counts_path.read_bytes())
        counts = {op: {metric: values[i * len(METRICS) + j] for j, metric in enumerate(METRICS)}
                  for i, op in enumerate(OPS)}
        epoll = {metric: sum(counts[op][metric] for op in OPS[10:13]) for metric in METRICS}
        row = {"backend": backend, "kind": kind, "width": width, "operations": probe["operations"],
               "counts": counts, "epoll_per_operation": {k: v / probe["operations"] for k, v in epoll.items()}}
        rows.append(row)
        print(name, json.dumps(row["epoll_per_operation"]), flush=True)
(a.output / "counts.json").write_text(json.dumps(rows, indent=2) + "\n")
