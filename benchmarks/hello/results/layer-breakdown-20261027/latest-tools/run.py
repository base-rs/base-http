#!/usr/bin/env python3
"""Interleave fixed-work layer probes; no compiler or profiler during timing."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import statistics
import subprocess

p = argparse.ArgumentParser()
p.add_argument("--binary", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
p.add_argument("--repetitions", type=int, default=5)
p.add_argument("--smoke", action="store_true")
p.add_argument("--kinds", nargs="+")
p.add_argument("--backends", nargs="+")
p.add_argument("--widths", nargs="+", type=int)
p.add_argument("--compare-candidate", type=Path)
a = p.parse_args()
specs = []
def add(kind, backends, widths, total, per_width=True):
    for backend in backends:
        for width in widths:
            iterations = max(10, total // width) if per_width else total
            specs.append(dict(backend=backend, kind=kind, width=width, iterations=iterations))

add("tasks", ["base", "base-noio", "iouring", "tokio", "tokio-noio"], [1, 64], 1_000_000)
add("supervisor", ["base", "base-noio", "iouring", "tokio", "tokio-noio", "manual"], [16, 64], 1_000_000)
add("readiness", ["base", "tokio"], [1, 16, 64], 2_000_000)
add("readiness-list", ["tokio"], [1, 16, 64], 2_000_000)
add("dispatch", ["base", "tokio"], [1, 16, 64], 1_000_000)
add("dispatch-waiting", ["base", "tokio"], [1, 16, 64], 1_000_000)
add("empty-poll", ["base", "iouring", "tokio", "mio"], [1], 1_000_000)
add("events", ["base", "mio"], [1, 16, 64], 10_000, False)
add("events-driver", ["base", "tokio"], [1, 16, 64], 10_000, False)
add("ready-read", ["std", "base", "iouring", "tokio"], [1], 2_000, False)
add("ready-write", ["std", "base", "iouring", "tokio"], [1], 2_000, False)
add("ready-short-read", ["std", "base", "tokio"], [1], 100_000, False)
if a.kinds:
    specs = [s for s in specs if s["kind"] in a.kinds]
if a.backends:
    specs = [s for s in specs if s["backend"] in a.backends]
if a.widths:
    specs = [s for s in specs if s["width"] in a.widths]
expanded = []
for spec in specs:
    expanded.append(spec | {"variant": "before" if a.compare_candidate and spec["backend"] == "base" else spec["backend"], "binary": str(a.binary)})
    if a.compare_candidate and spec["backend"] == "base":
        expanded.append(spec | {"variant": "inline", "binary": str(a.compare_candidate)})
specs = expanded
if a.smoke:
    for s in specs: s["iterations"] = 20
    a.repetitions = 1
a.output.mkdir(parents=True, exist_ok=False)
meta = {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "configuration": {k: str(v) if isinstance(v, Path) else v for k,v in vars(a).items()},
        "specs": specs, "binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(),
        "binary_sha256_by_path": {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in sorted({s["binary"] for s in specs})},
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "cpu": subprocess.check_output(["lscpu"], text=True),
        "uname": list(os.uname()), "affinity": {"all_probe_threads": [0], "controller": [1]},
        "loadavg": Path("/proc/loadavg").read_text()}
(a.output / "environment.json").write_text(json.dumps(meta, indent=2) + "\n")
rng = random.Random(20261027)
rows = []
with (a.output / "samples.jsonl").open("w") as journal:
    for rep in range(1, a.repetitions + 1):
        order = specs.copy(); rng.shuffle(order)
        for spec in order:
            name = f'{spec["kind"]}-{spec["variant"]}-w{spec["width"]}-r{rep}'
            command = ["taskset", "-c", "0", spec["binary"], spec["backend"], spec["kind"], str(spec["iterations"]), str(spec["width"])]
            if not a.smoke:
                warm = command.copy(); warm[-2] = str(max(10, min(1000, spec["iterations"] // 20)))
                completed = subprocess.run(warm, text=True, capture_output=True, check=True, timeout=30)
                (a.output / (name + ".warmup.json")).write_text(completed.stdout)
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            completed = subprocess.run(command, text=True, capture_output=True, timeout=60)
            (a.output / (name + ".stderr")).write_text(completed.stderr)
            assert completed.returncode == 0, (name, completed.stderr)
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            row = json.loads(completed.stdout)
            row.update(repetition=rep, variant=spec["variant"], process_user_seconds=after.ru_utime-before.ru_utime,
                       process_system_seconds=after.ru_stime-before.ru_stime)
            rows.append(row); journal.write(json.dumps(row) + "\n"); journal.flush()
            print(f'{name}: {row["ns_per_operation"]:.3f} ns/op', flush=True)

summary = []
for spec in specs:
    group = [r for r in rows if all(r[k] == spec[k] for k in ("backend", "variant", "kind", "width"))]
    values = [r["ns_per_operation"] for r in group]
    summary.append(spec | {"median_ns_per_operation": statistics.median(values), "min": min(values), "max": max(values), "values": values})
(a.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
