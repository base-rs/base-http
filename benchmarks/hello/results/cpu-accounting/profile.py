#!/usr/bin/env python3
"""Capture macOS wall-clock stack samples while the hello server is under load."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import select
import subprocess
import time

from run import cpu_and_rss, parse_wrk, validate
from cpu_usage import cpu_usage


def capture(args, backend):
    with (args.output / f"{backend}.server.stderr").open("w") as errors:
        server = subprocess.Popen(
            [str(args.binary), backend], stdout=subprocess.PIPE, stderr=errors, text=True
        )
        try:
            if not select.select([server.stdout], [], [], 10)[0]:
                raise RuntimeError("server readiness timed out")
            address = server.stdout.readline().strip()
            if not address:
                raise RuntimeError("server exited before readiness")
            validate(address)
            command = [str(args.wrk), "-t4", "-c64", "--latency", f"http://{address}/"]
            warmup = subprocess.run(
                [*command, "-d2s"], capture_output=True, text=True, timeout=15, check=True
            )
            parse_wrk(warmup.stdout)
            before, _ = cpu_and_rss(server.pid)
            usage_before = cpu_usage(server.pid)
            started = time.monotonic()
            with (args.output / f"{backend}.wrk.txt").open("w") as load_log:
                load = subprocess.Popen(
                    [*command, f"-d{args.seconds + 5}s"], stdout=load_log, stderr=subprocess.STDOUT
                )
                try:
                    if not args.no_sample:
                        time.sleep(1)
                        sampled = subprocess.run(
                            ["/usr/bin/sample", str(server.pid), str(args.seconds), "1", "-mayDie", "-fullPaths", "-file", str(args.output / f"{backend}.sample.txt")],
                            capture_output=True, text=True, timeout=args.seconds + 45,
                        )
                        (args.output / f"{backend}.sampler.txt").write_text(sampled.stdout + sampled.stderr)
                        sampled.check_returncode()
                    load.wait(timeout=30)
                    if load.returncode:
                        raise RuntimeError("wrk failed")
                finally:
                    if load.poll() is None:
                        load.terminate()
                        load.wait(timeout=10)
            elapsed = time.monotonic() - started
            after, rss = cpu_and_rss(server.pid)
            usage_after = cpu_usage(server.pid)
            output = (args.output / f"{backend}.wrk.txt").read_text()
            requests = int(re.search(r"(\d+) requests in", output)[1])
            usage = {key: usage_after[key] - usage_before[key] for key in usage_before}
            result = {
                "backend": backend, "sample_seconds": args.seconds,
                "server_cpu_percent_one_core": (after - before) / elapsed * 100,
                "server_rss_kib_after": rss,
                "requests": requests, "cpu_usage": usage,
                "user_cpu_us_per_request": usage["user_seconds"] * 1e6 / requests,
                "system_cpu_us_per_request": usage["system_seconds"] * 1e6 / requests,
                **parse_wrk(output),
            }
            (args.output / f"{backend}.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result), flush=True)
        finally:
            server.terminate()
            server.wait(timeout=10)
            server.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wrk", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/base-http-hello-benchmark"))
    parser.add_argument("--output", type=Path, default=Path("results/profiles"))
    parser.add_argument("--backends", nargs="+", default=["base-native", "base-tokio", "hyper-tokio"])
    parser.add_argument("--seconds", type=int, default=8)
    parser.add_argument("--no-sample", action="store_true", help="record uninstrumented CPU accounting; load runs for --seconds + 5 seconds")
    args = parser.parse_args()
    args.wrk = args.wrk.resolve()
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    metadata = {
        "utc_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "configuration": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "uname": list(os.uname()),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "wrk": subprocess.run([str(args.wrk), "--version"], capture_output=True, text=True).stdout,
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    for backend in args.backends:
        capture(args, backend)


if __name__ == "__main__":
    main()
