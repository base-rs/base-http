#!/usr/bin/env python3
"""Run isolated, interleaved wrk samples against the four static HTTP servers."""

import argparse
import datetime
import http.client
import json
import os
from pathlib import Path
import random
import re
import select
import statistics
import subprocess
import sys
import time

if sys.platform == "darwin":
    from cpu_usage import cpu_usage
else:
    cpu_usage = None


def cpu_and_rss(pid):
    fields = subprocess.check_output(
        ["ps", "-p", str(pid), "-o", "cputime=", "-o", "rss="], text=True
    ).split()
    seconds = 0.0
    for part in fields[0].split(":"):
        seconds = seconds * 60 + float(part)
    return seconds, int(fields[1])


def validate(address, raw=False):
    host, port = address.rsplit(":", 1)
    connection = http.client.HTTPConnection(host, int(port), timeout=5)
    observations = []
    try:
        for index in range(3):
            connection.request("GET", "/", headers={"Connection": "close"} if index == 2 and not raw else {})
            response = connection.getresponse()
            body = response.read()
            fields = {name.lower(): value for name, value in response.getheaders()}
            assert response.status == 200 and response.version == 11, response.status
            assert body == b"Hello, world!", body
            assert fields["content-length"] == "13", fields
            assert fields["content-type"] == "text/plain", fields
            assert "date" not in fields, fields
            assert response.will_close == (index == 2 and not raw)
            if index == 0:
                retained_socket = connection.sock
            if index == 1 or (index == 2 and raw):
                assert connection.sock is retained_socket, "server did not reuse the connection"
            observations.append({"status": response.status, "fields": fields, "body": body.decode()})
    finally:
        connection.close()
    return observations


def milliseconds(value):
    match = re.fullmatch(r"([\d.]+)(us|ms|s)", value)
    if match is None:
        raise ValueError(f"unexpected wrk latency: {value!r}")
    return float(match[1]) * {"us": 0.001, "ms": 1, "s": 1000}[match[2]]


def parse_wrk(output):
    errors = re.search(r"Socket errors: (.*)", output)
    if errors and any(int(count) for count in re.findall(r"\w+ (\d+)", errors[1])):
        raise RuntimeError(output)
    if "Non-2xx or 3xx responses:" in output:
        raise RuntimeError(output)
    return {
        "requests_per_second": float(re.search(r"Requests/sec:\s+([\d.]+)", output)[1]),
        "latency_mean_ms": milliseconds(re.search(r"Latency:?\s+([\d.]+(?:us|ms|s))", output)[1]),
        "latency_p50_ms": milliseconds(re.search(r"^\s*50%\s+(\S+)", output, re.MULTILINE)[1]),
        "latency_p99_ms": milliseconds(re.search(r"^\s*99%\s+(\S+)", output, re.MULTILINE)[1]),
        "errors": 0,
    }


def sample(args, backend, concurrency, repetition):
    stem = f"{backend}-c{concurrency}-r{repetition}"
    stderr_file = args.output / f"{stem}.server.stderr"
    with stderr_file.open("w") as errors:
        server = subprocess.Popen(
            [str(args.binary), backend], stdout=subprocess.PIPE, stderr=errors, text=True
        )
        try:
            if not select.select([server.stdout], [], [], 10)[0]:
                raise RuntimeError("server did not report readiness")
            address = server.stdout.readline().strip()
            if not address:
                raise RuntimeError(stderr_file.read_text())
            validation = validate(address, raw=backend.endswith("-raw"))
            threads = min(args.threads, concurrency)
            command = [str(args.wrk), f"-t{threads}", f"-c{concurrency}", "--latency", "--timeout", "2s"]
            warmup = subprocess.run(
                [*command, f"-d{args.warmup}s", f"http://{address}/"],
                capture_output=True, text=True, check=True, timeout=args.warmup + 15,
            )
            (args.output / f"{stem}.warmup.txt").write_text(warmup.stdout + warmup.stderr)
            parse_wrk(warmup.stdout)
            cpu_before, _ = cpu_and_rss(server.pid)
            usage_before = cpu_usage(server.pid) if cpu_usage else None
            started = time.monotonic()
            measured = subprocess.run(
                [*command, f"-d{args.duration}s", f"http://{address}/"],
                capture_output=True, text=True, check=True, timeout=args.duration + 15,
            )
            elapsed = time.monotonic() - started
            usage_after = cpu_usage(server.pid) if cpu_usage else None
            cpu_after, rss_kib = cpu_and_rss(server.pid)
            (args.output / f"{stem}.wrk.txt").write_text(measured.stdout + measured.stderr)
            result = {
                "backend": backend, "concurrency": concurrency, "repetition": repetition,
                "load_threads": threads, "measured_wall_seconds": elapsed,
                "server_cpu_percent_one_core": (cpu_after - cpu_before) / elapsed * 100,
                "server_rss_kib_after": rss_kib, "validation": validation,
                **parse_wrk(measured.stdout),
            }
            result["requests"] = int(re.search(r"(\d+) requests in", measured.stdout)[1])
            if usage_before is not None:
                usage = {key: usage_after[key] - usage_before[key] for key in usage_before}
                result["cpu_usage"] = usage
                result["user_cpu_us_per_request"] = usage["user_seconds"] * 1e6 / result["requests"]
                result["system_cpu_us_per_request"] = usage["system_seconds"] * 1e6 / result["requests"]
            print(
                f"{backend:12} c={concurrency:3} r={repetition}: "
                f"{result['requests_per_second']:10.0f} req/s, "
                f"p99 {result['latency_p99_ms']:.3f} ms, "
                f"CPU {result['server_cpu_percent_one_core']:.0f}%",
                flush=True,
            )
            return result
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
            server.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wrk", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/base-http-hello-benchmark"))
    parser.add_argument("--output", type=Path, default=Path("results/macos"))
    parser.add_argument("--backends", nargs="+", default=["base-native", "base-tokio", "hyper-tokio", "base-direct"])
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 16, 64])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--duration", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.wrk = args.wrk.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    metadata = {
        "utc_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "configuration": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "uname": list(os.uname()),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "wrk": subprocess.run([str(args.wrk), "--version"], capture_output=True, text=True).stdout,
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    jobs = []
    rng = random.Random(20261026)
    for repetition in range(1, args.repetitions + 1):
        round_jobs = [(backend, concurrency, repetition) for backend in args.backends for concurrency in args.concurrency]
        rng.shuffle(round_jobs)
        jobs.extend(round_jobs)
    samples = []
    with (args.output / "samples.jsonl").open("w") as journal:
        for job in jobs:
            result = sample(args, *job)
            samples.append(result)
            journal.write(json.dumps(result) + "\n")
            journal.flush()
    summary = []
    for concurrency in args.concurrency:
        for backend in args.backends:
            group = [row for row in samples if row["backend"] == backend and row["concurrency"] == concurrency]
            summary.append({
                "backend": backend, "concurrency": concurrency,
                "requests_per_second_min": min(row["requests_per_second"] for row in group),
                "requests_per_second_max": max(row["requests_per_second"] for row in group),
                **{key: statistics.median(row[key] for row in group) for key in (
                    "requests_per_second", "latency_mean_ms", "latency_p50_ms", "latency_p99_ms",
                    "server_cpu_percent_one_core", "server_rss_kib_after",
                )},
            })
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
