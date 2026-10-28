#!/usr/bin/env python3
"""Compare pooled HTTP/1 clients against one controlled server implementation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import resource
import selectors
import shutil
import signal
import statistics
import subprocess
import tempfile
import tarfile
import time

BASELINE_IMPLEMENTATIONS = {"before-open": "base-open", "before-connection": "base-connection"}


def cpu_usage():
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def execute(binary, identity, out, implementation, tls, fresh, concurrency, seconds, warmup, baseline_binary=None):
    suffix = ["--tls"] if tls else []
    server_cmd = [str(binary), "server", "--identity", str(identity), *suffix]
    server = subprocess.Popen(server_cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    measured = None
    failure = None
    try:
        selector = selectors.DefaultSelector()
        selector.register(server.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=10):
            raise RuntimeError("server did not become ready")
        selector.close()
        ready = server.stdout.readline().strip()
        if not ready.startswith("READY "):
            raise RuntimeError(f"invalid server startup: {ready!r}")
        address = ready.removeprefix("READY ")
        client_binary = baseline_binary if implementation in BASELINE_IMPLEMENTATIONS else binary
        wire_implementation = BASELINE_IMPLEMENTATIONS.get(implementation, implementation)
        client_cmd = [str(client_binary), "client", "--implementation", wire_implementation,
                      "--address", address, "--identity", str(identity), "--concurrency", str(concurrency),
                      "--seconds", str(seconds), "--warmup", str(warmup), *suffix]
        if fresh:
            client_cmd.append("--fresh")
        client = subprocess.run(client_cmd, capture_output=True, text=True, timeout=seconds + warmup + 25)
        out.with_suffix(".client.stderr").write_text(client.stderr)
        if client.returncode:
            raise RuntimeError(f"client failed ({client.returncode}): {client.stderr}")
        measured = json.loads(client.stdout)
        measured["implementation"] = implementation
        measured["client_command"] = client_cmd
        measured["server_command"] = server_cmd
    except Exception as error:
        failure = error
    finally:
        before_join = cpu_usage()
        server.send_signal(signal.SIGTERM)
        try:
            stdout, stderr = server.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            stdout, stderr = server.communicate()
        server_cpu = cpu_usage() - before_join
        out.with_suffix(".server.stderr").write_text(stderr)
        out.with_suffix(".server.stdout").write_text(stdout)
    if failure:
        raise failure
    if server.returncode:
        raise RuntimeError(f"server failed ({server.returncode}): {stderr}")
    stats = [json.loads(line.removeprefix("STATS ")) for line in stdout.splitlines() if line.startswith("STATS ")]
    if len(stats) != 1 or stats[0]["failures"]:
        raise RuntimeError(f"invalid server statistics: {stats}, {stderr}")
    stats = stats[0]
    if stats["requests"] != measured["requests"] + measured["warmup_requests"]:
        raise RuntimeError(f"request accounting mismatch: {stats}, {measured}")
    if fresh and stats["connections"] != stats["requests"]:
        raise RuntimeError(f"fresh-connection mode unexpectedly reused a connection: {stats}")
    measured["server"] = stats
    measured["server_cpu_seconds_including_warmup"] = server_cpu
    out.with_suffix(".json").write_text(json.dumps(measured, indent=2) + "\n")
    return measured


def summarize(rows):
    groups = {}
    for row in rows:
        key = (row["tls"], row["fresh"], row["concurrency"], row["implementation"])
        groups.setdefault(key, []).append(row)
    summary = []
    for (tls, fresh, concurrency, implementation), samples in sorted(groups.items()):
        entry = dict(tls=tls, fresh=fresh, concurrency=concurrency, implementation=implementation, runs=len(samples))
        for metric in ["requests_per_second", "client_cpu_us_per_request"]:
            values = [row[metric] for row in samples]
            entry[metric] = dict(median=statistics.median(values), min=min(values), max=max(values))
        entry["p50_us_median"] = statistics.median(row["latency_us"]["p50"] for row in samples)
        entry["p99_us_median"] = statistics.median(row["latency_us"]["p99"] for row in samples)
        entry["accepted_connections"] = [row["server"]["connections"] for row in samples]
        summary.append(entry)
    for row in summary:
        candidates = [other for other in summary if other["implementation"] == "hyper" and
                      all(row[key] == other[key] for key in ["tls", "fresh", "concurrency"])]
        reference = candidates[0] if candidates else None
        if reference is not None:
            row["throughput_vs_hyper"] = row["requests_per_second"]["median"] / reference["requests_per_second"]["median"]
    return summary



def snapshot_sources(output, cargo_config=None):
    """Retain the local packages and exact harness alongside the registry lockfile."""
    manifest = Path(__file__).resolve().with_name("Cargo.toml")
    metadata = json.loads(subprocess.check_output([
        "rustup", "run", "1.87.0", "cargo", "metadata", "--manifest-path", str(manifest),
        "--locked", "--offline", "--format-version", "1",
        *(["--config", cargo_config] if cargo_config else []),
    ], text=True))
    hashes = {}
    with tarfile.open(output / "sources.tar.gz", "w:gz") as archive:
        for package in metadata["packages"]:
            if package["source"] is not None:
                continue
            root = Path(package["manifest_path"]).parent
            files = [root / "Cargo.toml", *sorted((root / "src").rglob("*.rs"))]
            if root == manifest.parent:
                files += [root / "Cargo.lock", root / "run.py", root / "readme.md"]
            for file in files:
                relative = f'{package["name"]}/{file.relative_to(root)}'
                archive.add(file, arcname=relative)
                hashes[relative] = hashlib.sha256(file.read_bytes()).hexdigest()
    (output / "source-hashes.json").write_text(json.dumps(hashes, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("/tmp/base-http-validation-target/release/base-http-client-benchmark"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--baseline-sources", type=Path, help="Source archive belonging to the baseline executable")
    parser.add_argument("--cargo-config", help="Cargo --config used for the measured build and source snapshot")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--warmup", type=float, default=1)
    parser.add_argument("--concurrency", default="1,32")
    parser.add_argument("--modes", default="reuse")
    parser.add_argument("--transports", default="tcp,tls")
    parser.add_argument("--fresh-seconds", type=float, default=2)
    parser.add_argument("--fresh-cooldown", type=float, default=10,
                        help="Pause before fresh-connection samples to limit macOS TIME_WAIT pressure")
    parser.add_argument("--implementations", default="base-open,base-call,hyper")
    options = parser.parse_args()
    options.output.mkdir(parents=True, exist_ok=False)
    binary = options.binary.resolve()
    metadata = dict(platform=platform.platform(), machine=platform.machine(), logical_cpus=os.cpu_count(),
                    started=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), arguments=vars(options).copy())
    metadata["arguments"] = {key: str(value) if isinstance(value, Path) else value for key, value in metadata["arguments"].items()}
    metadata["cpu"] = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
    metadata["rustc"] = subprocess.check_output(["rustup", "run", "1.87.0", "rustc", "-Vv"], text=True)
    (options.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    snapshot_sources(options.output, options.cargo_config)
    if BASELINE_IMPLEMENTATIONS.keys() & set(options.implementations.split(",")):
        if options.baseline_binary is None or options.baseline_sources is None:
            parser.error("baseline implementations require --baseline-binary and --baseline-sources")
        baseline = options.output / "baseline"
        baseline.mkdir()
        shutil.copyfile(options.baseline_sources, baseline / "sources.tar.gz")
        shutil.copyfile(options.baseline_sources.with_name("source-hashes.json"), baseline / "source-hashes.json")
        (baseline / "metadata.json").write_text(json.dumps(dict(
            binary_sha256=hashlib.sha256(options.baseline_binary.read_bytes()).hexdigest(),
            source_archive=str(options.baseline_sources.resolve()),
        ), indent=2) + "\n")
    rows = []
    transports = [transport == "tls" for transport in options.transports.split(",")]
    cases = [(tls, False, concurrency) for tls in transports for concurrency in map(int, options.concurrency.split(","))] if "reuse" in options.modes.split(",") else []
    if "fresh" in options.modes.split(","):
        cases += [(tls, True, 1) for tls in transports]
    implementations = options.implementations.split(",")
    with tempfile.TemporaryDirectory(prefix="base-http-client-identity-") as identity:
        subprocess.run([str(binary), "identity", "--identity", identity], check=True)
        (options.output / "certificate.der").write_bytes((Path(identity) / "certificate.der").read_bytes())
        all_cases = cases
        for fresh_stage in [False, True]:
            stage_cases = [case for case in all_cases if case[1] == fresh_stage]
            for repetition in range(options.repeats):
                cases = list(stage_cases)
                random.Random(20261028 + repetition).shuffle(cases)
                for tls, fresh, concurrency in cases:
                    order = implementations[repetition % len(implementations):] + implementations[:repetition % len(implementations)]
                    if (tls + fresh + concurrency) % 2:
                        order = list(reversed(order))
                    for implementation in order:
                        if fresh and options.fresh_cooldown > 0:
                            time.sleep(options.fresh_cooldown)
                        label = f'{"tls" if tls else "tcp"}-{"fresh" if fresh else "reuse"}-c{concurrency}-{implementation}-r{repetition + 1}'
                        row = execute(binary, Path(identity), options.output / label, implementation, tls, fresh, concurrency,
                                      options.fresh_seconds if fresh else options.seconds, options.warmup, options.baseline_binary)
                        row["repetition"] = repetition + 1
                        rows.append(row)
                        print(f'{label}: {row["requests_per_second"]:.0f} req/s, {row["client_cpu_us_per_request"]:.2f} CPU us/req, {row["server"]["connections"]} connections', flush=True)
                        (options.output / "summary.json").write_text(json.dumps(summarize(rows), indent=2) + "\n")
        (options.output / "summary.json").write_text(json.dumps(summarize(rows), indent=2) + "\n")


if __name__ == "__main__":
    main()
