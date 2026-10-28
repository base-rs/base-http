#!/usr/bin/env python3
"""Isolate static-response write coalescing and receive scratch-buffer reuse.

All codec changes are applied to disposable copies under target/. The coalescing
patch deliberately supports only this known, immediately available 13-byte body;
it is a diagnostic, not a general streaming implementation.
"""

import argparse
import datetime
import difflib
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
from types import SimpleNamespace

from run import sample


ROOT = Path(__file__).resolve().parent
HTTP = ROOT.parent.parent
VARIANTS = ("baseline", "receive-reuse", "coalesced", "combined")


def replace_once(source, old, new):
    assert source.count(old) == 1, f"expected one match: {old!r}"
    return source.replace(old, new, 1)


def reuse_input(source):
    source = replace_once(source, "    buffer: Vec<u8>,", "    buffer: Vec<u8>,\n    scratch: Vec<u8>,")
    source = replace_once(
        source, "            buffer: Vec::new(),",
        "            buffer: Vec::new(),\n            scratch: vec![0; options.read_size],",
    )
    return replace_once(
        source,
        "        let mut chunk = vec![0; self.options.read_size];\n"
        "        let n = self.reader.read_into(&mut chunk, cancellation).await?;\n"
        "        self.buffer.extend_from_slice(&chunk[..n]);",
        "        let n = self.reader.read_into(&mut self.scratch, cancellation).await?;\n"
        "        self.buffer.extend_from_slice(&self.scratch[..n]);",
    )


def coalesce_output(source):
    source = replace_once(source, "    writer: W,", "    writer: W,\n    pending: Vec<u8>,")
    source = replace_once(source, "            writer,", "            writer,\n            pending: Vec::new(),")
    # Route every flush through the diagnostic buffer, including interim output.
    source = source.replace("self.writer.flush(cancellation).await?", "self.flush(cancellation).await?")
    source = source.replace("self.output.writer.flush(cancellation).await?", "self.output.flush(cancellation).await?")
    source = replace_once(
        source,
        "    pub(crate) async fn write_all(\n        &mut self,\n        mut bytes: &[u8],",
        "    pub(crate) async fn write_all(\n"
        "        &mut self,\n        bytes: &[u8],\n        cancellation: &Cancellation,\n"
        "    ) -> Result<(), Error> {\n        cancellation.check()?;\n"
        "        self.pending.extend_from_slice(bytes);\n        Ok(())\n    }\n\n"
        "    async fn send_all(\n        &mut self,\n        mut bytes: &[u8],",
    )
    source = replace_once(
        source,
        "        // Commit the handshake before the first potentially blocking input read.\n"
        "        self.flush(cancellation).await?;",
        "        // DIAGNOSTIC ONLY: this benchmark's body cannot block.\n"
        "        // Do not apply this policy to generic streaming or upgrade bodies.\n"
        "        assert!(!plan.tunnel && matches!(plan.framing, Framing::Fixed(13))\n"
        "            && response.head.status == Status::OK && response.body.is_ready());",
    )
    return replace_once(
        source,
        "    pub(crate) async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {\n"
        "        Ok(self.flush(cancellation).await?)\n    }",
        "    pub(crate) async fn flush(&mut self, cancellation: &Cancellation) -> Result<(), Error> {\n"
        "        let mut pending = std::mem::take(&mut self.pending);\n"
        "        self.send_all(&pending, cancellation).await?;\n"
        "        pending.clear();\n        self.pending = pending;\n"
        "        Ok(self.writer.flush(cancellation).await?)\n    }",
    )


def digest(file):
    return hashlib.sha256(file.read_bytes()).hexdigest()


def prepare(args):
    args.stage.mkdir(parents=True, exist_ok=True)
    provenance = {}
    for variant in VARIANTS:
        stage = args.stage / variant
        stage.mkdir(exist_ok=True)
        for name in ("base-http1", "base-http-server"):
            original = HTTP.parent / name
            copied = stage / name
            shutil.copytree(original / "src", copied / "src", dirs_exist_ok=True)
            shutil.copyfile(original / "readme.md", copied / "readme.md")
            manifest = (original / "Cargo.toml").read_text().replace(
                'path = "../base-http"', f'path = "{HTTP}"'
            )
            (copied / "Cargo.toml").write_text(manifest)
        codec = stage / "base-http1" / "src"
        if variant in ("receive-reuse", "combined"):
            file = codec / "input.rs"
            file.write_text(reuse_input(file.read_text()))
        if variant in ("coalesced", "combined"):
            file = codec / "output.rs"
            file.write_text(coalesce_output(file.read_text()))
        patch = ""
        for name in ("input.rs", "output.rs"):
            patch += "".join(difflib.unified_diff(
                (HTTP.parent / "base-http1" / "src" / name).read_text().splitlines(keepends=True),
                (codec / name).read_text().splitlines(keepends=True),
                fromfile=f"base-http1/src/{name}", tofile=f"base-http1/src/{name}",
            ))
        (args.output / f"{variant}.patch").write_text(patch)
        bench = stage / "hello"
        shutil.copytree(ROOT / "src", bench / "src", dirs_exist_ok=True)
        manifest = (ROOT / "Cargo.toml").read_text()
        manifest = manifest.replace('path = "../.."', f'path = "{HTTP}"')
        manifest = manifest.replace('path = "../../crates/server"', 'path = "../base-http-server"')
        manifest = manifest.replace('path = "../../crates/http1"', 'path = "../base-http1"')
        (bench / "Cargo.toml").write_text(manifest)
        shutil.copyfile(ROOT / "Cargo.lock", bench / "Cargo.lock")
        subprocess.run([
            "cargo", "build", "--release", "--locked", "--features", "profile-tools",
            "--bin", "base-http-hello-benchmark", "--bin", "allocation-probe",
            "--manifest-path", str(bench / "Cargo.toml"),
            "--target-dir", str(args.stage / "build"),
        ], check=True)
        binary = stage / "server"
        shutil.copyfile(args.stage / "build/release/base-http-hello-benchmark", binary)
        binary.chmod(0o755)
        allocation = subprocess.check_output([str(args.stage / "build/release/allocation-probe")], text=True)
        (args.output / f"{variant}.allocations.json").write_text(allocation)
        files = [*codec.glob("*.rs"), *bench.glob("src/**/*.rs"), bench / "Cargo.toml", bench / "Cargo.lock"]
        files.extend((stage / "base-http-server" / "src").glob("*.rs"))
        files.extend((HTTP / "src").rglob("*.rs"))
        provenance[variant] = {
            "binary_sha256": digest(binary), "patch_sha256": digest(args.output / f"{variant}.patch"),
            "sources_sha256": {str(file): digest(file) for file in sorted(files)},
        }
    (args.output / "sources.json").write_text(json.dumps(provenance, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wrk", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "results/experiments")
    parser.add_argument("--stage", type=Path, default=ROOT / "target/profile-experiments")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--duration", type=int, default=5)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.stage = args.stage.resolve()
    args.wrk = args.wrk.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if not args.skip_build:
        prepare(args)
    metadata = {
        "utc_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "configuration": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "uname": list(os.uname()),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "wrk": subprocess.run([str(args.wrk), "--version"], capture_output=True, text=True).stdout,
        "concurrency": 64, "load_threads": 4, "warmup_seconds": 1,
        "order_seed": 20261027,
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    samples = []
    rng = random.Random(20261027)
    with (args.output / "samples.jsonl").open("w") as journal:
        for repetition in range(1, args.repetitions + 1):
            jobs = [(variant, backend) for variant in VARIANTS for backend in ("base-native", "base-tokio")]
            jobs.append(("baseline", "hyper-tokio"))
            rng.shuffle(jobs)
            for variant, backend in jobs:
                output = args.output / variant
                output.mkdir(exist_ok=True)
                print(f"{variant:14} ", end="", flush=True)
                result = sample(SimpleNamespace(
                    binary=args.stage / variant / "server", wrk=args.wrk, output=output,
                    threads=4, warmup=1, duration=args.duration,
                ), backend, 64, repetition)
                result["variant"] = variant
                samples.append(result)
                journal.write(json.dumps(result) + "\n")
                journal.flush()
    summary = []
    for variant, backend in sorted({(row["variant"], row["backend"]) for row in samples}):
        group = [row for row in samples if row["variant"] == variant and row["backend"] == backend]
        summary.append({
            "variant": variant, "backend": backend,
            "requests_per_second_min": min(row["requests_per_second"] for row in group),
            "requests_per_second_max": max(row["requests_per_second"] for row in group),
            **{key: statistics.median(row[key] for row in group) for key in (
                "requests_per_second", "latency_p99_ms", "server_cpu_percent_one_core",
            )},
        })
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
