#!/usr/bin/env python3
"""Check general HTTP changes separately from static-only benchmark patches."""

import difflib
from pathlib import Path
import shutil
import subprocess

from experiment import ROOT, HTTP, coalesce_output
from experiment_next import common_methods, compact_exchange, parser_storage, response_storage


def main():
    stage = ROOT / "target/http-validation"
    output = ROOT / "results/http-validation"
    output.mkdir(parents=True, exist_ok=True)
    for name in ("base-http1", "base-http-server"):
        original = HTTP.parent / name
        copied = stage / name
        shutil.copytree(original, copied, dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("target", ".git"))
        manifest = (copied / "Cargo.toml").read_text().replace('path = "../base-http"', f'path = "{HTTP}"')
        (copied / "Cargo.toml").write_text(manifest)
    codec = stage / "base-http1/src"
    file = codec / "input.rs"
    file.write_text(compact_exchange(parser_storage(file.read_text())))
    file = codec / "wire.rs"
    file.write_text(common_methods(file.read_text()))
    file = codec / "output.rs"
    source = file.read_text()
    candidate = response_storage(coalesce_output(source))
    start, end = "fn plan(", "pub(crate) fn connection_option("
    old_plan = start + source.split(start, 1)[1].split(end, 1)[0]
    new_plan = start + candidate.split(start, 1)[1].split(end, 1)[0]
    # Preserve the original writer/batching policy; validate only the plan,
    # parser storage, method interning and shared exchange representation.
    file.write_text(source.replace(old_plan, new_plan).replace("exchange.head.", "exchange.head()."))
    patch = ""
    for name in ("input.rs", "output.rs", "wire.rs"):
        patch += "".join(difflib.unified_diff(
            (HTTP.parent / "base-http1/src" / name).read_text().splitlines(keepends=True),
            (codec / name).read_text().splitlines(keepends=True),
            fromfile=f"base-http1/src/{name}", tofile=f"base-http1/src/{name}",
        ))
    (output / "general-http.patch").write_text(patch)
    for name in ("base-http1", "base-http-server"):
        with (output / f"{name}.txt").open("w") as log:
            subprocess.run([
                "cargo", "test", "--offline", "--manifest-path", str(stage / name / "Cargo.toml"),
                "--target-dir", str(stage / "build"),
            ], stdout=log, stderr=subprocess.STDOUT, check=True)
        print(f"{name}: passed", flush=True)


if __name__ == "__main__":
    main()
