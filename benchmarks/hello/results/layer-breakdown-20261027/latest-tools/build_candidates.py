#!/usr/bin/env python3
"""Build one measured scalar-reader hint, preserving all source behavior."""
from pathlib import Path
import difflib
import hashlib
import json
import os
import shutil
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
old = Path(sys.argv[2]).resolve()
target = Path(sys.argv[3]).resolve()
shutil.copytree(old / "sources", root / "sources")
hello = root / "sources/base-http/benchmarks/hello"
manifest = hello / "Cargo.toml"
text = manifest.read_text().replace("../../../../base-scheduler", "../../../../deps/base-scheduler")
manifest.write_text(text + f'\ntokio = {{ path = "{root / "deps/tokio"}" }}\n')
reader = root / "deps/base-scheduler/crates/selector/src/readiness_socket_stream_reader.rs"
original = reader.read_text()
(root / "baseline-selector-reader.rs").write_text(original)
hashes = {}

def build(manifest, binary, name):
    subprocess.run(["cargo", "build", "--release", "--offline", "--manifest-path", str(manifest), "--bin", binary],
                   env=dict(os.environ, CARGO_TARGET_DIR=str(target)), check=True)
    path = root / "binaries" / name
    shutil.copyfile(target / "release" / binary, path)
    path.chmod(0o755)
    hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()

try:
    build(root / "probe/Cargo.toml", "base-http-layer-probe", "probe-baseline")
    build(manifest, "base-http-hello-benchmark", "server-baseline")
    old_text = "    async fn read_into("
    assert original.count(old_text) == 1
    candidate = original.replace(old_text, "    #[inline]\n" + old_text)
    reader.write_text(candidate)
    (root / "candidate-selector-reader.rs").write_text(candidate)
    (root / "candidate.patch").write_text("".join(difflib.unified_diff(original.splitlines(keepends=True), candidate.splitlines(keepends=True),
         fromfile="before/crates/selector/src/readiness_socket_stream_reader.rs", tofile="after/crates/selector/src/readiness_socket_stream_reader.rs")))
    build(root / "probe/Cargo.toml", "base-http-layer-probe", "probe-reader-inline")
    build(manifest, "base-http-hello-benchmark", "server-reader-inline")
finally:
    reader.write_text(original)
    (root / "candidate-binary-sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
