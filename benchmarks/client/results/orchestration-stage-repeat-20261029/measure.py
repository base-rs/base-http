"""Compare cumulative optimization stages using a fixed common server."""
from pathlib import Path
import hashlib
import importlib.util
import json
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent
HARNESS = ROOT.parent.parent / "run.py"
spec = importlib.util.spec_from_file_location("harness", HARNESS)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)
CONFIG = 'patch.crates-io.base-pool.path="/Users/samuel/Developer/base-rs/base-pool"'
variants = {label: Path(f"/tmp/base-http-orchestration-{label}-20261029")
            for label in ["baseline", "combined", "cancellation", "replace"]}
harness.snapshot_sources(ROOT, CONFIG)
shutil.copyfile(HARNESS, ROOT / "harness.py")
(ROOT / "binaries.json").write_text(json.dumps({
    label: {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    for label, binary in variants.items()
}, indent=2) + "\n")
sources = {"baseline": "orchestration-baseline-20261029",
           "combined": "orchestration-ablation-20261029",
           "cancellation": "orchestration-cancellation-build-20261029"}
for label, name in sources.items():
    destination = ROOT / label
    destination.mkdir()
    for filename in ["sources.tar.gz", "source-hashes.json"]:
        shutil.copyfile(ROOT.parent / name / filename, destination / filename)

rows = []
with tempfile.TemporaryDirectory(prefix="base-http-stages-") as identity:
    subprocess.run([str(variants["replace"]), "identity", "--identity", identity], check=True)
    for repetition in range(3):
        labels = list(variants)
        order = labels[repetition:] + labels[:repetition]
        for tls in [True, False][::1 if repetition % 2 == 0 else -1]:
            for label in order:
                name = f'{"tls" if tls else "tcp"}-{label}-r{repetition + 1}'
                row = harness.execute(variants["replace"], Path(identity), ROOT / name,
                                      "before-open", tls, False, 32, 3, 0.5, variants[label])
                row.update(implementation=label, repetition=repetition + 1)
                (ROOT / f"{name}.json").write_text(json.dumps(row, indent=2) + "\n")
                rows.append(row)
                print(name, round(row["requests_per_second"]),
                      round(row["client_cpu_us_per_request"], 3), flush=True)
(ROOT / "samples.json").write_text(json.dumps(rows, indent=2) + "\n")
(ROOT / "summary.json").write_text(json.dumps(harness.summarize(rows), indent=2) + "\n")
