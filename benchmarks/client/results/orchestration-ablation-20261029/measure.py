"""Run frozen client variants against the same server, with exact accounting."""
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
variants = {
    label: Path(f"/tmp/base-http-orchestration-{label}-20261029")
    for label in ["baseline", "pending-only", "pool-only", "combined"]
}
harness.snapshot_sources(ROOT, CONFIG)
shutil.copyfile(HARNESS, ROOT / "harness.py")
(ROOT / "binaries.json").write_text(json.dumps({
    label: {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    for label, binary in variants.items()
}, indent=2) + "\n")
for label in ["baseline", "pending-only", "pool-only"]:
    source = ROOT.parent / f"orchestration-{label}-20261029"
    destination = ROOT / label
    destination.mkdir()
    for name in ["sources.tar.gz", "source-hashes.json", "metadata.json"]:
        shutil.copyfile(source / name, destination / name)

pool_rows = []
for repetition in range(3):
    order = ["baseline", "combined"][::1 if repetition % 2 == 0 else -1]
    for concurrency in [1, 32]:
        for mode in ["pool", "pool-observed"]:
            for label in order:
                command = [str(variants[label]), "pool", "--implementation", mode,
                           "--concurrency", str(concurrency), "--seconds", "1", "--warmup", "0.2"]
                result = json.loads(subprocess.check_output(command, text=True))
                result.update(variant=label, repetition=repetition + 1, command=command)
                pool_rows.append(result)
                print(label, concurrency, mode, round(result["cpu_us_per_operation"], 3),
                      result["retirement_wakes_per_operation"], flush=True)
(ROOT / "pool-samples.json").write_text(json.dumps(pool_rows, indent=2) + "\n")

rows = []
with tempfile.TemporaryDirectory(prefix="base-http-ablation-") as identity:
    subprocess.run([str(variants["combined"]), "identity", "--identity", identity], check=True)
    for repetition in range(3):
        labels = list(variants)
        order = labels[repetition:] + labels[:repetition]
        for tls in [True, False][::1 if repetition % 2 == 0 else -1]:
            for label in order:
                name = f'{"tls" if tls else "tcp"}-{label}-r{repetition + 1}'
                row = harness.execute(variants["combined"], Path(identity), ROOT / name,
                                      "before-open", tls, False, 32, 3, 0.5, variants[label])
                row.update(implementation=label, repetition=repetition + 1)
                (ROOT / f"{name}.json").write_text(json.dumps(row, indent=2) + "\n")
                rows.append(row)
                print(name, round(row["requests_per_second"]),
                      round(row["client_cpu_us_per_request"], 3), flush=True)
(ROOT / "samples.json").write_text(json.dumps(rows, indent=2) + "\n")
(ROOT / "summary.json").write_text(json.dumps(harness.summarize(rows), indent=2) + "\n")
