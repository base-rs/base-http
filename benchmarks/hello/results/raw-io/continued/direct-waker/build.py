#!/usr/bin/env python3
"""Freeze a deliberately single-waiter ceiling experiment; always restore source."""
from pathlib import Path
import difflib
import hashlib
import json
import re
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
BENCHMARK = HERE.parents[3]
SCHEDULER = BENCHMARK.parents[2] / "base-scheduler"
SOURCE = SCHEDULER / "crates/selector/src/readiness_driver.rs"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(name, command, cwd):
    print(name, flush=True)
    with (HERE / f"{name}.log").open("w") as log:
        subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, check=True)


original = SOURCE.read_text()
(HERE / "before-readiness_driver.rs").write_text(original)
baseline = HERE.parent / "native-events/server"
baseline_metadata = json.loads((baseline.parent / "provenance.json").read_text())
assert digest(baseline) == baseline_metadata["server_sha256"]
assert digest(SOURCE) == baseline_metadata["source_sha256"]["readiness_driver.rs"]
shutil.copyfile(baseline, HERE / "before-server")
(HERE / "before-server").chmod(0o755)

candidate = original.replace("mod notification;\nuse notification::Notification;\n", "")
candidate = candidate.replace("registration.readable.changed.notify();", "registration.readable.wake();")
candidate = candidate.replace("registration.writable.changed.notify();", "registration.writable.wake();")
candidate = candidate.replace("changed: Notification,", "waker: Mutex<Option<Waker>>,")
candidate = candidate.replace("changed: Notification::new(),", "waker: Mutex::new(None),")
candidate = candidate.replace("self.changed.notify();", "self.wake();")
candidate = candidate.replace(
    "    pub(crate) async fn wait(&self, group: &DriverGroup) -> io::Result<()> {\n"
    "        let mut listener = self.changed.listen();",
    "    fn wake(&self) {\n"
    "        let waker = self.waker.lock().unwrap().take();\n"
    "        if let Some(waker) = waker {\n"
    "            waker.wake();\n"
    "        }\n"
    "    }\n"
    "    pub(crate) async fn wait(&self, group: &DriverGroup) -> io::Result<()> {",
)
candidate = candidate.replace(
    "            let result = listener.poll(cx);\n"
    "            // Registration is lazy. A publication or closure between the\n"
    "            // initial check and registration must not leave this wait asleep.",
    "            // Comparison-only: one waiting task per direction, no listener guard.\n"
    "            // Clone/drop/wake callbacks still execute outside the slot lock.\n"
    "            let waker = cx.waker().clone();\n"
    "            let previous = self.waker.lock().unwrap().replace(waker);\n"
    "            drop(previous);",
)
candidate = candidate.replace("                result\n", "                Poll::Pending\n")
assert "notification::" not in candidate and "self.changed" not in candidate
assert candidate.count("Mutex<Option<Waker>>") == 1
metadata = {"before_server_sha256": digest(baseline)}

try:
    SOURCE.write_text(candidate)
    run("format", ["rustfmt", "--edition", "2024", str(SOURCE)], SCHEDULER)
    candidate = SOURCE.read_text()
    (HERE / "direct-readiness_driver.rs").write_text(candidate)
    (HERE / "change.diff").write_text("".join(difflib.unified_diff(
        original.splitlines(keepends=True), candidate.splitlines(keepends=True),
        fromfile="before/readiness_driver.rs", tofile="direct/readiness_driver.rs")))
    metadata["experimental_source_sha256"] = digest(SOURCE)
    for name, selector in [
        ("reader-tests", "readiness_socket_stream_reader::tests"),
        ("writer-tests", "readiness_socket_stream_writer::readiness_tests"),
        ("collected-pointer-test", "collected_pointers_survive_remote_retirement"),
        ("cross-worker-test", "closing_one_worker_preserves_another_workers_collected_batch"),
    ]:
        run(name, ["cargo", "test", "--locked", "-p", "base-scheduler-selector", "--lib", selector], SCHEDULER)
    executable = re.search(r"Running unittests src/lib.rs \(([^)]+)\)", (HERE / "cross-worker-test.log").read_text())[1]
    shutil.copyfile(SCHEDULER / executable, HERE / "tests")
    (HERE / "tests").chmod(0o755)
    metadata["tests_sha256"] = digest(HERE / "tests")
    run("build", ["cargo", "build", "--release", "--locked", "--features", "profile-tools", "--bins"], BENCHMARK)
    shutil.copyfile(BENCHMARK / "target/release/base-http-hello-benchmark", HERE / "server")
    (HERE / "server").chmod(0o755)
    metadata["server_sha256"] = digest(HERE / "server")
    metadata["rustc"] = subprocess.check_output(["rustc", "-Vv"], text=True)
finally:
    SOURCE.write_text(original)
    assert SOURCE.read_text() == original
    metadata["restored_source_sha256"] = digest(SOURCE)
    (HERE / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print("Original general-waiter source restored.", flush=True)

run("restored-build", ["cargo", "build", "--release", "--locked", "--features", "profile-tools", "--bins"], BENCHMARK)
assert digest(BENCHMARK / "target/release/base-http-hello-benchmark") == digest(baseline)
print("Restored release binary matches the preserved baseline byte for byte.", flush=True)
