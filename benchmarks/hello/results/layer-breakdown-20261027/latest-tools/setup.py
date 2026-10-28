#!/usr/bin/env python3
"""Install benchmark-only hooks into disposable dependency copies on Hana."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
here = Path(__file__).resolve().parent
deps = root / "deps"
deps.mkdir(exist_ok=True)
base = deps / "base-scheduler"
assert base.exists(), "Extract the merged scheduler source archive first."
tokio_source = next(Path.home().glob(".cargo/registry/src/*/tokio-1.54.1"))
tokio = deps / "tokio"
shutil.copytree(tokio_source, tokio)
sources = [*base.rglob("*.rs"), *tokio.rglob("*.rs")]
before = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}

def append(path, content):
    path.write_text(path.read_text() + "\n" + content + "\n")

selector = base / "crates/selector/src"
shutil.copyfile(here / "base_probe.rs", selector / "readiness_driver/layer_probe.rs")
append(selector / "readiness_driver.rs", "pub(crate) mod layer_probe;")
append(selector / "lib.rs", "pub use readiness_driver::layer_probe::probe as layer_probe;")
append(selector / "readiness_driver/epoll.rs", '''
pub(super) fn layer_events(tokens: &[usize]) -> Events {
    let mut events = Events::new();
    for &token in tokens {
        events.raw.push(libc::epoll_event { events: libc::EPOLLIN as u32, u64: token as u64 });
    }
    events
}
''')
shutil.copyfile(here / "tokio_probe.rs", tokio / "src/runtime/io/layer_probe.rs")
append(tokio / "src/runtime/io/mod.rs", "pub(crate) mod layer_probe;")
append(tokio / "src/runtime/io/driver.rs", (here / "tokio_driver.rs").read_text())
append(tokio / "src/lib.rs", '''
#[doc(hidden)]
pub fn layer_probe(kind: &str, iterations: u64, width: usize, waker: &std::task::Waker) -> (u64, usize) {
    runtime::io::layer_probe::probe(kind, iterations, width, waker)
}
''')
package = root / "probe"
shutil.copytree(here, package, ignore=shutil.ignore_patterns("target", "__pycache__", "results", "Cargo.lock"))
patches = ['[patch.crates-io]']
for name, relative in [("base-scheduler", ""), ("base-scheduler-native", "crates/native"),
                        ("base-scheduler-selector", "crates/selector"), ("base-scheduler-timers", "crates/timers"),
                        ("base-scheduler-iouring", "crates/iouring")]:
    patches.append(f'{name} = {{ path = "{base / relative}" }}')
patches.append(f'tokio = {{ path = "{tokio}" }}')
append(package / "Cargo.toml", "\n".join(patches))
(root / "before-source-sha256.json").write_text(json.dumps(before, indent=2) + "\n")
subprocess.run(["cargo", "build", "--release", "--offline", "--manifest-path", str(package / "Cargo.toml")], check=True)
