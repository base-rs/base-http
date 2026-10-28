#!/usr/bin/env python3
"""Extend the pinned scheduler echo benchmark in a fresh disposable tree."""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import tarfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--archive", type=Path, required=True)
a = p.parse_args()
a.root.mkdir(parents=True, exist_ok=False)
source = a.root / "scheduler"
source.mkdir()
with tarfile.open(a.archive) as archive:
    archive.extractall(source, filter="data")
echo = source / "benchmarks/echo"
patch = []

def edit(relative, transform):
    path = echo / relative
    before = path.read_text()
    after = transform(before)
    assert before != after, relative
    path.write_text(after)
    patch.extend(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True),
        fromfile="before/benchmarks/echo/" + relative, tofile="after/benchmarks/echo/" + relative))

native = '''        #[cfg(target_os = "linux")]
        "native-poll" => {
            use base_scheduler_native::{NativeOptions, NativeScheduler};
            let scheduler = Arc::new(
                NativeScheduler::with_options(NativeOptions {
                    workers,
                    ..Default::default()
                })
                .expect("Native epoll setup"),
            );
            ready(&listener);
            scheduler.block_on(scheduler_server(
                Arc::clone(&scheduler), listener, connections, true, false,
            ));
            scheduler.block_on(scheduler.shutdown());
        }
'''

def server(text):
    marker = '        #[cfg(target_os = "linux")]\n        "iouring" |'
    assert text.count(marker) == 1
    text = text.replace(marker, native + marker)
    start = text.index("async fn echo_monitor<S>")
    end = text.index('#[cfg(target_os = "linux")]', start)
    borrowed = text[start:end]
    assert borrowed.count('            return;') == 1
    borrowed = borrowed.replace('            return;', '            break;')
    ending = '    }\n}\n\n'
    assert borrowed.endswith(ending)
    borrowed = borrowed[:-len(ending)] + '''    }
    reader.close().await.expect("retire stream reader");
    writer.close().await.expect("retire stream writer");
}

'''
    return text[:start] + borrowed + text[end:]

edit("src/server.rs", server)
edit("src/lib.rs", lambda s: s.replace('Backends: tokio,', 'Backends: native-poll (Linux), tokio,'))
edit("tests/smoke.rs", lambda s: s + '''
#[cfg(target_os = "linux")]
#[test]
fn native_epoll_poll() {
    smoke("native-poll");
}
''')
(a.root / "echo.patch").write_text("".join(patch))
(a.root / "source-provenance.json").write_text(json.dumps({
    "scheduler_commit": "f05a630db60686526b6145efb284a9c2f61fe8b4",
    "scheduler_archive_sha256": hashlib.sha256(a.archive.read_bytes()).hexdigest(),
    "changes": ["add native-poll echo mode", "explicit borrowed-handle retirement after EOF", "native epoll smoke test"],
}, indent=2) + "\n")
print(a.root)
