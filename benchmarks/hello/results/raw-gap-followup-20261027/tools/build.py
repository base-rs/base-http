#!/usr/bin/env python3
"""Build frozen diagnostic controls, then restore staged source exactly."""
import argparse, difflib, hashlib, json, os, shutil, subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
HELLO = ROOT / 'sources/base-http/benchmarks/hello'
TARGET = Path('/tmp/base-http-hana-epoll-uring-7nxlaica/target')
p = argparse.ArgumentParser(); p.add_argument('variant', choices=['parity', 'counts', 'cas', 'direct', 'inline', 'retained']); a = p.parse_args()
paths = [HELLO/'src/main.rs', HELLO/'src/raw.rs']
if a.variant in ['cas','direct']: paths.append(ROOT/'base-scheduler/crates/selector/src/readiness_driver.rs')
if a.variant == 'inline':
    paths.extend(ROOT/'base-scheduler/crates/selector/src'/name for name in ['readiness_driver.rs','readiness_selector.rs','native_socket.rs'])
original = {path: path.read_bytes() for path in paths}
for path, data in original.items():
    saved = HERE / ('original-' + path.name)
    if not saved.exists(): saved.write_bytes(data)
try:
    main = original[paths[0]].decode()
    main = main.replace('"base-tokio" | "hyper-tokio" |', '"base-tokio" | "hyper-tokio" | "hyper-task-tokio" |')
    main = main.replace('} else if backend == "hyper-tokio" {', '} else if backend == "hyper-task-tokio" {\n                    tokio::spawn(hyper_server()).await?\n                } else if backend == "hyper-tokio" {')
    raw = original[paths[1]].decode()
    if a.variant == 'counts':
        main = main.replace('mod raw;', 'mod raw;\nmod observe;')
        main = main.replace('scheduler.spawn(async move {', 'scheduler.spawn(observe::supervisor(async move {', 1)
        main = main.replace('            })?;\n            let result = scheduler', '            }))?;\n            let result = scheduler', 1)
        main = main.replace('tokio::spawn(raw::tokio_server()).await?', 'tokio::spawn(observe::supervisor(raw::tokio_server())).await?')
        main = main.replace('tokio::spawn(hyper_server()).await?', 'tokio::spawn(observe::supervisor(hyper_server())).await?')
        raw = raw.replace('connections.push(base_connection(scheduler, socket, &cancellation));', 'connections.push(super::observe::connection(base_connection(scheduler, socket, &cancellation)));')
        raw = raw.replace('connections.push(tokio_connection(socket));', 'connections.push(super::observe::connection(tokio_connection(socket)));')
        shutil.copyfile(HERE/'observe.rs', HELLO/'src/observe.rs')
    paths[0].write_text(main); paths[1].write_text(raw)
    snapshot = HERE / a.variant; snapshot.mkdir(exist_ok=True)
    if a.variant == 'inline':
        changes=[]
        for path in paths[2:]:
            source=original[path].decode();candidate=source
            names={'readiness_driver.rs':['snapshot','clear'],'readiness_selector.rs':['check_open'],'native_socket.rs':['get_ref']}[path.name]
            for name in names:
                before='    pub(crate) fn '+name+'('
                assert candidate.count(before)==1
                candidate=candidate.replace(before,'    #[inline]\n'+before)
            path.write_text(candidate)
            changes.extend(difflib.unified_diff(source.splitlines(keepends=True),candidate.splitlines(keepends=True),fromfile='before/'+path.name,tofile='inline/'+path.name))
        (snapshot/'change.diff').write_text(''.join(changes))
    if a.variant in ['cas','direct']:
        source=original[paths[2]].decode();candidate=source
        if a.variant=='cas':
            before='        self.tick.fetch_add(8, Ordering::AcqRel);\n        self.tick.fetch_or(1, Ordering::Release);'
            after='        let _ = self.tick.fetch_update(Ordering::AcqRel, Ordering::Acquire, |tick| {\n            Some(tick.wrapping_add(8) | 1)\n        });'
            assert candidate.count(before)==1;candidate=candidate.replace(before,after)
        else:
            candidate=candidate.replace('mod notification;\nuse notification::Notification;\n','')
            candidate=candidate.replace('registration.readable.changed.notify();','registration.readable.wake();').replace('registration.writable.changed.notify();','registration.writable.wake();')
            candidate=candidate.replace('changed: Notification,','waker: Mutex<Option<Waker>>,').replace('changed: Notification::new(),','waker: Mutex::new(None),').replace('self.changed.notify();','self.wake();')
            candidate=candidate.replace('    pub(crate) async fn wait(&self, group: &DriverGroup) -> io::Result<()> {\n        let mut listener = self.changed.listen();','    fn wake(&self) {\n        let waker = self.waker.lock().unwrap().take();\n        if let Some(waker) = waker { waker.wake(); }\n    }\n    pub(crate) async fn wait(&self, group: &DriverGroup) -> io::Result<()> {')
            candidate=candidate.replace('            let result = listener.poll(cx);\n            // Registration is lazy. A publication or closure between the\n            // initial check and registration must not leave this wait asleep.','            // Diagnostic only: one waiter per direction; no abandonment guard.\n            let waker = cx.waker().clone();\n            let previous = self.waker.lock().unwrap().replace(waker);\n            drop(previous);').replace('                result\n','                Poll::Pending\n')
            assert 'self.changed' not in candidate and 'notification::' not in candidate
        paths[2].write_text(candidate)
        (snapshot/'change.diff').write_text(''.join(difflib.unified_diff(source.splitlines(keepends=True),candidate.splitlines(keepends=True),fromfile='before/readiness_driver.rs',tofile=a.variant+'/readiness_driver.rs')))
    for path in paths: shutil.copyfile(path, snapshot/path.name)
    env = dict(os.environ, CARGO_TARGET_DIR=str(TARGET))
    subprocess.run(['cargo', 'build', '--release', '--locked', '--manifest-path', str(HELLO/'Cargo.toml'), '--bin', 'base-http-hello-benchmark'], env=env, check=True)
    binary = ROOT/'binaries'/('raw-followup-' + a.variant)
    shutil.copyfile(TARGET/'release/base-http-hello-benchmark', binary); binary.chmod(0o755)
    provenance = {'variant': a.variant, 'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'original_sources': {str(path): hashlib.sha256(data).hexdigest() for path,data in original.items()}}
    (snapshot/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
finally:
    for path, data in original.items(): path.write_bytes(data)
    (HELLO/'src/observe.rs').unlink(missing_ok=True)
