# Worker-owned readiness and direct kqueue

The default local Native scheduler now drives socket readiness on its task workers. On macOS each worker owns a kqueue directly. This removes the separate `async-io` reactor-to-worker handoff from pending socket operations. The existing executor, work stealing, timers, bound I/O capabilities, and HTTP implementation remain in use.

The measurements below isolate scheduler changes with the original HTTP implementation. The subsequent [HTTP optimization integration](http-optimization.md) measures the revised HTTP code with this finished scheduler.

The HTTP crates and this benchmark patch their scheduler dependencies to the sibling `base-scheduler` workspace. The [resolved graph](results/local-scheduler/scheduler-graph.json) confirms that all six scheduler packages in the benchmark are local, including the shared interface.

## Implementation

`ReadinessSelector::with_workers(options, workers)` returns a selector and one `ReadinessDriver` per worker. `NativeScheduler::with_options` composes them through the same `SelectorDriver`/`SelectorWorker` interface used by io_uring. Implementation lives in `base-scheduler/crates/selector/src/readiness_driver.rs` and its `kqueue.rs` module.

- The macOS backend calls `kqueue`, `kevent`, and `fcntl` through libc. Persistent `EV_CLEAR` read/write filters avoid per-wait rearming. Fixed stack receipt storage avoids allocating a change list. `EVFILT_USER` interrupts idle workers. Interrupted registration changes and wakeups retry; interrupted waits return to the executor; fatal wait errors close the readiness group and wake its waiters.
- A ready transfer attempts its nonblocking syscall before routing or registration lookup. A stalled socket lazily registers with the explicitly selected selector's current worker, or an external round-robin route. It keeps that owner within the selector group across task migration. Another compatible selector can register the same socket independently.
- Readiness generations prevent a racing `WouldBlock` from clearing a newer event. Independent inline listeners support concurrent same-direction waits and release only their own notification state on abandonment.
- Registrations never retain borrowed payload pointers or a strong native-socket owner. The final socket alias synchronously deregisters before releasing the native owner, including on another thread. Numeric keys never repeat within a driver, while live table entries are removed on deregistration. A collected stale event cannot address a recycled descriptor.

`ReadinessSelector::new` and `with_options` retain independent `async-io` progress for standalone use. Native sockets keep their existing `retain_native` interoperability. Other edge-capable targets use worker-owned `polling`; targets without edge support keep independent progress. The direct kqueue implementation is currently macOS-specific.

Apple documents the filter and receipt semantics in the [kevent manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/kevent.2.html).

## Measurements

Apple M4 Pro, macOS arm64, release builds, one application worker, wrk 4.2.0 with four load threads and 64 persistent connections. Each timed sample has one second of warmup. There are no concurrent builds, tests, profilers, or other controlled benchmark jobs during timing. CPU affinity and unrelated host activity are uncontrolled.

The [final recheck](results/local-scheduler/final/summary.json) uses the finished release code: three interleaved eight-second runs per mode, with unchanged production HTTP sources.

| Backend | Median requests/s | Run range | Median p99 | Process CPU |
| --- | ---: | ---: | ---: | ---: |
| Native, background readiness | 60,697 | 58,236–61,868 | 1.340 ms | 118.9% |
| Native, direct worker-owned kqueue | 68,258 | 62,037–68,619 | 1.260 ms | 99.5% |
| Base HTTP / Tokio | 69,200 | 65,090–70,663 | 1.230 ms | 99.6% |
| Hyper / Tokio | 131,590 | 129,234–131,762 | 0.693 ms | 99.5% |

Direct kqueue improves throughput by **12.5%** against the matched background-reactor control, and is **1.4% below Base/Tokio** by median. CPU falls from approximately 119% to one core. Run ranges overlap; these measurements do not establish exact parity or a general scaling result.

An earlier [five-run screening comparison](results/local-scheduler/comparison/summary.json) also includes a saved worker-owned `polling` checkpoint. Its medians were 60,533 requests/s for background readiness, 67,850 for direct kqueue, 67,582 for worker-owned `polling`, and 69,585 for Base/Tokio. The direct/polling difference was **0.4%**, smaller than run variation. Worker ownership and persistent readiness account for the demonstrated improvement; bypassing the `polling` wrapper has no demonstrated additional throughput benefit here. The experiment does not isolate worker placement from persistent registration and readiness caching.

Across both comparisons: **37 timed samples, 23,795,686 timed requests, zero reported wrk errors**. Before every sample Python checks response bytes, fields, connection reuse, and connection closure. Wrk does not validate body bytes during timing. Raw output, binary/source hashes, and configuration are retained in the result directories.

These are the original production HTTP paths. SHA-256 comparison with `results/macos/sources.json` confirms no changes to the HTTP library sources. Previous 114–120k Base results used separate HTTP optimization experiments, so those absolute rates are not comparable to this unchanged-HTTP comparison. The remaining gap to Hyper includes the HTTP implementation and its I/O bindings; the earlier profiles identify output batching and allocation work.

No Linux/io_uring throughput or Windows measurements were collected. Multi-worker correctness tests are not a scaling benchmark.

## Validation

- The macOS workspace coverage task passed all tests and **28,778/28,778 measured source regions**, with three existing invariant-only unreachable regions excluded. The all-feature run reports 1,375 passing tests including documentation and tooling tests.
- New tests cover actual default-worker I/O with one and three workers, root/external callers, independent full-duplex handles, future migration, concurrent directional listeners, cancellation and abandonment, cross-selector sockets, cross-thread exclusive close, registration/key cleanup, descriptor churn, initialization failures, interrupted/fatal waits, and shutdown.
- A task on another scheduler's worker continues reading through its explicitly selected scheduler even after its executor's own driver closes. This makes the scheduler-identity routing test observable.
- Workspace Clippy with warnings denied, formatting, and Rust 1.85.1 checks for Native/Selector and all their targets pass. The three HTTP packages also pass their tests using the local manifest patches.
- Linux and Windows execution remain unverified on this host. The unsupported-edge fallback is exercised through the private poller factory.

[Check logs](results/local-scheduler/checks/) and the [scheduler patch snapshot](results/local-scheduler/scheduler.patch) retain the evidence. The release code stayed unchanged during the final throughput recheck; subsequent test review removed an assertion susceptible to descriptor reuse by parallel tests.

## Reproduce

From this benchmark directory:

```sh
cargo build --release --locked
python3 compare_local_scheduler.py --wrk /absolute/path/to/wrk \
  --repetitions 3 --duration 8 --output results/local-scheduler/recheck
```

The optional `--polling-binary results/local-scheduler/polling-worker` adds the saved macOS arm64 checkpoint binary used in the first screening. It is an experimental control, not a current runtime switch.

From the scheduler workspace:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
rustup run 1.85.1 cargo check -p base-scheduler-native -p base-scheduler-selector --all-targets --locked
rustup run stable cargo bake --locked test:coverage --all-targets true --all-features true
```

`cargo test --locked` in `base-http`, `base-http1`, and `base-http-server` now resolves the local scheduler through their manifest patches.
