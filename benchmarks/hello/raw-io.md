# Raw read/write scheduler control

Measured 2026-10-27 on macOS arm64 / Apple M4 Pro. This control follows the [HTTP ownership investigation](remaining-gap.md) and removes request parsing, semantic messages, body polling, handler calls and response serialization from the serving loop. The remaining user CPU difference led to a measured scheduler notification change. Production HTTP semantics and framing are unchanged.

The later [continued investigation](#continued-investigation-ready-writes-and-direct-event-dispatch) replaces registration lookup during event delivery with direct pointer tokens and driver-owned retirement, and moves successful writes ahead of wait-future construction. The earlier tables below retain their original measured revisions; the final continued cohort reports the new combined revision.

## What runs

The [raw server](src/raw.rs) reads arbitrary bytes into one reusable 16 KiB buffer, ignores their contents and writes this complete 78-byte response from one static byte string:

```text
HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!
```

The steady-state Base loop is:

```rust
loop {
    if reader.read_into(&mut buffer, cancellation).await? == 0 {
        return Ok(());
    }
    check_write(writer.write(RESPONSE, cancellation).await?)?;
}
```

The direct Tokio loop uses `socket.read` and `socket.write` with the same buffer size and response bytes. Neither uses `write_all`: a short write terminates the connection and reports an error. Base constructs its scheduler-bound reader/writer once, uses `Cancellation::never()`, closes both lanes and then exclusively closes its socket. Flush/retirement happens after input EOF, outside the response loop.

Every variant retains one application worker, `FuturesUnordered` connection supervision, a 1,024-connection admission bound and `TCP_NODELAY`. Native executes on its native worker; direct Tokio and Base/Tokio use current-thread Tokio runtimes. Within each initial control cohort, variants use the same executable, release profile and Homebrew Rust 1.99.1 / LLVM 23 build. Final before/after measurements deliberately compare preserved executables. Tokio's `io-util` feature enables its standard read/write extensions; no dependency versions changed.

| Mode | Protocol work | Transport |
| --- | --- | --- |
| `base-native-raw` | Fixed byte response after any positive read | Native scheduler's bound reader/writer |
| `tokio-raw` | Identical raw operation | Direct Tokio TCP read/write |
| `base-tokio-raw` | Identical raw operation | Base's bound capabilities on Tokio |
| `tokio-task-raw` | Identical raw operation | Direct Tokio, supervisor spawned as one task |
| `base-native` | Complete Base HTTP/1 server | Native scheduler |
| `hyper-tokio` | Complete Hyper HTTP/1 server | Direct Tokio through `TokioIo` |

## Validation and counting

The raw modes deliberately do not implement HTTP request framing or `Connection: close`. Their preflight checks three persistent responses and client-initiated closure; full HTTP modes retain their existing close-policy validation. A [separate validator](diagnostics/validate_raw.py) checks that non-HTTP input produces the exact response, the connection remains reusable and peer half-close reaches EOF. All raw modes pass. [Validation](results/raw-io/validation.json), [five-mode preflight](results/raw-io/preflight.json).

TCP reads are not message boundaries. This control assumes small, nonpipelined requests, with one complete GET arriving per positive read. An independently instrumented three-second wrk t4/c64 diagnostic observes exactly **41 bytes per positive read and 78 bytes per successful write**, with matching positive read/write counts for all five modes. This supports the assumption for that observed diagnostic, not arbitrary segmentation/pipelining or a proof about every uninstrumented sample.

Native raw and direct Tokio raw issue approximately one receive per completed response, plus occasional WouldBlock/connection-termination calls; their 10,000-request sequential diagnostics record exactly one positive receive and write per request and no receive-WouldBlock. Base/Tokio raw performs an additional receive-WouldBlock probe per request under both workloads. Interposed runs supply syscall counts only; their timings are excluded. Responses already sent when wrk stops explain the roughly 60 more successful writes than completed responses in each t4/c64 run. [Sequential counts](results/raw-io/socket-counts-sequential.json), [wrk counts](results/raw-io/socket-counts-wrk.json).

## Measurements

| Variant | Median req/s | User µs/request | System µs/request |
| --- | ---: | ---: | ---: |
| Base HTTP/1, Native | 127,106 | 0.882 | 6.983 |
| Hyper HTTP/1, Tokio | 129,489 | 0.685 | 7.005 |
| Native raw | 132,776 | 0.388 | 7.115 |
| Base/Tokio raw | 130,227 | 0.432 | 7.212 |
| Direct Tokio raw (root) | 133,864 | 0.265 | 7.171 |

Native raw is **0.8% behind direct Tokio raw by median throughput**, but consumes **0.124 µs/request more user CPU** (about **47% more**). The user CPU difference persists in every paired round after all HTTP parsing, semantic allocation, handler and body work is removed. Full Base/Hyper user CPU differs by 0.197 µs/request in this same cohort. This leaves scheduler/I/O/runtime overhead in scope; the full HTTP gap cannot be assigned entirely to HTTP semantic ownership. Do not subtract these differences to claim an exact scheduler share: generated futures, readiness timing and kernel behavior interact with protocol work. All samples have zero errors. [Samples](results/raw-io/comparison/samples.jsonl), [summary](results/raw-io/comparison/summary.json), [environment](results/raw-io/comparison/environment.json).

System time is about seven microseconds per request, so the extra user CPU is only a small part of total cost. This explains why raw throughput can look close despite a clear user CPU difference. A separate 10,000-request copy diagnostic observes **zero exported libc copy calls** for all three raw modes; inlined copies remain unmeasured. The previously measured large HTTP ownership copies are absent from this raw path. [Copy counts](results/raw-io/memory-counts.json).

### Task-dispatch confirmation

Native already runs its supervisor as one spawned Native task; the direct Tokio control above runs as the `Runtime::block_on` root. To check this execution-context difference, `tokio-task-raw` spawns the same Tokio raw supervisor once and awaits its join handle. It does not spawn connection tasks. The fresh comparison measures Native raw, Tokio root raw and Tokio task raw from the same rebuilt executable. Existing raw loop code is unchanged. All four raw modes pass byte/reuse/EOF validation; Native/root/task sequential counts remain one receive and write per request. [Task validation](results/raw-io/task-validation.json), [socket counts](results/raw-io/task-socket-counts.json), [build](results/raw-io/task-build-provenance.json).

| Variant | Median req/s | User µs/request | System µs/request |
| --- | ---: | ---: | ---: |
| Native raw | 133,195 | 0.385 | 7.091 |
| Direct Tokio raw, root | 135,116 | 0.282 | 7.109 |
| Direct Tokio raw, spawned supervisor | 133,190 | 0.265 | 7.210 |

Spawning the Tokio supervisor does not explain Native's additional user CPU. Native consumes more user CPU than both Tokio modes in every paired round; Native and spawned Tokio median throughput are essentially equal in this cohort because system time offsets the difference. [Samples](results/raw-io/task-comparison/samples.jsonl), [summary](results/raw-io/task-comparison/summary.json).

The confirmation uses five interleaved eight-second samples per variant, each with a fresh process and a one-second warmup, four load threads and 64 persistent connections. No builds, checks, profilers or other load tests run concurrently. Host activity and CPU placement remain uncontrolled. User/system CPU are process-accounting deltas per completed request, not stack samples. A raw comparison measures the scheduler and I/O path under this loop; it cannot independently isolate every interaction with the full HTTP future layout and readiness behavior.

## Attribution and scheduler change

Fresh 12-second wall-clock stack profiles locate Native readiness work in Event listener insertion, waker registration, notification and listener removal, including their mutex operations. Native's application thread has 1.50% of wall samples in event/cancellation/mutex code, against 0.20% for spawned Tokio. Send/receive/readiness syscalls dominate both profiles. These percentages include time in the kernel and are not on-CPU cost attribution. [Profiles and category counts](results/raw-io/profiles/summary.json).

Worker readiness formerly used a general Event listener, with separate insertion, registration, notification and retirement bookkeeping. The replacement private broadcast notifier stores a waiter and its waker together under one lock. Notification detaches all current entries under one lock, then wakes them outside it. An atomic cutoff over strictly increasing IDs proves an old listener was notified, so its completion/drop skips the removal lock. An abandoned live listener removes only its own entry. Recycled vector slots cannot be removed by older notified listeners. The vector retains capacity; ordinary one-waiter notification allocates no overflow list.

Lazy registration checks both readiness and closure before and after registering. A notified listener completes even if a different reader has already cleared advisory readiness. Clone, replacement/drop and wake callbacks run outside the notifier mutex. Multiple simultaneous waiters, cancellation, moved futures and group closure retain their existing behavior. This is confined to worker readiness; runtime-independent cancellation and other Event users are unchanged.

Four screens compare retained binaries with identical raw loops. Each has five interleaved five-second samples per variant:

| Experiment | Before user µs/request | Candidate user µs/request | Decision |
| --- | ---: | ---: | --- |
| Resolve routing under one lock per event batch | 0.3833 | 0.3827 | Reverted; 0.15% difference did not justify added state |
| Fuse waiter registration and notification retirement | 0.3803 | 0.3357 | 11.7% less user CPU; improved every paired round |
| Replace generation increment/readiness OR with one CAS update | 0.3344 | 0.3440 | Reverted; no user CPU benefit |
| Inline scalar bound I/O and cancellation wrappers | 0.3360 | 0.3352 | Reverted; 0.23% CPU difference and noisy throughput did not establish benefit |

The notifier screen records zero errors and median throughput of 131,919 before / 132,878 after / 133,833 spawned Tokio req/s. This is a CPU reduction, not evidence of a throughput win over Tokio. [Routing screen](results/raw-io/scheduler/batch/comparison/summary.json), [notifier samples](results/raw-io/scheduler/notifier/comparison/samples.jsonl), [notifier summary](results/raw-io/scheduler/notifier/comparison/summary.json), [atomic screen](results/raw-io/scheduler/atomic-tick/comparison/summary.json).

The final inline screen followed the longer notifier confirmation and fresh profiles. It tests ordinary compiler hints on `read_into`/`read_with`, `write`/`write_with` and `cancellable`; it does not establish an exact cost for nested future/drop glue. Its zero-error measurements show no repeatable CPU benefit, so the hints are absent from the retained source. [Inline screen](results/raw-io/scheduler/inline/comparison/summary.json).

### Longer confirmation with complete HTTP

Five interleaved eight-second samples per variant, with the same t4/c64 workload and one-second warmup, compare the preserved old binary against the retained notifier. No compilation, testing, profiling or other load test runs during these measurements. All 30 samples have zero errors.

| Variant | Median req/s | User µs/request | System µs/request |
| --- | ---: | ---: | ---: |
| Native raw, before | 133,733 | 0.3744 | 7.066 |
| Native raw, after | 134,884 | 0.3388 | 7.040 |
| Tokio raw, spawned supervisor | 134,162 | 0.2696 | 7.162 |
| Complete Base HTTP, before | 129,130 | 0.8619 | 6.865 |
| Complete Base HTTP, after | 129,877 | 0.8130 | 6.855 |
| Complete Hyper/Tokio | 130,865 | 0.6798 | 6.926 |

The retained change reduces median user CPU **9.5% in the raw control** and **5.7% in complete HTTP**. Every paired round improves: raw reductions span 8.8–11.1%, complete HTTP 2.1–9.0%. This repeatable CPU benefit justifies the added private notifier. Raw median throughput rises 0.9% and complete HTTP 0.6%; system time dominates, placement is uncontrolled and throughput variation is larger than these differences. There is no established throughput win over Tokio/Hyper. Complete Base HTTP remains **0.75% below Hyper's median throughput** in this cohort. [Samples](results/raw-io/scheduler/final/comparison/samples.jsonl), [summary](results/raw-io/scheduler/final/comparison/summary.json), [environment and source/binary hashes](results/raw-io/scheduler/final/comparison/environment.json).

This fixes a concrete scheduler cost but does not eliminate the user CPU difference: Native raw remains 0.069 µs/request above spawned Tokio; complete HTTP remains 0.133 µs/request above Hyper. Native still performs explicit independent-waiter lifecycle work, readiness-generation/byte-hint maintenance and locked weak-registration lookup in event routing. Tokio's raw `AsyncRead`/`AsyncWrite` path uses reserved reader/writer slots and direct dispatch to retained I/O records. These are source-level differences, not measured cost assignments; batching routing was inconclusive and was reverted. The residual cannot be assigned wholly to the executor or to HTTP from these measurements.

A fresh after-profile has **zero Event-listener samples on the Native serving thread**. The event/cancellation/mutex category, including the new notifier, falls from 1.50% of Native wall samples before to 0.65% after; spawned Tokio is 0.24% in the new profile. These remain wall-clock locators, with instrumented timings excluded from performance comparisons. Independent final counters record exactly one positive receive/write per sequential request in both raw modes and exactly 41 received / 78 sent bytes per positive operation under wrk. Native has 125 WouldBlock receives over 415,220 positive receives, versus Tokio's eight over 419,466; neither performs a WouldBlock probe per response. [After-profile](results/raw-io/scheduler/final/profiles/summary.json), [sequential counters](results/raw-io/scheduler/final/socket-counts.json), [wrk counters](results/raw-io/scheduler/final/socket-counts-wrk.json).

## Verification

The retained notifier's regressions cover broadcast, independent abandonment, replacement wakers, old guards after index reuse, zero-listener notification, simultaneous notifications, racing cancellation, notification during a waker clone, reentrant clone/drop/wake callbacks, ID exhaustion, closure during lazy registration and completion after another reader clears readiness. Existing socket tests cover real multiple readers, cancellation, worker migration, stale routing, EOF and urgent-data boundaries.

The canonical scheduler workspace gate passes **29,117/29,117 measured host regions**, with three pre-existing unreachable exclusions. Workspace Clippy with all targets/features and warnings denied passes. Rust 1.85.1 checks all library/benchmark targets and features, excluding private Bake tooling: its existing `bake-macros` dependency requires newer syntax. The benchmark itself passes Rust 1.85.1 all-target/all-feature checks and Clippy. HTTP/1's 45 codec tests and the server's 28 tests including documentation and five proxy scenarios pass. [Coverage](results/raw-io/scheduler/final/coverage.log), [MSRV](results/raw-io/scheduler/final/msrv-libraries.log), [Clippy](results/raw-io/scheduler/final/clippy.log), [HTTP/1](results/raw-io/scheduler/final/http1-tests.log), [server](results/raw-io/scheduler/final/server-tests.log).

## Reproduction

Run from this directory, adjusting the saved executable paths in the variant map when moving checkouts:

```sh
cargo build --release --locked --features profile-tools --bins
python3 diagnostics/validate_raw.py \
  --binary target/release/base-http-hello-benchmark --output results/raw-validation.json
python3 compare_candidates.py --wrk /absolute/path/to/wrk \
  --variants results/raw-io/scheduler/final/variants.json \
  --output results/new-raw-comparison --repetitions 5 --duration 8
```

The initial [build provenance](results/raw-io/build-provenance.json) and [preserved executable](results/raw-io/server), plus the final [provenance](results/raw-io/scheduler/final/provenance.json), [executable](results/raw-io/scheduler/final/server), [source snapshot](results/raw-io/scheduler/final/source/readiness_driver.rs), validation/check logs and raw sample files retain the measured revisions. The scope remains persistent plaintext loopback on one macOS application worker; Linux/io_uring, TLS, streaming and production HTTP conformance are outside this control.


## Continued investigation: ready writes and direct event dispatch

Continued profiling and controlled source swaps found additional work in our adapter. These changes are private to selector readiness; HTTP semantics, service ownership, body backpressure and socket operation contracts remain the same.

### Successful write path

Worker-owned borrowed stream writes first retire any older owned submission, check writer state, cancellation and scheduler admission, and make the nonblocking send. Success, short counts, zero progress and established errors return before constructing cancellation or readiness wait futures. Most writers never need their own cached registration, so that path has no routing lookup, readiness mutex, waker registration or per-write allocation. If a writer already has a registration, it snapshots the direction before sending so a blocked attempt cannot clear a newer event.

Only `WouldBlock` enters the waiting path. A new registration gets a retry across the registration boundary; an existing registration waits after clearing only the observed generation. Independent async-io selectors retain their original path. Accepted owned output keeps its independent progress/retirement behavior. Regression tests cover lazy and rejected admission, results winning concurrent cancellation/closure, newer edges surviving a blocked send, no redundant same-generation probe, registration failures and closure during the first wait.

A five-round five-second screen measured 0.32277 versus 0.31239 user microseconds/request before/after the ready-write change. An additional extraction into a synchronous helper directly inside the outer write future regressed: 0.31517 versus 0.33094. Forcing that helper inline removed the outlined call but still did not improve the comparison (0.31960 versus 0.33051). Both extractions were reverted. The retained simpler implementation makes the early attempt inside `write_with`. [Write screen](results/raw-io/continued/ready-write/comparison/summary.json), [outlined helper](results/raw-io/continued/direct-write/comparison/summary.json), [inline helper](results/raw-io/continued/direct-inline/comparison/summary.json).

### Event dispatch and registration lifetime

Tokio 1.54.1 uses an address token for stable `ScheduledIo` state; Mio carries the token through kqueue `udata`. The driver updates readiness and wakes waiters directly, while deferred registration ownership protects already-collected events. [Tokio driver](https://github.com/tokio-rs/tokio/blob/tokio-1.54.1/tokio/src/runtime/io/driver.rs#L345), [registration set](https://github.com/tokio-rs/tokio/blob/tokio-1.54.1/tokio/src/runtime/io/registration_set.rs).

Our former event path locked a routing table, hashed a monotonic key and upgraded a Weak reference before publishing each event. Direct dispatch now uses an opaque address token on macOS and the Linux/Android epoll backend. It performs neither map lookup nor reference-count upgrade during delivery. Other polling backends retain numeric-key routing until their filter-deletion semantics have been audited.

The controller owns each registration before its token reaches the kernel. A registration holds a weak controller reference, advisory readiness and notifier state; it owns neither socket nor payload. Deregistration marks it inactive and synchronously deletes its kernel filters. Successful deletion queues it for release only after the driver clears its preceding event batch and before the next poll. A record observed inactive is skipped; publication admitted just before retirement may finish against retained old state. That can never target a replacement connection.

Failed additions, including a partial addition with failed rollback, and failed deletions remain quarantined until that driver stops. Normal group close wakes waiters without releasing live-batch storage. Driver destruction clears its batch and releases its own records after dispatch has ended. Weak controller ownership avoids an ownership cycle. Sixteen pending retirements interrupt an idle driver; fewer can remain until another turn or stop. Callbacks and destruction occur outside routing locks. Kqueue change receipts must be complete; `EPIPE` is accepted for adding a closed write side, but does not establish successful deletion.

The direct-dispatch screen compares otherwise equivalent ready-write binaries: median user CPU falls from **0.32381 to 0.29763 microseconds/request (8.1%)**, improving all five paired rounds. Spawned Tokio is 0.26692. Median throughput is 133,415 before / 132,411 pointer / 134,393 Tokio req/s: this screen establishes a CPU saving, not a throughput win. All errors are zero. [Samples](results/raw-io/continued/pointer-routing/comparison/samples.jsonl), [summary](results/raw-io/continued/pointer-routing/comparison/summary.json).

### Rejected alternatives and churn

A copy-on-write routing snapshot reduced steady-state CPU (0.34919 to 0.32973 microseconds/request), but overlapping mutations copy its routing map. A dedicated diagnostic forces each read publication to overlap a remote thread registering/dropping eight UDP sockets. Seven interleaved repetitions per variant use 2,000 rounds after twenty warmup rounds, with no other test/build/load running. At 512 idle registrations, snapshot user CPU per publication rises from 13.115 to 14.9025 microseconds (13.6%). This is a deliberately forced overlap, not a production connection distribution. [Snapshot churn](results/raw-io/continued/churn/summary.json), [fixture and provenance](results/raw-io/continued/churn/build-provenance.json).

A reusable resolved event batch matched the snapshot's steady-state CPU (0.30757 versus 0.30695) without copying the routing map. A separate internal-integer hash screen also reduced lookup cost. Neither is retained because pointer dispatch removes the lookup and batch-copy machinery. Splitting strong routing state into an additional allocation and flattening the reader into a retained poll_fn wait did not establish improvements; both were reverted. [Buffered routing](results/raw-io/continued/buffered-routing/comparison/summary.json), [lookup screen](results/raw-io/continued/routing-cost/comparison/summary.json), [strong state decision](results/raw-io/continued/strong-routing/decision.md), [reader decision](results/raw-io/continued/poll-reader/decision.md).

Repeating the forced-overlap diagnostic against the pointer revision found no observed churn regression at the two tested sizes. At 64 idle registrations, original/pointer median user CPU per publication is 12.738/12.5645 microseconds; at 512 it is 12.8755/12.662. These small deltas do not establish a general churn performance gain. There is no map-size-dependent mutation copy in the pointer design. [Pointer churn samples](results/raw-io/continued/churn-pointer/samples.json), [summary](results/raw-io/continued/churn-pointer/summary.json), [frozen fixture builds](results/raw-io/continued/churn-pointer/provenance.json).

### Combined confirmation

Five interleaved eight-second samples per variant use the same t4/c64 control and one-second warmup, with all builds, checks, profilers and other load tests finished before timing. The baseline is the preserved notifier-only revision; the candidate adds the early ready-write return and direct event dispatch. All thirty samples have zero errors.

| Variant | Median req/s | User µs/request | System µs/request |
| --- | ---: | ---: | ---: |
| Native raw, notifier baseline | 134,847 | 0.3379 | 7.045 |
| Native raw, ready write + pointer | 135,705 | 0.2922 | 7.038 |
| Direct Tokio raw, spawned supervisor | 136,764 | 0.2608 | 7.017 |
| Base HTTP, notifier baseline | 129,594 | 0.8153 | 6.862 |
| Base HTTP, ready write + pointer | 130,148 | 0.7783 | 6.877 |
| Hyper/Tokio HTTP | 131,125 | 0.6975 | 6.893 |

The combined changes reduce median user CPU **13.5% in raw I/O** and **4.5% in complete HTTP**, improving every paired round. Raw CPU reductions span 2.9–16.5%; HTTP reductions span 3.2–8.2%. Median throughput rises 0.64% raw and 0.43% HTTP, but system time dominates and uncontrolled host/kernel variation is material. Full Base HTTP remains **0.75% below Hyper median throughput**, and raw Native is **0.77% below spawned Tokio**. There is no established throughput win. [Samples](results/raw-io/continued/final/comparison/samples.jsonl), [summary](results/raw-io/continued/final/comparison/summary.json), [source/binary/environment hashes](results/raw-io/continued/final/comparison/environment.json).

The remaining user CPU difference is **0.0314 microseconds/request** in raw I/O and **0.0807 microseconds/request** in complete HTTP. These cannot be subtracted to assign an exact HTTP-versus-scheduler share. Native retains generation/byte-hint maintenance and independent waiter lifecycle handling; the full HTTP path also retains its owned semantic-message allocations and transfers. The pointer improvement demonstrates that hot routing lookup was unnecessary work in our adapter; it does not attribute the entire remainder or establish an external-dependency defect. [Machine-readable comparison](results/raw-io/continued/final/results.json).

A final reader screen moved the `Cancellation::never()` admission check out of the general wrapper and awaited the pinned read operation directly. Its five-round median was 0.29092 versus 0.29247 user microseconds/request (0.53%), with mixed paired results. That difference does not justify duplicated admission logic; the original reader is retained. This rejects another suspected wrapper cost rather than attributing the remaining gap to it. [Screen](results/raw-io/continued/read-admission/comparison/summary.json), [decision](results/raw-io/continued/read-admission/decision.md).

Final syscall diagnostics on the measured binary record exactly one positive receive and write for each of 10,000 sequential requests in all four modes (Native/Tokio raw and Base/Hyper HTTP). Under wrk, each positive receive consumes 41 bytes and each positive write sends 78 bytes; no mode records a write returning `WouldBlock`. Raw Native records 127 receive-WouldBlock calls over 358,547 positive receives, versus Tokio's 19 over 403,783. This is occasional readiness retry, not a probe per response. Connection resets in the diagnostic server logs coincide with client shutdown. Interposition timings are excluded from performance results. [Sequential counters](results/raw-io/continued/final/socket-counts.json), [wrk counters](results/raw-io/continued/final/socket-counts-wrk.json).

Fresh eight-second wall profiles still place most samples in send/receive syscalls. The cancellation/event/mutex category accounts for 0.6% of Native raw wall samples versus 0.2% for spawned Tokio. These sparse wall samples locate remaining code but cannot assign its on-CPU cost. The rebuilt binary after reverting the reader experiment matches the measured binary byte for byte; retained source hashes also match. [Profiles](results/raw-io/continued/final/profiles/summary.json), [provenance](results/raw-io/continued/final/provenance.json).

### Validation of the combined revision

The canonical scheduler gate passes **29,264/29,264 measured host regions**, retaining the same three pre-existing unreachable exclusions. Selector tests pass 183 cases, with one pre-existing ignored test. Workspace Clippy and Rust 1.85.1 all-target/all-feature checks pass, excluding private Bake from the MSRV check because its existing macro dependency requires newer syntax. HTTP/1's 45 codec cases, the server's 22 unit cases, five real proxy cases and documentation test pass. Base HTTP's semantics/body tests and the benchmark's MSRV/Clippy checks also pass. [Coverage](results/raw-io/continued/final/coverage.log), [selector](results/raw-io/continued/final/selector-tests.log), [Clippy](results/raw-io/continued/final/clippy.log), [MSRV](results/raw-io/continued/final/msrv.log), [HTTP/1](results/raw-io/continued/final/http1-tests.log), [server and proxies](results/raw-io/continued/final/server-tests.log).

Cross-compilation checks cover all selector/native targets and features for Linux and Windows. Android library checks pass; Android all-target checking encounters an existing unrelated process-wait test cfg error (`name` undefined at process_wait/tests.rs:135). No Linux/Android/Windows runtime or region-coverage result is claimed. [Linux](results/raw-io/continued/final/cross-x86_64-unknown-linux-gnu.log), [Windows](results/raw-io/continued/final/cross-x86_64-pc-windows-gnu.log), [Android libraries](results/raw-io/continued/final/cross-android-libraries.log), [Android test limitation](results/raw-io/continued/final/cross-aarch64-linux-android.log).

New lifetime fixtures collect actual kernel events, drop every external record owner and remotely close the socket, then dispatch the old pointers before reclamation. Other regressions exercise remote replacement of later sources in the same batch, closure during callbacks, one worker dropping while another retains a batch, actual partial filter addition with failed rollback, failed deletion quarantine, idle-driver retirement notification, driver drop without another turn, and sockets outliving the driver group. Existing cancellation, concurrent waiter, migration, backpressure, urgent-data and EOF cases remain. Independent implementation and duplex reviews found no blocking correctness issue; the measured scope remains this macOS host.

### Direct iteration over native kqueue events

The next retained revision removes macOS event staging. The kernel writes into the spare capacity of one reusable `Vec<libc::kevent>`; its initialized length advances only after a successful wait. Dispatch lazily interprets those native entries, filtering user interrupts and retaining the same direction, error, byte-count and EOF mapping. There is no normalized event vector or separate conversion traversal. Normal driver turns clear and reuse the allocation. Direct test waits can append multiple native batches; a failed wait leaves already collected events intact. Controller ownership and clear-before-reclamation ordering are unchanged. The private wait result now counts this wait's native events, including interrupts; production checks only its error result. [Exact source change](results/raw-io/continued/native-events/change.diff), [source and binary hashes](results/raw-io/continued/native-events/provenance.json).

The first comparison again uses five interleaved eight-second samples per variant, one-second warmups, t4/c64 and one application worker. The baseline is the preserved ready-write-plus-pointer binary from the preceding confirmation. All thirty samples are error-free, with no simultaneous builds, tests, profiles or other load tests.

| Variant | Median req/s | User µs/request | System µs/request |
| --- | ---: | ---: | ---: |
| Native raw, staged events | 134,463 | 0.3072 | 7.090 |
| Native raw, native iteration | 132,813 | 0.2926 | 7.203 |
| Tokio raw | 134,387 | 0.2762 | 7.148 |
| Base HTTP, staged events | 129,603 | 0.7616 | 6.930 |
| Base HTTP, native iteration | 129,491 | 0.7664 | 6.934 |
| Hyper/Tokio HTTP | 131,464 | 0.6880 | 6.893 |

Raw user CPU falls 4.8%, improving four of five paired rounds. Full HTTP's user CPU median is 0.6% higher, although four paired rounds show small reductions: this does not establish an HTTP CPU improvement. HTTP throughput changes −0.09%. Raw throughput median changes −1.23% alongside higher measured kernel time; this is not a throughput win. [First comparison](results/raw-io/continued/native-events/comparison/summary.json), [paired calculations](results/raw-io/continued/native-events/results.json).

A separate five-round raw-only confirmation repeats the user CPU reduction: 0.30368 to 0.28857 microseconds/request (5.0%), again improving four paired rounds. Throughput medians are 133,963 before / 132,292 after / 134,995 Tokio req/s, with zero errors. Three paired rounds improve throughput and two regress; the candidate median remains 1.25% lower. Higher measured kernel time offsets the user-space saving, but its cause is not established. CPU placement and host activity remain uncontrolled; neither throughput improvement nor throughput neutrality is established by these cohorts. The simpler representation is retained for removing actual staging work and one per-worker buffer allocation, with this performance limitation recorded explicitly. [Confirmation](results/raw-io/continued/native-events/confirmation/summary.json), [paired confirmation calculations](results/raw-io/continued/native-events/confirmation/results.json).

Final instrumented raw-I/O counters retain exactly 41 received and 78 sent bytes per positive operation, equal positive read/write counts, and no write-WouldBlock calls before or after. Receive-WouldBlock counts are 127/335,323 positive receives before and 128/422,261 after; there is no new probe per response. These diagnostic timings are excluded from the performance results. [Before counters](results/raw-io/continued/native-events/before-socket-counts.json), [after counters](results/raw-io/continued/native-events/after-socket-counts.json).

The canonical all-target/all-feature scheduler gate passes **29,267/29,267 measured host regions**, retaining the same three pre-existing unreachable exclusions. Selector tests pass 184 cases with one pre-existing ignored case. Formatting, workspace Clippy and Rust 1.85.1 checks pass (private Bake excluded from MSRV as before). Server validation passes 22 unit cases, five real proxy cases and one doctest. The new real UDP fixture verifies native byte metadata, retained events after interrupted waits, append and clear behavior. Existing urgent-data, EOF, collected-pointer and remote-callback regressions remain passing. Independent implementation and duplex reviews found no blocking issue. This change is specific to macOS; other backend code is unchanged. [Coverage](results/raw-io/continued/native-events/coverage.log), [Clippy](results/raw-io/continued/native-events/clippy.log), [MSRV](results/raw-io/continued/native-events/msrv.log), [proxy validation](results/raw-io/continued/native-events/server-tests.log), [scoped review](results/raw-io/continued/native-events/review.md).

### Dedicated reader/writer waker slots: comparison-only experiment

The user requested the simplest direct reader/writer path to measure its possible advantage before implementing a general fallback. The experiment embeds one `Mutex<Option<Waker>>` in each direction. Waiting clones the task waker, replaces the slot, drops the previous waker outside the lock and rechecks readiness/closure. Publication takes and wakes the slot outside the lock. There are no listener objects, listener IDs, collection scans, cutoff atomics, abandonment guards or concurrent-waiter fallbacks. Generation/byte-hint handling, kernel event iteration, I/O, HTTP framing and supervision remain unchanged. This changes storage size and future layout as well as removing bookkeeping; it does not assign a cost to one instruction. [Exact experimental source](results/raw-io/continued/direct-waker/direct-readiness_driver.rs), [diff](results/raw-io/continued/direct-waker/change.diff), [reproducible build](results/raw-io/continued/direct-waker/build.py).

The binary was frozen and the general implementation restored before timing. Its rebuilt production binary matches the preceding native-events baseline byte for byte. The experiment deliberately supports only one waiting task per socket direction. Concurrent same-direction waiters overwrite each other, and abandoned or completed waits can leave a stale task waker retained. Those reduced semantics are confined to the frozen comparison, fitting the raw and static HTTP benchmarks; no production concurrency or cancellation contract is changed. The comparison environment's source hashes describe the restored tree; the separate experimental source and binary hashes identify the measured candidate. [Provenance](results/raw-io/continued/direct-waker/provenance.json), [scoped independent review](results/raw-io/continued/direct-waker/review.md).

The first cohort uses five interleaved eight-second samples of general/direct raw, Tokio raw, general/direct HTTP and Hyper HTTP. A raw-only confirmation uses five interleaved five-second samples of general/direct raw and Tokio raw. Both use t4/c64, one application worker and one-second warmups. All 45 samples are error-free. No builds, tests, profiles or other load tests run concurrently with timing.

| Workload | General user µs/request | Direct user µs/request | General median req/s | Direct median req/s |
| --- | ---: | ---: | ---: | ---: |
| Raw, first cohort | 0.2913 | 0.3018 | 132,943 | 134,536 |
| Raw, confirmation | 0.2873 | 0.2783 | 135,604 | 133,309 |
| HTTP, first cohort | 0.7833 | 0.7809 | 129,210 | 129,786 |

There is **no repeatable advantage in these measurements**. Direct raw user CPU is 3.6% higher in the first cohort and 3.1% lower in the confirmation. Only two of five paired rounds improve raw CPU in the first cohort, and three of five improve in the repeat. HTTP's 0.3% lower CPU median also has mixed paired results. Raw median throughput changes +1.2% first and −1.7% in confirmation; HTTP changes +0.45%. Host activity/core placement and kernel time remain uncontrolled. These results do not establish a throughput win over Tokio/Hyper or prove that waiter bookkeeping has zero cost. They do not support identifying the listener collection as a demonstrated cause of the remaining gap. The general implementation remains retained. [First measurements](results/raw-io/continued/direct-waker/comparison/summary.json), [paired first results](results/raw-io/continued/direct-waker/results.json), [confirmation](results/raw-io/continued/direct-waker/confirmation/summary.json), [paired confirmation results](results/raw-io/continued/direct-waker/confirmation/results.json).

Focused candidate validation passes five reader tests, seven readiness-writer tests including real TCP backpressure, a collected-pointer retirement test and a cross-worker closure test. Raw response/reuse/half-close checks pass. Sequential diagnostics validate 10,000 raw and 10,000 static HTTP responses, each with exactly one positive read and write. Independent reviewers found no lost-wakeup issue under the single-waiter assumption. These checks validate the experiment's narrow paths rather than general scheduler conformance; previous full coverage continues to describe the unchanged production implementation. [Reader checks](results/raw-io/continued/direct-waker/reader-tests.log), [writer and backpressure checks](results/raw-io/continued/direct-waker/writer-tests.log), [raw validation](results/raw-io/continued/direct-waker/raw-validation.json), [sequential diagnostics](results/raw-io/continued/direct-waker/socket-counts.json).
