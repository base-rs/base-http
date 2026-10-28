# Layered Linux performance investigation

The remaining gap is not one uniform scheduler cost. The isolated controls identify excessive I/O polling for self-waking tasks and a redundant receive in the io_uring borrowed-reader path. Kernel event collection and ready writes are close to Tokio. Readiness publication and the scalar read wrapper have smaller measurable differences, but these probes do not fully account for the epoll raw-server gap.

The [harness](layers/readme.md) separates these layers with matching futures, private dependency hooks, nonblocking standard-library controls, and a raw server. All changes in this investigation are diagnostics. The one reader-inline candidate was rejected; no production optimization is retained.

## Component comparisons

Times below are medians in nanoseconds per operation on Hana. The final five-round cohort measures execution, empty driver turns, and ready I/O in one executable. Event collection uses the original five-round screen; readiness and publication use the corrected seven-round confirmation.

| Component | Base epoll | Tokio / control | Cohort |
| --- | ---: | ---: | --- |
| One self-waking task, I/O enabled | 601.00 | 54.54 | Final |
| One self-waking task, I/O driver detached | 81.93 | 45.74 | Final |
| 64 self-waking tasks, I/O enabled | 77.45 | 55.28 | Final |
| `FuturesUnordered`, 64 self-waking children | 392.58 | 98.68 | Final |
| Same supervisor, I/O driver detached | 110.34 | 88.93 | Final |
| Empty driver turn | 509.04 | 495.94 | Final |
| Kernel event collection/decode, batch 64 | 83.27 | 82.82 (Mio) | Screen |
| Kernel collection plus publish to reader, batch 64 | 120.99 | 113.10 | Confirmation |
| Readiness arm/publish/poll/clear cycle, width 64 | 70.06 | 58.34 | Confirmation |
| Decode/publish prepared event, batch 64, pending reader | 37.66 | 31.20 | Confirmation |
| Ready one-byte receive | 664.52 | 654.41 | Final |
| Ready short receive: 41 bytes into 16 KiB | 2295.54 | 2258.67 | Confirmation |
| Ready one-byte send | 2306.76 | 2332.66 | Final |

The manual executor-free supervisor costs 62.66 ns per child wake in the final cohort. The Base `base-noio` control retains its Native task machinery while removing the attached driver. Tokio's no-I/O control disables its I/O driver. This separates task execution cost from repeated driver work.

The seven-round ready one-byte receive comparison was 659.36 ns for Base versus 626.93 ns for Tokio. Refactoring the final harness to share a generic ready-I/O helper with io_uring reduced that gap to about 10 ns. Both cohorts show extra read cost, but its magnitude depends on code layout and probe state. A microbenchmark delta is not a stable estimate of complete-request overhead.

Base's readiness mechanism supports multiple waiters. Tokio's usual read slot supports one; its general linked-list readiness future costs 106.28 ns per cycle in the original screen, compared with Base's 70.93 ns in that same screen. The dedicated slot is cheaper, but the comparison does not justify discarding general waiter semantics.

## Task execution versus driver policy

Separate syscall diagnostics establish why the one-task result is so different:

| Workload | Base empty epoll calls per child/task wake | Tokio |
| --- | ---: | ---: |
| One self-waking task | 1.000002 | 0.016394 |
| 64 independent self-waking tasks | 0.015627 | 0.016395 |
| Supervisor with 64 self-waking children | 0.500000 | 0.008197 |

Base freezes a batch of ready tasks and makes a nonblocking driver turn between batches. A single self-waking task produces batches of one; the synthetic `FuturesUnordered` repeatedly yields its parent after two child polls. Tokio amortizes driver polling over more scheduled work. The approximately 500 ns empty epoll call explains most of the large enabled-versus-detached difference in these synthetic cases. Base's remaining task cost without a driver is also higher than Tokio's.

This is a concrete scheduling weakness for runnable/self-waking workloads. It is not demonstrated as the cause of the raw server's remaining gap: in the fresh raw diagnostic, Base makes only 0.03091 epoll calls per response, of which approximately 0.000001 are empty. Tokio makes 0.02371, with no empty returns observed. The earlier diagnostic also showed nearly zero empty polls, with closer nonempty batch sizes. Actual socket workloads mostly wait for useful events and amortize the driver work.

## io_uring is a different issue

The final component cohort gives these medians:

| Operation | io_uring | Base epoll | Tokio |
| --- | ---: | ---: | ---: |
| One self-waking task | 117.45 | 601.00 | 54.54 |
| Supervisor, 64 children | 128.33 | 392.58 | 98.68 |
| Empty driver turn | 34.44 | 509.04 | 495.94 |
| Ready one-byte receive | 672.86 | 664.52 | 654.41 |
| Ready one-byte send | 2304.44 | 2306.76 | 2332.66 |

An idle ring turn is cheap and ready writes are comparable. The raw-server syscall diagnostic instead finds a difference on the receive/wait boundary:

| Calls per raw response, 64 connections | Base epoll | Tokio | io_uring |
| --- | ---: | ---: | ---: |
| Successful receive | 1.00005 | 1.00005 | 1.00008 |
| Receive returning `WouldBlock` | 0.00014 | 0.00006 | 1.00012 |
| Total receive calls | 1.00026 | 1.00018 | 2.00028 |
| Send calls | 1.00005 | 1.00005 | 1.00008 |

The io_uring borrowed reader goes through `socket_monitor_recv_poll` and the syscall-first `monitor_io` helper. Its next read probes an empty socket before waiting. The optimized epoll scalar reader can wait using its post-short-read state. The counters confirm the extra io_uring receive; they do not prove how much of its complete-server gap that call explains. The counter library records libc socket/epoll calls, not ring submissions or completions, so zero epoll calls for io_uring does not mean no kernel readiness work.

These are borrowed-buffer tests using nonblocking payload syscalls, not a measurement of owned-buffer or multishot io_uring receive performance.

## Raw-server confirmation and rejected candidate

Five randomized interleaved rounds, 64 persistent connections, six-second samples and one-second warmups:

| Variant | Median responses/s | User CPU, µs/response | Total CPU, µs/response |
| --- | ---: | ---: | ---: |
| Base epoll | 233,377 | 0.7095 | 3.0674 |
| Base epoll, reader-inline candidate | 229,775 | 0.7209 | 3.1049 |
| Tokio, spawned supervisor | 243,996 | 0.6371 | 2.9025 |
| Base io_uring | 198,149 | 0.9632 | 3.7892 |

Median within-round throughput ratios put Base epoll 4.71% behind Tokio, and io_uring 19.17% behind Tokio. Base epoll uses 10.52% more user CPU and 6.15% more total CPU in the paired comparison. These are raw socket servers, bypassing all HTTP parsing and encoding; they are not a new full Base HTTP versus Hyper measurement. The [last full HTTP confirmation](raw-gap-followup.md) remains about 1.5% behind spawned Hyper, with mixed rounds.

An isolated `#[inline]` on the scalar reader's trait method improved the short-read probe from 2295.54 to 2244.07 ns. It did not improve the always-ready one-byte read. In the raw server, paired throughput fell 0.79%, paired user CPU changed -0.06%, and paired total CPU increased 0.57%. This does not justify retaining the hint. The disposable scheduler source was restored.

## Focused implementation work

1. Amortize nonblocking driver turns over a bounded amount of runnable task work. Preserve bounded I/O/timer latency and external-wake fairness. Validate with the one-task and supervisor controls, then mixed runnable/socket workloads. The current raw benchmark cannot establish its benefit.
2. Give io_uring's scalar borrowed reader an appropriate receive/wait policy, avoiding the measured empty receive where the stream/OOB semantics permit it. Keep message/flagged operations' probing semantics separate. Verify urgent-data boundaries, aliasing, EOF, cancellation, abandonment, and ring rearming before timing a candidate.
3. For the remaining epoll gap, target the read future and readiness arm/publication path. Their excess work is measurable after excluding the kernel collector and ready write. Read/write future sizes are 560/584 bytes for Base epoll, 384/408 for io_uring, and 24/24 for the Tokio extension futures in this harness. These types expose different capabilities and cancellation semantics; size is a clue to inspect generated code, not evidence that shrinking a future will improve throughput. Confirm any simplification in the raw server before retaining it.

There is no evidence here for replacing the epoll collector or adding complexity to the ordinary write fast path.

## Validation and limits

The component cohorts contain 300 screen, 252 confirmation and 170 final samples; the raw cohort contains 20 samples. All completed samples pass their progress/data assertions, and all raw samples and warmups pass independent response validation without reported load-generator socket or status errors. The task and socket operation-count runs are separate from timing. The 26 previously dirty scheduler files still match their recorded hashes.

Hana is a Xeon E3-1240 v6 running Linux 7.2.11 and Rust 1.99.1. Scheduler source is merged main `f05a630db60686526b6145efb284a9c2f61fe8b4` (package version 0.1.2), Tokio 1.54.1, Mio 1.2.4. Every probe/server thread runs on CPU 0; the controller runs on CPU 1; raw load uses CPUs 2, 3, 6 and 7. CPU 4, the server's SMT sibling, is unused. No compilation or profiling runs concurrently with timing.

Probe windows differ in setup, kernel/cache state, assertion overhead and payload size. Their costs overlap and must not be summed to explain request latency. Short-read readiness preparation also differs between backends. Medians and within-round ratios are different summaries. This investigation is limited to one execution worker on this host and does not establish multi-worker, TLS, streaming, proxy or HTTP/2/3 performance.

[Evidence, source snapshots, executable hashes, raw samples and reproduction](results/layer-breakdown-20261027/readme.md).
