# Full comparison repeat — Hana, 2026-10-27

The complete repeat confirms the borrowed io_uring reader improvement. Concurrent echo improves 14–17% over the old reader; raw response improves 16.37% and full HTTP improves 15.22%. Fixed io_uring still trails native epoll in the main placement. Smaller differences between fixed io_uring, epoll and Tokio depend on client placement.

These are new measurements of the same frozen binaries as the [previous comparison](readme.md), kept as a separate cohort. Rates below are medians; percentages are medians of paired within-round throughput changes, so they need not equal ratios of the displayed medians.

## Main echo comparison

Validated echoes/second, five interleaved rounds:

| Connections | Bytes | Before io_uring | Fixed io_uring | Native epoll | Tokio | Fixed vs before | Fixed vs epoll | Fixed vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 | 78,840 | 78,859 | 81,174 | 77,664 | -0.66% | -2.71% | +0.35% |
| 1 | 4096 | 69,731 | 68,722 | 70,195 | 69,536 | -1.24% | -2.10% | -0.76% |
| 64 | 64 | 202,572 | 235,815 | 243,956 | 248,758 | +16.76% | -3.17% | -4.46% |
| 64 | 4096 | 177,476 | 203,302 | 209,475 | 209,036 | +14.01% | -2.93% | -2.73% |

Every concurrent before/after pair improves: +15.44–18.82% at 64 bytes and +13.70–14.92% at 4 KiB. The smaller epoll deficit is also consistent within this repeat: -3.48 to -2.41% and -4.12 to -2.27%, respectively. Single-connection before/after pairs cross zero; the repeat does not demonstrate a single-connection benefit.

## Alternate client placement

Three interleaved rounds, 64 connections, three client workers on physical cores 1, 2 and 3:

| Bytes | Before io_uring | Fixed io_uring | Native epoll | Tokio | Fixed vs before | Fixed vs epoll | Fixed vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 194,875 | 228,590 | 219,556 | 218,187 | +17.73% | +3.69% | +3.13% |
| 4096 | 163,730 | 182,519 | 186,406 | 189,944 | +12.30% | -2.87% | -3.49% |

The 64-byte ordering reverses under this placement, as in the previous comparison. The gain over the old reader survives both placements, but these results do not establish a universal backend winner or isolated server capacity.

## Raw response and full HTTP

Requests/second, three interleaved rounds at 64 persistent connections:

| Workload | Before io_uring | Fixed io_uring | Native epoll | Tokio / Hyper | Fixed vs before | Fixed vs epoll | Fixed vs control |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw response | 200,018 | 231,871 | 238,246 | 248,882 | +16.37% | -2.68% | -6.91% |
| Full HTTP | 156,553 | 179,294 | 182,617 | 186,099 | +15.22% | -1.82% | -3.66% |

Raw-response improvements range from +14.39 to +16.48% across the three pairs; full HTTP improves +11.96 to +15.23%. Full HTTP's epoll deficit ranges from -2.51 to -1.65%, and its Hyper deficit from -6.44 to -1.91%.

The raw control uses spawned Tokio tasks. The full Hyper control runs in the Tokio runtime root, whereas Base uses its spawned supervisor. This repeats the supported frozen harness exactly; it is a secondary control with a different task arrangement, and must not be substituted for earlier spawned-Hyper cohorts.

Timed server CPU cost also falls. Median total CPU per request is 3.776 → 3.102 µs for raw response and 5.007 → 4.246 µs for full HTTP. Epoll measures 2.992 and 4.151 µs; the Tokio/Hyper controls measure 2.836 and 4.057 µs. These are timed HTTP process counters, distinct from whole-lifetime echo CPU accounting.

## Comparison with the previous cohort

Paired throughput changes, with neither cohort pooled into the other:

| Workload | Previous fixed vs before | Repeat fixed vs before | Previous fixed vs epoll | Repeat fixed vs epoll |
| --- | ---: | ---: | ---: | ---: |
| Echo, 64 connections, 64 bytes | +16.73% | +16.76% | -0.99% | -3.17% |
| Echo, 64 connections, 4 KiB | +14.34% | +14.01% | -2.52% | -2.93% |
| Raw response | +14.83% | +16.37% | -1.43% | -2.68% |
| Full HTTP | +9.84% | +15.22% | -3.31% | -1.82% |

The large improvement repeats. The residual gaps move by a few percentage points, especially in the short HTTP cohorts; the full HTTP gap to Hyper changes from -6.16% to -3.66%. This supports keeping the fix while continuing to treat small backend differences as workload- and placement-dependent.

## Separate syscall confirmation

Receive syscalls per echo at 64 connections; instrumented rates are excluded from throughput evidence:

| Backend | 64 bytes | 4 KiB |
| --- | ---: | ---: |
| Before io_uring | 2.001273 | 2.001444 |
| Fixed io_uring | 1.001088 | 1.001280 |
| Native epoll | 1.001579 | 1.001857 |
| Tokio | 1.001051 | 1.001252 |

The old reader makes approximately one `WouldBlock` receive per echo; the fixed reader makes about 0.0005–0.0006 at this concurrency. The extra empty receive is removed in all four tested connection/payload cases. Counts normalize by whole-process received bytes divided by payload size, including warmup and boundary echoes. The interposer does not observe ring submissions or completions.

## Method and validation

Hana was idle before the run. The suite ran 23:03:56–23:14:27 NZDT on 2026-10-27, with no concurrent compilation or profiling. Hardware and software remain Xeon E3-1240 v6, Linux 7.2.11 and Rust 1.99.1. The server runs on CPU 0, leaving its SMT sibling CPU 4 unused. Main echo and HTTP clients use CPUs 2, 3, 6 and 7 with the controller on CPU 1. The alternate placement uses clients 1, 2 and 3 with the controller on CPU 5.

Echo uses one-second warmup plus four-second measurement, five main rounds and three alternate-placement rounds. Raw and full HTTP each use one-second warmup plus three-second measurement, three rounds. Sixteen separate syscall samples use 0.1-second warmup and 0.4-second measurement. The same original echo client validates all payloads across all backends, and the same frozen `wrk` drives every HTTP variant.

All 128 uninstrumented samples and 16 instrumented samples form complete matrices. Echo stderr is empty and every payload-validating client succeeds. HTTP probes pass and `wrk` reports no socket or status errors. HTTP server stderr contains only peer resets at load-generator teardown: 961 complete raw-response reports plus two interrupted report prefixes, and 550 complete full-HTTP reports. The validator checks and retains these details.

Executable, client, interposer and runner hashes match the previous run. No production source changed or required retesting. All 680 prior evidence checksums and all 26 pending scheduler-file hashes remain unchanged; the scheduler fix worktree is clean at `587e746`. No benchmark processes remain.

The [new evidence](../results/uring-readiness-repeat-20261027/readme.md) contains every raw sample, validation result, environment record and paired range. The original evidence remains separate. These loopback closed-loop borrowed-buffer measurements do not establish physical-network performance, owned/multishot payload performance, multi-worker scaling or general backend superiority.
