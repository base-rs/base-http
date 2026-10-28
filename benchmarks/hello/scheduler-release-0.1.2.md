# Hana: published scheduler 0.1.1 → 0.1.2

Measured 27 October 2026 on `hana.local`. Both builds use identical current Base HTTP library and launcher sources, published scheduler packages, and matched non-scheduler dependency versions and features. Hyper/Tokio is an interleaved control. Only released defaults are measured; the separate experimental TCP queue hints are excluded.

The released fallback readiness fix reproduces the earlier experimental gain: io_uring throughput improves **23.8%** at one connection, **8.8%** at 16, and **2.9%** at 64. Whole-process CPU/request falls **41.4%** at one connection. Every io_uring round improves throughput at every measured load. Ordinary epoll does not show a consistent gain across loads: its 64-connection change is **-0.1%**, within the observed variation. Small epoll differences are not attributed to the fallback fix.

At one connection, both updated Base modes are close to Hyper throughput. At 64 connections, epoll remains **15.4%** behind Hyper and io_uring **17.6%** behind, by paired median throughput. The release fixes the identified fallback overhead; a higher-concurrency gap remains.

## Throughput

Rates are medians of five samples. Changes are medians of paired per-round ratios, so they need not equal ratios of the displayed medians.

| Connections | Backend | 0.1.1 req/s | 0.1.2 req/s | Change | Observed round range |
| ---: | --- | ---: | ---: | ---: | --- |
| 1 | Epoll | 59,640 | 60,598 | +1.9% | +0.6% to +3.1% |
| 1 | io_uring | 47,449 | 58,837 | +23.8% | +21.4% to +27.0% |
| 16 | Epoll | 154,061 | 158,869 | +3.7% | -1.3% to +5.9% |
| 16 | io_uring | 142,066 | 156,762 | +8.8% | +6.4% to +11.2% |
| 64 | Epoll | 159,478 | 160,053 | -0.1% | -5.5% to +2.1% |
| 64 | io_uring | 149,975 | 155,575 | +2.9% | +1.0% to +5.6% |

Observed round ranges are minimum–maximum changes, not confidence intervals.

## Server CPU cost

Whole-process user plus kernel CPU time in microseconds per completed request, including helper threads. Lower is better.

| Connections | Backend | 0.1.1 µs/req | 0.1.2 µs/req | Paired change |
| ---: | --- | ---: | ---: | ---: |
| 1 | Epoll | 9.362 | 9.239 | -1.0% |
| 1 | io_uring | 16.379 | 9.616 | -41.4% |
| 16 | Epoll | 5.076 | 4.933 | -4.0% |
| 16 | io_uring | 5.545 | 5.020 | -8.3% |
| 64 | Epoll | 4.924 | 4.874 | -0.6% |
| 64 | io_uring | 5.232 | 5.047 | -3.5% |

## Hyper/Tokio comparison

| Connections | Epoll 0.1.2 req/s | io_uring 0.1.2 req/s | Hyper req/s | Epoll vs Hyper | io_uring vs Hyper |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 60,598 | 58,837 | 59,140 | +2.6% | -0.5% |
| 16 | 158,869 | 156,762 | 180,576 | -14.0% | -14.4% |
| 64 | 160,053 | 155,575 | 189,100 | -15.4% | -17.6% |

## Workload and controls

- Physical Intel Xeon E3-1240 v6, four cores/eight threads, 62 GiB RAM; Linux `7.2.11-arch1-1`, x86_64. Before timing, the host averaged 98.9% idle. Ordinary background services remained active. No global machine settings were changed.
- Server and all helper threads pinned to CPU 0; controller CPU 1; wrk CPUs 2,3,6,7. CPU 4, the server core's SMT sibling, was unused by this experiment. Shared-host activity and frequency changes remain possible.
- Rust 1.99.1 (`58cf8f9c35b355199727627d07ae34a4e2846d67`), Arch LLVM 23.1.3; release profile with `debug = 1`. Hyper 1.12.0, Tokio 1.54.1, io-uring 0.7.15, wrk 4.2.0. No compilation or profiling during timing.
- All seven runtime scheduler packages come from crates.io: interface, Direct, Native, Selector, io_uring, Timers and Tokio. They are uniformly 0.1.1 in the baseline and 0.1.2 in the updated build. No local scheduler patches. The non-scheduler dependency versions, sources and features are identical. All 24 HTTP library Rust files and launcher source match across builds.
- Full HTTP/1.1 GET parsing, handler dispatch, fresh response construction and serialization: `200 OK`, Content-Type `text/plain`, Content-Length 13, body `Hello, world!`. Hyper automatic Date disabled. Persistent loopback TCP, TCP_NODELAY, one outstanding request per connection, no TLS or pipelining.
- One application worker and one FuturesUnordered supervisor, 1,024 connection admission bound. Base executes the supervisor as one Native task; Hyper uses current-thread Tokio's root. No HTTP library implementation was changed.
- io_uring has one task/ring worker, 256 entries, fixed-file capacity 256, transient capacity 1,024 and one readiness fallback worker. Borrowed body buffers use nonblocking payload recv/send syscalls; io_uring supplies readiness. This does not measure ring-owned payload transfers or zero-copy sends.
- Five randomized rounds across five variants at 1, 16 and 64 connections: 75 samples. Each starts a fresh server, checks three exchanges, warms for one second, then measures six seconds. One wrk thread at one connection; four at higher concurrency. Shuffle seed 20261027.
- Whole-process `/proc/PID/stat` CPU accounting at 100 Hz. p99 values in the raw summary are medians of per-run percentiles, not pooled percentiles. This closed-loop static-response test does not establish streaming, TLS, multi-worker, connection-churn or production overload performance.

## Validation and evidence

All 75 preflights passed (225 independently checked exchanges), including status, fields, body, reuse and close. All 150 warmup/timed wrk logs reparse without socket, timeout or non-success-status errors. Wrk does not validate body bytes during timing. Server stderr contains only peer-reset diagnostics from load connections closing with a response in flight; exact counts and any truncated final lines are retained in validation.json.

The [evidence directory](results/scheduler-release-0.1.2-hana-20261027/readme.md) preserves samples, summary, environments, raw logs, graph verification, manifests/lockfiles, source snapshots, frozen binaries, runner and reproduction command. SHA256 values cover the artifacts. The original scheduler checkout's pending files were verified unchanged.
