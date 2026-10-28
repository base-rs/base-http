# Base HTTP hello world: scheduler 0.1.0 → 0.1.1

Measured 27 October 2026. Identical current Base HTTP, HTTP/1 and server sources were built against the complete published scheduler 0.1.0 and 0.1.1 dependency graphs. Hyper/Tokio from the candidate build provides an interleaved control. This measures the combined scheduler release improvements; it does not attribute gains to individual changes.

On macOS, throughput increases about 29% at 16–64 connections and whole-process CPU time per request falls about 40%. The new stack is approximately level with Hyper on this workload. Linux VM results are recorded separately below; neither platform establishes an overall advantage over Hyper.

At 64 connections in the Linux VM, the paired median throughput change is +101.1%, and total CPU time per request changes -52.6%. The new stack is 8.4% behind Hyper by the same paired measure.

## Throughput

Rates are medians of five samples. Percentage changes are the median of five per-round ratios, so they need not equal the ratio of the displayed rate medians. A round contains a randomized matched triple at the same concurrency.

| Platform | Connections | Base 0.1.0 req/s | Base 0.1.1 req/s | Hyper req/s | New vs old | New vs Hyper |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS/kqueue | 1 | 33,605 | 36,591 | 36,768 | +9.3% | -1.0% |
| macOS/kqueue | 16 | 100,051 | 129,739 | 129,012 | +29.5% | +0.1% |
| macOS/kqueue | 64 | 100,485 | 129,500 | 130,696 | +29.5% | -0.1% |
| Linux VM/epoll | 1 | 29,004 | 31,362 | 30,717 | +10.0% | +1.9% |
| Linux VM/epoll | 16 | 159,623 | 315,220 | 355,949 | +96.0% | -11.7% |
| Linux VM/epoll | 64 | 188,733 | 382,618 | 410,529 | +101.1% | -8.4% |

## Server CPU cost

User plus kernel CPU time for the entire server process, including helper threads, in microseconds per completed request. Lower is better. CPU savings use the same per-round ratios as throughput.

| Platform | Connections | Base 0.1.0 µs/req | Base 0.1.1 µs/req | Hyper µs/req | New vs old |
| --- | ---: | ---: | ---: | ---: | ---: |
| macOS/kqueue | 1 | 15.868 | 14.201 | 14.209 | -10.9% |
| macOS/kqueue | 16 | 12.865 | 7.678 | 7.722 | -40.1% |
| macOS/kqueue | 64 | 12.794 | 7.691 | 7.619 | -39.8% |
| Linux VM/epoll | 1 | 11.749 | 6.535 | 6.994 | -44.2% |
| Linux VM/epoll | 16 | 4.586 | 2.019 | 1.679 | -55.7% |
| Linux VM/epoll | 64 | 4.029 | 1.891 | 1.650 | -52.6% |

## Repeatability

These are observed minimum–maximum per-round throughput changes, not confidence intervals. Five rounds on a shared development host cannot resolve a tiny difference reliably.

| Platform | Connections | New vs old range | New vs Hyper range |
| --- | ---: | ---: | ---: |
| macos | 1 | +7.2% to +10.2% | -2.2% to +0.9% |
| macos | 16 | +25.1% to +30.6% | -1.5% to +1.7% |
| macos | 64 | +24.4% to +32.2% | -4.4% to +0.0% |
| linux | 1 | +5.7% to +18.4% | +0.1% to +11.0% |
| linux | 16 | +94.7% to +98.8% | -12.1% to -11.4% |
| linux | 64 | +95.1% to +103.2% | -10.5% to -6.5% |

## Workload and controls

- Full HTTP/1.1 parsing, application dispatch and serialization; no raw I/O shortcut or cached wire response. Fresh `200 OK`, `Content-Type: text/plain`, `Content-Length: 13`, body `Hello, world!` on every request. Hyper automatic Date is disabled.
- Loopback persistent connections, one outstanding GET per connection, no TLS or pipelining. TCP_NODELAY enabled. One application worker, one `FuturesUnordered` connection supervisor and an admission limit of 1,024 in both stacks. Base runs the supervisor as one Native task; Hyper uses the current-thread Tokio root.
- Five randomized matched triples at each of 1, 16 and 64 connections: 45 samples per platform, 90 total. Each sample starts a fresh server, validates three requests including reuse and close, warms up for one second, then measures for eight seconds. wrk uses one load thread at one connection and four at higher concurrency. Random seed 20261027.
- Platforms ran sequentially, with no compilation or profiling during measurements. All preflight checks and timed/warmup wrk checks passed. wrk reported no socket, timeout or non-success-status errors. Timed wrk does not verify body bytes.
- Base server stderr contains peer-reset diagnostics, consistent with load connections closing with a response in flight. All stderr lines were audited and contain only this connection-reset diagnostic; a few final Linux lines are truncated when the runner terminates the process. Raw counts are retained in validation.json. These diagnostics are distinct from the zero errors reported by wrk.
- Rust 1.98.1, commit `48a229ceaefd4985c50990b14116b6d856af0985`, release profile with `debug = 1` on both platforms. Hyper 1.12.0, Tokio 1.54.1. All non-scheduler dependency versions and sources are identical across the two builds.
- All six resolved scheduler crates (`base-scheduler`, `-direct`, `-native`, `-selector`, `-timers`, `-tokio`) come from crates.io: all 0.1.0 in the baseline and all 0.1.1 in the candidate. No local scheduler patches. The baseline timer version was explicitly locked to 0.1.0 to avoid mixing releases.
- macOS: Apple M4 Pro, 12 cores, 48 GiB, Darwin 27, wrk 4.2.0. CPU affinity uncontrolled; normal host activity remains. CPU time uses calibrated libproc rusage accounting; a one-second process-CPU calibration passed.
- Linux: 6-vCPU, 8-GiB ARM64 AppleHV VM on the same host, kernel 6.12.13, Rust bookworm container, wrk 4.1.0. Server and all helper threads pinned to guest CPU 0, controller CPU 1, load CPUs 2–5. CPU time uses whole-process `/proc/PID/stat` at 100 Hz. Guest affinity does not fix host core placement or frequency. `hana.local` was unavailable, so the permitted local VM was used.

## Scope

This is the complete published scheduler 0.1.0 → 0.1.1 delta on current HTTP sources, including worker-owned readiness, notification and write-path improvements, direct registration pointers, and native kqueue/epoll event handling. It differs from the earlier epoll-only experiment, whose baseline already contained the other scheduler improvements.

Release commits: 0.1.0 `d68cea1b5f264e08a68025a426147446c635fe33`; 0.1.1 `3f628896d84a226e53e285fc0f07b7fc25f95531`.

Do not compare absolute macOS and Linux rates: kernels, virtualization, affinity and load-generator versions differ. This static loopback test does not establish performance for multiple workers, connection churn, streaming bodies, proxies, TLS, HTTP/2/3 or production tail latency. It uses readiness I/O, not io_uring. No HTTP implementation optimizations were added in this measurement task.

## Evidence and reproduction

The [evidence directory](results/scheduler-release-0.1.1-20261027/readme.md) contains readable per-platform samples, summaries and environments, plus [validation](results/scheduler-release-0.1.1-20261027/validation.json), dependency graphs, source/lockfile snapshots, full raw logs and frozen binaries. Source and binary SHA256 values are recorded in each environment; `sha256.json` covers all preserved artifacts.

The benchmark manifest and lockfile now use published scheduler 0.1.1 and no local scheduler patch. The original scheduler checkout’s 26 pending files were verified unchanged.
