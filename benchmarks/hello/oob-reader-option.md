# Compile-time OOB reader option

Implemented 27 October 2026 against scheduler v0.1.2, in the isolated `feat/socket-stream-oob` worktree at `../base-scheduler-oob`. The original scheduler checkout and its 26 pending-file hashes are unchanged. The ordinary benchmark manifest continues to select published 0.1.2.

The public option is a consuming, compile-time reader adapter:

```rust
let reader = scheduler.socket_stream_reader(&socket)?; // OOB disabled
let reader = reader.with_oob_support::<true>();         // preserve urgent-mark progress
```

`SocketStreamReader::with_oob_support` has a conservative identity implementation for adapters that need no specialization. The readiness reader has a const-generic specialization. Converting it moves its existing scheduler, socket, registration, pool, established-read, EOF and close state; the flag needs no runtime storage or option branch. Existing factory calls retain their signatures.

Linux native epoll defaults to clearing its observed readiness after a positive short scalar read, except when EOF is reported. Directional EOF and urgent conditions are carried through dispatch and remain sticky. An observed urgent condition returns `Unsupported` in the default mode; the HTTP server closes that connection. OOB-enabled Linux readers keep readiness until `WouldBlock`, preserving progress even if an alias consumes the urgent byte. Disabled support forbids aliases consuming urgent data and is not a filter or a promise to detect every urgent byte. Flagged, vectored and owned paths retain their conservative receive behavior.

The macOS byte-count and EOF policy is retained. Linux compatibility polling also stays conservative because it lacks directional EOF information. The io_uring reader uses its existing conservative implementation for both option selections. A rejected io_uring experiment received `POLLIN | POLLPRI` for ordinary priming data; treating that advisory completion as definitive urgent data would reject ordinary connections. The kernel can use a wakeup mask directly when producing a poll completion: see [Linux poll implementation](https://github.com/torvalds/linux/blob/master/io_uring/poll.c). No extra event queue or per-connection task is introduced.

The Linux HTTP diagnostic primes a real connection, pauses its own server while queuing a complete request around an urgent byte, then resumes it without further peer traffic. Native epoll resets that connection in about 0.6 ms. The conservative io_uring server responds successfully; ordinary controls also respond successfully.

Three interleaved six-second rounds on Hana compare the published epoll server, the default OOB-disabled epoll server, and pinned Hyper 1.12.0 / Tokio 1.54.1. Each sample validates response status, fields and body and uses a one-second warmup. Server/helpers use CPU 0, the controller CPU 1, and wrk CPUs 2, 3, 6 and 7. All 27 samples report zero load-generator errors. These are short comparison cohorts, not a release benchmark.

| Connections | Published epoll req/s | OOB disabled req/s | Hyper req/s | Paired gain vs published | Paired difference vs Hyper |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 61,146 | 58,935 | 59,666 | -3.62% | -1.07% |
| 16 | 158,606 | 179,356 | 184,727 | +13.33% | -2.01% |
| 64 | 157,793 | 181,025 | 183,732 | +11.55% | -4.90% |

The table gives throughput medians and medians of within-round percentage changes; the latter can differ from ratios of medians. At 16 and 64 connections, paired CPU/request falls 13.97% and 12.81%. At one connection, CPU/request falls 2.41% while throughput falls 3.62%. This is a useful busy-server improvement with a low-concurrency throughput tradeoff. A 2–5% paired gap to Hyper remains under load, and the io_uring performance gap is still unresolved.

Validation includes the complete workspace tests and Clippy on macOS and Hana Linux, urgent-data and alias regressions for both option selections, EOF and notification-generation regressions, pool and live-state preservation, cancellation, and real HTTP requests. macOS workspace region coverage passes at 28,386/28,386 measured regions. Linux workspace region coverage passes at 39,734/39,734 measured regions. Its Arch compiler requires explicit LLVM tool paths and exclusion of its external prebuilt standard-library source paths from reporting. No project source is excluded by that workaround.

The scheduler has no tracked `design/*.md` at the release base or the feature revision. Its `context/design.md` and `context/io.md` contracts were reviewed directly and updated for this option. The implementation retains scheduler binding, on-demand reads, inline borrowed-buffer access, cancellation and retirement ownership. The simple epoll specialization is supported by repeated throughput and CPU improvements; the io_uring experiment was rejected when its event assumption failed.

[Raw samples, patch, probes and validation logs](results/oob-default-20261027/readme.md).

A [fresh comparison with Tokio](oob-tokio-comparison.md) adds matched single-read/single-write controls. It measures a roughly 6% busy-server HTTP gap and a roughly 3% raw socket gap; see that report for the new cohort and its limits.
