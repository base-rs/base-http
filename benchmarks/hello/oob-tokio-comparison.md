# OOB-disabled epoll compared with Tokio

Measured on Hana on 27 October 2026. The compile-time OOB-disabled native epoll implementation remains slower than Tokio in this workload. At 16 and 64 connections, the full HTTP server trails Hyper/Tokio by about 6%, while a single-read/single-write control trails Tokio's socket API by about 3%.

The subsequent [gap investigation](raw-gap-followup.md) adds a spawned-Hyper context control, operation counts, fresh CPU profiles and an isolated HTTP field-construction improvement. The figures below remain the original pre-change cohort.

The scheduler feature is `da90905140213e169ea21ad6267f511ffdbbb077`, based on v0.1.2. This comparison makes no implementation changes. The original scheduler checkout's 26 pending-file hashes remain unchanged, and the feature worktree is clean.

## Method

All four variants use the same frozen release binary, SHA256 `85abc0a14e0b21d8f6f131056da653ca769940cbdd826ff07204f0420e85c541`. Dependencies are Tokio 1.54.1, Hyper 1.12.0 and hyper-util 0.1.21. The server has one application worker, with its threads pinned to CPU 0. The controller uses CPU 1; wrk uses CPUs 2, 3, 6 and 7. CPU 4, the server CPU's SMT sibling, is unused. Initial host load was 0.01 / 0.25 / 0.78.

Three rounds randomize concurrency and variant order, with a one-second warmup and six-second measurement per sample. Each server is checked for HTTP/1.1 status, response fields, body and persistent connection reuse. The HTTP implementations additionally validate connection-close behavior. All 36 measured samples and their warmups have zero load-generator errors.

Both HTTP servers construct a fresh static hello response per request. Hyper's automatic Date field is disabled to match Base. Both use a `FuturesUnordered` connection supervisor and TCP_NODELAY. Base HTTP runs in a spawned native task; Hyper runs in Tokio's current-thread runtime root future, retaining the existing HTTP benchmark arrangement.

The raw controls deliberately ignore request framing and perform one read into a 16 KiB buffer, then one write of an identical complete response. A partial write fails the control instead of introducing retries. Both raw servers run in a spawned task and use the same connection-supervision pattern. They isolate much of the scheduler/socket path, but are not HTTP implementations.

## HTTP throughput

| Connections | Base HTTP req/s | Hyper/Tokio req/s | Paired Base difference |
| --- | ---: | ---: | ---: |
| 1 | 59,257 | 59,956 | -0.58% |
| 16 | 177,071 | 186,143 | -5.97% |
| 64 | 177,686 | 189,840 | -6.40% |

## Raw socket throughput

| Connections | Base req/s | Tokio req/s | Paired Base difference |
| --- | ---: | ---: | ---: |
| 1 | 68,633 | 69,218 | -0.95% |
| 16 | 235,789 | 242,833 | -2.90% |
| 64 | 239,552 | 248,471 | -3.37% |

Throughputs are medians of three samples. Percentage differences are medians of within-round changes, so they need not equal ratios of throughput medians. At one connection, the HTTP difference varies from -1.17% to +0.56%; that result does not establish a clear throughput advantage. Every busy-server HTTP pair favors Hyper: differences range from -7.15% to -4.49% at 16 connections, and -8.48% to -5.67% at 64. The busy raw-control pairs also consistently favor Tokio.

Base's paired CPU time per request is higher by 6.12% and 7.76% for HTTP at 16 and 64 connections. For the raw controls it is higher by 4.25% and 4.69%. At one connection, the corresponding differences are +4.51% for HTTP and +13.96% for raw sockets, despite similar throughput. CPU measurements include all server threads and use process accounting at 100 ticks per second.

The raw result supports a remaining cost below HTTP; it does not establish which scheduler component causes it. The larger full-stack gap also leaves HTTP processing as a candidate. Subtracting these percentage gaps would not measure HTTP-only overhead because the controls have different throughput, task context and runtime behavior. These are short comparison cohorts, not confidence intervals or a claim about other workloads. The earlier OOB-option cohort showed a smaller full-stack gap; the fresh cohort does not support calling the implementations equal.

## Readiness and OOB behavior

For ordinary positive short scalar reads, the default native epoll reader now clears observed readiness, avoiding an additional `WouldBlock` receive. Tokio's standard `AsyncRead` path uses the same optimization on epoll/kqueue; see its [PollEvented implementation](https://docs.rs/tokio/latest/src/tokio/io/poll_evented.rs.html). The pinned local 1.54.1 source was also checked. Tokio's shared readiness implementation preserves close notifications when clearing readiness.

Base's default additionally rejects an observed urgent-data condition. `reader.with_oob_support::<true>()` selects the conservative read-until-`WouldBlock` policy at compile time. This concerns progress through ordinary data around TCP urgent marks; it does not itself deliver the urgent byte. Disabled support is not a filter or a guarantee that every urgent byte is detected. Owned, vectored and flagged receive paths retain their conservative behavior.

Existing urgent-data diagnostics reproduce a 200 ms timeout in Tokio's standard read path with five ordinary bytes still queued, both with and without an alias consuming the urgent byte. A real Hyper connection similarly produced no response during a 400 ms observation without a header deadline; an explicit header deadline closed it. The Base default epoll HTTP probe rejected its observed urgent condition promptly. These are reproducer observations, not evidence that all Tokio read APIs stall or that Base detects every urgent byte. See the [Tokio read diagnostic](results/linux-gap-20261027/diagnostics/tokio-urgent-probe.log), [Hyper deadline diagnostic](results/linux-gap-20261027/diagnostics/hyper-oob-header-deadline.log), and [Base HTTP diagnostic](results/oob-default-20261027/diagnostics/default-oob-http-probe.log).

The io_uring implementation remains conservative and is not measured in this comparison. See the [OOB option report](oob-reader-option.md) for the rejected advisory-priority experiment and completed Linux/macOS validation.

[Raw measurements, environment, source snapshots and hashes](results/oob-tokio-comparison-20261027/readme.md).
