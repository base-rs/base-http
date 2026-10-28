# Following the remaining Linux gap

Measured on Hana on 27 October 2026. Field construction contained a measurable HTTP cost: seven `#[inline]` hints reduce the matched in-memory processing time by 10.7%, with unchanged validation and allocations. These hints are retained. Full HTTP user CPU per response improves by 3–9% across two cohorts. The longer confirmation still puts Base 1.49% behind spawned Hyper by the paired throughput comparison, with individual rounds going both ways.

The raw scheduler/socket gap remains unresolved. Operation counts do not show redundant receives, sends, delivered events or connection polls. Notification/listener polling is a profile hotspot, but the isolated scheduler candidates do not establish a production change worth keeping. All scheduler experiments from this follow-up were reverted.

## Retained implementation

The production change adds `#[inline]` to `is_token`, `NameStorage::new`, `ValueStorage::new`, `Field::new`, `Fields::new`, `Fields::add` and `Fields::append`. This exposes field construction and validation to the optimizer across call boundaries. Field storage, allocation counts, ordering, trailer policy, validation and public interfaces stay the same. No validation is skipped for this workload.

The benchmark adds `hyper-task-tokio`, which runs the Hyper connection supervisor in one spawned Tokio task. The existing `hyper-tokio` backend remains available as the runtime-root control. Neither backend spawns a task per connection.

The [retained patch](results/raw-gap-followup-20261027/retained.patch) contains the complete implementation change. The final Linux rebuild is byte-for-byte identical to the measured field candidate. The scheduler feature remains `da90905140213e169ea21ad6267f511ffdbbb077`, based on v0.1.2; the original scheduler checkout's 26 pending-file hashes remain unchanged.

## Full HTTP results

Both cohorts use 64 persistent connections and five interleaved rounds, with a one-second warmup before each sample. Hyper runs in a spawned task to match Base's task context. Every HTTP request constructs a fresh response, with Hyper's automatic Date field disabled.

| Cohort | Before req/s | Fields req/s | Spawned Hyper req/s | Paired fields vs before | Paired fields vs Hyper |
| --- | ---: | ---: | ---: | ---: | ---: |
| Eight-second screen | 175,255 | 184,343 | 184,893 | +3.93% | +0.93% |
| Ten-second confirmation | 175,186 | 180,092 | 179,944 | +0.83% | -1.49% |

Throughputs are medians of samples. Percentage changes are medians of within-round ratios, not ratios of those medians. In the confirmation, paired fields-versus-Hyper throughput ranges from -3.14% to +1.96%. These cohorts do not establish a throughput win over Hyper or parity with it. The changing baseline also means the earlier approximately 6% HTTP gap cannot all be credited to this code change.

Paired field user CPU per response falls 9.08% in the screen, improving in all five rounds. In the confirmation it falls 3.12%, improving in four rounds and regressing in one. Paired total process CPU per response falls 2.52% and 1.34%, respectively.

Confirmation process CPU medians, in microseconds per response:

| Variant | User | System | Total |
| --- | ---: | ---: | ---: |
| Before | 1.6503 | 2.7015 | 4.3518 |
| Fields | 1.5284 | 2.6860 | 4.2223 |
| Spawned Hyper | 1.4141 | 2.7628 | 4.1762 |

Base still spends more user CPU than Hyper. CPU accounting includes all server threads, at 100 ticks per second. These medians and paired CPU changes are different summaries of the same samples; neither should be substituted for the other. Confirmation paired p99 increases 2.96% against the baseline and 2.24% against Hyper; this experiment does not establish a tail-latency improvement.

## Operation counts and profiles

Separate instrumented raw controls at 16 and 64 connections show approximately one receive, one send, one delivered epoll event and one connection poll/wake per response on both implementations. There are small startup, shutdown and accept contributions. An epoll wait returns a batch of events, so one delivered event per response does not imply one epoll syscall per response.

| Connections | Base supervisor polls/response | Tokio supervisor polls/response |
| --- | ---: | ---: |
| 16 | 0.08256 | 0.08422 |
| 64 | 0.02849 | 0.03225 |

These counts provide no evidence that additional connection or supervisor polls explain the remaining gap. Instrumentation changes timing and event batching; its throughput is excluded from performance comparisons.

An initial external `perf stat` diagnostic at 64 connections records:

| Variant | Instructions/response | Cycles/response |
| --- | ---: | ---: |
| Base raw | 13,420 | 16,603 |
| Tokio raw, spawned | 13,059 | 16,003 |
| Base HTTP | 20,552 | 22,099 |
| Hyper | 18,599 | 21,152 |

These are diagnostic snapshots, not the uninstrumented throughput cohorts. Fresh user-space cycle profiles place Base notification and listener polling at approximately 16% of user-space samples at both 16 and 64 connections. That locates work to investigate; it does not mean 16% is removable overhead or that Tokio has no equivalent work.

A separate fixed-rate diagnostic targets 50,000 requests/second with 64 connections, three paired six-second runs. Each run delivers within 0.2% of the target. Median user instructions per response are 2,726 for Base and 2,586 for Tokio, approximately 5.4% more for Base. Median user cycles are 3,723 versus 3,251; kernel cycles are 27,675 versus 27,314. Cycle measurements vary, so these do not establish a precise time cost for the listener machinery. The initial 100,000/second trial failed the delivery threshold and is excluded.

## Scheduler candidates rejected

Five interleaved six-second rounds at 64 connections compare the original raw path with two isolated readiness experiments:

| Variant | Median req/s | Median user microseconds/response | Paired throughput vs Base | Paired user CPU vs Base |
| --- | ---: | ---: | ---: | ---: |
| Base raw | 237,988 | 0.7073 | — | — |
| CAS publication | 239,414 | 0.6947 | +0.67% | -0.66% |
| Single-waiter slot | 240,549 | 0.6747 | +0.92% | -4.99% |
| Tokio raw, spawned | 247,971 | 0.6082 | +4.04% | -13.80% |

Combining readiness publication into a compare-and-swap update gives mixed small changes. The direct waker slot improves user CPU in all five pairs, but remains 2.99% behind Tokio in paired throughput. It supports only one waiter per direction and omits proper abandonment cleanup and general waiter support. It also changes object/future layout. This is useful diagnostic evidence, but cannot identify listener IDs alone as the cause or justify weakening production behavior.

Four inline hints in scheduler helpers initially improve throughput 1.19% and user CPU 3.21%. An independent longer confirmation reverses both results: throughput falls 1.17% and user CPU increases 3.49%. That candidate was also reverted.

The raw Base-versus-Tokio screen shows paired throughput -3.88%, user CPU +16.01% and total CPU +5.25%. There is still work below HTTP, but this follow-up does not identify a repeatable scheduler fix.

## Task context and HTTP isolation

A same-executable context control compares Base HTTP, Hyper in the runtime root and Hyper in a spawned task. Five six-second rounds give medians of 182,648, 185,083 and 187,201 req/s. Spawned Hyper versus root Hyper improves paired throughput 1.86%, with mixed rounds. Base versus spawned Hyper is -2.37%, ranging from -9.57% to +1.57%. Matching task context matters for the comparison, but does not explain Base's higher user CPU.

An in-memory diagnostic then removes TCP and both runtimes. Base uses the existing allocation-probe input/output; Hyper uses its HTTP/1 connection driver over equivalent in-memory I/O. Each read supplies one complete GET. Output is black-boxed, and each variant checks N writes and 78×N response bytes. Hyper enables `half_close(true)` to drain the final response after EOF; the first attempt without it failed with `IncompleteMessage` and was not used.

In the five-round phase diagnostic, baseline medians are 617 ns/request for Base's full processing and 535 ns for Hyper. Base request parsing/admission alone takes 370 ns and response construction/encoding alone takes 214 ns. Separate phases have different optimization opportunities and are not precisely additive. An earlier full-driver-only cohort gives different absolute times; compare candidates within their matched cohort.

The Base profile places authority validation at 8.16%, request admission at 8.11%, strict CRLF scanning at 4.29%, token validation at 4.02%, `Fields::add` variants at approximately 8.33%, `NameStorage::new` at 2.41% and `Fields::append` at 2.22%. This led to the retained field hints. Strict parsing remains intact.

The matched five-round field comparison reduces the median from 619.961 to 553.453 ns/request, a paired reduction of 10.73%; all five pairs improve. The allocation phase is separate from timing and reports unchanged 2.0003 allocations and 16.6672 allocated bytes/request, including connection setup amortized over 10,000 requests. Those amortized figures should not be confused with earlier steady-state allocation measurements. The allocation-counting allocator remains linked, so absolute microbenchmark timings are diagnostic rather than production latency estimates.

## Validation and scope

All 129 tests and documentation examples pass on each platform: 56 in base-http, 45 in base-http1 and 28 in base-http-server. All three crates pass Clippy across all targets with `-D warnings` on Linux and macOS. Formatting passes, and the benchmark including `hyper-task-tokio` passes `cargo check`. Linux uses Rust 1.99.1 with the OOB scheduler 0.1.2 staging tree; macOS uses Rust 1.85.1 with the existing development scheduler checkout.

The first Linux validation accidentally omitted integration-test files and covered only five documentation examples. The complete suites were then copied and all checks rerun. The evidence preserves those superseded attempts and identifies the final logs.

Hana is a Xeon E3-1240 v6 running Linux 7.2.11. Tokio is pinned to 1.54.1, Hyper to 1.12.0 and hyper-util to 0.1.21. All server threads use CPU 0; the controller uses CPU 1; wrk uses CPUs 2, 3, 6 and 7. CPU 4, the server's SMT sibling, is unused. Initial SMB activity was allowed to subside before timing. Interleaving controls drift but cannot remove host or frequency variation. Compilation and profiling run separately from throughput measurement.

All 90 timed samples and their warmups pass independent response checks and report no load-generator socket or non-success errors. This follow-up measures one application worker serving small persistent HTTP/1 responses. It does not establish results for io_uring, multi-worker scaling, TLS, streaming, proxies or HTTP/2/3. The retained hints' code-size and other workload effects are not measured here.

[Evidence, reproduction instructions, source snapshots, profiles and hashes](results/raw-gap-followup-20261027/readme.md).
