# Static hello: initial macOS baseline

Initial baseline measured 2026-10-26 with registry scheduler 0.1.0 and the original HTTP implementation. The [production HTTP integration](http-optimization.md) records the earlier batching stage; the [latest cost-reduction report](hyper-target.md) records compact exchange state and fresh comparisons. The tables below retain the original baseline.

In this initial cohort, at 64 persistent connections, Hyper/Tokio achieves 129,175 requests/s versus 58,386 for Base/native: 2.21 times the throughput. Base/Tokio reaches 69,438 requests/s, so a substantial gap persists when both use Tokio. That control includes differences in HTTP handling and bound I/O integration; it does not isolate semantics or executor cost.

## Conditions

Apple M4 Pro (Mac16,8), 12 cores (8 performance + 4 efficiency), 48 GiB RAM, macOS 27.0.1, arm64 Rust 1.99.1. All binaries use the same release profile. Scheduler packages are registry 0.1.0, Hyper is 1.12.0 and Tokio is 1.54.1.

Each server uses one application execution thread and a shared connection-future supervisor. Runtime/I/O helper threads are additional and count toward measured process CPU. The service returns a static 13-byte `Hello, world!` body with matching Content-Type and Content-Length; automatic Date is disabled. TCP_NODELAY is enabled. There is no TLS, pipelining or per-request logging.

An independent wrk 4.2.0 process drives HTTP/1.1 keepalive connections on loopback. Results are medians of three five-second runs after one second of warmup per run, with fresh servers and interleaved order. Load generation uses one thread at one connection and four at 16/64. See the [method and reproduction commands](readme.md), [environment](results/macos/environment.json), [raw sample journal](results/macos/samples.jsonl) and [aggregate JSON](results/macos/summary.json).

## Throughput

| Server | 1 connection | 16 connections | 64 connections |
| --- | ---: | ---: | ---: |
| Base / native readiness | 28,299 | 58,143 | 58,386 |
| Base / Tokio adapter | 29,348 | 68,811 | 69,438 |
| Hyper / Tokio | 36,924 | 130,484 | 129,175 |
| Base / Direct | 26,411 | 57,813 | 59,599 |

Requests per second; higher is better. Native readiness is the portable native backend on this host. Direct is included because the earlier examples use it; it is not the io_uring backend.

## Latency and CPU at 64 connections

| Server | Throughput min–max | p50 | p99 | Server CPU |
| --- | ---: | ---: | ---: | ---: |
| Base / native readiness | 56,547–61,854 req/s | 1.050 ms | 1.340 ms | 118.9% |
| Base / Tokio adapter | 64,860–71,403 req/s | 0.870 ms | 1.220 ms | 99.5% |
| Hyper / Tokio | 124,313–133,083 req/s | 0.444 ms | 0.689 ms | 99.3% |
| Base / Direct | 56,945–61,750 req/s | 1.030 ms | 1.400 ms | 118.4% |

Percentiles are medians of per-run percentiles. CPU is process CPU time divided by measurement wall time; 100% is one core and helper-thread work can take it above 100%. These are closed-loop wrk latencies, not fixed-arrival-rate or production tail-latency measurements.

## Validation and interpretation

All 36 timed samples completed with zero wrk-reported socket/time-out or HTTP status errors. Before every sample, Python independently checked status, body, fields, reuse across two requests, and explicit closure on a third. Timed wrk runs do not check body bytes. Server stderr includes connection-reset messages from Base, consistent with wrk closing connections with outstanding work at the end of warmup/measurement; these logs are preserved alongside each sample. They are not counted as successful extra requests.

At 16/64 connections, process CPU is approximately one core for the Tokio variants and 1.19 cores for native/Direct including helper work. Throughput largely plateaus while latency rises. This is consistent with server CPU saturation, although this run does not separately measure each thread or isolate client contention. The initial three-repetition ranges are shown rather than treating a single result as definitive. CPU affinity was not controlled; ordinary host load and clock/scheduling changes can affect absolute values.

Follow-up [profiling and isolated experiments](profiling.md) identify separate response header/body writes as the largest demonstrated opportunity. Buffer reuse removes substantial allocation volume but has a much smaller measured throughput effect. The baseline above remains unchanged; diagnostic codec patches and their measurements are recorded separately.

**These results do not evaluate Linux io_uring.** They also do not establish multi-worker scaling, connection churn, TLS, HTTP/2/3, larger/streaming bodies, proxy performance or broad runtime conformance. The result is a baseline for the current static HTTP/1 path, not evidence about the final stack’s performance target.

The benchmark release build, Clippy with warnings denied and formatting checks pass. The package also checks on Rust 1.85.1; measurements use Rust 1.99.1. Source/binary hashes are recorded in [sources.json](results/macos/sources.json).
