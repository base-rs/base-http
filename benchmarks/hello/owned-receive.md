# Owned receives in the raw response control

The isolated receive experiment on Hana (2026-10-28) finds a repeatable throughput gain at 64 connections: **+8.62% over borrowed receives**, with **8.24% less CPU/request**. Owned receives finish **5.42% ahead of spawned Tokio**. The tradeoff is higher p99 latency, and single-connection CPU cost increases. This is a raw transport result; the HTTP parser and production input policy have not changed.

The subsequent [fixed offered-rate comparison](fixed-rate.md) makes the latency conclusion workload-specific: at 240k requests/s, borrowed and owned send-to-response p99 are approximately equal (339/344 µs). Scheduled-arrival p99 additionally includes waiting for a connection; its owned median is lower at 240k, with wide round-to-round ranges. The original unrestricted wrk results below remain unchanged.

## Experiment

`base-iouring-owned-raw` changes only the raw control's receive call from `read_into` to `read_owned`. Both Base variants retain one initialized 16 KiB `Vec` per connection and reuse the returned allocation. Each positive receive triggers the same single borrowed write of the same 78-byte fixed response. Connection admission, supervision, cancellation source, cleanup and scheduler configuration match. There are no per-connection tasks or HTTP parsing in these controls.

The earlier echo owned mode changes both reads and writes. This experiment establishes that the concurrent gain survives with only the receive side changed.

The candidate contains both receive variants and the Tokio control. A fourth variant runs the previous frozen borrowed binary, retaining its stack-backed receive storage. Its paired throughput difference from the new borrowed control is +0.30% / -0.36% / -1.48% at 1 / 16 / 64 connections. The frozen control does not remove compiler-layout effects, but the owned comparison also wins against that control at 64 connections in every round.

The scheduler source is the previously measured deferred-completion implementation `77fe0f8`, whose executable implementation matches the subsequently merged scheduler change. All 525 retained scheduler source/manifest hashes verify before the build. This experiment does not modify scheduler code or benchmark published 0.1.2 defaults.

## Results

Five interleaved rounds, one second of warmup and four seconds timed per sample; all **60 samples validate**. Percentages are medians of within-round ratios. Absolute rates and p99 values below are medians of the five measurements; ratios of those displayed medians can differ from the paired changes.

| Connections | Borrowed requests/s | Owned requests/s | Tokio requests/s | Owned vs borrowed | Owned CPU/request vs borrowed | Owned p99 vs borrowed |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 69,260 | 69,142 | 68,031 | +0.55% | +9.23% | +4.00% |
| 16 | 233,490 | 237,638 | 235,115 | +1.28% | -4.26% | +24.53% |
| 64 | 235,303 | 255,578 | 242,435 | +8.62% | -8.24% | +21.49% |

At 64 connections all five owned/borrowed throughput pairs are positive, ranging **+6.04% to +11.55%**. Against Tokio, all five are positive, ranging **+2.10% to +6.81%**. Owned p99 is 0.458 ms versus borrowed 0.380 ms and Tokio 0.357 ms. The paired p99 changes are +21.49% and +29.33%, respectively; every pair is worse. At 16 connections, throughput ordering is mixed while the p99 regression is consistent.

At one connection there is no convincing throughput benefit, and owned CPU/request is **9.23% higher** than borrowed in the paired median, positive in every round. Observed post-run RSS is modestly higher for owned mode; the [complete tables](results/owned-receive-20261028/tables.md) retain all medians and paired ranges. RSS is not peak memory.

## Separate diagnostics

Traces and CPU profiles run after the throughput matrix and are excluded from its rates. The traces select the two rings created by the server's recorded threads and report no CQ overflow. Socket byte counts reconcile exactly: each response corresponds to 41 received request bytes and 78 sent response bytes.

| Instrumented metric, per response | Borrowed | Owned |
| --- | ---: | ---: |
| Receive syscalls | 1.0004 | 0 |
| Send syscalls | 1.0000 | 1.0000 |
| Positive readiness-poll completions | 1.0002 | 0.00002 |
| Positive `RECV` completions | 0 | 1.0000 |
| Ring-entry syscalls on ring-creating threads | 0.0512 | 0.0165 |

Borrowed input receives a readiness completion and then calls `recv`. Owned input submits the receive itself and gets its result through a CQE. The latter removes the separate receive syscall while introducing one receive SQE per response. Both deliver approximately one completion per response, so the improvement is not a reduction in CQE count. The instrumented run also observes fewer ring entries and deferred-work batches. Those counts describe this run, rather than establishing a universal batching ratio or isolating the cause of the latency regression.

Short CPU profiles show the borrowed receive syscall entry/return stacks, which disappear with owned receives. Owned operation management remains visible. These profiles are qualitative evidence; approximately 1,000 samples per variant are insufficient for precise attribution of small percentage differences.

The first trace attempt rejected the worker's custom thread name during analysis. It is retained as `trace-initial-worker-name`; the final traces validate ring creators against recorded server thread IDs instead. No throughput samples were excluded or repeated for that tooling correction.

## Scope and next step

The gain warrants an HTTP input prototype that retains parser-owned storage across owned receives. It does not yet justify changing the default: the single-connection CPU regression and consistent concurrent p99 regression need examination, along with cancellation/abandonment behavior when a parser transfers its buffer to an in-flight operation. Streaming bodies, fragmentation, duplex operation, backpressure and bounded retention remain separate tests. No new public HTTP API is needed for this raw experiment.

Hana is an Intel Xeon E3-1240 v6 running Linux 7.2.11, Rust 1.99.1 and Tokio 1.54.1. One server worker and all server threads are pinned to CPU 0; its SMT sibling 4 is unused by the benchmark. Wrk has affinity 2,3,6,7, using one thread at one connection and four at the other loads. The controller uses CPU 1. Preflight aggregate CPU idle is approximately 97%; ordinary background activity is retained in the environment records, and whole-run host isolation is not asserted. Closed-loop wrk latency does not establish fixed-arrival-rate overload behavior or multicore capacity. Server CPU is measured during the timed window and includes all process threads.

Before each timed sample an independent client checks status, fields, body and connection reuse. Separate arbitrary-byte/reuse/half-close checks pass for both receive variants, native epoll and Tokio. Wrk reports no socket/status errors. The evidence retains 2,061 complete peer-reset messages from load-generator teardown and four final partial diagnostic lines interrupted by server termination; these are recorded separately from client errors and successful requests.

The [evidence directory](results/owned-receive-20261028/) contains the raw outputs, verification, paired tables, source patch/archive, scheduler hashes, compressed executable, separate traces/profiles and reproduction scripts. `benchmark.sh` runs the matrix in the archived remote layout; `build.sh` and the archived manifests identify the exact scheduler/dependency paths. Earlier frozen binary and wrk hashes match the previous completion-policy evidence. Reproduction elsewhere requires recreating that layout or updating those paths. No compilation or profiler ran during the timed matrix.
