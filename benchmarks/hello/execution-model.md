# Worker/reactor model and submission-policy experiment

The scheduler now has a [worker/reactor execution guide](../../../base-scheduler-execution-model/context/execution.md), [progress requirements](../../../base-scheduler-execution-model/design/worker-progress.md), and a [research ledger](../../../base-scheduler-execution-model/benchmarks/echo/worker-reactor-research.md). These connect the earlier optimizations to one model: remove unnecessary work, amortize necessary kernel work, and measure delay at each progress boundary. Requirements preserve wakeup, ownership, issuance and retirement semantics; batch counts and numerical latency targets remain policy/research questions.

## First model-derived experiment

Baseline is scheduler main `5cc179985803e1da812353df73868db668234202`, including post-submit CQ publication. Its nonblocking submission-deferral condition can defer owned reads whose SQE preparation cannot wake tasks, although its comment described readiness-only batches. Existing staged SQEs require a submission attempt on the next reactor turn; kernel errors can still leave them unconsumed.

The prototype adds a local readiness-only classification and excludes payload and cancellation SQEs from deferral. Transient `PollAdd` and persistent monitor batches retain the existing rule. Worker policy, CQ publication and payload ownership are unchanged. The hypothesis was that deferral delayed useful input behind another application batch; its possible cost was extra kernel entries.

## Equal-rate latency and separate capacity

Hana was idle before measurements. Five randomized interleaved rounds at 240,000 offered requests/s use 64 connections, two seconds of warmup and eight measured seconds. Every planned request remains in the sample, including late responses. Server threads use CPU 0, controller CPU 1, and four independent client threads CPUs 2, 3, 6 and 7; CPU 4 is left unused. A separate unpaced `wrk` cohort uses five rounds at one and 64 connections. See the [fixed-rate methodology](fixed-rate.md).

Medians of individual runs:

| Metric | Baseline owned | Immediate payload prototype | Borrowed control | Tokio control |
| --- | ---: | ---: | ---: | ---: |
| 240k/s RTT p50, us | 58.271 | 58.367 | 33.855 | 29.967 |
| 240k/s RTT p99, us | 308.223 | 265.471 | 268.799 | 261.759 |
| 240k/s scheduled p99, ms | 2.230 | 2.916 | 2.664 | 3.019 |
| Unpaced 64-connection requests/s | 262,015 | 263,380 | 241,785 | 248,719 |
| Unpaced 64-connection CPU us/request | 2.690 | 2.663 | 2.935 | 2.834 |
| Unpaced 1-connection CPU us/request | 6.653 | 7.116 | 6.222 | 5.825 |

Paired prototype/baseline changes:

- 64-connection throughput: +0.54%, range -1.45% to +5.03%.
- One-connection throughput: +1.75%, all five pairs higher.
- One-connection CPU/request: +5.65%, all five pairs worse (+5.07% to +10.83%).
- Equal-rate RTT p50: -2.20%, broad mixed range (-39.05% to +55.53%).
- Equal-rate RTT p99: -7.40%, four pairs lower, range -22.15% to +54.98%.
- Equal-rate scheduled-arrival p99: +11.52%, broad mixed range (-94.95% to +2310.09%).

All 20 paced runs meet target within 0.043%, with 38.4 million measured and 48 million total responses validated byte for byte. Maximum drain is 0.554 ms. The 40 capacity runs pass the harness's response and error checks. Differences between medians are not paired median differences. Controls also vary between this and earlier cohorts; do not interpret cross-cohort latency changes as additional scheduler gains. This does not establish latency parity with Tokio.

## Separate boundary probes

Instrumented executables are excluded from the timing cohorts. They count deferred payload batches/requests and measure staging-end to the next kernel entry attempt, excluding time inside that call. Sampled owned reads separately measure queued-command publication to entry preparation and result publication to consumption. Prefix reports are emitted every 4096 turns; they are not whole-run totals or kernel completion timestamps.

Across the first three baseline prefixes, deferral averages 0.540–0.726 us; the largest observed interval is 22.016 us. The first prefix defers 0.51% of payload batches containing 3.31% of payload requests. Corresponding sampled queue-to-preparation means are 19.87–25.80 us, and publication-to-consumption means 21.54–24.81 us. The prototype eliminates deferral without reducing those surrounding means in its diagnostic runs.

The initial probe selects every 128th observation, which can alias recurring connection order. A separate deterministic pseudorandom-sampling control uses the high seven bits of a full-period 64-bit LCG and records its actual sample count. Its three prefixes average 0.630–0.708 us for deferral, 28.59–34.91 us for queue preparation and 30.40–35.92 us for result consumption. The difference in scale persists, while the changed means show the sensitivity of instrumented batching. Source, raw runs and summaries are retained separately. All probes change execution overhead and batching; their distributions identify research targets and must not be added to explain uninstrumented RTT.

## Decision and retained changes

Reject the immediate-submission prototype as the default: its small inconsistent capacity gain and mixed equal-rate latency do not justify the repeatable low-concurrency CPU increase. Preserve its exact patch, source and binaries.

The retained scheduler work documents the execution model and progress requirements, corrects the deferral comments, and adds a regression checking that owned-read deferral remains bounded with and without a monitor batch. Two lifecycle fixtures explicitly keep staged requests unissued through a supported interrupted submission rather than relying on the batching policy. The existing runtime submission predicate remains.

The next research target is delay around application batching and result consumption, with task topology made explicit. This evidence does not yet justify a task budget, an eager-submission path, or a public tuning flag. Queued reads may be prepared before their next request arrives, overlapping client scheduling time; correlate input availability before treating the queue interval as response-critical latency. Result publication-to-consumption still combines CQ-batch and supervisor/executor effects that need to be separated.

Linux io_uring tests, raw-response smoke checks, formatting and Clippy pass. The shared Bake coverage gate passes 7,552/7,552 measured source regions for the retained implementation; the rejected prototype separately passes 7,558/7,558. The coverage wrapper excludes only unavailable external compiler source mappings under `/usr/src/debug/rust/`; no project source is excluded.

[Full evidence](results/execution-model-20261028/) contains per-sample data, environment metadata, separate diagnostics, logs, source archives, patches, checksums and frozen uninstrumented executables. Artifact verification confirms the archived baseline source and binary hashes. The raw workload performs one arbitrary read and one static response write with reused 16 KiB storage; it bypasses HTTP framing. Streaming, backpressure and multi-worker scaling remain separate workloads. The original dirty scheduler checkout is untouched.

The retained model, requirements, evidence and regression changes are in [scheduler PR #66](https://github.com/base-rs/base-scheduler/pull/66), reviewed head `b515ad0928bd8637c05a7abda046abbfdd1c6f5c`.

All required PR CI checks pass across Linux, macOS, Windows and MSRV. Windows required an unchanged-code rerun after an existing `tokio_adapter_poll` smoke test timed out; the first failure log and rerun record are preserved in the artifact bundle. This does not diagnose or fix that timeout.

The [matched task-topology follow-up](task-topology.md) now separates supervisor and per-connection executor scheduling, with equal-rate 200k/240k cohorts and separate response-progress probes. Its low-concurrency CPU benefit and load-dependent latency findings guide the next receive-lifecycle investigation; no scheduler policy change is retained.
