# Publish io_uring completions after nonblocking submission

This experiment starts from scheduler main `082d9283fdd5e4deb44c7462e28700b62a80fea9` and the frozen owned-receive raw-server benchmark. The isolated scheduler checkout is `/Users/samuel/Developer/base-rs/base-scheduler-post-submit`, branch `perf/iouring-post-submit-completions`. The original dirty scheduler checkout is untouched.

## Mechanism

The reactor collected a completion snapshot at the beginning of each turn and after a blocking wait. A successful nonblocking submission returned without collecting completions produced by that submission. An already runnable task could therefore run before the newly completed operations had published their results and wakeups.

The change calls the existing snapshot collector after a successful nonblocking submit. It adds no ring-submission syscall, task budget, public API, or unbounded completion-draining loop; collecting a notification CQE can perform the existing eventfd read. Completion callbacks continue to enqueue task wakeups; application futures run through the executor.

A NOP regression fails on the baseline and passes with the change: after one nonblocking submitting turn, its owner must already observe completion. Existing notification fixtures now allow completion during submission instead of assuming a later direct ring wait can still find the CQE.

## Separate diagnostic runs

Temporary counters were compiled into separate before/after executables, excluded from performance comparisons. Both used 64 connections and 240k requests/s for 1 second of warmup and 4 measured seconds. Counters print cumulative snapshots every 4,096 turns; the retained final snapshots describe prefixes of each run, not full-run totals.

Before the change, the owned-read server made 32,452 nonblocking submissions in its retained prefix. **32,444 (99.98%) already had CQEs available**, totaling 552,848 CQEs, about 17 per submission. This is a frequent delivery delay in the actual workload. With immediate publication, the loop shifted almost entirely to its blocking-capable submit/wait branch: only six nonblocking submissions occurred in the retained after prefix, versus 68,111 blocking entries.

The borrowed-read control made just one nonblocking submit in each prefix. Its payload uses ordinary receive syscalls and persistent readiness monitoring, so this experiment primarily concerns owned reads.

The supposedly readiness-only deferral condition can also defer payload batches. Counters see that in 1.41% of prepared payload batches before and 2.23% after. That is a separate hypothesis; submission deferral is unchanged in this candidate.

## Fixed 240k/s comparison

Five interleaved randomized rounds compare the frozen owned-read baseline, the owned-read candidate, frozen borrowed reads and frozen spawned Tokio. Each sample uses 64 connections, 2 seconds of warmup and 8 measured seconds. Server threads use CPU 0; client threads use CPUs 2, 3, 6 and 7, with controller CPU 1. The client preserves absolute arrival schedules and includes late completions. See [client methodology](fixed-rate.md).

Medians of the five per-run statistics:

| Metric | Owned before | Owned after | Borrowed control | Tokio control |
| --- | ---: | ---: | ---: | ---: |
| RTT p50 | 127.6 µs | 74.8 µs | 124.6 µs | 46.1 µs |
| RTT p99 | 364.3 µs | 357.4 µs | 348.7 µs | 315.1 µs |
| Scheduled-arrival p50 | 129.1 µs | 75.8 µs | 125.8 µs | 48.0 µs |
| Scheduled-arrival p99 | 6.193 ms | 1.801 ms | 6.832 ms | 2.292 ms |

Within-round owned after/before comparisons:

- RTT p50: **−41.47%** median, all five pairs lower, range −67.90% to −25.41%.
- Mean RTT: **−33.94%** median, all five pairs lower, range −49.95% to −16.65%.
- RTT p99: **−0.70%** median, range −6.32% to +7.30%; no consistent tail improvement.
- Scheduled-arrival p99: **−54.80%** median, four of five pairs lower, range −76.72% to +38.25%. Queueing remains variable.

The candidate still has greater RTT than Tokio: paired p50 is 47.54% higher and p99 12.87% higher. The scheduled-arrival p99 medians alone would misleadingly suggest the opposite ordering; the paired differences have mixed signs and a broad range. This change addresses a real typical-latency penalty without resolving the remaining tail gap.

## Unrestricted throughput and CPU

A separate unpaced `wrk` cohort uses the same placement, five randomized interleaved rounds at one and 64 connections, 1 second of warmup and 4 measured seconds. The fixed-rate client and `wrk` have different arrival patterns; their latency percentiles are separate workload measurements.

Medians of the five samples:

| Connections | Variant | Requests/s | Server CPU µs/request | RTT p99 |
| --- | --- | ---: | ---: | ---: |
| 1 | Owned before | 68,675 | 6.842 | 26 µs |
| 1 | Owned after | 69,574 | 6.591 | 20 µs |
| 1 | Borrowed control | 68,568 | 6.368 | 26 µs |
| 1 | Tokio control | 68,439 | 5.812 | 22 µs |
| 64 | Owned before | 250,626 | 2.775 | 469 µs |
| 64 | Owned after | 257,767 | 2.744 | 468 µs |
| 64 | Borrowed control | 232,404 | 3.033 | 400 µs |
| 64 | Tokio control | 240,672 | 2.929 | 355 µs |

At 64 connections, paired after/before throughput improves **2.74%** (all five positive, +0.21% to +4.12%) and CPU/request falls **0.95%** (all five lower). The candidate runs **6.63% ahead of Tokio** in paired throughput and consumes **6.23% less CPU/request**, but still has **31.83% higher p99** in this unrestricted workload. Its latency improvement at a fixed rate does not eliminate that saturation tradeoff.

At one connection, paired throughput changes +1.38%, with one negative pair; CPU/request falls 3.86% in the paired median, all five lower. It still consumes 11.95% more CPU/request than Tokio. This experiment does not establish that owned reads should become the default for all workloads.

## Validation and retained evidence

All 20 paced samples deliver within 0.03% of target. Every response byte validates: **38.4 million measured responses, 48 million including warmup**, no payload errors, maximum final drain 2.67 ms. All planned requests remain in the latency cohort. All 40 unrestricted samples pass the harness's response and error checks. Separate smoke checks cover arbitrary input, persistent reuse and peer half-close for owned io_uring, borrowed io_uring, epoll and Tokio.

The scheduler regression is observed failing before the production change and passing after. The Linux io_uring suite covers cancellation, abandoned operations, buffer ownership, multi-worker routing and shutdown. Formatting, whitespace checks and Clippy with warnings denied pass. The shared Bake Linux coverage task passes **7,552/7,552 measured source regions**, excluding three invariant-only unreachable regions. Hana’s Arch compiler embeds external standard-library mappings under `/usr/src/debug/rust/` without installing those sources; the coverage wrapper excludes only that compiler-source path. No project source is excluded. The duplicate receipt/completion wake fixture now checks that its follow-up command is enqueued once even when woken again.

[Raw evidence](results/post-submit-20261028/) includes the exact patch, tracked scheduler source archive, frozen candidate, SHA-256 hashes, separate diagnostic source files and counters, per-sample results, environment/affinity metadata, runners and logs. The original frozen baseline and client remain the artifacts from the [owned-receive](owned-receive.md) and [fixed-rate](fixed-rate.md) investigations. Remote experiment: `/tmp/base-http-post-submit-20261028` on Hana.

This measures raw one-read/one-write responses and scheduling, with HTTP framing bypassed. It does not measure the full HTTP implementation, streaming/backpressure performance or multi-worker scaling. The server has one supervisor task polling connection futures, so these results do not justify changing the executor's task-count polling budget.

Merged [scheduler PR #65](https://github.com/base-rs/base-scheduler/pull/65) as `5cc179985803e1da812353df73868db668234202`, after all required Linux/macOS/Windows/MSRV CI checks passed. Final reviewed head: `aed6df7974efc9cc1d912859385498d36b8b0d08`. Independent adversarial/design review found no substantive issues. Production executable content matches the benchmark. PR preparation adds comment formatting, release notes and deterministic fixture corrections: an interrupted initial submission and bounded wait for notification starvation, plus empty FIFO reads for three abandoned-driver/close-wait tests. Final focused io_uring coverage remains 7,552/7,552. See [PR validation records](results/post-submit-20261028/pr-validation/readme.md).
