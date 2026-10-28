# Task topology and response progress

The matched topology experiment finds a useful low-concurrency CPU saving, but
no general throughput or tail-latency win from changing connection scheduling.
The remaining typical-latency gap to Tokio grows substantially with offered load.
No scheduler policy or production HTTP server behavior changes in this study.

## Workload and controls

Hana runs one application worker, with all server threads pinned to CPU 0,
controller CPU 1, four client threads on CPUs 2, 3, 6 and 7, and server SMT sibling
CPU 4 unused. The host was idle before measurement; no remote builds, tests or
profiles ran alongside uninstrumented timing. Source baseline is scheduler
`5cc179985803e1da812353df73868db668234202`; Tokio is 1.54.1.

All six controls do one arbitrary read followed by one complete static 78-byte
response write. They bypass HTTP framing and parsing. Every connection reuses an
initialized 16 KiB `Vec`, including Tokio (the older control embedded an array).
Base owned and borrowed modes share their connection loop and borrowed response
write; only the receive API changes. Each connection has one outstanding client
request, with TCP_NODELAY enabled.

Each receive mode runs with either a spawned `FuturesUnordered` supervisor
polling connection futures, or one executor task per connection. In the latter,
the accept supervisor retains and reaps task handles, while connection tasks do
all steady-state I/O. Base dropping a handle cancels execution, so handles remain
owned until completion. Tokio spawns its task when the wrapper is first polled;
Base spawns immediately after accept. Paced connections are established before
their schedule starts. Unpaced runs use fresh connections for the measured wrk
process, so admission is included and amortized over its measured responses.

Five randomized interleaved rounds measure unpaced `wrk` throughput and process
CPU separately at 1 and 64 connections. Two further five-round cohorts offer
200k and 240k requests/s at 64 connections, with 2 seconds warmup and 8 measured
seconds. The independent epoll client keeps absolute schedules, retains late
requests and validates every response byte. See [fixed-rate methodology](fixed-rate.md).
All variants in a cohort use the same frozen executable; traces use a separate
executable and separate runs.

## Unrestricted throughput and CPU

Medians of individual runs:

| Receive and topology | 1-connection CPU, us/request | 64-connection requests/s | 64-connection CPU, us/request |
| --- | ---: | ---: | ---: |
| Owned, supervisor | 6.627 | 263,800 | 2.665 |
| Owned, tasks | 5.992 | 261,927 | 2.691 |
| Borrowed, supervisor | 6.142 | 241,904 | 2.924 |
| Borrowed, tasks | 5.542 | 241,453 | 2.940 |
| Tokio, supervisor | 5.899 | 248,966 | 2.831 |
| Tokio, tasks | 5.560 | 248,855 | 2.837 |

Paired owned tasks/supervisor changes:

- One-connection CPU/request: **−8.94%**, all five lower, range −11.72% to −2.47%.
- One-connection throughput: +3.00%, four higher, range −0.21% to +3.39%.
- 64-connection throughput: **−0.95%**, all five lower, range −1.30% to −0.28%.
- 64-connection CPU/request: +0.97%, mixed range −0.07% to +2.12%.

Owned throughput exceeds matched-topology Tokio in every concurrent pair:
+5.85% with a supervisor and +5.16% with per-connection tasks. CPU/request is
6.25% and 4.94% lower, respectively. These are closed-loop throughput observations;
server process CPU is about 70–71% of one core, so they do not establish a
server-only saturation limit. Client, loopback and scheduling effects remain.

At one connection, per-task owned and Tokio user CPU are 1.430 and 1.392 us/request;
system CPU is 4.634 and 4.141. These are medians of separate components. The
remaining owned CPU difference is mostly in system time in this cohort, which
motivates kernel receive-lifecycle profiling; aggregate accounting does not
identify a particular kernel function or prove bookkeeping is free.

All 60 unpaced runs report zero `wrk` socket/status errors. Preflight responses
validate content and reuse. Recorded server stderr includes peer resets when
`wrk` closes connections with requests in flight; it is not universally empty.
Unpaced measured responses do not have the paced client's byte-by-byte check.

## Equal offered-rate latency

Medians of individual runs; RTT is measured from actual send. Scheduled latency
starts at the planned arrival and includes waiting to send.

| Receive and topology | 200k RTT p50, us | 200k RTT p99, us | 200k scheduled p99, ms | 240k RTT p50, us | 240k RTT p99, us | 240k scheduled p99, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Owned, supervisor | 15.703 | 244.351 | 0.441 | 66.047 | 280.063 | 4.563 |
| Owned, tasks | 14.015 | 238.975 | 0.311 | 58.335 | 269.055 | 1.580 |
| Borrowed, supervisor | 15.175 | 275.455 | 0.737 | 42.367 | 290.303 | 7.533 |
| Borrowed, tasks | 13.247 | 245.375 | 0.297 | 35.007 | 319.999 | 11.043 |
| Tokio, supervisor | 13.807 | 237.055 | 0.263 | 29.759 | 269.055 | 3.557 |
| Tokio, tasks | 12.991 | 249.215 | 0.544 | 25.599 | 261.887 | 3.023 |

Paired topology changes for owned receives:

| Metric, tasks / supervisor | 200k/s paired median | 240k/s paired median |
| --- | ---: | ---: |
| RTT p50 | −10.53% | −12.82% |
| RTT p99 | +10.15% | −0.19% |
| Scheduled-arrival p99 | +17.30% | −33.76% |

At 200k, owned RTT p50 improves in every pair (range −13.65% to −3.52%). At 240k,
four improve, with range −29.36% to +26.42%. Tail changes remain mixed at both
rates: the 240k scheduled-p99 range alone is −82.25% to +92.80%. Differences
between table medians are not paired median changes.

Owned tasks retain higher RTT p50 than Tokio tasks in every pair: +7.88% at
200k and +68.87% at 240k. This strong load dependence argues against explaining
the gap with one large constant cost per request. It does not yet identify the
mechanism producing that dependence.

At 240k, owned tasks have lower scheduled-arrival p99 than Tokio tasks in all
five pairs (paired median −34.65%, range −48.45% to −1.84%), while RTT p99 has
mixed ordering. Treat those as separate results, not general latency parity or
a universal tail win. Full per-run values and ranges are preserved.

All 60 paced runs validate **132 million responses including warmup**, with
105.6 million measured responses and zero payload errors. Every planned measured
request is retained, including late completion. Client and server stderr are
empty. Maximum completion-window target deviation is 0.073% at 240k and 0.0048%
at 200k; maximum drain is 4.620 ms and 0.314 ms. Actual send/window completion
counts are retained separately; achieving the window rate does not imply every
planned request completed inside its original measurement window.

## Separate response-progress probes

Three runs per topology at each rate use 1 second warmup plus 4 seconds measured.
The probe samples successful positive owned-read publications using the high
seven bits of a full-period 64-bit LCG. All runs have exactly the independently
predicted counts: 9,296 samples at 240k and 7,709 at 200k. Across 12 runs, all
13.2 million responses validate and all **102,030 samples** complete.

Samples track publication, end of that collected CQ snapshot, latest driver
return, outer task poll start, connection poll start, result consumption, read
return and completed response write. CQ ends are keyed by collection identity.
The driver timestamp is literally the latest return before consumption; it is
not keyed to the publishing turn. Its preceding interval can therefore include
intervening application/driver work if a result survives another turn.

Connection identities retain pending samples across a suspended write. An
interleaving test checks that both sampled and unsampled responses on another
connection cannot consume that record. Stage-order assertions, exact row sums,
collection identities and EOF-flush counts guard attribution and completeness.

Ranges of instrumented per-run means, us:

| Rate and topology | Latest driver return → outer task poll | Outer task start → connection poll | Publication → response write completes |
| --- | ---: | ---: | ---: |
| 200k, supervisor | 0.085–0.095 | 15.10–17.73 | 19.24–22.09 |
| 200k, tasks | 14.86–24.91 | 0.030–0.032 | 18.85–28.96 |
| 240k, supervisor | 0.104–0.128 | 67.95–84.50 | 73.51–90.90 |
| 240k, tasks | 83.84–107.37 | 0.030–0.031 | 89.64–113.75 |

With a supervisor, elapsed time before reaching a connection includes earlier
child polls and their I/O work. With per-connection tasks, the corresponding
elapsed interval moves before that task's poll. It is not eliminated by changing
which queue owns the connection. These are elapsed intervals, not isolated
ready-queue implementation costs. Mean sampled CQ batch lengths are roughly
9–11 at 200k and 35–57 at 240k in these instrumented runs.

Every task/child poll and CQ/driver boundary is timestamped, TLS state changes,
and worker-thread trace dumps synchronously write batches of rows. These probes
substantially change batching and latency. Their successful-read starting point
excludes input arrival, SQ staging/issuance, kernel receive/CQ production,
pre-collection waiting and client/network work. Do not subtract these means from
uninstrumented RTT, sum stage percentiles, or use them as proof of a precise
production latency contribution. They locate candidate boundaries and demonstrate
topology-dependent delay placement; they do not establish a scheduler fix.

## Decision and next research

Retain the benchmark modes and evidence. Production HTTP still uses its existing
supervisor; scheduler executable code is unchanged. Per-connection tasks earn a
low-concurrency CPU benefit but have a small repeatable concurrent throughput
cost, with no dependable tail benefit across both offered rates. This does not
justify a universal topology change, task budget, eager submission or public knob.

The next research should measure the receive lifecycle and work per request:

1. Count ring entries, receives, completions and wakeups at both offered rates
   and at one connection, with separate kernel profiles. Attribute the residual
   system CPU before changing owned-read synchronization.
2. Correlate input availability through CQ publication with response progress.
   The current probes start too late to explain the full load-dependent gap.
3. Evaluate amortizing repeated receive setup through persistent/multishot
   receives and bounded provided buffers. Treat it as a candidate, preserving
   cancellation, original completion retirement, migration and backpressure.
   Streaming/full-duplex workloads remain a separate validation requirement.

[Artifacts and reproduction](results/task-topology-20261028/readme.md) retain raw
rows, exact production/probe source archives, source patches, frozen binaries,
checksums, scripts, validation and independent review. Linux and macOS Clippy,
Linux raw-mode smoke checks, macOS compile checks and the probe interleaving test
pass. This is a raw I/O experiment; it does not measure HTTP allocations, parser
copies, body backpressure or multi-worker scaling.
