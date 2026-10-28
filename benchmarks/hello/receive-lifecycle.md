# Receive lifecycle and the poll-first experiment

Owned io_uring receives perform an extra unsuccessful kernel TCP receive when
submitted before the next request arrives. Skipping that attempt with
`IORING_RECVSEND_POLL_FIRST` is a mechanism experiment, not sufficient evidence
of an optimization: the five-round unrestricted response test loses concurrent
throughput. No scheduler runtime change is retained.

## Controls

Hana runs the raw response loops from the [matched topology study](task-topology.md),
using scheduler `5cc179985803e1da812353df73868db668234202` and Tokio 1.54.1.
Each connection has its own executor task, a reused initialized 16 KiB `Vec`,
one arbitrary read and one borrowed write of the same 78-byte response. HTTP
parsing is bypassed. Clients use persistent TCP connections with TCP_NODELAY
and one outstanding request per connection.

All server threads use CPU 0. The paced client's coordinator uses CPU 1 and its
concurrent load threads CPUs 2, 3, 6 and 7. Unpaced wrk processes use CPUs 2, 3,
6 and 7. Single-connection diagnostic load uses CPU 2. CPU 4 is excluded from
server/load placement, but the Python orchestrators inherit SSH affinity and are
not pinned; the copied unpaced metadata's `controller: [1]` assertion is inaccurate.
Linux is 7.2.11-arch1-1. Builds, uninstrumented timing, clean counters
and CPU profiles are separate phases. One intermediate corrected-counter cohort
overlapped builds and is retained but excluded from clean comparisons.

The timing control and candidate build sequentially from the same source path
and locked Cargo release configuration. The candidate adds only `.ioprio(1)` to
scalar owned `opcode::Recv` construction. The hint asks io_uring to check polling
before its initial receive attempt. It does not remove the SQE, CQE, ownership
transfer or executor wake. The [liburing manual](https://github.com/axboe/liburing/blob/master/man/io_uring_prep_recv.3)
dates it to Linux 5.19; this diagnostic provides no older-kernel fallback.

## Kernel work

Initial separate perf counts record TCP receive entry calls, receive submissions,
internal poll arms and CQ completions. Owned reads submit approximately one Recv
and produce approximately one CQ completion per response; at one connection they
enter `tcp_recvmsg` almost twice. Borrowed Base reads and Tokio enter it about once.
The extra kernel receive is invisible to a userspace recv syscall count.

The original return probe incorrectly decoded `tcp_recvmsg`'s signed `int` return
as `s64`. On this machine a negative return can be zero-extended, so filtering
for `-11` incorrectly counted zero EAGAIN results. Those counts are invalid.
A new `s32` return probe fixes the measurement; original raw files remain intact.

The clean pass uses only TCP entry/return probes and owned receive poll-arm
events. Calls and EAGAIN results per validated response:

| Mode | 1 connection, 50k/s: calls / EAGAIN | 64 connections, 200k/s: calls / EAGAIN |
| --- | ---: | ---: |
| Owned control | 1.9992 / 0.9992 | 1.9284 / 0.9283 |
| Poll-first | 1.0000 / 0 | 1.0001 / 0 |
| Borrowed Base | 1.0000 / ~0 | 1.0001 / 0.000064 |
| Tokio | 1.0000 / ~0 | 1.0001 / 0.000064 |

The hint removes the predicted failed attempts. It does not eliminate internal
poll setup: the 200k owned and poll-first cases record approximately 0.928 and
0.962 poll arms per response. This establishes the removed work, not an isolated
time cost or the entire explanation for the concurrent slowdown.

The heavily perturbed nominal-240k diagnostic cases complete at only 212k–227k/s.
The owned control then makes about 1.0002 receive calls and 0.0001 EAGAIN results
per response: batching and backlog alter the amount of empty-socket work. This
must not be compared as an equal-rate latency result or extrapolated to the
uninstrumented 240k workload. All 12 clean cases complete with zero payload
errors and 100% counter running time. Kernel probe hit/miss inventory was empty
and is unavailable; no zero-miss claim is made.

These are **server-context counts**, not system-wide operation totals. Work
executing in client tasks, interrupt context or other workers can be absent.
In particular, near-zero task-add events do not establish that wakeup work is
free. Denominators include warmup and measured responses; event counts include
connection setup, terminal EOF and drain. Instrumentation changes batching and
can prevent the offered rate being met; these are diagnostic ratios, not
equal-rate performance measurements.

Six separate CPU-clock profiles cover owned, borrowed and Tokio at one connection
and 50k/s, and at 64 connections and 240k/s. Reports show zero lost samples and
substantial syscall, TCP and loopback work shared across paths. The unresolved
libc offset `0xa08f2` is the return immediately after a syscall instruction,
verified by disassembly; it is not evidence of a mutex hotspot. Profiles sample
executing CPU, not off-CPU waits or input-to-publication latency. Inlining and
bounded DWARF stack capture also prevent treating a task-wrapper symbol's share
as pure scheduling overhead.

## Unrestricted response test

Five randomized paired rounds use wrk, one second warmup and four measured
seconds at 1 and 64 connections. Poll-first changes relative to owned control:

| Metric | 1 connection, paired median | 64 connections, paired median |
| --- | ---: | ---: |
| Throughput | +0.68% | −6.87% |
| Process CPU/response | −2.21% | +8.49% |

Every concurrent throughput pair loses, with a range of −8.94% to −5.72%; every
concurrent CPU pair rises, +7.54% to +11.34%. Single-connection signs are mixed:
throughput −2.27% to +1.06%, CPU −3.29% to +4.06%. A smaller receive-entry count
does not translate into a faster concurrent response loop.

Absolute medians at 64 connections are 263,993 responses/s and 2.661 us of CPU
per response for the owned control, 244,496 and 2.903 for poll-first, and 249,314
and 2.827 for Tokio. Server CPU is about 70–71% of one core; these closed-loop
rates do not establish server-only saturation. All 30 wrk runs have zero reported
socket/status errors and validate preflight response contents and reuse. End-of-run
peer resets remain in some server logs; wrk does not byte-validate every measured
response.

This agrees with the earlier [echo study](../../../base-scheduler/benchmarks/echo/single-worker-research.md),
which rejected poll-first after an 8.11% concurrent single-worker throughput loss
on a different machine and earlier runtime. The current experiment tests the
response loop and equal-rate latency rather than treating that earlier result as
a universal property.

## Equal offered-rate latency

Five randomized paired rounds at each rate use 64 connections, two warmup seconds
and six measured seconds. Every response byte is validated by the same independent
paced client. RTT starts at actual send; scheduled latency starts at the planned
arrival and includes waiting to send. Individual-run medians:

| Mode | 200k RTT p50, us | 200k RTT p99, us | 200k scheduled p99, ms | 240k RTT p50, us | 240k RTT p99, us | 240k scheduled p99, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Owned control | 13.911 | 235.135 | 0.499 | 73.919 | 300.799 | 1.323 |
| Poll-first | 14.055 | 272.639 | 0.801 | 68.351 | 327.167 | 6.054 |
| Tokio | 12.983 | 219.903 | 0.221 | 40.959 | 282.879 | 2.372 |

Poll-first/control **paired** median RTT p99 rises 15.95% at 200k and 14.25% at
240k, with four of five pairs worse at each rate. Ranges remain broad and mixed:
−16.12% to +173.92%, and −12.23% to +17.98%, respectively. Scheduled p99 paired
medians rise 60.68% and 357.59%, also with mixed signs. These data provide no
repeatable tail improvement to offset the throughput loss.

The 240k p50 illustrates why the table and pairing must stay distinct: the
poll-first median is lower, yet its paired median change is **+12.55%**, with
four pairs higher and range −37.14% to +170.65%. At 200k p50 rises in every pair,
paired median +1.14%. Do not infer a typical-latency win from the difference
between the two table medians.

All 30 paced samples validate **52.8 million responses including warmup** and
39.6 million measured responses, with zero payload errors. Send and completion
rates, window counts and drain remain in the retained evidence; nominal offered
rate alone is not proof that requests completed on schedule. Timing runs use no
perf counters or profiles, and no builds or clean diagnostics run alongside them.
The largest completion-window rate deviation is 0.132%, send-window deviation
0.128%, and drain 6.228 ms. Every planned request remains represented, including
late completions.

## Decision and remaining question

The unconditional poll-first hint is unsuitable as the default on the concurrent
response workload. The clean mechanism test and equal-rate results are retained
alongside the unrestricted measurements; none of these counts alone assigns the
remaining latency gap to the scheduler or kernel.

The next receive-lifecycle hypothesis is whether retaining a receive or poll
registration across messages removes enough setup and rearming work to help.
That is a separate experiment, requiring explicit bounds on buffering and
backpressure, concurrent-reader behavior, cancellation, socket close and ring
migration. The existing pull-reader contract has no hidden receive-ahead queue;
changing it is not justified by this failed one-bit candidate.

Complete [artifacts and replay instructions](results/receive-lifecycle-20261028/readme.md)
include exact source and binary snapshots, raw profiles/counters, invalid and
build-overlapped cohorts, performance samples, probe cleanup and review.
