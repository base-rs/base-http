# Performance overhead

The layered Linux benchmarks identify several substantial differences between Base and Tokio. The initial investigation identified frequent driver polling for self-waking tasks and an additional receive syscall in the io_uring borrowed-reader path. The extra receive now has a confirmed fix, described below. A subsequent completion-policy change closes the residual borrowed io_uring echo gap in the latest cohort and improves full HTTP. The remaining epoll raw-server gap is not yet fully explained.

The initial layer-breakdown measurements came from Hana on 2026-10-27: one execution worker, scheduler main `f05a630db60686526b6145efb284a9c2f61fe8b4`, Tokio 1.54.1, and 742 timed samples. Newer studies below state their own baselines and cohorts. See the [full investigation](benchmarks/hello/layer-breakdown.md), [harness](benchmarks/hello/layers/readme.md), and [saved evidence](benchmarks/hello/results/layer-breakdown-20261027/readme.md).

## Receive lifecycle — 2026-10-28

The [receive-lifecycle experiment](benchmarks/hello/receive-lifecycle.md) tests the extra kernel TCP receive attempt made by scalar owned io_uring reads. A poll-first prototype loses **6.87%** concurrent raw-response throughput and raises process CPU/response **8.49%**, with every pair unfavorable. At equal offered rates, paired RTT p99 rises **15.95% at 200k/s** and **14.25% at 240k/s**, with four of five pairs worse at each rate and broad mixed ranges. All **52.8 million paced responses** validate. The unconditional hint is rejected; scheduler runtime behavior stays unchanged.

Counters and CPU profiles remain separate from timing. An invalid 64-bit return probe and a build-overlapped confirmation cohort are explicitly excluded from the corrected mechanism comparison. These observations concern work executing in server tasks, not the complete system-wide ring lifecycle. Removing a failed receive is insufficient justification for changing policy; persistent receive/poll registration remains a separate hypothesis requiring ownership, buffering and backpressure validation.

## Matched task topology — 2026-10-28

The [task-topology study](benchmarks/hello/task-topology.md) compares the same raw I/O loops under one supervisor and one task per connection on Base and Tokio. Five paired rounds reduce owned-read one-connection CPU/request **8.94%** with per-connection tasks; concurrent throughput is **0.95% lower**, all five pairs. Owned throughput remains **5.85% / 5.16% above matched-topology Tokio** for supervisor/tasks. These are closed-loop observations, not a proven server saturation limit.

At equal offered rates, task topology reduces typical owned RTT, but tails remain mixed. Per-task owned/Tokio median RTT is **14.0/13.0 µs at 200k/s**, versus **58.3/25.6 µs at 240k/s**. The remaining gap depends strongly on load. Scheduled-arrival p99 is reported separately; no inference from unrestricted p99 is used. All **132 million paced responses** validate.

Separate probes show elapsed time moving from earlier child work inside a supervisor to time before the connection task is polled. They substantially perturb batching and do not explain precise production RTT. The next target is receive-lifecycle/system work per request and pre-publication input progress. Benchmark modes and evidence are retained; scheduler policy and production HTTP topology are unchanged.

## Cohesive execution model — 2026-10-28

The [worker/reactor model study](benchmarks/hello/execution-model.md) separates per-operation work from delay between queueing, preparation, issuance, completion publication and application consumption. The scheduler now records the progress boundaries performance changes must preserve, with batching policy and missing latency targets kept explicit.

The first experiment removes owned-payload submission deferral. Five paired rounds show only **+0.54%** concurrent throughput with mixed signs, while one-connection CPU/request rises in every pair, with a **5.65% paired median**. Equal-rate RTT p99 improves in four pairs but has a broad mixed range; scheduled-arrival p99 also remains mixed. The prototype is rejected as the default, with exact artifacts retained. Existing submission policy remains, with corrected comments and a bounded-deferral regression in [scheduler PR #66](https://github.com/base-rs/base-scheduler/pull/66).

Separate probes find sub-microsecond average explicit deferral and roughly 20–36 µs means around queued-read preparation and result consumption, depending on probe configuration. These instrumented distributions guide the next research; they cannot be summed to explain production RTT, and queued-read time can overlap client waiting. The retained Linux suite passes **7,552/7,552 measured source regions**, with Clippy and raw-response validation passing.

## Owned receive control — 2026-10-28

The [raw response experiment](benchmarks/hello/owned-receive.md) changes only receives to owned I/O, with matching reused 16 KiB buffers and unchanged borrowed response writes. At 64 connections, five paired rounds improve throughput **8.62% over borrowed receives** and **5.42% over spawned Tokio**, with every throughput pair positive. CPU/request falls **8.24%** versus borrowed. Separate traces reconcile request/response bytes and confirm removal of one receive syscall per response, while both paths still deliver approximately one CQE per response.

This has a latency and low-concurrency tradeoff: concurrent p99 increases **21.49%** versus borrowed (**29.33%** versus Tokio), and single-connection CPU/request increases **9.23%** with little throughput change. All 60 client samples validate. The benchmark variant is retained; production HTTP input and scheduler code are unchanged. Next evaluate owned input through the HTTP parser while investigating the tail-latency and cancellation/abandonment behavior, before choosing a default.

A [fixed offered-rate follow-up](benchmarks/hello/fixed-rate.md) uses the same frozen executable and a paced, payload-validating client. All 45 runs meet 200k/220k/240k targets within 0.03%. At 240k, observed send-to-response p99 is **339 µs borrowed, 344 µs owned and 316 µs Tokio**; the paired owned/borrowed median difference is +1.36%, so the unrestricted cohort's large RTT penalty does not recur here. Scheduled-arrival p99, which includes waiting behind previous requests, is **6.668/4.387/3.484 ms**, respectively, with broad ranges and mixed owned/borrowed pair ordering. The arrival pattern and client also change; these are separate workload measurements, not proof that rate alone explains the earlier difference. Every measured request is retained, including late completions, and 99 million responses including warmup validate.

## Immediate post-submit completion delivery — 2026-10-28

The [post-submit experiment](benchmarks/hello/post-submit-completions.md) finds that the reactor publishes completions before a nonblocking submit but leaves newly produced completions until a subsequent turn. Separate owned-read diagnostics find CQEs immediately available after 99.98% of those submissions. The candidate invokes the existing snapshot collector after successful submission, adding no ring-submission syscall or public API; collecting a notification CQE can perform the existing eventfd read.

At a fixed 240k requests/s and 64 connections, five paired rounds reduce median RTT **127.6 → 74.8 µs**. Paired p50 improves **41.47%** and mean RTT **33.94%**, all five pairs lower. RTT p99 changes only **−0.70%**, with mixed signs. Scheduled-arrival p99 generally improves but remains variable. Tokio retains lower RTT: **46.1 µs p50 / 315.1 µs p99**, versus candidate **74.8 / 357.4 µs**. All 48 million paced responses including warmup validate.

A separate five-round unrestricted cohort improves owned throughput **2.74%** and CPU/request **0.95%** at 64 connections, all pairs favorable. Candidate throughput is **6.63% above spawned Tokio**, while unrestricted p99 remains **31.83% higher**. One-connection CPU/request improves **3.86%** over the baseline. This addresses a typical-latency penalty and preserves capacity; the tail gap remains open. Submission deferral and executor task budgets are unchanged, and production HTTP still uses its existing receive path. Linux regression/conformance tests and Clippy pass, with **7,552/7,552 measured source regions covered**. The change is merged in [scheduler PR #65](https://github.com/base-rs/base-scheduler/pull/65), commit `5cc1799`; all required CI checks pass.

## Retained io_uring fix

The [borrowed-reader follow-up](benchmarks/echo/uring-readiness/readme.md) removes the extra empty scalar receive when OOB support is disabled. A short positive read clears only its observed readiness generation; subsequent calls wait first. Sticky closure state preserves EOF, and OOB-enabled and message paths retain conservative retries.

Five interleaved echo rounds at 64 connections improve paired throughput **16.73% for 64-byte messages and 14.34% for 4 KiB**. The remaining paired gap to epoll is **0.99% and 2.52%**. A three-physical-core client control gives +19.32%/+11.04% over the old reader and +6.00%/-0.98% versus epoll. The large improvement is repeatable; the smaller backend ordering depends on client placement. One-connection throughput is 1.2–1.5% lower than the old reader in the main cohort.

Separate final counters confirm approximately **one receive per echo instead of two**. The raw response control improves 14.83% and full HTTP 9.84% in paired throughput. Full HTTP still trails epoll by 3.31% in its shorter cohort; these measurements do not establish a general win over Tokio or Hyper. Production io_uring coverage is 7,531/7,531 measured regions, and regression/conformance tests pass.

The change is local scheduler commit `587e746` on `perf/iouring-stream-readiness`. The original dirty checkout is preserved. The historical measurements below refer to the pre-fix implementation unless stated otherwise.

A [full repeat with the same frozen binaries](benchmarks/echo/uring-readiness/repeat.md) confirms **+16.76%/+14.01%** concurrent echo gains, **+16.37%** raw-response throughput and **+15.22%** full HTTP throughput over the old reader. Main-placement fixed io_uring trails epoll by 3.17%/2.93% in echo, 2.68% in raw response and 1.82% in full HTTP; full HTTP trails the runtime-root Hyper control by 3.66%. Alternate placement again reverses the small-message echo ordering (+3.69% versus epoll, +3.13% versus Tokio), while 4 KiB stays behind. All 128 throughput samples and 16 separate syscall samples validate; the counters again show approximately one receive instead of two. The repeat is retained separately from the original cohort.

## Retained io_uring completion batching

The [completion-policy investigation](benchmarks/echo/ring-overhead/readme.md) builds on `587e746`. Traces rule out per-message socket re-registration: 64 connections use 64 persistent multishot polls, with no unsupported-operation fallback. A matched loop without scheduler tasks reproduces a **4.05% / 4.14%** cooperative io_uring deficit to epoll for 64 B / 4 KiB echo. Switching that loop to deferred task work removes the deficit within round-to-round variation.

Local scheduler commit `77fe0f8` on `perf/iouring-deferred-taskrun` prefers `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG` for fresh thread-bound workers, falling back to the previous cooperative configuration and then the original ring. Movable and already-driven drivers retain their configuration. Existing nonblocking turns drive taskrun progress; the io-uring minimum becomes 0.7.15 so taskrun-only submits request `GETEVENTS`. This is a small setup change, with no new hot-path bookkeeping or public API.

Five interleaved rounds at 64 connections improve Base borrowed echo **5.78% / 2.86%** over the previous fixed reader. Deferred Base is **0.25% / 0.13% behind Tokio**, with mixed signs across rounds: approximate parity in this cohort. A separate three-round one-connection check improves **2.89% / 2.80%** over the baseline. Owned I/O is a distinct execution model and runs **7–8% ahead of Tokio** in the concurrent echo cohort; these results do not show a consistent owned-I/O gain from the configuration change itself.

Kernel traces show the mechanism: completion work averages **1.21 → 19.19 tasks per batch**. Explicit ring entries increase, but completion-work batches per echo drop **0.828 → 0.052**. Minimizing ring-entry count alone would miss this improvement. This is completion-processing policy, not evidence of a kernel defect.

Full HTTP improves **8.01%** in paired throughput and reduces timed server CPU/request **8.60%**, with the HTTP implementation unchanged. It finishes **1.70% ahead of the runtime-root Hyper control** in paired throughput, a smaller margin with mixed round ordering and an existing task-layout difference. The echo and HTTP results are separate cohorts and do not establish a universal backend winner.

The separate raw HTTP control improves **3.98%** in throughput and reduces CPU/request **4.82%**, with all five throughput pairs positive. It remains **2.37% behind spawned Tokio** and uses **2.77% more total CPU/request**, including **9.96% more user CPU/request**. This is the concrete remaining raw-path target; full HTTP's small lead is not proof that all scheduler/interface overhead is gone.

The Linux regression/conformance tests, formatting and Clippy pass; the production coverage gate covers **7,543/7,543 measured regions**, excluding three invariant-only unreachable regions. Independent review identified and resolved the minimum-dependency requirement. The inherited OOB design document still has an unrelated outdated statement about conservative io_uring scalar reads; this change does not resolve that documentation inconsistency.

A [full 166-sample repeat with the same binaries](benchmarks/echo/results/ring-overhead-repeat-20261028/readme.md) confirms **+4.84% / +2.45%** concurrent borrowed echo gains and **+4.22% / +4.46%** at one connection. Concurrent borrowed io_uring remains approximately level with Tokio (+0.22% / −0.83%); deferred owned I/O is **+7.44% / +7.56% ahead**. Full HTTP gains **5.05%**, reducing CPU/request **5.26%**, and is effectively level with Hyper (+0.48%, mixed rounds). Raw throughput gains **5.48%**, reducing CPU/request **6.49%**, but remains **0.94% behind spawned Tokio** with **0.95% more CPU/request**. The previous 2.37% raw gap and the repeat's smaller gap use identical executables; their difference is variation between cohorts, not another fix. Single-connection 4 KiB p99 is 6.29% higher than Tokio in the paired median, so throughput improvement does not imply uniformly better tail latency.

A separate **48-sample off-host check** validates payloads at one/four workers and 64/256 connections. The client reaches Hana over Wi-Fi and leaves the server underutilized; this is a coarse network-load check, not a maximum-scaling measurement. A wired independent client remains necessary to establish multicore capacity.

The original dirty scheduler checkout is preserved. The later sections retain historical measurements and should not be read as measurements of this candidate.

## Real-workload check: echo

The [echo comparison](benchmarks/echo/results.md) does **not** reproduce the large self-waking scheduling gap. In the initial five-round cohort, native epoll is within about 1–1.5% of Tokio at one connection and 2.6–2.9% behind at 64 connections. Across client placement controls, the busy native gap ranges from approximately 0–3% behind. This does not establish general parity or a throughput win.

Separate counters observe no native empty epoll returns in any echo case. Both io_uring and the Base/Tokio adapter instead make approximately one additional `WouldBlock` receive per echo. Their larger throughput gaps persist under different client configurations, including when Tokio executes both direct and adapter versions.

For this I/O workload, receive/wait handling is the better first implementation target. Keep the self-waking results below as diagnostics for repeated cooperative yielding, rather than treating them as the demonstrated echo or HTTP bottleneck. The echo cohorts contain 116 uninstrumented samples and use a payload-validating client with one actual server task per connection.

## Large differences

| Component | Base | Tokio | Difference |
| --- | ---: | ---: | --- |
| One self-waking task, epoll enabled | 601 ns/wake | 55 ns/wake | About 11× the time |
| Supervisor with 64 self-waking `FuturesUnordered` children, epoll enabled | 393 ns/child wake | 99 ns/child wake | About 4× the time |
| One self-waking task, I/O driver disabled | 82 ns/wake | 46 ns/wake | About 1.8× the time |
| io_uring borrowed receives per raw response | 2 calls | 1 call | One additional `WouldBlock` receive |

### Driver polling cadence

Base freezes a batch of ready tasks and polls the I/O driver between batches. A lone self-waking task creates batches of one. The syscall counters show approximately one empty epoll call per wake for Base, versus one per 61 wakes for Tokio. Each empty driver turn costs approximately 500 ns on this host.

The self-waking supervisor also exposes this policy: Base makes one empty epoll call per two child wakes, versus approximately one per 122 for Tokio. Detaching the I/O driver reduces Base's supervisor cost from 393 to 110 ns per child wake; Tokio without its driver takes 89 ns.

With 64 independently scheduled self-waking tasks, Base amortizes the poll over the batch and takes 77 ns/wake versus Tokio's 55 ns. There is still task execution overhead after removing repeated driver work, but its exact source needs further isolation.

This scheduling weakness does **not** explain the measured raw socket gap: Base's raw server makes only about 0.031 epoll calls per response, with almost no empty returns.

### io_uring receive/wait handling

Before the retained fix, the borrowed io_uring reader used the syscall-first `socket_monitor_recv_poll` / `monitor_io` path. After receiving a request, its next read probes the empty socket before waiting. Counters show approximately one successful receive and one `WouldBlock` receive per response. Base epoll and Tokio each make approximately one receive in total.

This is concrete additional work on the real socket workload. The controlled follow-up above measures the benefit of removing it. These tests use borrowed buffers and synchronous nonblocking payload syscalls; they do not measure owned-buffer or multishot receive performance.

## Smaller differences and structural clues

- **Readiness bookkeeping:** Base's arm/publish/poll/clear cycle takes 70 ns versus Tokio's dedicated reader slot at 58 ns. Base supports multiple waiters; Tokio's general multi-waiter readiness future takes 106 ns in the separate matched screen. Preserve that distinction when considering simplifications.
- **Scalar read wrapper:** Base is about 10–37 ns behind across the ready-read probes. The measured difference changes with payload, readiness preparation, and harness layout.
- **Future storage:** read/write futures occupy 560/584 bytes for Base epoll, 384/408 bytes for io_uring, and 24/24 bytes for the Tokio extension futures in this harness. The interfaces have different capabilities and cancellation semantics. Size is a reason to inspect generated code, not proof of runtime cost.

Kernel event collection is approximately equal at 83 ns/event for a batch of 64, comparing Base with Mio. Ready one-byte writes are also comparable at 2.31 µs for Base versus 2.33 µs for Tokio. Neither currently justifies a rewrite.

## Complete-workload checks

At 64 persistent connections, the latest raw-server cohort puts Base epoll **4.71% behind Tokio** and Base io_uring **19.17% behind Tokio**, using median within-round throughput ratios. Base epoll consumes 10.52% more user CPU per response. These servers bypass HTTP parsing and encoding.

The [last full HTTP confirmation](benchmarks/hello/raw-gap-followup.md) puts Base approximately 1.5% behind spawned Hyper, with mixed rounds. It is a separate cohort and must not be substituted for the raw-server comparison.

An inline hint improved the short-read microbenchmark but failed the raw-server confirmation and was discarded. Those earlier layered experiments retained no production optimization; the subsequent borrowed-reader fix is recorded above.

## Implementation priorities

1. Retain the io_uring borrowed scalar receive fix and deferred completion batching, both validated in echo and HTTP. Next evaluate owned and pooled multishot receive on streaming/duplex HTTP and under backpressure; the Tokio adapter's additional receive remains separate work.
2. Simplify useful task execution and epoll read/readiness bookkeeping, using isolated controls to locate work and real socket workloads to confirm benefits.
3. Amortize driver polling over bounded runnable work for self-waking/mixed CPU-and-I/O workloads, preserving I/O/timer latency and external-wake fairness. Echo does not currently demonstrate excess empty polling or establish a benefit from this change.

The component measurements have overlapping costs and different kernel/cache state. Do not add their deltas to claim an explanation of complete-request overhead.
