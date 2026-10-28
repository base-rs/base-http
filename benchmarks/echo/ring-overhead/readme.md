# io_uring completion-policy investigation

This follow-up separates socket registration, kernel completion processing, scheduler overhead and owned-buffer I/O. It follows the [short-read fix](../uring-readiness/readme.md), using that fixed implementation as the baseline. The retained candidate prefers deferred task work for a fresh thread-bound worker; it does not add a scheduler fast path or change the socket API.

## Full repeat

The [2026-10-28 repeat](../results/ring-overhead-repeat-20261028/readme.md) validates another 166 samples using the same frozen binaries. Deferred borrowed echo improves **4.84% / 2.45%** over cooperative io_uring at 64 connections, remaining approximately level with Tokio; deferred owned I/O is **7.44% / 7.56% ahead of Tokio**. Single-connection borrowed throughput also improves. Full HTTP improves **5.05%**, with **5.26% less timed CPU/request**, and is approximately level with Hyper (+0.48%, mixed rounds). The raw control improves **5.48%** but remains **0.94% behind spawned Tokio**. The differing small control margins across unchanged-binary cohorts are measurement variation, not further code improvements. Full tables and observed ranges are retained separately; the measurements below remain the original cohort.

## Retained change

The scheduler candidate is in `../base-scheduler-deferred-taskrun`, branch `perf/iouring-deferred-taskrun`, based on `587e74620d1dc1468a588f02b7ea1fd37b722db7`. Final local commit: `77fe0f8c9082391d7178f720f3f2d6f08c82eaff`.

Fresh thread-bound workers try `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG`, then the previous `COOP_TASKRUN | TASKRUN_FLAG`, then the original validated ring. Movable and already-driven drivers keep their original ring. Existing busy-turn taskrun checks preserve progress without waiting for the worker to become idle. The dependency minimum is io-uring 0.7.15: its submitter requests `GETEVENTS` for taskrun-only entries. Earlier allowed versions did not.

Linux documents deferred task work as processing completions at explicit `GETEVENTS` entries, requiring a single issuer and application-driven progress. See [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html). The intended benefit is batching completion work instead of running it at arbitrary syscall returns; this is a configuration choice, not evidence of a kernel defect.

## Controlled echo results

Hana, one server worker pinned to CPU 0, its SMT sibling CPU 4 unused, validating four-worker client pinned to 2,3,6,7. Five interleaved rounds, 64 connections, 1-second warmup and 4-second timed samples. All 90 samples validate. Rates below are medians; percentage comparisons use median **within-round ratios**, which need not equal the ratio of these medians.

| Server | 64 B echoes/s | 4 KiB echoes/s |
| --- | ---: | ---: |
| Base borrowed, cooperative ring | 223,243 | 194,513 |
| Base borrowed, deferred ring | 233,553 | 200,078 |
| Base epoll | 234,865 | 194,911 |
| Tokio | 235,947 | 199,922 |
| Minimal epoll | 259,762 | 218,703 |
| Minimal cooperative ring | 248,660 | 208,504 |
| Minimal deferred ring | 260,090 | 218,327 |
| Base owned, cooperative ring | 253,654 | 213,066 |
| Base owned, deferred ring | 247,676 | 213,806 |

Deferred borrowed I/O improves **5.78% / 2.86%** over cooperative borrowed I/O, with positive gains in all five rounds for each payload. It is **0.25% / 0.13% behind Tokio** and **2.41% / 2.28% ahead of Base epoll** in paired throughput. Mixed round ordering against Tokio means approximate parity in this cohort, not a general performance guarantee.

The minimal cooperative ring is **4.05% / 4.14% behind minimal epoll**. Changing only its ring policy to deferred puts it **0.13% / 1.02% ahead**, with mixed round ordering. This removes a similar gap without any Base scheduler code, strongly implicating completion policy rather than repeated registration or task scheduling.

The owned-buffer paths are separate execution models: reads submit payload operations to io_uring and writes can complete synchronously. Cooperative owned I/O is **7.27% / 6.57% ahead of Tokio**; deferred owned I/O is **8.09% / 6.94% ahead**. This does not establish that the ring-policy change itself improves owned I/O. Compare paired results, not differences between these percentages or displayed median rates.

A separate three-round single-connection check (36 samples, six variants) gives deferred borrowed **+2.89% / +2.80%** versus cooperative borrowed and **+3.54% / +2.77%** versus Tokio. It demonstrates no single-connection regression in this check.

Borrowed p99 round-trip latency changes by **−3.36% / −1.57%** versus cooperative borrowed; versus Tokio it changes **−8.36% / +2.78%**. Throughput gains do not imply uniformly lower tail latency.

## Registration and profiles

Separate kernel traces and syscall counters are excluded from throughput results. The baseline creates exactly 64 persistent socket `POLL_ADD` requests for 64 connections and cancels 64 at shutdown. Normal socket completions retain `MORE`; there is no repeated per-message registration or unsupported-multishot fallback. At 64 B the trace covers 93,132 echoes, 93,137 normal readiness completions and 1,098 ring entries. The 4 KiB control covers 81,564 echoes and 962 entries, again with 64 registrations.

The 64 B trace records cooperative completion work in 77,114 batches for 93,315 tasks (1.21 tasks/batch). Deferred processing records 5,629 local-work batches for 108,043 tasks (19.19 tasks/batch). Normalized by whole-process echoes, this is approximately **0.828 → 0.052 batches/echo**, while explicit ring entries rise **0.0118 → 0.0522/echo**. Fewer kernel completion batches can be more useful than minimizing the enter count alone. The minimal loops show the same direction: 1.24 → 19.99 tasks/batch. These instrumented runs establish the mechanism, not the uninstrumented percentage gain.

The first cooperative-owned diagnostic had a one-message discrepancy between scalar send counters and receive CQE bytes; it is retained as rejected and excluded from normalized conclusions. A fresh diagnostic passes exact sent/received byte reconciliation, as do the deferred-owned and borrowed/minimal traces. Do not silently use non-reconciling counters.

Minimal loops use the same nonblocking scalar receive/send calls, 64 KiB per-connection buffers and TCP_NODELAY, with complete partial-send and EOF handling. They contain no executor tasks. Their absolute advantage includes removing Base machinery and using a different execution model, so it is not an isolated estimate of task-scheduler overhead.

Cycles profiles include kernel stacks and use 999 Hz sampling with DWARF stacks. Both epoll and io_uring spend substantial sampled CPU in syscall entry/return and TCP processing. Sampled percentages are relative distributions; subtracting percentages between profiles does not measure absolute saved cycles.

## HTTP confirmation

Five interleaved rounds at 64 persistent connections use 1-second warmup and 4-second measurements. This rebuild changes the scheduler and leaves the HTTP implementation and response unchanged. The Hyper control uses its existing runtime-root connection layout; the raw Tokio control uses spawned connection tasks. Preserve that task-layout distinction when comparing the protocols.

| Full HTTP server | Median requests/s | Median timed CPU µs/request |
| --- | ---: | ---: |
| Base io_uring, cooperative | 169,455 | 4.491 |
| Base io_uring, deferred | 182,814 | 4.096 |
| Base epoll | 173,306 | 4.367 |
| Hyper/Tokio, runtime-root | 179,423 | 4.187 |

Within-round throughput improves **8.01%** over the cooperative baseline, while timed server CPU/request falls **8.60%** and p99 latency falls **10.34%**. Deferred Base is **5.70% ahead of epoll** and **1.70% ahead of the runtime-root Hyper control** in paired throughput. The much smaller control ordering needs the per-round range and host caveats; it does not establish a general win over Hyper.

The separate raw HTTP control bypasses parsing/encoding and uses a fixed response:

| Raw server | Median requests/s | Median timed CPU µs/request |
| --- | ---: | ---: |
| Base io_uring, cooperative | 224,341 | 3.196 |
| Base io_uring, deferred | 233,708 | 3.037 |
| Base epoll | 228,790 | 3.135 |
| Tokio, spawned connection tasks | 239,626 | 2.948 |

Deferred raw throughput improves **3.98%**, with all five pairs positive, and CPU/request falls **4.82%**. It is **1.96% ahead of epoll** but still **2.37% behind spawned Tokio**, with all five pairs behind. Timed CPU/request is **2.77% higher** than Tokio, including **9.96% higher user CPU/request**. This remaining raw-path gap is real in this cohort; the small full-HTTP lead does not erase it or prove all abstraction overhead is gone.

## Off-host scope

The direct TCP control runs the same payload-validating client from macOS to Hana at one/four server workers and 64/256 connections, interleaving the old/new io_uring builds with epoll and Tokio. SSH carries process control and the readiness address only. The source changes only the benchmark listener address; production scheduler source remains hash-verified.

The route uses macOS `en0`, confirmed by `networksetup` to be **Wi-Fi**, with a 1500-byte MTU. Request latency is several milliseconds and the server is far below saturation. These results check payload integrity and ordinary operation under an actual network, but do not isolate scheduler throughput or establish maximum multicore scaling. A wired independent load generator is still required for that claim. Network CPU records cover whole process lifetimes including serial connection establishment, warmup and cleanup.

All **48 off-host samples** complete successfully with byte validation and empty stderr. Median rates range from approximately 10k echoes/s at 64 connections to 21–23k at 256. Whole-lifetime server CPU estimates stay below one core even with four workers; the deferred server medians use about 0.09/0.13 cores at one worker and 0.18/0.24 at four workers (64/256 connections). Paired old/new throughput changes vary with load and round, and are not treated as scheduler gains or regressions on this network-limited path. The raw samples and paired ranges are retained.

## Evidence and reproduction

[Saved evidence](../results/ring-overhead-20261027/readme.md) contains raw samples, environment records, validation, scheduler patch and review, source/binary hashes and separate diagnostics. Earlier frozen baseline executables remain in the previous investigation's evidence.

- `Cargo.toml`, `src/main.rs`: standalone minimal Linux loops; io-uring 0.7.15.
- `run.py`: interleaved echo variants using the frozen canonical validating client.
- `diagnose.py`, `summarize_trace.py`: separate perf trace/profile capture and ring-identity summaries.
- `http-run.py`: interleaved raw/full HTTP using the existing frozen wrk harness.
- `client/`, `network.py`, `network-server.py`: exact echo client logic for direct off-host TCP, with SSH used only for process control.
- `summarize.py`: asserts complete matrices, successful validating clients, matching raw JSON and expected HTTP diagnostics before producing paired summaries.
- `compare_elf.py`: checks allocated runtime sections when a dependency-minimum/documentation rebuild changes only build identity/debug metadata.
- `finish.sh`: host-specific sequential workflow; run only the remaining stages when resuming because output directories are immutable.

Do not compile, profile, trace or perform heavy analysis concurrently with throughput samples. All final loopback cohorts follow that rule. Hana had some background SMB activity (about 8% of one CPU observed) and absolute rates drifted during the long experiment; interleaved paired changes are the primary comparison. Echo CPU accounting covers whole processes including setup/warmup/cleanup and must not be presented as timed CPU per request. Full HTTP uses timed CPU accounting.

## Remaining performance work

The next useful targets depend on the workload:

1. Keep the small deferred-worker configuration improvement and its fallbacks. It removes unnecessary completion-work dispatch without a new executor architecture.
2. Exercise owned reads and pooled multishot receive with simultaneous input/output, streaming bodies and real HTTP payloads. The current echo comparison proves benefit for owned I/O but serializes each request/response; it does not measure overlapped duplex traffic, zero-copy send or pooled receive throughput.
3. Inspect Base user-space readiness and task execution against the minimal loops using absolute cycles/request and representative backpressure. The minimal-loop advantage is a headroom indicator, not attribution to a single abstraction.
4. Treat self-waking-task driver cadence as a separate scheduling issue. This echo workload does not exhibit the excess empty polling that motivated it. Any runnable-work budget needs I/O, timers, cancellation and external-wake latency controls before retention.
5. Use independent load machines and sufficient network bandwidth for scaling. An underutilized server cannot establish maximum multicore capacity.

No universal backend winner follows from one host, TCP echo and a static HTTP response. These results do show that io_uring need not trail epoll when its completion policy matches the runtime's thread ownership.
