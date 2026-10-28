# Full comparison repeat — 2026-10-28

All **166 samples validate**, using exactly the same frozen executables, harness and CPU placement as the previous cohort. See the [complete tables, paired ranges, latency and CPU results](tables.md). Raw results and reproducible commands remain alongside this report. These are fresh measurements, not implementation changes.

At 64 connections, deferred borrowed io_uring improves **4.84% / 2.45%** over cooperative borrowed I/O for 64 B / 4 KiB echo. Its paired comparison with Tokio is **+0.22% / −0.83%**, with mixed round ordering: approximately level in these workloads. Deferred owned io_uring remains **7.44% / 7.56% ahead of Tokio**, positive in every pair. The owned and borrowed APIs use different execution models.

At one connection, deferred borrowed I/O improves **4.22% / 4.46%** over the cooperative baseline and **5.37% / 1.44%** over Tokio. No single-connection throughput regression appears. Tail latency does not improve uniformly: single-connection 4 KiB p99 is **6.29% higher than Tokio** in the paired median, with a range from −2.01% to +7.53%.

Full HTTP improves **5.05%** over cooperative io_uring and reduces timed CPU/request **5.26%**. Compared with the runtime-root Hyper control, paired throughput is **+0.48%**, ranging from −6.22% to +1.11%: approximate parity, rather than evidence of a reliable win. The original cohort's +8.01% implementation gain and +1.70% Hyper comparison remain separate historical results.

The raw fixed-response server improves **5.48%** over cooperative io_uring and reduces timed CPU/request **6.49%**. It remains **0.94% behind spawned Tokio**, with **0.95% more total CPU/request**. The throughput pairs range from −4.10% to +0.01%. The same binaries were 2.37% behind in the original cohort, so the smaller gap is measurement variation, not another optimization.

The minimal-loop control also repeats the mechanism: cooperative io_uring is **3.79% / 3.33% behind epoll**, while deferred io_uring is **1.23% / 0.68% ahead**. These minimal loops contain no scheduler tasks; their absolute advantage is not an isolated measure of task-scheduling cost.

The repeat supports retaining deferred completion processing. It confirms the borrowed echo and HTTP gains and the owned-I/O advantage; it does not establish universal superiority over Tokio/Hyper or eliminate the remaining raw-path difference. Off-host maximum scaling remains unmeasured: the earlier Wi-Fi check did not saturate Hana and was not repeated.

The matrix ran sequentially from 07:47:40 to 08:01:44 NZDT. There was no compilation or profiling during throughput. Preflight aggregate CPU idle was about 99%; whole-run host isolation is not asserted. Six executable hashes match before/after, four harness sources match retained copies, and wrk matches its frozen hash. Echo checks validate every payload. HTTP response probes pass and wrk reports no socket/status errors; expected forced-teardown resets are recorded separately.

`verification.json`, `harness-verification.json`, `provenance.json`, `validated-summary.json`, `repeat.sh`, initial/final host records and the individual sample files retain the evidence. Source archives and frozen binaries are referenced from [the original evidence](../ring-overhead-20261027/readme.md). `sha256.json` hashes all other repeat files. The earlier evidence is preserved.
