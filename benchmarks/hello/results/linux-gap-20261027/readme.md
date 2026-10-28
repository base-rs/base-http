# Linux gap diagnostics

These are investigation artifacts, not a retained optimization. See the [findings](../../linux-gap.md).

`results/probe-ceiling/` measures the diagnostic short-read-clear change against unchanged Base/Hyper and raw-I/O controls. Other result cohorts measure rejected deferred-retry, level-triggered, duplicate-publication, one-shot ring, dual-queue and authority-scanning experiments. The `snapshots/` directory preserves experimental sources. Instrumented rates in `diagnostics/` are not benchmark rates.

`readiness_probe.rs` is the standalone reproducer built against pinned Tokio 1.54.1; `diagnostics/tokio-urgent-probe.log` records both stalls and independent confirmation of queued ordinary data. It requires Linux's MSG_OOB and MSG_DONTWAIT values and the recorded hello benchmark dependencies.

`oob_http_probe.rs` extends this to a real Hyper 1.12.0 HTTP/1 server on pinned Tokio 1.54.1. `diagnostics/hyper-oob-header-deadline.log` records a successful ordinary request, a complete request with urgent data stalled for the 400 ms observation window with the header deadline disabled, and connection closure with Hyper `HeaderTimeout` when a 100 ms header deadline and Tokio timer are configured. This is a controlled Linux loopback test, not a deployment exposure assessment.

The experimental scheduler worktree was restored to its clean v0.1.2 base. Original scheduler pending-file hashes remain unchanged. No experimental scheduler or HTTP implementation is integrated into the main development directories.
