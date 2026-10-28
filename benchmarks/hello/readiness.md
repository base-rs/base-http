# Readiness before scalar stream receives

Measured 2026-10-26 on macOS arm64 / Apple M4 Pro. The local Native scheduler now avoids the redundant WouldBlock receive identified in the [code-path audit](code-paths.md). A focused confirmation improves Native throughput **3.7%** and reduces its deficit against Hyper/Tokio from **6.1% to 2.6%**. Hyper remains faster in every confirmation round.

The subsequent [HTTP hot-path work](http-hot-path.md) reduces writer lookup, repeated field scans and empty-exchange allocation. Its measurements and reliability investigation are recorded separately.

## Implementation and correctness

The bound scalar stream reader retains its readiness registration. After successful stream progress, a subsequent read waits before calling `recv` when the recorded readiness is clear. Initial reads still attempt the syscall, and lazy registration retries across the registration boundary, preserving immediate OS errors and progress races.

On macOS, clearing readiness after a positive short read requires consuming at least the readable-byte count reported by kqueue. A short read alone is insufficient: TCP urgent-data boundaries can leave ordinary bytes queued, including after an alias consumes the urgent byte. The byte-count check preserves readiness in that case. The byte hint is not decremented across full reads; those cases conservatively retain readiness until WouldBlock or a later usable event.

Readable EOF remains ready through the final data read. Clears compare against the generation captured before the syscall, preserving concurrent notifications. Publication stores the byte hint before advancing that generation. Cancellation, scheduler closure, borrowed-buffer lifetime and receive-pool behavior retain their existing contracts.

The byte-count/EOF policy applies to worker-owned macOS kqueue scalar `read_into` operations. Other readiness backends keep readiness until WouldBlock, and message/vector/owned receives keep their existing behavior. The Tokio adapter is unchanged: its `AsyncFd` interface does not expose kqueue's readable-byte count. No public scheduler or HTTP API changed.

Sources: [bound reader](../../../base-scheduler/crates/selector/src/readiness_socket_stream_reader.rs), [direction state](../../../base-scheduler/crates/selector/src/readiness_driver.rs), [kqueue events](../../../base-scheduler/crates/selector/src/readiness_driver/kqueue.rs), [regression tests](../../../base-scheduler/crates/selector/src/readiness_socket_stream_reader/tests.rs). The [isolated change patch](results/readiness/readiness-change.patch) distinguishes this work from the scheduler's preexisting local changes.

## Receive-call evidence

For exactly 10,000 sequential GETs after warmup, with every response status and body validated:

| Variant | Successful receives | WouldBlock receives | Successful writes |
| --- | ---: | ---: | ---: |
| Native before | 10,000 | 10,000 | 10,000 |
| Native after | 10,000 | **0** | 10,000 |
| Base/Tokio, unchanged | 10,000 | 10,000 | 10,000 |
| Hyper/Tokio | 10,000 | 0 | 10,000 |

With four wrk threads and 64 persistent connections, Native after completed 341,726 requests with 341,787 successful receives and **127 WouldBlock receives**, versus roughly one WouldBlock per request before. Hyper had 16 WouldBlock receives over 361,254 completed requests. Startup/connection turnover and shutdown can still require probes. Timed shutdown includes additional in-flight requests and peer resets; wrk reported no socket or status errors. These instrumented runs establish call counts, not throughput.

[Before diagnostic](diagnostics/socket-counts-sequential.json), [after sequential](results/readiness/socket-counts-sequential.json), [after wrk](results/readiness/socket-counts-wrk.json).

## Uninstrumented performance

Five interleaved eight-second samples per variant, fresh processes, one-second warmup, four wrk threads, 64 persistent connections and one application worker. Both servers construct the same 13-byte response per request, with TCP_NODELAY enabled and automatic Date disabled. Builds use the same default release settings (`debug=1`); compilation, tests and instrumented diagnostics finish before timing.

The first four-variant cohort measured 122,580 req/s before, 126,548 after, 123,427 for unchanged Base/Tokio and 129,518 for Hyper. That is a 3.2% Native gain by medians, but its first round had substantial host drift: Native after ranged from 111,015 to 129,038 req/s. All samples are retained. A focused three-variant confirmation produced:

| Variant | Median req/s | Min–max req/s | p99 | User CPU µs/request | Kernel CPU µs/request |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native before | 123,401 | 121,886–123,836 | 0.740 ms | 1.098 | 6.961 |
| Native after | **127,944** | 126,945–128,584 | 0.715 ms | 1.018 | 6.773 |
| Hyper/Tokio | 131,413 | 130,012–132,933 | 0.697 ms | 0.689 | 6.888 |

Native improves in all five confirmation rounds: paired gains range from 2.5% to 5.0%, with a median of 3.6%. Comparing overall medians gives **3.7%**. User CPU/request falls 7.3%, and kernel CPU/request falls 2.7%. Native remains 1.1–3.6% below Hyper by paired rounds, with a median deficit of 2.6%. No timed sample reported an error.

These are process-accounting CPU deltas per completed request, not function-level samples. Host background activity and CPU placement remain uncontrolled. The remaining median user CPU difference is about 0.328 µs/request; Native's median kernel CPU is now slightly lower than Hyper's. This supports investigating the remaining parsing, exchange-state and response work in the audit, but does not assign their individual costs. Linux/io_uring, TLS, connection churn, streaming and multi-worker scaling remain unmeasured.

[Initial samples](results/readiness/comparison/samples.jsonl), [summary](results/readiness/comparison/summary.json), [environment](results/readiness/comparison/environment.json); [confirmation samples](results/readiness/confirmation/samples.jsonl), [summary](results/readiness/confirmation/summary.json), [environment](results/readiness/confirmation/environment.json).

## Validation and reproduction

The scheduler workspace passes its all-target/all-feature coverage gate on macOS: **28,916/28,916 measured source regions**, with the same three invariant-only unreachable exclusions. Five new tests cover waiting without a syscall, full-read fallback, registration reuse, concurrent event publication, EOF, urgent-data boundaries, cancellation, closure and immediate errors. All **119 HTTP tests including documentation tests** pass on Rust 1.85.1. Scheduler workspace Clippy with warnings denied, formatting and Native/Selector/Tokio Rust 1.85.1 checks pass.

[Validation logs, source snapshots, binaries and hashes](results/readiness/), [provenance](results/readiness/provenance.json). The saved before binary is exactly the previous compact-context baseline. Rebuild platform-specific binaries elsewhere, and update the variant paths before running:

```sh
cargo build --release --locked --features profile-tools --bins
python3 diagnostics/count_socket_calls.py \
  --binary results/readiness/after-server \
  --library diagnostics/socket_counts.dylib \
  --output results/readiness/new-socket-counts.json
python3 compare_candidates.py --wrk /absolute/path/to/wrk \
  --variants results/readiness/confirmation-variants.json \
  --output results/new-readiness-comparison --repetitions 5 --duration 8
```
