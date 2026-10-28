# HTTP/1 hot-path reductions

Measured 2026-10-27 on macOS arm64 / Apple M4 Pro. Four changes following the [readiness work](readiness.md) reduce Native user CPU and eliminate one allocation per ordinary empty server request. A fresh error-free Base/Hyper repeat measures **124,812 versus 126,935 req/s: Native is 1.7% behind**. Hyper leads in every paired round. The earlier six-variant cohort showed a 0.8% gap; these results support close performance on this workload, with a small Hyper lead. They do not demonstrate a Native win or exact parity.

## Implementation

The bound readiness writer attempts an immediately writable borrowed scalar/vector send without a registration-table lookup. After WouldBlock, it retains the registration across operations. It retries across lazy registration and clears only the readiness generation observed before the send, preserving concurrent notifications. Scheduler admission, cancellation, standalone-selector fallback and independently progressing owned writes retain their contracts. The [implementation](../../../base-scheduler/crates/selector/src/readiness_socket_stream_writer.rs) and [tests](../../../base-scheduler/crates/selector/src/readiness_socket_stream_writer/tests.rs) include actual TCP send-buffer saturation and worker-driven resumption.

HTTP/1 request field construction gathers Host, Content-Length, Transfer-Encoding, Connection, Expect and Upgrade facts in the same walk that constructs ordered fields. Fragmented terminator searches resume with three bytes of overlap. Strict CRLF, duplicate singleton rejection, framing checks and field validation remain. Response planning gathers its framing/connection facts in one walk, and the common HTTP/1.1 200 status line uses a literal with the existing bounded-output fallback. For a message with multiple independent defects, the first reported validation error can change; accepted/rejected inputs retain the same policy.

Immutable exchange facts are copied. Ordinary fixed-zero server requests omit the shared allocation and completion atomics. CONNECT, offered upgrades, nonempty input and all chunked input retain shared completion/switch/abandonment state. Client response state remains shared even when its outgoing request is empty: an empty GET can still have a live streaming response. These distinctions preserve the [duplex contract](../../design/duplex.md), including early final responses, independent upload/download, abandonment and reuse only after joined completion.

The body API and EOF read remain unchanged. `ReadableBody::is_empty` does not guarantee that subsequent reads cannot fail, so it cannot safely replace terminal polling or `finish`.

## Work and CPU evidence

The in-memory, warmed codec probe falls from **3 allocations / 55 bytes to 2 allocations / 15 bytes per request**, retaining one transport write per response. Path and authority are the remaining allocations. This excludes networking/runtime allocation and is not a whole-stack allocation comparison against Hyper. [Current allocation probe](results/http-hot-path/response/allocations.json).

A separate interposer diagnostic with exactly 10,000 sequential validated requests counts 10,000 successful receives, zero WouldBlock receives and 10,000 successful writes for Native and Hyper. The unchanged Base/Tokio binding still incurs roughly one WouldBlock receive per request. These diagnostics establish syscall counts, not throughput. [Socket counts](results/http-hot-path/socket-counts-sequential.json).

Five interleaved eight-second samples per variant, four wrk threads, 64 persistent connections, a fresh one-worker server and one-second warmup per sample:

| Variant | Median req/s | User CPU µs/request | Kernel CPU µs/request |
| --- | ---: | ---: | ---: |
| Saved readiness baseline | 127,503 | 1.015 | 6.798 |
| Writer change | 127,130 | 0.977 | 6.853 |
| Plus request facts | 125,618 | 0.960 | 6.963 |
| Plus inline empty exchange | 126,362 | 0.958 | 6.929 |
| Plus response planning | 127,778 | 0.939 | 6.851 |
| Hyper/Tokio | 128,838 | 0.716 | 7.017 |

Combined user CPU/request falls **7.5% by medians**, and falls 7.5–9.0% in all five paired rounds. Throughput changes only +0.2% by medians, while host variation is several percent. The individual inline-exchange CPU effect is too small to establish here; its allocation reduction is deterministic. The final Native deficit against Hyper is 0.8% in this cohort. No sample in this completed cohort reports a socket or status error.

CPU figures are process-accounting deltas per completed request, not function samples. Most server CPU remains in the kernel; reducing user work alone does not imply a comparable throughput gain. This cohort cannot establish a causal narrowing from the prior readiness cohort's 2.6% gap. All variants use the same static 13-byte response, TCP_NODELAY, automatic Date disabled and the same release profile (`debug=1`). Builds, tests and profilers finish before throughput measurement. Normal host activity and CPU placement remain uncontrolled. [Samples](results/http-hot-path/comparison/samples.jsonl), [summary](results/http-hot-path/comparison/summary.json), [environment](results/http-hot-path/comparison/environment.json).

## Requested repeat

After the diagnostics, the user's requested fresh comparison used the original wrk, five interleaved eight-second samples per server, the same 64 persistent connections and four load threads, and one-second warmups. No builds, tests, profilers or resource monitors overlapped. All ten samples completed with zero reported socket or status errors.

| Variant | Median req/s | Min–max req/s | p99 | User CPU µs/request | Kernel CPU µs/request |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native current | 124,812 | 123,903–125,877 | 0.714 ms | 0.933 | 7.051 |
| Hyper/Tokio | 126,935 | 125,910–127,724 | 0.701 ms | 0.717 | 7.132 |

Native's deficit is **1.7% by medians**, ranging from 0.9% to 2.3% in paired rounds. Hyper leads in all five rounds. The gap remains small, but this repeat gives clearer evidence of a Hyper lead than the initial cohort. Absolute throughput drifted between cohorts; the comparison is within this fresh interleaved run. The timeout investigation remains unresolved even though this repeat completed cleanly.

[Samples](results/http-hot-path/parity-rerun/samples.jsonl), [summary](results/http-hot-path/parity-rerun/summary.json), [environment and binary/source hashes](results/http-hot-path/parity-rerun/environment.json), [paired assessment](results/http-hot-path/parity-rerun/assessment.json).

## Reliability investigation

Two focused before/current/Hyper cohorts aborted when the saved baseline reported one wrk timeout, after 1,008,660 and 1,017,476 completed requests respectively. A third fresh cohort aborted on the same counter in Hyper/Tokio after 959,879 requests. All three reported zero connect/read/write errors. Their full output and partial journals are preserved and excluded from aggregate comparisons; successful partial samples are not selected into a replacement cohort. Peer-reset messages in server stderr also occur during ordinary timed shutdown and do not establish the cause. [First failed cohort](results/http-hot-path/confirmation/), [retry](results/http-hot-path/confirmation-retry/), [first failure](results/http-hot-path/confirmation-failure.json), [retry failure](results/http-hot-path/confirmation-retry-failure.json), [third cohort](results/http-hot-path/confirmation-final/), [third failure](results/http-hot-path/confirmation-final-failure.json).

Socket exhaustion is a hypothesis raised by the user. The investigation also checks wrk's accounting: its completed-response callback increments the timeout counter when latency exceeds the histogram's two-second limit. Its `time_us` uses `gettimeofday`, so a backward wall-clock step can also produce an unsigned overflow. An isolated diagnostic records signed wall elapsed and monotonic elapsed on rejected samples. This is a possible mechanism, not an established explanation of the failures. [Diagnostic patch](results/http-hot-path/diagnostic-wrk.patch), [build provenance](results/http-hot-path/diagnostic-wrk.json).

The instrumented baseline completed **12 runs / 11,619,499 measured requests** with zero reported errors and no logged latency overflow. Across 28 intermittent resource observations, TCP entries peaked at 472, TIME_WAIT at 91, distinct local TCP ports at 340 and benchmark descriptors at 71. Inherited descriptor limits were 4,096; the ephemeral range was 49,152–65,535. System open files ranged from 23,282 to 23,319 against a 368,640 limit. The cumulative network-memory denial counter stayed at six. These observations show substantial headroom during reproduction but cannot establish or exclude a transient event in the earlier failures. Neither socket exhaustion nor a clock artifact was reproduced. [Investigation summary](results/http-hot-path/socket-investigation.json), [resource observations](results/http-hot-path/timeout-diagnostic-resources.jsonl), [diagnostic samples](results/http-hot-path/timeout-diagnostic/samples.jsonl).

The first instrumented build reused stale C objects after changing a connection struct. That attempt caused connection churn and is retained with an explicit invalidation; it contributes no evidence to the conclusions above. All application objects were rebuilt, and its sockets drained before the valid diagnostic run. Diagnostic instrumentation/resource monitoring timings are excluded from throughput comparisons.

The third comparison used the original wrk without resource monitoring and failed in Hyper/Tokio. This weakens a Base-specific regression hypothesis but does not establish the cause. A lower-overhead diagnostic then logged signed wall elapsed only on rejected samples, without extra per-request clock reads or connection-layout changes. All 15 samples (five per variant, 14,431,239 measured requests) completed without errors or logged overflows. These timings are retained as diagnostic data and excluded from throughput conclusions. The cause of the three original-wrk failures remains unestablished. [Minimal diagnostic patch](results/http-hot-path/outlier-wrk.patch), [diagnostic summary](results/http-hot-path/outlier-diagnostic/diagnostic-summary.json).

## Validation and reproducibility

All **124 HTTP tests, including documentation tests**, pass on Rust 1.85.1: 56 base-http, 40 base-http1 and 28 base-http-server, including all five TCP duplex proxy tests. Scheduler all-target/all-feature host coverage passes at **28,966/28,966 measured source regions**, with the same three invariant-only unreachable exclusions. The final real-backpressure test also passes independently. Scheduler workspace and HTTP Clippy pass with warnings denied; formatting and the Native/Selector/Tokio Rust 1.85.1 checks pass. Independent implementation/duplex reviews found no unresolved semantic defect; performance conclusions remain limited to the measurements above.

The [artifact directory](results/http-hot-path/) contains validation logs, stage binaries/source snapshots, hashes, manifests/locks and runner sources. The [isolated patch](results/http-hot-path/hot-path-change.patch) separates this task's Rust changes from preexisting scheduler edits. [Provenance](results/http-hot-path/provenance.json), [binary hashes](results/http-hot-path/binary-hashes.json). HTTP projects have no Git baseline here; preserved source snapshots supply the explicit comparison base.

From this benchmark directory, preserve the old binary, build the new binary, and adjust the variant paths for the local checkout:

```sh
cargo build --release --locked --features profile-tools --bins
python3 compare_candidates.py --wrk /absolute/path/to/wrk \
  --variants results/http-hot-path/parity-variants.json \
  --output results/new-hot-path-comparison --repetitions 5 --duration 8
```

These results cover plaintext persistent small-response HTTP/1 on one macOS application worker. Linux/io_uring, connection churn, TLS, large/streaming bodies and multi-worker scaling require separate measurements.

The [subsequent future-ownership investigation](remaining-gap.md) removes wrapper transfers, corrects benchmark handler ownership and preserves further matched measurements. The 1.7% result above remains the historical cohort, not the latest measurement.
