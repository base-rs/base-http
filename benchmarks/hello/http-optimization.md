# Integrated HTTP/1 batching and allocation reductions

The [subsequent cost-reduction report](hyper-target.md) records the next production changes and fresh measurements. This report preserves the earlier integration cohort.

Measured 2026-10-26 on the Apple M4 Pro/macOS arm64 host. The production HTTP code now includes bounded output batching and the general allocation improvements from the earlier profiling experiments. Native Base improves from **66,544 to 112,818 requests/s (+69.5%)** with the same finished worker-owned kqueue scheduler. Hyper/Tokio reaches **129,521 requests/s**: native Base remains **12.9% below Hyper** on this workload.

## Production changes

- `base-http` keeps empty/single-chunk `BufferedBody` storage inline, preserving its public slice view, replay cursor, length and append semantics. The benchmark continues using the ordinary generic handler and `BufferedBody<&'static [u8]>`.
- `base-http1` shares the immutable request framing snapshot and atomic completion/switch state in one `Arc<Context>`. Application heads remain independently mutable. Cloning an exchange no longer duplicates its fields and target.
- Each input lane reuses its initialized receive scratch region (default 16 KiB) across reads. Safe uninitialized stack space handles up to 100 parser fields; larger configured limits use a heap fallback. Both request and response parsers enforce the exact configured count. Common method constants avoid allocating method strings while extension tokens remain case-sensitive.
- Response planning builds initial wire fields once. The writer encodes status lines, fields and trailers incrementally into connection-local output storage, with at most 16 KiB of copied pending data. Larger slices are sent directly. Small static headers and body share one transport write.
- Before awaiting a body read that actually returns `Pending`, the writer flushes pending output and retains the same read future. The readiness predicate remains a batching hint. Explicit interim, tunnel-handshake and client-request-head flushes retain their prior behavior. Short writes retry in order; errors, cancellation or abandonment poison reuse, and close discards pending bytes without replaying them.

The scheduler release implementation and borrowed scalar socket-write path are unchanged during this integration. The diagnostic `writev` replacement and specialized fixed-200 serializer remain experiments. Generic body dispatch, protocol validation, trailer eligibility, length checks and independently driven input/output lanes remain in place.

The initialized receive scratch region is now retained for the lifetime of each reader, in addition to parser read-ahead storage. Output storage starts empty and grows only as needed, up to the batching bound. Receive leases, owned output submissions and vectored output remain future work.

## Matched comparison

Five interleaved eight-second samples per variant, each with a fresh process and one-second warmup; 64 persistent HTTP/1.1 connections, four wrk load threads, one application worker, loopback TCP, TCP_NODELAY, no TLS/pipelining. Both servers construct a 13-byte `Hello, world!` response per request, with matching Content-Type/Content-Length and automatic Date disabled. No compilation, tests, profiler or other controlled load runs overlapped timing. Normal host activity and CPU placement are uncontrolled.

Both before and after Base binaries use the finished local scheduler. The saved before binary matches the SHA-256 from the [original HTTP / finished scheduler cohort](results/local-scheduler/final/environment.json). Source comparison finds no scheduler release-code change; the sole intervening scheduler difference is the previously documented test correction. See [provenance](results/http-integration/provenance.json).

| Variant | Median requests/s | Min–max | Median p99 | Process CPU |
| --- | ---: | ---: | ---: | ---: |
| Native Base before HTTP changes | 66,544 | 63,313–67,738 | 1.300 ms | 99.6% |
| Native Base after HTTP changes | 112,818 | 101,210–114,294 | 0.773 ms | 99.5% |
| Base/Tokio before HTTP changes | 66,142 | 65,149–71,436 | 1.250 ms | 99.6% |
| Base/Tokio after HTTP changes | 120,618 | 111,176–120,927 | 0.750 ms | 99.5% |
| Hyper/Tokio | 129,521 | 119,090–131,509 | 0.694 ms | 99.5% |

Native improves 69.5% and Base/Tokio 82.4% by cohort medians. Comparing native samples within each repetition gives gains of 59.9–70.4%. The remaining native gap to Base/Tokio is 6.5%; Base/Tokio remains 6.9% below Hyper. Ranges overlap between the optimized variants, and these measurements do not establish an exact general-purpose ranking or multi-worker scaling result. p99 is a median of per-run percentiles, not a pooled percentile.

All **25 timed samples / 19,761,743 timed requests** report zero wrk socket/timeout or non-success status errors. Before each sample an independent Python client validates response bytes, fields, connection reuse, and explicit closure. Wrk does not validate body bytes during timing. Server stderr is retained, including peer-reset errors consistent with load-generator teardown; those are not counted as extra successful requests.

[Summary](results/http-integration/comparison/summary.json), [sample journal](results/http-integration/comparison/samples.jsonl), [environment and source/binary hashes](results/http-integration/comparison/environment.json), [reviewable HTTP patch](results/http-integration/http.patch), and before/after source snapshots and binaries are retained under `results/http-integration`.

## Allocation and CPU checks

The warmed in-memory codec probe processes 10,000 requests using the same semantic request/response construction as the server, excluding socket/runtime allocation:

| Probe | Before | After |
| --- | ---: | ---: |
| Allocations/request | 46 | 21 |
| Allocated bytes/request | 20,908 | 1,080 |
| Transport writes/static response | 2 | 1 |

See the saved [before](results/http-integration/before/allocations.json) and [after](results/http-integration/after/allocations.json) measurements. The reduction is 54.3% in allocation count and 94.8% in allocated bytes per request. Per-connection storage is warmed before counting.

A separate short, uninstrumented process-CPU accounting check (one five-second run per backend after two-second warmup) records:

| Backend | User CPU µs/request | Kernel CPU µs/request |
| --- | ---: | ---: |
| Native Base | 1.618 | 8.011 |
| Base/Tokio | 1.386 | 7.118 |
| Hyper/Tokio | 0.696 | 6.775 |

These sequential, unreplicated [accounting captures](results/http-integration/cpu-accounting/) are supporting diagnostics, not substitutes for the interleaved throughput cohort or a function-level CPU profile. They show remaining work in both user code and the native I/O path, but do not attribute the difference to particular functions or syscalls. Further isolation should examine the remaining request-head snapshot/field copies and serialization alongside the scheduler's borrowed/vectored output paths. This integration does not establish that Base meets or exceeds Hyper.

## Validation

All **115 tests including documentation tests pass on Rust 1.85.1**: 55 in `base-http`, 32 in `base-http1`, and 28 in `base-http-server`. The new regressions exercise single-write static responses, inaccurate readiness without restarting reads, bounded batches before EOF, large fields/chunks/trailers across batch boundaries, partial write failure without replay, parser limits below/at/above stack capacity, inline body storage transitions, and a real TCP streaming echo whose request pauses inside a chunk delimiter. Existing tests retain interim sequencing, managed upgrades/CONNECT, independent full-duplex proxy progress, backpressure, cancellation and retirement coverage.

Formatting and Clippy with warnings denied pass for all three HTTP packages and the benchmark; the new Python comparison script passes syntax compilation. The [validation record](results/http-integration/validation.json) lists the checks.

Linux/io_uring, TLS, large-body throughput, connection churn, multi-worker scaling and production tail latency are outside this measurement. The TCP semantic tests exercise streaming and proxy correctness; they are not streaming throughput benchmarks.

## Reproduce

From this benchmark directory, build first, then run throughput and optional CPU accounting separately:

```sh
cargo build --release --locked --features profile-tools --bins
python3 compare_http.py --wrk /absolute/path/to/wrk \
  --before-binary results/http-integration/before/base-http-hello-benchmark \
  --binary target/release/base-http-hello-benchmark \
  --output results/http-recheck --repetitions 5 --duration 8
target/release/allocation-probe
python3 profile.py --wrk /absolute/path/to/wrk \
  --binary target/release/base-http-hello-benchmark \
  --output results/http-recheck-cpu --backends base-native base-tokio hyper-tokio \
  --no-sample --seconds 0
```

The saved binaries are macOS arm64 artifacts. To reproduce elsewhere, build both versions on the target host using the preserved before sources/patch and the same scheduler, compiler, manifests and lockfiles. Historical experimental patch scripts expect the original source layout; use their preserved staged trees rather than applying them to the integrated code.
