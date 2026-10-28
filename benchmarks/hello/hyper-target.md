# Further HTTP/1 cost reductions

Measured 2026-10-26 on macOS arm64 / Apple M4 Pro, using production HTTP code and the local scheduler. This follows the [bounded output integration](http-optimization.md). **Base has not demonstrated a repeatable win over Hyper.**

The later [HTTP hot-path report](http-hot-path.md) measures writer lookup removal, fused protocol facts, ordinary empty-exchange allocation removal and response planning. Its baselines and results are separate from this earlier work.

## Removing unnecessary ownership

The first `Exchange` retained an immutable `RequestHead` to isolate framing from application mutations. Sharing that snapshot made exchange clones cheap, but constructing it still copied the path, authority and fields. This was an implementation shortcut.

Hyper 1.12.0, the version in the benchmark lockfile, derives decoding length, keep-alive, Expect and upgrade state while walking fields, retains the request method, then passes the head to the service (`src/proto/h1/role.rs`, `Server::parse`; connection state is in `src/proto/h1/conn.rs`). It needs no second complete request head for framing.

Base now retains only version, HEAD/CONNECT classification, persistence, Expect/Continue, upgrade offers and atomic completion/switch/abandonment state. Only upgrade offers copy field bytes, when present. The application owns the complete request head. Its mutations cannot alter established framing or authorize an unoffered upgrade. `Exchange::head()` is replaced by framing accessors such as `version()`, `is_head()` and `is_connect()`; application-facing `RequestHead` members remain unchanged.

## Other production changes

- Response planning borrows the semantic head and describes generated fields separately, avoiding a cloned collection, temporary length strings and trailer-name joins. Generated fields do not mutate the application head.
- Reads fill initialized parser storage directly; head parsing borrows that storage. Successful counts alone publish received bytes. Returned body chunks still own their data.
- Names/values up to 24 bytes and the first pair have inline storage, with heap fallback. Lazy index and policy tables are allocated separately when used. Name arguments now accept `AsRef<str>` rather than `Into<String>`.
- Small status lines and fields append synchronously when they fit the existing bounded buffer. Cancellation waits skip listener work for `Cancellation::never()`.
- Native readiness observes an existing registration's generation before the syscall, eliminating a duplicate WouldBlock retry. Initial registration still retries across its boundary. Worker-owned direction waits already receive closure notifications, so they avoid a second shutdown listener.

The 16 KiB output bound, suspension flush, short-write retries, poisoning, explicit handshake flushes and scalar socket-write contract remain. No encoded response is cached. Single-IoSlice sendmsg and macOS bound scalar writev screens showed no useful gain and were reverted. A thin-LTO build remains diagnostic; final builds use the same default release settings (`debug=1`) for Base and Hyper.

## Allocation evidence

The warmed codec probe processes 10,000 short-field requests, excluding networking/runtime allocation:

| Stage | Allocations/request | Allocated bytes/request | Writes/response |
| --- | ---: | ---: | ---: |
| Starting baseline | 21 | 1,080 | 1 |
| Borrowed planning/head parsing | 14 | 808 | 1 |
| Inline fields and smaller policy/index storage | 5 | 294 | 1 |
| Compact exchange context | 3 | 55 | 1 |

The final allocations are the request path, authority and shared exchange context. Counts apply to this specific probe. Allocation count falls 85.7% and allocated bytes fall 94.9%. [Before](results/hyper-target/before/allocations.json), [intermediate](results/hyper-target/final/allocations.json), [compact context](results/hyper-target/compact-exchange/allocations.json).

## Matched comparison before the compact context

Five interleaved eight-second samples per variant, fresh processes, one-second warmup, 64 persistent connections, four wrk threads and one application worker. Both servers construct equivalent 13-byte responses per request, with TCP_NODELAY enabled and automatic Date disabled. Builds, tests and profiling finish before timing. Host background activity and CPU placement remain uncontrolled.

| Variant | Median req/s | p99 | User CPU µs/request | Kernel CPU µs/request |
| --- | ---: | ---: | ---: | ---: |
| Starting native Base | 112,810 | 0.774 ms | 1.689 | 7.135 |
| Native Base, full-head snapshot | 120,170 | 0.740 ms | 1.194 | 7.099 |
| Base/Tokio, full-head snapshot | 124,997 | 0.734 ms | 1.086 | 6.872 |
| Hyper/Tokio | 129,955 | 0.694 ms | 0.687 | 6.987 |

Native improves 6.5% in throughput and reduces user CPU/request 29.3%, remaining 7.5% below Hyper in this cohort. CPU columns are medians of process accounting deltas per timed request, rather than function-level samples. [Samples](results/hyper-target/final/comparison/samples.jsonl), [summary](results/hyper-target/final/comparison/summary.json), [environment](results/hyper-target/final/comparison/environment.json), [provenance](results/hyper-target/final/provenance.json). The saved baseline exactly matches the preceding integration report's after binary.

## Compact-context measurement

The next five-variant cohort compared the starting native binary, full-head snapshot, compact context on Native/Tokio and Hyper. User CPU/request fell from 1.201 µs with the snapshot to 1.127 µs with the compact context. Throughput varied substantially as the host slowed in later repetitions: compact native ranged from 106,271 to 123,979 req/s, and Hyper from 115,746 to 129,839. The snapshot and compact-context throughput ranges overlap; this cohort alone does not establish a throughput gain from removing the snapshot.

Hyper was faster in every same-repetition comparison. The compact native deficit ranged from 3.2% to 8.5%, with a median paired deficit of 4.6%. This differs from comparing overall medians during host drift. [Samples](results/hyper-target/compact-exchange/comparison/samples.jsonl), [summary](results/hyper-target/compact-exchange/comparison/summary.json), [environment](results/hyper-target/compact-exchange/comparison/environment.json).

A focused confirmation used the same settings with only the snapshot, compact native context and Hyper:

| Variant | Median req/s | Min–max req/s | p99 | User CPU µs/request | Kernel CPU µs/request |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native with full-head snapshot | 120,340 | 117,965–122,371 | 0.741 ms | 1.194 | 7.076 |
| Native with compact context | 121,074 | 120,024–123,746 | 0.735 ms | 1.106 | 7.127 |
| Hyper/Tokio | 128,868 | 124,988–130,366 | 0.698 ms | 0.693 | 7.060 |

The compact context lowers user CPU/request 7.4%; throughput rises 0.6% by medians, with overlapping ranges. It remains 6.0% below Hyper. The remaining median user CPU difference is about 0.413 µs/request, versus about 0.066 µs/request in kernel CPU. These component medians are explanatory rather than an exact decomposition of the throughput ratio. [Confirmation samples](results/hyper-target/compact-exchange/confirmation/samples.jsonl), [summary](results/hyper-target/compact-exchange/confirmation/summary.json), [environment](results/hyper-target/compact-exchange/confirmation/environment.json).

The subsequent [code-path audit and socket-call diagnostic](code-paths.md) found an additional transport difference: both Base/Native and Base/Tokio performed about one extra WouldBlock receive per request. Hyper/Tokio usually avoids it by clearing readiness after a short successful stream read and checking readiness before another receive. Therefore the Base/Tokio control does not isolate this cost or establish that most remaining work is above the scheduler binding. Separate framing/connection field scans, empty-body exchange sharing, body completion and registration lookup also differ; exact per-function costs remain unmeasured. Wall-clock stack samples include blocked syscall time and must not be called on-CPU percentages.

The [Native readiness fix](readiness.md) removes the ordinary extra receive while preserving urgent-data and EOF behavior. A fresh matched confirmation improves Native from 123,401 to 127,944 req/s (3.7%), versus Hyper's 131,413 req/s: the deficit falls from 6.1% to 2.6%. The Tokio adapter keeps its conservative receive policy. The linked report contains current correctness evidence, diagnostic counts and both measurement cohorts.

## Validation and reproduction

All **119 HTTP tests including documentation tests** pass on Rust 1.85.1: 56 semantic/body, 35 HTTP/1 and 28 server/proxy tests. New tests cover inline/spilled storage, independent indexed mutation, maximum generated lengths, unchanged semantic heads and application mutations preserving original HEAD/CONNECT/persistence and upgrade authorization. Existing duplex, interim, trailer, backpressure, cancellation and retirement tests pass.

Scheduler workspace tests pass with **28,796/28,796 measured source regions** covered on macOS, with the same three invariant-only unreachable exclusions. Regressions cover one attempt before waiting, readiness published during a syscall and concurrent registration sharing one record. Workspace Clippy, formatting and Native/Selector Rust 1.85.1 checks pass. HTTP/benchmark Clippy and formatting pass. [Scheduler validation logs](results/hyper-target/final/) and [compact-context validation logs](results/hyper-target/compact-exchange/) are retained.

```sh
cargo build --release --locked --features profile-tools --bins
target/release/allocation-probe
python3 compare_candidates.py --wrk /absolute/path/to/wrk \
  --variants results/hyper-target/compact-exchange/variants.json \
  --output results/new-comparison --repetitions 5 --duration 8
```

Variant files reference preserved macOS arm64 binaries; rebuild the snapshots elsewhere. [HTTP sources/patch](results/hyper-target/compact-exchange/), [scheduler patch/untracked sources](results/hyper-target/final/), runner copies, manifests, locks and hashes are retained. The profile summarizer's indentation fix avoids incorrect negative exclusive counts. Linux/io_uring, TLS, connection churn, multi-worker scaling and streaming throughput remain unmeasured.
