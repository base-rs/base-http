# TLS progress and client overhead — 28 October 2026

The direct pooled HTTP/1 client now matches Hyper's single-connection throughput for both HTTP and HTTPS in this workload. Single-connection HTTPS improves **54%** over the preserved connection-owned-state baseline. The connection-only HTTPS path exceeds Hyper by **5.6%** at concurrency one. At concurrency 32 the pooled client remains **10.1% behind for HTTP and 7.5% behind for HTTPS**; we have not beaten Hyper across the board.

The useful distinction is where progress happens. Keeping state in retained futures works well. Requiring another future to poll them adds coordination, and reading ahead before returning available plaintext adds work to the response path. No scheduler or pool changes are needed for the retained implementation.

## Measured result

The final cohort has **72 interleaved samples**, three repetitions per scenario, each with 0.5 seconds of warmup and two seconds of measurement after finite connection preparation. Every response is parsed and its status and complete 11-byte payload are checked. All **8,788,408 measured responses** passed validation and exact server accounting, with zero server failures. Every sample accepted exactly one or 32 persistent connections, matching its concurrency.

Both clients use one worker and the same separate Hyper server, HTTP/1 without pipelining, and verified loopback TLS 1.3 with the same rustls/ring and self-signed identity. See the [harness method](readme.md#method) for task placement and configuration. The machine is a shared Apple M4 Pro Mac, Rust 1.87. These are saturation measurements, **not equal-request-rate P99 comparisons**, Linux measurements, or multiworker scaling results. CPU below is client process CPU per validated response; server CPU is recorded separately.

The baseline here is the executable from the [connection-owned-state investigation](connection-state-20261028.md), rerun in this cohort. Absolute numbers from older cohorts should not be compared directly because machine conditions differ.

### Pooled client paths

`base-open` takes codec directions in the caller and drives writes/response reads directly. `base-call` retains the existing queued pull-body path. `hyper` is Hyper's pooled client. `before-open` is the preserved baseline's direct client.

| Transport | Concurrency | Client | Median responses/s | Responses/s range | CPU µs/response |
| --- | ---: | --- | ---: | ---: | ---: |
| HTTP | 1 | `before-open` | 35,983 | 35,844–36,177 | 16.15 |
| HTTP | 1 | `base-open` | 35,930 | 35,710–36,089 | 15.85 |
| HTTP | 1 | `base-call` | 22,623 | 22,458–22,942 | 18.96 |
| HTTP | 1 | `hyper` | 35,541 | 35,088–35,727 | 14.15 |
| HTTP | 32 | `before-open` | 82,546 | 81,697–83,102 | 12.07 |
| HTTP | 32 | `base-open` | 85,009 | 84,643–85,018 | 11.72 |
| HTTP | 32 | `base-call` | 84,468 | 83,986–84,567 | 11.81 |
| HTTP | 32 | `hyper` | 94,521 | 93,698–95,202 | 10.54 |
| HTTPS | 1 | `before-open` | 21,876 | 21,779–21,996 | 20.36 |
| HTTPS | 1 | `base-open` | 33,714 | 33,300–33,745 | 16.96 |
| HTTPS | 1 | `base-call` | 21,854 | 21,752–21,902 | 20.20 |
| HTTPS | 1 | `hyper` | 33,270 | 32,962–33,630 | 15.56 |
| HTTPS | 32 | `before-open` | 79,353 | 78,639–79,440 | 12.57 |
| HTTPS | 32 | `base-open` | 82,233 | 81,329–82,248 | 12.11 |
| HTTPS | 32 | `base-call` | 77,114 | 75,925–77,252 | 12.92 |
| HTTPS | 32 | `hyper` | 88,854 | 88,586–89,201 | 11.22 |

Single-connection HTTPS moves from **21,876 to 33,714 responses/s**; Hyper reaches **33,270**. The final Base and Hyper ranges overlap, so treat that as throughput parity. Base still consumes more CPU (16.96 versus 15.56 µs/response). Concurrent HTTPS improves **3.6%** over the baseline and concurrent HTTP improves **3.0%**. The queued `call` path has not acquired all of the direct path's gains.

### Established connections without pooling

Each lane retains one codec/transport pair. Both implementations still perform full HTTP/TLS processing, and Hyper retains its connection driver. Connection setup and retirement are excluded. The application/task orchestration differs as described in the harness; this is not an isolated pool microbenchmark.

| Transport | Concurrency | Client | Median responses/s | Responses/s range | CPU µs/response |
| --- | ---: | --- | ---: | ---: | ---: |
| HTTP | 1 | `base-connection` | 41,014 | 40,258–41,374 | 11.83 |
| HTTP | 1 | `hyper-connection` | 40,688 | 40,389–41,313 | 11.85 |
| HTTP | 32 | `base-connection` | 103,393 | 103,092–104,117 | 9.64 |
| HTTP | 32 | `hyper-connection` | 102,444 | 102,418–102,543 | 9.73 |
| HTTPS | 1 | `base-connection` | 36,829 | 36,684–36,941 | 13.62 |
| HTTPS | 1 | `hyper-connection` | 34,888 | 34,863–34,993 | 14.03 |
| HTTPS | 32 | `base-connection` | 94,415 | 94,305–94,887 | 10.55 |
| HTTPS | 32 | `hyper-connection` | 97,137 | 96,590–97,304 | 10.26 |

Plain HTTP is effectively equal in these connection-only cases. Single-connection HTTPS is **36,829 versus 34,888 responses/s**, a 5.6% Base advantage with non-overlapping observed ranges and slightly lower CPU. Concurrent connection-only HTTPS remains **2.8% behind Hyper**.

## Retained implementation

1. **Poll retained TLS output directly.** `EncryptedSocketStreamWriter` polls the connection's pinned transmitter during writes and flushes. An ordinary successful write no longer requires notifying supervision and waiting for it to poll the transport. The same future retains partial ciphertext, socket capability and accepted/delivered ticket accounting across suspension. There is one boxed future per direction per connection, with no allocation per frame.

2. **Make TLS input follow application reads.** `EncryptedSocketStreamReader` first consumes authenticated plaintext. When empty, it polls the retained receiver; that future stops as soon as plaintext is available and resumes after consumption. It does not start another socket read before returning available data. Incoming TLS control records are processed on application reads. The enclosing future supervises generated control output and joins both socket lanes during retirement. Independent upload/download polling still supports full duplex.

3. **Keep wake ownership stable.** A private `IoProgress` serializes polls and caches completion. Its stable socket waker forwards wakes through separate foreground and supervised `AtomicWaker` slots, so changing the polling future cannot overwrite another live poller's wake target. Completion releases both registrations and never repolls the completed future. No mutex guard spans an asynchronous suspension.

4. **Reuse initialized output storage.** The TLS transmitter tracks valid ciphertext with separate position/end indexes. Small writes reuse initialized capacity instead of clearing and zeroing a 16 KiB buffer repeatedly. Transmit demand uses a dedicated `AtomicWaker`; the retained transmit cancellation waiter is reused. The input-side general notification events are removed.

5. **Reduce response-future movement.** HTTP/1 response parsing uses a synchronous helper for its scratch array of 100 `httparse` headers. That array no longer lives across an asynchronous interim-response callback. On Rust 1.87/aarch64 the response-read future shrank from **4,064 to 800 bytes**. Status/version/field validation, limits and interim sequencing are preserved. The [layout probe](results/probe-response-parser-scope-20261028/future_layout_probe.rs) lives with experimental results.

The changed production code is [endpoint duplex TLS](../../../base-endpoint/src/tls/rustls/rustls_connection/duplex.rs) and the [HTTP/1 client parser](../../crates/http1/src/client.rs). HTTP metadata/body semantics and the client API are unchanged. The endpoint's encrypted directional handles now carry their scheduler lifetime. Registry scheduler/pool **0.2.0** remain in use, with no local patches.

## What the profiles establish

Original diagnostic profiles showed substantial future movement, TLS buffer initialization and notification machinery. The final [diagnostic profiles](results/tls-demand-profiles-20261028/metadata.json) use the frozen final executable: six seconds of macOS `sample` at 1 ms, with concurrency 32. Their throughput is profiled and is not included in the benchmark tables.

The final HTTPS pooled Base profile has **149 leaf samples in `memmove`**, compared with **48 for pooled Hyper**; the connection-only profiles have **43 versus 11**. Call stacks locate part of Base's remaining movement in `writable::open`, alongside allocation, cancellation-child and pool/accounting work. These are statistical samples, not byte counts, allocation counts or syscall counters. Blocked reactor and cleanup threads also appear prominently in Base's all-thread sample, so their waiting stacks cannot be treated as CPU consumption.

The measured high-level-versus-connection CPU difference at concurrency 32 is approximately **2.08 µs/response for Base HTTP versus 0.82 for Hyper**, and **1.55 versus 0.96 for HTTPS**. Those differences include client API, task orchestration and pool/lifecycle work. They must not be labelled an isolated pool cost.

The next experiment should count and attribute per-exchange allocations and movement, then test retaining/reusing the boxed sink/response stream and completion scaffolding with the connection. Current candidates include completion/mode Arcs, the output receipt channel and cancellation-child setup. Their exact contribution is **not yet established**. Any reuse must preserve independent upload/download, late response ownership, abandonment and resource accounting. The remaining 2.8% concurrent connection-only TLS gap also deserves separate investigation; client scaffolding alone cannot explain it.

## Experiments removed

- Boxing codecs once per connection showed no consistent gain.
- Lazy client cancellation registration and conditional pending-response notifications showed no clear gain.
- Separating pool retirement notification and activating reservations in place made essentially no end-to-end difference. Pool source, manifest and lockfile were restored.
- Polling both TLS directions from both application and supervisor added work without a worthwhile gain.
- Polling input before the application improved the single-connection diagnostic but regressed concurrent throughput. That join-order change was reverted. The retained design instead polls receive on demand and stops at available plaintext, using the original application-first join order.

Individual [parser](results/probe-response-parser-scope-20261028/), [output-buffer](results/probe-tls-buffer-reuse-20261028/), [direct-output](results/probe-tls-direct-transmit-20261028/), [pool](results/probe-pool-retirement-20261028/) and [input-order](results/probe-tls-input-first-20261028/) probes retain raw evidence. Exploratory cohorts are not extra repetitions of the final result. Earlier reports describe earlier implementations; this report records the final combination.

## Correctness checks

Rust 1.87 validation passed **167 tests and five documentation tests**: endpoint 43 + four docs, client 39 integration tests, HTTP/1 codec 58 tests, and server/proxy 27 + one doc. Endpoint and client Clippy passed for all targets with rustls enabled and warnings denied; codec Clippy passed for all targets with warnings denied. Formatting passed for the changed packages.

The new endpoint integration regression establishes TLS, obtains directional handles, then freezes supervision. Application reads/writes complete a **256 KiB echo**, flush and incoming `close_notify` with both TLS 1.2 and TLS 1.3 against an independent rustls peer. Resuming supervision joins retirement. A separate wake regression checks distinct foreground/background wake delivery, release of both waker registrations and no repoll after completion. Existing tests cover backpressure, independent proxy upload/download, early final responses, interims/trailers, cancellation, abandonment, truncation and joined cleanup.

An existing abort test now accepts either `UnexpectedEof` or `ConnectionReset` from the peer: aborting a TCP connection with unread records can reset it. It still checks cancellation and transport closure.

## Reproduce and audit

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release --locked

python3 benchmarks/client/run.py \
  --output benchmarks/client/results/tls-demand-repeat \
  --modes reuse --repeats 3 --seconds 2 --warmup 0.5 --concurrency 1,32 \
  --implementations before-open,base-open,base-call,base-connection,hyper-connection,hyper \
  --baseline-binary /tmp/base-http-connection-state-20261028 \
  --baseline-sources benchmarks/client/results/connection-state-20261028/sources.tar.gz
```

Preserve baseline executables before rebuilding. `before-connection` is also available to compare the baseline's low-level connection API; it is used in exploratory probes, not this final 72-sample cohort.

Final [summary](results/tls-demand-io-20261028/summary.json), [machine/compiler/arguments](results/tls-demand-io-20261028/metadata.json), [source hashes](results/tls-demand-io-20261028/source-hashes.json), [measured sources](results/tls-demand-io-20261028/sources.tar.gz), and [baseline sources](results/tls-demand-io-20261028/baseline/sources.tar.gz) are retained. The archive contains production Rust sources, Cargo manifests/lockfile and harness files; integration tests and subsequently edited documentation are separately preserved in the [validation supplement](results/tls-demand-io-20261028/validation-supplement.tar.gz). All 66 measured source hashes matched the working files after profiling, before documentation-only harness readme edits.

Final executable: `/tmp/base-http-tls-demand-io-20261028`, SHA-256 `a04dc438fe8443dab9cf2e37e9e1aeb4fd2fe98bac6f2638740b3a5dde606faf`.

Baseline executable: `/tmp/base-http-connection-state-20261028`, SHA-256 `3313f2ca01177f9894a97af7664269d751a586e1518daf7db00a764b6d5c5308`.
