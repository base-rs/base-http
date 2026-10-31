# Connection-owned HTTP/1 state — 28 October 2026

The direct HTTP/1 client now takes its persistent codec directions from the connection in the calling task. Opening an established request no longer submits an operation to the client driver, and normal completion restores both directions synchronously. The public client/body APIs are unchanged.

The strongest measured gain is single-connection plaintext HTTP: about 44% higher throughput, reaching Hyper’s throughput in this run. Concurrent HTTPS improves about 11%. Concurrent plaintext shows no clear gain, and single-connection HTTPS changes little. This addresses one coordination boundary; it does not close every performance gap.

## Matched before/after results

Median validated responses/second, three repetitions per scenario. Each sample has one second of warmup and three seconds of measurement, after finite connection preparation. The preserved previous optimized executable and current clients are interleaved against the same independent Hyper server and verified self-signed TLS identity. Both use one client worker, persistent HTTP/1 connections, an empty GET and an 11-byte response. See the [harness method](readme.md#method) for exact TLS, pooling and scheduling configuration.

| Transport | Concurrency | Previous direct Base | Connection-owned Base | Change | Hyper pooled |
| --- | ---: | ---: | ---: | ---: | ---: |
| HTTP | 1 | 22,112 | 31,848 | +44.0% | 31,108 |
| HTTP | 32 | 99,409 | 97,168 | -2.3% | 119,348 |
| HTTPS | 1 | 20,769 | 20,953 | +0.9% | 29,939 |
| HTTPS | 32 | 87,769 | 97,347 | +10.9% | 110,987 |

The concurrent plaintext medians differ by −2.3%, with overlapping ranges and run-to-run variation: treat this as no demonstrated improvement. All samples accepted exactly one persistent connection per lane (1 or 32); connection churn does not explain the changes.

## CPU cost and ranges

| Transport | Concurrency | Client | CPU µs/request, median | Requests/s, median | Requests/s range |
| --- | ---: | --- | ---: | ---: | ---: |
| HTTP | 1 | `before-open` | 20.42 | 22,112 | 22,006–22,329 |
| HTTP | 1 | `base-open` | 17.59 | 31,848 | 31,641–31,866 |
| HTTP | 1 | `base-call` | 19.91 | 22,075 | 22,062–22,138 |
| HTTP | 1 | `hyper` | 16.01 | 31,108 | 30,993–31,571 |
| HTTP | 32 | `before-open` | 10.03 | 99,409 | 95,380–102,676 |
| HTTP | 32 | `base-open` | 10.27 | 97,168 | 95,326–104,401 |
| HTTP | 32 | `base-call` | 10.47 | 95,217 | 94,881–97,867 |
| HTTP | 32 | `hyper` | 8.36 | 119,348 | 116,977–120,618 |
| HTTPS | 1 | `before-open` | 22.90 | 20,769 | 20,569–20,824 |
| HTTPS | 1 | `base-open` | 21.87 | 20,953 | 20,866–21,026 |
| HTTPS | 1 | `base-call` | 22.83 | 16,580 | 16,577–16,583 |
| HTTPS | 1 | `hyper` | 17.53 | 29,939 | 29,751–30,328 |
| HTTPS | 32 | `before-open` | 11.32 | 87,769 | 86,403–89,784 |
| HTTPS | 32 | `base-open` | 10.25 | 97,347 | 94,232–97,575 |
| HTTPS | 32 | `base-call` | 13.50 | 68,075 | 68,066–68,172 |
| HTTPS | 32 | `hyper` | 8.99 | 110,987 | 108,504–111,909 |

## Ownership and execution

- A connection stores its typed `ResponseReader<R>` and `RequestWriter<W>`, including retained buffers and framing state. Short mutex sections transfer ownership; no lock is held during I/O.
- `open` acquires the capacity-one pool use and takes the codecs directly. Request metadata encoding, body writes, response parsing and response body reads run in the application future. There is no per-request operation dispatch or pair of codec-recovery channels.
- Direction guards restore codecs before publishing completion. Only after both directions return successfully does the pool make HTTP/1 reusable. Failed, cancelled or abandoned exchanges retire the connection; resource capacity stays charged through asynchronous lane/socket cleanup.
- Split upload/download remains fully duplex. Response completion alone never releases an unfinished upload. Static transport dispatch and retained boxed body adapters are preserved; there is no new per-frame future allocation.
- The driver remains responsible for construction, legacy queued pull-body `call`, TLS receive/transmit progress, deferred synchronous `close_write(None)` finalization, and retirement. Normal direct completion does not wait for a supervisor acknowledgment. Pool/accounting notifications may still wake lifecycle work.
- Deferred finalization owns a return guard before enqueueing. Driver abandonment rejects and drops new work outside the queue mutex, recovering codecs and admission rather than retaining a queue/work ownership cycle.
- The existing scoped driver already multiplexed futures within its task. Removing its request handoff does not mean every previous request necessarily switched OS threads. The established direct path now has no mandatory driver poll or separate task handoff at all.

Connection ownership and background progress are separate decisions. HTTP/2/3 will need independently schedulable connection processing; this HTTP/1 refactor does not implement that placement or change the scheduler interface. The current TLS adapter still has supervised progress futures, so HTTPS cannot run with its supervisor completely unpolled.

## Validation

- Rust 1.87: all **39** client integration tests pass with rustls enabled, including HTTP/HTTPS duplex echo, independent peers, proxy backpressure, interims/trailers, early responses, cancellation and abandonment. The default feature configuration also passes its 32 integration tests.
- New regression: warm one connection through the queued client, stop polling the client driver, then complete four direct requests with chunked response bodies and trailers on that same connection. Pool uses return to zero after each request, and shutdown resumes the driver and joins cleanup.
- Clippy passes with warnings denied; formatting passes.
- All **48** timed samples validate complete responses and exact server request accounting: **8,744,951 measured responses**, zero server errors. Compiled Rust sources remained unchanged throughout measurements. Documentation edits do not affect the measured executable.

## Reproduce

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release --locked --offline

python3 benchmarks/client/run.py \
  --output benchmarks/client/results/connection-state-repeat \
  --modes reuse --repeats 3 --seconds 3 --warmup 1 --concurrency 1,32 \
  --implementations before-open,base-open,base-call,hyper \
  --baseline-binary /tmp/base-http-client-driver-dispatch-20261028 \
  --baseline-sources benchmarks/client/results/performance-20261028/sources.tar.gz
```

Raw [summary](results/connection-state-20261028/summary.json), [metadata](results/connection-state-20261028/metadata.json), [current sources](results/connection-state-20261028/sources.tar.gz) and [baseline sources](results/connection-state-20261028/baseline/sources.tar.gz) are retained.

Previous executable SHA-256: `e3e7e5c488f793bcdd0e3f48f9498c0682fdc8976627120c04e9fdc404cc81c3`. Current executable SHA-256: `3313f2ca01177f9894a97af7664269d751a586e1518daf7db00a764b6d5c5308`.

This is a small-response loopback saturation benchmark on a shared Apple M4 Pro development Mac. These are not equal-request-rate P99 measurements, Linux results, or evidence of multiworker scalability. HTTPS, pool/accounting costs and concurrent plaintext remain areas for further investigation.
