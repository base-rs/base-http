# HTTP/1 client comparison

Measured results: [initial comparison](results-20261028.md), [performance changes](performance-20261028.md), [connection-owned state](connection-state-20261028.md), [TLS progress](tls-progress-20261028.md), [exchange allocations](exchange-overhead-20261029.md), [operation storage reuse](exchange-reuse-20261029.md) and [full comparison rerun](full-rerun-20261029.md).

This compares the current `base-http-client` prototype with Hyper/Tokio against the same independent Hyper HTTP/1 server process. Both HTTP and HTTPS use loopback TCP. The payload is a static 11-byte `Hello World` response to `GET /hello`, with the entire status/body checked on every request.

The high-level client paths are:

- `base-call`: semantic request with `EmptyBody`, with upload/download driven by the existing queued client.
- `base-open`: the new direct writable request, declared content length zero, then `finish` and direct response consumption.
- `hyper`: Hyper's high-level pooled client from `hyper-util`, with the `hyper-rustls` connector for HTTPS.

Two connection-only paths are also available: `base-connection` and `hyper-connection`. Each lane establishes exactly one connection, retains its codec/transport state across warmup and measurement, and invokes the low-level HTTP/1 API directly. There is no pool or per-request dispatch queue. Shared asynchronous barriers synchronize preparation, warmup, measurement and cleanup; CPU/time are captured before connection cleanup begins. Hyper retains its connection driver task; Base retains supervised TLS progress when encrypted. These isolate the client orchestration layer, rather than removing protocol/TLS work.

`before-open` runs a preserved pre-change Base executable against the current common server; `before-connection` runs that executable's connection-only path. Supply `--baseline-binary` and `--baseline-sources`; its original source archive and executable hash are stored separately. This allows before/after samples to be interleaved with the same server configuration and certificate.

The benchmark pins Base scheduler/pool 0.2.0, Hyper 1.12.0, hyper-util 0.1.21, Tokio 1.54.1, rustls 0.23.46, tokio-rustls 0.26.6 and hyper-rustls 0.27.10. HTTP crates use the local source tree. The lockfile fixes all other dependencies. Rust 1.87 builds an optimized release binary.

## Method

Each client runs on one scheduler worker, as a spawned application task. Concurrent request loops share that worker. Hyper runs each request loop as a separate Tokio task; Base uses scoped concurrent futures borrowing its client. Base direct requests run in their application futures with connection-owned state; its scoped driver still polls queued calls, TLS progress and retirement. Hyper schedules its connection futures as separate tasks. This is an end-to-end comparison of the current clients, including their actual orchestration. It is not an isolated codec or scheduler benchmark.

The common server runs in a separate process with two Tokio workers. Every sample gets a new server and client process; the server implementation and configuration are identical for all clients. Server connection/request/failure counters verify accounting, detect accidental connection churn and confirm fresh-connection operation. Client process CPU is measured over the timed phase, including its scheduler and cleanup workers. Server CPU is stored separately and includes warmup.

Each sample first completes one request per lane and allows 50 ms for connection setup and pool returns to settle, then runs continuous warmup. Keep-alive cases exclude both preparation and warmup from measurement. Without this finite preparation, the Hyper plaintext baseline can speculatively open many connections while earlier connections are still returning to its pool; accepted-connection counts must be checked. One or 32 requests are in flight; no HTTP/1 pipelining is used. Base's pool permits twice that many live resources; Hyper's idle cache permits twice that many idle connections. These are different pool contracts, and actual accepted connection counts are retained with every sample. The extra capacity accommodates connection-return bookkeeping. Automatic retries and idle timeouts are disabled in the Hyper baseline; Base has no automatic replay. Both clients explicitly send Content-Length zero for the request. The server selects the same fixed response framing.

Client TCP_NODELAY is disabled for both clients, matching Base's current endpoint default; it is enabled on the common server. Both secure clients use the same rustls/ring implementation and TLS 1.3, offer `http/1.1` via ALPN, verify the IP identity `127.0.0.1`, and trust exactly the generated self-signed certificate. The server checks the selected TLS version and ALPN. No verification bypass is used. TLS session resumption/tickets are disabled, so fresh HTTPS measures full handshakes. Reused HTTPS measures encrypted HTTP requests on established sessions.

The script rotates implementation order across repetitions and shuffles scenario order with a fixed seed. Default runs use one second of warmup, five seconds of measurement, and five repetitions. Throughput is completed, fully consumed responses divided by actual elapsed time, including the final in-flight drain. Raw histograms provide response latency percentiles; these are saturation tests with different achieved request rates, so their P99s are **not equal-load latency comparisons**.

Fresh-connection cases use Connection: close on each request and run after all keep-alive measurements to avoid contaminating those results with large TIME_WAIT populations. The runner pauses before each fresh sample (10 seconds by default); the shorter fresh run below uses five seconds. These pauses are outside measurement. These include connection setup, full TLS handshake where applicable, HTTP I/O and client retirement. They are a diagnostic rather than the primary keep-alive comparison.

## Run

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/base-http-validation-target \
  rustup run 1.87.0 cargo build --manifest-path benchmarks/client/Cargo.toml --release --locked

python3 benchmarks/client/run.py \
  --output benchmarks/client/results/keepalive-run \
  --modes reuse --repeats 5 --seconds 5 --warmup 1 --concurrency 1,32

python3 benchmarks/client/run.py \
  --output benchmarks/client/results/fresh-run \
  --modes fresh --repeats 3 --fresh-seconds 1 --warmup 0.2 --fresh-cooldown 5
```

Use a new output directory for each invocation. The runner generates a temporary local identity, shares it across clients, retains its public certificate, and removes its private key when done. Each sample contains the client result, server counters, commands and stderr. `metadata.json` records the machine, compiler, arguments and executable hash; `summary.json` gives medians and ranges. The runner also archives local dependency and harness sources in `sources.tar.gz`, with individual SHA-256 hashes in `source-hashes.json`. Direct comparisons should use the same source snapshot and binary hash. Directories beginning `smoke-` are harness validation and must not be used as benchmark results.

## Interleaved optimization comparison

```sh
python3 benchmarks/client/run.py \
  --output benchmarks/client/results/performance-run \
  --modes reuse --repeats 3 --seconds 3 --warmup 1 --concurrency 1,32 \
  --implementations before-open,base-open,base-call,base-connection,hyper-connection,hyper \
  --baseline-binary /tmp/base-http-client-before-20261028 \
  --baseline-sources benchmarks/client/results/keepalive-20261028/sources.tar.gz
```

Preserve the baseline executable before rebuilding. Both executables must use the same dependency versions and response validation. The archived baseline sources must belong to that executable. Each dated report records its own baseline and final executable hashes. The [TLS progress report](tls-progress-20261028.md#reproduce-and-audit) and [exchange allocation report](exchange-overhead-20261029.md#reproduce-and-audit) give their exact comparison commands.

## Allocation diagnostics

The [orchestration investigation](orchestration-20261029.md) isolates pool cycles, retirement wakes, cancellation and pinned adapter transitions. Its measurements use a local pool patch; pass the identical Cargo setting through `--cargo-config` when archiving sources for a patched executable. The ordinary dependency remains the released pool.

Build with `--features count-allocations` to emit `allocation_diagnostic` in raw sample JSON. It counts whole-process allocations, reallocations, frees and requested bytes during the timed phase, plus frequent exact allocation sizes up to 32 KiB. Setup and warmup are excluded; phase scaffolding and concurrent client cleanup workers are included. Requested bytes measure allocation traffic, not live memory. Preserve diagnostic executables separately and compare them with other counting builds. The counters add overhead: use ordinary builds, with the feature disabled, for throughput comparisons. The [exchange allocation report](exchange-overhead-20261029.md#reproduce-and-audit) records both kinds of executable and their hashes.
