# Remaining Base versus Hyper HTTP/1 work

Audited 2026-10-26 against the exact benchmark sources: Base's compact exchange implementation, Hyper 1.12.0, hyper-util 0.1.21, Tokio 1.54.1, http 1.5.0 and http-body-util 0.1.5. Production code was unchanged during this audit.

The subsequent [HTTP hot-path implementation](http-hot-path.md) addresses writer registration, field-fact scans, ordinary empty-exchange state and response planning. The audit below remains a record of the earlier code paths.

The subsequent [Native readiness implementation and measurements](readiness.md) use kqueue's reported byte count: a short receive alone does not establish exhaustion at a TCP urgent-data boundary. It also preserves directional EOF and retains the bound reader's registration. Tokio's `AsyncFd` adapter retains its conservative policy because it does not expose that byte count. The paths, counts and priorities below describe the pre-change implementation.

The pre-change uninstrumented confirmation measured **121,074 versus 128,868 requests/s**, a 6.0% deficit. Median process accounting differed by approximately **0.413 µs user CPU and 0.066 µs kernel CPU per request**. These component medians do not precisely decompose the throughput difference. The following audit establishes differences in work; it does not assign a measured fraction of that 6% to each one.

## A missed transport difference: an extra receive

The ordinary native read path is:

```text
RequestReader::fill
  ReadinessSocketStreamReader::read_into
    socket_recv_poll → socket_operation → cancellable → socket_io
      find and retain registration
      snapshot readiness generation
      recv
      on WouldBlock: clear observed readiness, wait, retry recv
```

`socket_io` attempts the receive even when its existing readiness bit is clear. A successful short read does not clear the bit. After sending the response, the server immediately starts reading the next request and usually calls `recv` before it has arrived. That fails with WouldBlock before the connection waits.

Hyper's path through `TokioIo` and `TcpStream` uses `PollEvented::poll_read`. It **checks readiness before receiving** and, on macOS/kqueue, **clears the observed readiness after a positive short read**. A 41-byte GET into a large receive buffer therefore leaves the next read waiting for a new event, without first discovering the empty socket through another syscall. Both steps matter: clearing readiness alone would not fix Base's unconditional attempt.

This was confirmed using a macOS dyld interposer that counts libc I/O calls on accepted sockets. It measures operation counts, not usable performance timings.

With one persistent connection and exactly 10,000 sequential GETs after warmup:

| Variant | Positive receives | WouldBlock receives | Positive writes |
| --- | ---: | ---: | ---: |
| Base/Native | 10,000 | **10,000** | 10,000 |
| Base/Tokio | 10,000 | **10,000** | 10,000 |
| Hyper/Tokio | 10,000 | **0** | 10,000 |

With the benchmark's four wrk threads and 64 persistent connections for three seconds:

| Variant | Completed wrk requests | Positive receives | WouldBlock receives | Positive writes |
| --- | ---: | ---: | ---: | ---: |
| Base/Native | 356,945 | 357,005 | **357,081** | 357,005 |
| Base/Tokio | 361,860 | 361,921 | **361,929** | 361,921 |
| Hyper/Tokio | 383,584 | 383,644 | **7** | 383,644 |

Every positive receive contains exactly one 41-byte request; every positive write contains one 78-byte response. Timed wrk shutdown leaves about 60 additional in-flight requests per variant and peer-close/reset results, so completed-request and I/O totals differ slightly. The sequential probe has exact totals and no errors. wrk reports no socket or HTTP status errors. These instrumented runs must not replace the uninstrumented throughput comparison.

**The prior inference from the Base/Tokio control was too strong.** Our Tokio adapter uses its own socket operation strategy, and retains the same extra receive. Comparing Base/Native to Base/Tokio does not isolate this cost or establish that it lives exclusively above the scheduler binding.

Base source: [read binding](../../../base-scheduler/crates/selector/src/readiness_socket_stream_reader.rs), [operation loop](../../../base-scheduler/crates/selector/src/socket_operations.rs), [generation tracking](../../../base-scheduler/crates/selector/src/readiness_driver.rs). Pinned Tokio source: `src/io/poll_evented.rs:161`, especially the short-read clear at line 213; `src/net/tcp/stream.rs:1451`. The `TokioIo` adapter forwards reads at `hyper-util/src/rt/tokio.rs:162`.

## Request parsing and semantic construction

| Stage | Base | Hyper |
| --- | --- | --- |
| Complete head detection | Scan for CRLFCRLF, scan all bytes again for strict CRLF, then httparse. | Parse the first available buffer directly with httparse. Only after a partial parse, scan for completion starting three bytes before the previous boundary. |
| Fragmentation | Terminator search restarts at the beginning of available data. | Completion scan covers the new suffix and delimiter overlap. |
| Field creation | `Fields::add` validates names and values again after httparse, then copies into inline or heap storage. | Records offsets, splits/freezes the input buffer, uses shared byte slices for values. Known names become typed constants. |
| Framing/connection facts | Separate scans for Host, content length, transfer coding, upgrade, persistence and Expect. | Derives decoding length, persistence, Expect and upgrade while constructing the header map in one field loop. |
| Request target | Allocates owned path and authority strings; validates Host syntax and port and requires one Host for HTTP/1.1. | Parses the target into `Uri`; `/` specifically uses a static path representation. Host remains a field value rather than an additional owned authority for an origin-form request. |

Base's stricter line ending, Host and framing policies are deliberate behavior. They should be preserved while fusing scans. Repeated parsing/validation and extracting protocol facts in separate passes are implementation choices. The ordered `Fields` representation does not require those extra passes: parsing can retain the same ordered pairs while recording framing facts as it goes.

For the one-Host-field GET, the current Base source has seven separate field lookup passes before dispatch, in addition to constructing the collection. These tiny scans still execute name comparisons for absent fields. This is a source-level count, not seven separately measured costs.

Base source: [read_head/read_request](../../crates/http1/src/input.rs), [request/fields/framing](../../crates/http1/src/wire.rs), [Field::new](../../src/fields.rs). Hyper: `src/proto/h1/role.rs:73` and `Server::parse` at line 145. http: `src/uri/mod.rs:293` has the `/` specialization. Hyper also has HeaderMap, input-buffer ownership and refcounting costs; this audit does not claim it is allocation-free.

## Empty incoming bodies and shared exchange state

An ordinary empty GET still creates `Arc<Context>`. Base clones it into `RequestReader::previous` and `IncomingBody`, and initializes atomic completion to true. The handler drops the empty input body, output checks abandonment, and the connection checks completion/tunnel/abandonment for reuse both after output and before the next admission. There are two explicit Arc clones per request, plus corresponding drops and shared state observations.

Hyper's `poll_read_head` selects `IncomingBody::empty()` for zero decoded length. It does not allocate a body channel for this request; ordinary protocol state remains in the connection.

The existing Base codec allocation probe reports three allocations: path, authority and exchange context. There is no matched whole-stack Hyper allocation count, so those three cannot be presented as an allocation-count difference against Hyper. The narrower confirmed difference is that Base allocates a shared exchange context and Hyper does not create a body channel for the empty request.

A future empty-body fast path must still account for CONNECT and an offered Upgrade: their initially empty input can become a live tunnel body. We can specialize ordinary requests without losing that behavior.

Base: [Exchange and IncomingBody](../../crates/http1/src/input.rs). Hyper: `src/proto/h1/dispatch.rs:302`.

## Response construction, serialization and body completion

| Stage | Base | Hyper |
| --- | --- | --- |
| Handler's Content-Type | `Fields::add("Content-Type", "text/plain")` goes through normal validation and owned inline field construction. | Typed `CONTENT_TYPE` and static `HeaderValue`, inserted into a HeaderMap. |
| Response planning | Several independent name scans for framing, Connection, Upgrade and Trailer before serialization. | Derives relevant state while encoding the drained field collection. |
| Status line | General version/status/reason serialization. | Explicit literal `HTTP/1.1 200 OK\r\n` fast path. |
| Single-chunk body | First `read` yields data; another `read` discovers EOF; then `finish`. | One `poll_frame`; `Full::is_end_stream` is true after taking its data, so the final chunk also ends the body. |
| Small body buffering | Copies the 13-byte body into the contiguous pending output buffer. | Queues the static body Bytes separately and writes the head and body together with writev. |
| Successful transport writes | One per response. | One per response. |

Base's `ReadableBody::is_empty` explicitly does not suppress later read errors. It is therefore not currently equivalent to Hyper's end-of-stream guarantee. Skipping the EOF read based solely on that predicate would change semantics. A terminal completion guarantee, or a dedicated buffered-body path, would allow the same optimization while retaining errors and trailer finalization.

Nested async functions and the explicit first body poll describe more source control flow, but do not prove a specific cost: generic static response code can inline and simplify. Similarly, the temporary empty `Fields` in trailer validation may optimize away. Neither is counted here as an independently measured allocation or timing penalty.

Base: [plan, serialization and write_body](../../crates/http1/src/output.rs), [ReadableBody contract](../../src/body/mod.rs), [BufferedBody](../../src/body/buffered.rs). Hyper: `Server::encode` at `src/proto/h1/role.rs:390`, `encode_headers` at line 662, `dispatch.rs:350`, `encode.rs:224`, `io.rs:278` and `io.rs:549`. Full: `http-body-util/src/full.rs:50`.

## Bound transport overhead beyond the receive attempt

Native bound readers/writers retain the socket and scheduler but not the readiness registration. Each new I/O operation calls `SocketRegistrations::existing`: mutex lock, group lookup and Arc clone/drop. A waiting read retains that registration across its retries, so this is approximately two lookups per request, not one lookup per receive attempt.

Tokio's `PollEvented` owns its registration and uses it directly. It still checks readiness, cooperative scheduling and resource state; the comparison is not between checks and no checks.

Base also checks cancellation/admission in both `socket_operation` and `cancellable`, plus lane state in the bound reader/writer. The static benchmark uses `Cancellation::never()`: the HTTP handler/body fast paths already avoid active cancellation listener registration. Owned write submission, buffer-pool reads, per-request background tasks and dynamic body dispatch are not on this workload's ordinary path.

## Priority implied by the audit

1. **Avoid the extra receive:** specialize the bound stream read path to consume readiness and clear the observed generation after a short successful default read. Preserve the generation race protection, closure wakeup and EOF behavior. Datagram and peek semantics must not inherit stream short-read assumptions.
2. **Retain the registration in the bound capability:** avoid repeatedly discovering a stable scheduler/socket association. This can preserve the generic socket API and cancellation contract.
3. **Record protocol facts during parsing:** combine field walks and investigate eliminating full-head pre-scans while retaining strict rejection behavior. Measure HTTP-only work with a matched control.
4. **Specialize ordinary empty input:** avoid shared body/exchange allocation when neither live input nor a tunnel needs it.
5. **Improve terminal-body and common response handling:** consider a true terminal guarantee, synchronous head encoding and the ordinary 200 fast path. Keep framing checks and errors.

This order follows the new operation-count evidence and the amount of avoidable work. It is not a ranking of independently measured nanoseconds. Each candidate still needs correctness checks and uninstrumented interleaved throughput measurements before claiming that it closes the remaining gap.

## Diagnostic reproduction

[Interposer](diagnostics/socket_counts.c), [driver](diagnostics/count_socket_calls.py), [sequential results](diagnostics/socket-counts-sequential.json), [wrk results](diagnostics/socket-counts-wrk.json). Results retain binary/library SHA-256 hashes; the server is the preserved compact-context benchmark binary. The C library was compiled with Clang warnings denied. The sequential driver validates every status and body. These diagnostics do not alter production Rust code.

```sh
clang -std=c11 -O2 -Wall -Wextra -Werror -dynamiclib \
  diagnostics/socket_counts.c -o diagnostics/socket_counts.dylib
python3 diagnostics/count_socket_calls.py \
  --binary results/hyper-target/compact-exchange/server \
  --library diagnostics/socket_counts.dylib \
  --output diagnostics/socket-counts-sequential.json
python3 diagnostics/count_socket_calls.py \
  --binary results/hyper-target/compact-exchange/server \
  --library diagnostics/socket_counts.dylib \
  --wrk /absolute/path/to/wrk \
  --output diagnostics/socket-counts-wrk.json
```

The [future-ownership follow-up](remaining-gap.md) measures remaining copies around request admission and handler/body waits, with compiler-matched before/after controls.
