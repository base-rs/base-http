# Build roadmap

Updated 2026-10-26. This is the ordered implementation plan for the [semantic design](proposal.md), informed by the [source review](source-review.md) and [component evaluation](components.md). It describes planned work; the [implementation notes](implementation.md) record completed code and checks.

The first application milestone is a streaming HTTP/1 client and server over plain TCP, with connection reuse and explicit shutdown. HTTPS follows, then HTTP/2. Native QUIC is a separate transport milestone before HTTP/3. Each protocol should become independently usable and interoperable as it is added.

## Starting point and boundaries

The semantic core and pull-body model are implemented, together with the first native HTTP/1 server path in sibling `base-http1` and `base-http-server` packages. Tests exercise streaming, persistence, interim responses, trailer delivery, managed upgrades/CONNECT and cancellation over real Direct-backend TCP. A deterministic transport covers fragmentation and partial writes. Native HTTP/1 client framing and a duplex proxy experiment now exercise both directions. Generic singleplex and multiplex pools are implemented independently in sibling [base-pool](../../base-pool/readme.md). High-level client orchestration, HTTP pool integration, managed client tunnels, TLS and other backends remain follow-up work. See [implementation evidence](implementation.md).

The scheduler rename and release are complete: `base-scheduler` and its seven companion crates are published at `0.1.0`, verified against crates.io on 2026-10-26. See the [published interface](https://crates.io/crates/base-scheduler/0.1.0) and [scheduler release notes](../../base-scheduler/releases.md). This HTTP prototype is renamed to `base-http` and now uses the released registry baseline.

The planned package boundaries are:

- `base-http` owns semantic messages, `Fields` and bodies in `base_http::body`.
- `base-http1`, `base-http2` and `base-http3` own protocol-specific connection APIs and drivers.
- `base-http-client` and `base-http-server` compose the enabled protocols into application APIs. The client also uses `base-pool`.
- `base-pool` implements distinct `SingleplexPool` and `MultiplexPool` contracts with a common admission interface. HTTP protocol consumers remain to be integrated and must exercise the contracts before API stabilization.
- `base-http-hpack` and `base-http-qpack` own compression interfaces. A private dependency facade, focused fork or native implementation can sit behind each interface. Upstream types do not become public aliases or re-exports in the codec API. Start with one implementation; no pluggable backend framework is required.
- `base-tls` integrates rustls with byte streams. Native `base-quic` uses rustls's separate QUIC handshake/key API; `base-http3` uses QUIC connections.

Create packages when their implementation starts, using sibling HTTP packages with explicit local path dependencies while they are unpublished; avoid creating a parent workspace over unrelated projects. A separate repository or published release is not a prerequisite for the next local consumer. Hyper and the `h2` engine remain external comparison peers; focused parser and compression dependencies are allowed.

## Ordered milestones

| Stage | Deliverable | Usable result |
| --- | --- | --- |
| M0 — complete | Scheduler rename and release | Interface and all seven companions are published at `0.1.0`. |
| M1 — core implemented | Semantic messages and fields | Applications construct and transform native requests/responses. |
| M2 | Streaming bodies and owned-buffer transport lifecycle | Bounded producers/consumers, pull bodies and bounded producer/consumer endpoints and supervised I/O with demonstrated cleanup. |
| M3 | Native HTTP/1 connection APIs | One streaming exchange, then persistent exchanges, over memory and TCP. |
| M4 | Client/server orchestration and singleplex pool | First application milestone: a reusable plain HTTP/1 client and server. |
| M5 | TLS and negotiation | HTTPS client/server using the same HTTP/1 engine. |
| M6 | HPACK, HTTP/2 and multiplex pooling | Concurrent HTTP/2 exchanges through the application APIs. |
| M7 | Native QUIC transport | Independently interoperable QUIC streams, without HTTP. |
| M8 | QPACK and HTTP/3 | HTTP/3 through the shared semantic and application APIs. |

This is dependency ordering, not a calendar estimate. HTTP/2 and QUIC are substantial implementations. Focused experiments can happen earlier where they resolve a current interface question; the full later engines need not be prerequisites for the HTTP/1 path.

### M0: scheduler rename and release — complete

Use the released `base-scheduler` family at `0.1.0`, which declares Rust 1.85. No scheduler publication work is a prerequisite for HTTP implementation. Use one consistent scheduler version across the graph; a deliberate local override is available if later co-development requires it.

The release uses explicitly constructed, scheduler-bound reader/writer handles, with cancellation per operation and uncancellable handle closure for retirement. Protocol objects may retain these capabilities; a handle does not switch schedulers between calls. The old prototype's per-read scheduler switching is historical evidence, not a requirement of the released API.

**Completed evidence:** all eight `0.1.0` packages are present on crates.io and not yanked. The HTTP prototype now uses the released dependencies with registry checksums in its lockfile.

### M1: shared semantic core — implemented

The package/import rename and scheduler migration are implemented. `ReadableBody::read` now takes cancellation only, and the file-body integration test borrows a released bound reader with separate asynchronous cleanup. Existing body behavior remains intact.

`Fields` implements ordered `pairs`, an optional trailer split, the lazy policy index, combination/trailer defaults and custom overrides. Indexing and indexed mutations validate singleton rules; failed mutations preserve previous state. Rich date/range/authentication/negotiation parsers remain focused follow-up work, distinct from combination.

Method/status/version, protocol intent and request-target components are implemented with `Request<B>`/`Response<B>`, public heads, consuming `map_body`/`with_body`, and `into_parts`/`from_parts`. Informational and final responses share the response representation; exchange sequencing belongs to the protocol/responder API.

Existing static bodies remain usable. Generic body transformations are tested without cloning, body trait bounds or boxing. Prototype optional `BoxBody` when a real heterogeneous return boundary needs it, explicitly choosing its chunk/error and future erasure.

**Completed core evidence:** behavior tests cover field order, duplicates and policy errors, idempotent trailer marking, header/trailer iteration, request-target forms, status predicates, body mapping and preservation of head/body ownership. Readiness, EOF, representation-length and replay behavior remains intact. Tests, README examples, Clippy and Rust 1.85.1 checks pass; see [implementation scope](implementation.md).

### M2: prove streaming and I/O ownership — initial Direct path implemented

`ReadableBody` combines observations and lazy reads; `WritableBody` describes producer operations. `BodyReader`/`BodyWriter` bound bytes and chunks. There is no separate callable-stream abstraction. A handler can return incoming input as its outgoing body without spawning. `InterimResponses` provides explicit interim sequencing.

Trailer delivery now reaches the message's split collection through `Request::read`/`Response::read` and `ReadableBody::finish`. Real TCP tests exercise streaming, backpressure, cancellation and managed duplex use. The server awaits both lane closures and drops the handles before closing sockets. Cancellation also interrupts handlers and output bodies that ignore their signal.

**Remaining evidence:** TLS storage/progress experiments, pooled receive-buffer optimization, and broader runtime verification on Tokio and Linux io_uring. The initial driver uses bounded borrowed parser reads and copies returned chunks. A [static hello benchmark](../benchmarks/hello/results.md) now exercises native readiness, Tokio and Direct backends on macOS; it does not establish the remaining streaming/cancellation or Linux performance evidence.

### M3: native HTTP/1 — initial client/server codecs implemented

`base-http1` provides `RequestReader`, `IncomingBody`, per-request `Exchange`, `ResponseWriter`, client `RequestWriter`/`ResponseReader`, interim hooks, configurable parsing limits, fixed/chunked framing, close-delimited responses, trailers and persistence. Server upgrades/CONNECT retain managed duplex bodies; client switch parsing exposes raw input, while outgoing client switch coordination remains planned. `httparse` is a private dependency for head parsing; native code owns framing and connection decisions.

The tests include fragmented and coalesced messages, ambiguous framing, truncation, partial/zero writes, interim/final sequencing, static and computed trailers, bodyless responses, cancellation and upgrade read-ahead. The raw TCP peer is independent of the Base codec. Early final responses permit reuse when active uploads finish; abandoned uploads disable reuse. No unread upload is silently drained. The [proxy experiment](duplex.md) checks complete early responses, independent upload/download progress, interim/trailer forwarding, two-connection reuse and joined failure/cancellation cleanup.

**Remaining:** managed client tunnel coordination, high-level exchange supervision, broader protocol interoperability/fuzzing, deadlines, protocol-version upgrade identifiers, supported alternative scheduler backends and Linux measurements. This is an initial implementation, not a completed production-conformance milestone.

### M4: build the application client/server and singleplex pool

The initial `base-http-server` now exists with bounded connection futures, generic handlers and cooperative shutdown with joined retirement. Generic `base-pool::SingleplexPool` and `MultiplexPool` are also implemented, with bounded construction, FIFO/cancelled waiters and explicit retirement; a TCP test checks resource reuse and retirement accounting. HTTP integration remains open. First prototype client-owned upload supervision behind `call`, following the [symmetric application contract](duplex.md#symmetric-application-interface), then introduce `base-http-client` with HTTP/1 support and integrate `base-pool::SingleplexPool`. The prototype must return on final response headers, keep upload progress independent of response reads, and compose with an ordinary proxy handler using borrowed incoming bodies. Application request/response producers use the same bounded body pair; handlers continue to return responses. Keep HTTP construction policy in the client. Exercise construction permits, exclusive leases, cancelled waiters and supervised retirement through actual HTTP connections.

The client owns endpoint establishment, driver supervision and reuse. A lease remains charged until both directions are finished and the connection is synchronized or retired. Begin with conservative retry behavior; automatic retries require known processing state, semantic retryability and a reproducible body.

The server supplies bounded connection/handler admission, request handling and graceful shutdown. Test cancellation and error responses while a request upload is still active. Stopping the server must stop admission, drain or terminate admitted work, await owned drivers and close transports after retirement.

**Exit evidence:** an executable streaming client/server example, repeated requests over reused connections, saturation/cancelled waiters, stale connections, construction-limit races and shutdown with in-flight bodies. This is the first usable application milestone. Preserve the direct `base-http1` APIs for callers that do not need orchestration.

### M5: add rustls TLS and HTTPS

Turn the M2 experiment into `base-tls`: configuration, verification, handshake/progress, ALPN, retained record/plaintext storage and shutdown. Integrate it into the existing HTTP/1 establishment/serving path, including deadlines. Settle the smallest shared stream interface using both plain and TLS consumers.

**Exit evidence:** independent HTTPS peers, certificate/hostname failures, fragmented records, partial ciphertext writes, retained plaintext, required TLS traffic during application backpressure, cancellation, truncation and closure. Measure copies, record batching and retained bytes. Exercise HTTP/2 ALPN selection when M6 is available.

### M6: add HPACK, HTTP/2 and multiplex admission

First implement the Base-owned `base-http-hpack` interface: independent encoder/decoder state, raw byte fields, sensitive/never-indexed handling, reusable output storage, configuration and bounded decoding with typed errors. Evaluate a private codec dependency against these needs; port or fork where that is the better fit. `protocol-hpack` supplies reference behavior and fixtures whichever implementation is selected. Whole-block adapter buffering, if required, must be explicit and bounded.

Build the native `base-http2` framing and connection/stream state machines: preface/settings, field blocks and continuations, bodies/trailers, interim responses, flow control, reset, ping and GOAWAY. One driver orders connection output. Stream admission, data-flow credit and retained application memory require separate accounting.

Integrate the existing `base-pool::MultiplexPool`, validating reservations at the connection owner's stream-opening boundary. Exercise capacity updates, active-use abandonment and cleanup acknowledgment against the actual HTTP/2 state machine. Keep it distinct from `SingleplexPool`. Integrate the `http2` feature into client/server protocol selection and TLS ALPN.

**Exit evidence:** independent HPACK vectors and peer interoperability; malformed compression and frame inputs; concurrent streams, slow consumers, capacity reductions, retained receive data, resets during writes and GOAWAY/open races. Stream completion and cancellation must release each charge exactly once. Benchmark concurrency and backpressure as well as peak throughput.

### M7: implement native QUIC as its own transport project

Build `base-quic` with a deterministic core accepting datagrams and monotonic time, plus a scheduler-owned UDP/timer driver. Integrate rustls's QUIC handshake/key interface. Use existing QUIC implementations as references, independent peers and possible focused algorithm sources.

Stage packet parsing/protection and handshake first, then reliable bidirectional/unidirectional streams, acknowledgments/loss recovery, flow control, congestion control and pacing. Add connection-ID routing, transport parameters, path validation and the protocol-required error/closure behavior. Establish a tested QUIC v1 baseline before expanding optional transport capabilities.

**Exit evidence:** deterministic simulation of loss, duplication, reordering, delayed acknowledgments and timer expiry; independent QUIC interoperability; slow consumers, stream reset/credit behavior, congestion/recovery, packet-buffer retirement, cancellation and shutdown. The result must carry non-HTTP stream traffic before HTTP/3 depends on it.

### M8: add QPACK and HTTP/3

Implement `base-http-qpack` behind its own Base-owned interface, with the same private-dependency/fork/native choice as HPACK. Start with static references and literals, zero advertised dynamic-table capacity and zero blocked streams. Correct instruction/control-stream handling is still required. Add dynamic compression later with explicit blocked-section, acknowledgment and cancellation state.

Implement `base-http3` request streams, control/critical streams, settings, fields/data/trailers, informational responses, error mapping and GOAWAY over `base-quic`. Integrate the `http3` feature into the companion client/server APIs. HTTP/3 uses QUIC stream-opening credit; completion of a local stream does not itself grant another opening.

**Exit evidence:** independent HTTP/3 peers, invalid/missing critical streams and settings, slow consumers, upload/response concurrency, resets/GOAWAY, trailers and shutdown. Reuse the application semantic scenarios from HTTP/1 and HTTP/2 to expose version-specific gaps.

## Measurements and later work

Keep correctness, bounds and lifecycle checks in each milestone. Fuzz parsers/codecs and protocol transitions as those components appear. Preserve provenance, license notices and imported tests for ports and forks.

From M3 onward, compare named workloads on matched hardware and configuration: plain/TLS, protocol version, payloads, concurrency, connection reuse and resource limits. Record throughput, tail latency, CPU, allocations, copies and peak retained/queued bytes. Base's Direct/Tokio backends help distinguish protocol effects from io_uring effects; Hyper/Tokio belongs in the external benchmark harness. Optimize the first HTTP/1 path as evidence arrives rather than waiting for HTTP/3.

Dynamic QPACK, specialized provided-buffer paths, more aggressive batching and replacing codec facades are follow-up decisions driven by concrete workloads or missing capabilities. Multi-origin policies, proxies, compression middleware, expanded tunnel convenience APIs and optional `http`/`http-body`/Tower adapters can be scoped independently after the underlying connection lifecycle works. Separate repository extraction and publishing do not need to interrupt the usable protocol sequence.

The initial semantic/body, native HTTP/1 server and generic singleplex/multiplex pool slices are implemented. Next extend validation across backends, then add HTTP/1 client orchestration and integrate the singleplex pool; retain the TLS storage experiment before stabilizing the transport seam.
