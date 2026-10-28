# Source review for Base HTTP

Reviewed 2026-10-25. This is an architecture and scope review of local source snapshots and public specifications, not an exhaustive correctness audit or a claim that the local checkouts are the latest upstream revisions. The current `base-http` directory began empty, without a Cargo manifest or Git repository. Package names in these notes use the new `base-` prefix; source URLs and local checkout links retain their reviewed locations.

The [component evaluation](components.md), dated 2026-10-26, adds inspection of the local HPACK context/encoder/decoder and test structure, and current primary-source research on Rust compression, rustls and QUIC options. It records implementation recommendations and their remaining ownership/performance experiments.

Scheduler baseline update, 2026-10-26: `base-scheduler` and its seven companions are published at `0.1.0`. The original review below used the historical research interface; the [published baseline](#published-scheduler-baseline) supersedes its scheduler constraints. The HTTP prototype has now been renamed/migrated and the [implementation notes](implementation.md) record the initial semantic core.

## Snapshot provenance

Local Socketry checkouts are under `/Users/samuel/Developer/socketry`. Socketry review used the tracked implementations at these commits. Existing untracked notes/examples were left untouched and are not treated as project requirements.

| Project | Reviewed revision | Principal role |
| --- | --- | --- |
| [protocol-http](https://github.com/socketry/protocol-http/tree/dcbc0fe7ad6b9a55986f003d25c0c0023228db0b) | `dcbc0fe7ad6b9a55986f003d25c0c0023228db0b` | Shared messages, fields, bodies and middleware. |
| [protocol-http1](https://github.com/socketry/protocol-http1/tree/438ec14fd3a8ff6746141a67afddfb64e67b94cb) | `438ec14fd3a8ff6746141a67afddfb64e67b94cb` | HTTP/1 framing, stream state and body readers. |
| [protocol-http2](https://github.com/socketry/protocol-http2/tree/55f68ebaf60e140962e28f74fe377918bc396d2d) | `55f68ebaf60e140962e28f74fe377918bc396d2d` | HTTP/2 frames, compression integration, connection/stream state and flow control. |
| [protocol-http3](https://github.com/socketry/protocol-http3/tree/f84d33c5913407d81624a4a3c3deec06e788e730) | `f84d33c5913407d81624a4a3c3deec06e788e730` | Native ngtcp2/nghttp3 integration and Ruby body-stream wrapper. |
| [async-http](https://github.com/socketry/async-http/tree/f8261403c5b44cf74df1308787efb5dc7263539d) | `f8261403c5b44cf74df1308787efb5dc7263539d` | Runtime glue, semantic message subclasses, endpoint client/server, pooling and retries. |
| [async-pool](https://github.com/socketry/async-pool/tree/4539ee837207b80d7985827bde85d58887b871bc) | `4539ee837207b80d7985827bde85d58887b871bc` | Generic singleplex/multiplex controller. |
| [io-stream](https://github.com/socketry/io-stream/tree/91437ae3802884ead34f9a3a3f3b9b2bdfdaa82a) | `91437ae3802884ead34f9a3a3f3b9b2bdfdaa82a` | Supporting buffered byte-stream boundary. |
| [io-endpoint](https://github.com/socketry/io-endpoint/tree/c331a761e4ff1e1bd340f9bf572c0b7d6d2ce1dd) | `c331a761e4ff1e1bd340f9bf572c0b7d6d2ce1dd` | Supporting connect/accept/TLS boundary. |
| [protocol-hpack](https://github.com/socketry/protocol-hpack/tree/e572ccf0d291821915bc9d2d57425c7435b7483b) | `e572ccf0d291821915bc9d2d57425c7435b7483b` | Behavioral and fixture reference for the proposed `base-http-hpack` port. |
| [Historical core-scheduler snapshot](https://github.com/core-rs/core-scheduler/tree/7f9846882e0ca594daf2ca1137c5eee015df991d) | `7f9846882e0ca594daf2ca1137c5eee015df991d` | Original body-prototype execution, cancellation, task, socket and buffer contracts. |

The detailed behavioral inspection focused on the first six projects and the local scheduler contracts. Stream/endpoint/compression packages identify dependency boundaries; their complete implementations and cryptographic internals were not audited.

The scheduler API/contracts used here match its listed HEAD. Supplementary `benchmarks/protocol-buffers/` findings and the send-zero-copy report are currently untracked local research, rather than content guaranteed by that Git revision. They inform buffer-design questions and were left untouched; they do not establish new scheduler or HTTP requirements.

## Existing architecture and what carries over

| Source area | Observed behavior | Proposed Rust consequence |
| --- | --- | --- |
| `protocol-http/lib/protocol/http/request.rb`, `response.rb` | Shared scheme/authority/method/path/version, fields, body and protocol intent; requests also carry interim-response callbacks. | One generic semantic message pair, with explicit exchange capabilities outside the head. |
| `protocol-http/.../headers.rb` | Ordered field pairs with lazy indexed interpretation and a mutable boundary between initial headers and trailers. | Port the ordered fields, optional trailer split index and lazy policy-aware index directly; adapt ownership to Rust. |
| `protocol-http/.../body/readable.rb` and body adapters | Pull reads, optional producer streaming, close/discard distinction, lengths, replay helpers and completion wrappers. | Native chunk reads, bounded collection/drain, explicit abandonment and replay behavior. |
| `protocol-http/.../body/buffered.rb`, `writable.rb`, `file.rb`, `head.rb`, `streamable.rb` | Readiness differs from exhaustion and known length; files report ready; HEAD can retain length without emitted data; `stream?` selects a bidirectional callback. | Preserve each behavioral distinction and batching purpose; retain native readiness and validate its batching guarantee and producer/duplex ownership separately. |
| `protocol-http1/.../connection.rb` | Request/response lines, persistence, open/half-closed/idle state, fixed/chunked/remainder bodies and upgraded body I/O; explicit hijacking is separate. | Incremental pure codec plus owned exchange/driver lifecycle, retained through bidirectional bodies. |
| `async-http/.../protocol/http1/connection.rb` | HTTP/1 concurrency is one; viability and reuse are separate checks. | Exclusive lease; idle selection is not a guarantee that the next network operation succeeds. |
| `async-http/.../protocol/http1/client.rb`, `response.rb` | Request output may run concurrently with response input; connection release is bound to connection completion rather than header receipt. | Explicit progress owner; lease completion spans both directions. |
| `async-http/.../protocol/http1/finishable.rb` | Input may need waiting/draining before persistence. | Bounded drain and application-body completion are explicit policy. |
| `protocol-http2/.../connection.rb`, `stream.rb`, `framer.rb`, `window.rb`, `settings_frame.rb` | Frame/state/flow-control separation; settings affect concurrency and GOAWAY affects new work. | Distinct connection state, stream state, admission and data credit. |
| `async-http/.../protocol/http2/connection.rb` | Background reader, ordered frame writes, GOAWAY removal from reuse, and completion after draining. | Explicitly run and own the driver; draining differs from closed. |
| `async-http/.../protocol/http2/input.rb`, `output.rb`, `stream.rb`, `response.rb` | Input/output have separate lifetimes; stream closure can occur from either side; response pool release follows stream termination. | Reservation belongs to exchange state; input drop alone cannot blindly release a stream or cancel a still-needed response. |
| `async-http/.../client.rb` | Endpoint-scoped connection pool and retries; returned response takes responsibility for pooled use. | Preserve endpoint-scoped client as the first public client; separate retry facts from body ownership. |
| `async-http/.../endpoint.rb`, `protocol/https.rb` | TLS/ALPN chooses a protocol handler independently of the semantic message. | Connector returns transport, negotiated protocol and metadata; pool choice follows negotiation. |
| `async-http/.../server.rb` | Accept connections, turn them into protocol servers, dispatch requests, close connection in cleanup. | Accepted-transport API first; listener and task supervision compose above it. |
| `async-http/.../internet.rb`, `proxy.rb` | Multi-origin client cache and CONNECT-based endpoint wrapping. | Later bounded cache and composable tunnel connector; neither is necessary for the first endpoint client. |
| `async-pool/.../resource.rb`, `controller.rb` | One controller combines resource usage counts, concurrency, viability, reuse, construction, availability and retirement. | Share internal lifecycle machinery while exposing different lease/reservation contracts. |
| `protocol-http3/lib/protocol/http3.rb`, `http3/stream.rb`, gemspec/readme and native binding structure | Native HTTP/3/QUIC stack with body-chunk submission and stream reset/finish. | Separate QUIC/FFI selection; reuse semantic and admission boundaries rather than assume a direct Ruby-engine port. |

The concrete version-specific message subclasses are in `Async::HTTP::Protocol::HTTP1/HTTP2`; the reviewed `Protocol::HTTP1` package itself primarily supplies connection/framing/body machinery. The conceptual layering in the discussion is still the right model.

## Bidirectional bodies, upgrades and interim responses

Focused review, 2026-10-26, using the revisions above. The Ruby implementation supports an upgrade/CONNECT model based on bidirectional bodies with managed HTTP lifecycle. The earlier Base proposal describing `101` as transferring the transport out of HTTP ownership was incorrect.

- **Common body contract:** [`Body::Stream`](https://github.com/socketry/protocol-http/blob/dcbc0fe7ad6b9a55986f003d25c0c0023228db0b/lib/protocol/http/body/stream.rb#L303) pairs input and output, with read/write, flush and independent `close_read`/`close_write`. [`Streamable`](https://github.com/socketry/protocol-http/blob/dcbc0fe7ad6b9a55986f003d25c0c0023228db0b/lib/protocol/http/body/streamable.rb#L14) advertises `stream?`, invokes a single-use block through `call(stream)`, and has a pull-read fallback using a writable output. The block controls the stream during execution and is responsible for closing it. This body-level responsibility does not detach the transport from HTTP lifecycle management. `Streamable.response(request)` associates request input explicitly; `Streamable.request` provides an input fed later by `stream(response_body)`.
- **HTTP/1 server:** [`Server#each`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/http1/server.rb#L98) writes `101` for response protocol intent, then calls `body.call(write_upgrade_body(protocol))`. Successful CONNECT similarly calls `body.call(write_tunnel_body(version))`. Ordinary bodies use chunk iteration. [`write_upgrade_body`](https://github.com/socketry/protocol-http1/blob/438ec14fd3a8ff6746141a67afddfb64e67b94cb/lib/protocol/http1/connection.rb#L538) disables persistence, flushes the completed response head and returns the retained `@stream`. It does not clear the connection's stream reference. The outer [`Server#accept`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/server.rb#L58) still closes the connection in cleanup.
- **HTTP/1 client and pool:** [`Client#call`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/http1/client.rb#L36) produces request data concurrently with reading the response. The upgraded incoming body is a connection-backed remainder reader. [`Response#pool=`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/http1/response.rb#L49) retains pooled use on the connection until completion unless it is already idle/closed; disabling persistence prevents future HTTP reuse. Base should retain its own resource charge until duplex termination and retirement.
- **HTTP/2:** [`Output#stream`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/http2/output.rb#L116) obtains input, constructs `Body::Stream.new(input, self)` and calls `@body.call(stream)`. Output still obeys protocol frame sizes and flow-control windows. Input and output can close independently; orderly input closure can reach the body without immediately cancelling its callback. This applies to ordinary streaming as well as CONNECT.
- **Hijack naming:** [`Async::HTTP::Body::Hijack`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/body/hijack.rb#L14) is also a streamable response-body adapter. Actual connection [`hijack!`](https://github.com/socketry/protocol-http1/blob/438ec14fd3a8ff6746141a67afddfb64e67b94cb/lib/protocol/http1/connection.rb#L201) is a different escape hatch: it clears `@stream` and marks the connection hijacked. The upgrade/CONNECT body path does not require that operation.
- **Interim responses:** [`Request#send_interim_response` / `on_interim_response`](https://github.com/socketry/protocol-http/blob/dcbc0fe7ad6b9a55986f003d25c0c0023228db0b/lib/protocol/http/request.rb#L90) are explicit semantic hooks. HTTP/1's [`Response.read`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/http1/response.rb#L19) loops over interim heads and invokes the hook, returning the final response, including `101`. HTTP/2's [`receive_interim_headers`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/http2/response.rb#L102) invokes the same hook; its server emits interim HEADERS without ending the stream.

The shared [`body/streamable` tests](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/test/protocol/http/body/streamable.rb) exercise echo in both directions, write half closure and normal peer closure over HTTP/1 and HTTP/2. The client combines `response.body` and an outgoing writable request body with `Body::Stream`. The [`HTTP/2 input-close regression`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/test/async/http/protocol/http2/input_close.rb) verifies that closing response input does not prevent continuing request output and eventual pool release. These sources were inspected, not executed as part of this review.

Base should port these stream/body roles and preserve interim hooks. Wire adapters change framing, admission and flow control; the exchange/driver keeps lifecycle ownership. Validate buffered input at a protocol switch, handshake flushing, rejection/early writes, independent half closure and cleanup. One reviewed Ruby detail should not be copied: HTTP/1 client `read_response_body` recognizes CONNECT tunnelling only for status `200`, although its adjacent comment correctly describes all successful 2xx responses.

Follow-up source inspection on 2026-10-26 confirmed the HTTP/2 progress roles on the upstream main branch. [`Connection#read_in_background`](https://github.com/socketry/async-http/blob/main/lib/async/http/protocol/http2/connection.rb#L107) starts a shared frame-reading task. [`Input#read`](https://github.com/socketry/async-http/blob/main/lib/async/http/protocol/http2/input.rb#L22) consumes its writable-body queue and requests flow-control window updates. [`Output#start`](https://github.com/socketry/async-http/blob/main/lib/async/http/protocol/http2/output.rb#L28) starts stream output work that pulls the supplied body and obeys available flow-control credit. Application production into a writable body is separate from this protocol output work. Base's [symmetric interface](duplex.md#symmetric-application-interface) adopts these ownership roles; exact Rust task/future storage remains to be prototyped. This follow-up inspected source without executing the Ruby suite or changing the pinned review baseline above.

## Behaviors to adapt deliberately

These are proposed design choices, not claims that the Ruby implementation must change:

- Port Base-owned messages, ordered policy-aware headers and native bodies from `protocol-http`.
- Preserve the existing `trailer!` split marker and header/trailer iteration in the same field collection.
- Separate protocol-neutral values from callbacks, responder capabilities and transport ownership.
- Replace Ruby task-local/current-task lookup with an explicit running operation context.
- Make lease/reservation ownership and terminal accounting explicit instead of relying on ensure blocks and resource identity alone.
- Reserve construction capacity before asynchronous work. The reviewed pool documents that concurrent constructors can exceed its resource limit; the proposed hard limit counts reservations first.
- Distinguish idempotent semantics, proven-unprocessed attempts and body reproduction. Do not translate method/body retry heuristics into a universal safety rule.
- Use normal certificate verification defaults for every hostname. The reviewed endpoint contains a localhost verification exemption; that is not a default to inherit.
- Bound memory, queueing and maintenance explicitly. Runtime scheduling and process-global convenience state should not become hidden protocol requirements.

## Header model comparison

The reviewed `protocol-http` collection owns both raw fields and their interpretation. `fields` retains an ordered sequence and supplied name spelling; `to_h` lazily builds a lowercase-keyed cache, parsing and merging values through `POLICY`. Singleton errors occur during indexing or an indexed addition, rather than necessarily when an unindexed collection is constructed. `cookie` joins with semicolons; `set-cookie` retains separate fields; other policies handle comma lists, dates, authentication and structured values. The trailer tail marks later fields in the same collection, with eligibility checks on `add`.

Rust `http::HeaderMap` is a multimap with validated `HeaderName`/`HeaderValue` primitives. `insert` replaces all values; `append` adds a value; `get` returns the first raw value; `get_all` retrieves all raw values. It does not automatically choose field-specific combination or singleton policies. Global iteration order is unspecified, names normalize to lowercase, and the map does not carry an initial-header/trailer boundary. `http-body` represents trailers as a separate frame. The separate `headers` crate supplies opt-in typed parsing/encoding rather than the same integrated collection model. See the primary references below.

These are useful semantics to preserve in Base. An optional conversion can retain ordinary names/values and repeated fields, but cannot promise to preserve original name spelling, total field order or native trailer/lifecycle behavior through the standard containers alone.

The split marker is sufficient for the Base field model: `trailer!` records the current field count once, and later additions occupy the trailer suffix. Preserve the same collection and lazy interpretation. Completion and failure belong to the body/exchange; no additional metadata completion states, snapshot protocol or reader/writer abstraction is required by this design.

## Regression scenarios worth porting

Paths below are relative to the indicated repository's `test/` directory. These are starting points for behavioral fixtures, not a claim that the whole Ruby suite has already been ported or run.

| Repository | Cases |
| --- | --- |
| `protocol-http1` | `protocol/http1/connection/body_close_ordering.rb`, `connection/persistent.rb`, `connection/headers.rb`, `connection/bad.rb`, `body/fixed.rb`, `body/chunked.rb`, `trailer.rb`, `upgrade.rb`, `hijack.rb`. |
| `async-http` | `protocol/http/body/streamable.rb`, `async/http/proxy.rb`, `async/http/protocol/http11/desync.rb`, `http11/invalid_trailer.rb`, `http2/input_close.rb`, `http2/rst_stream_mid_write.rb`, `http2/graceful_goaway.rb`, `http2/connection_close_with_active_streams.rb`, `http2/request_failed_on_refused_stream.rb`, `http2/response_close_on_stream_error.rb`, `client/retry.rb`. |
| `async-pool` | `async/pool/multiplex.rb`, `retire_race.rb`, `failure.rb`, `controller.rb`. |
| `protocol-http` | `protocol/http/headers.rb`, `headers/merged.rb`, `body/readable.rb`, `body/completable.rb`, `body/rewindable.rb`, `body/stream.rb`, `body/streamable.rb`, `request.rb`, `response.rb`. |

The reviewed pool retirement test covers release while asynchronous resource close is already in progress. The HTTP/2 GOAWAY test covers draining accepted streams, refusing new work, and retrying unprocessed work on another connection. These motivate generation-aware, exactly-once terminal accounting.

## Published scheduler baseline

Crates.io checks on 2026-10-26 confirmed non-yanked `0.1.0` releases of `base-scheduler`, `base-scheduler-conformance`, `base-scheduler-direct`, `base-scheduler-tokio`, `base-scheduler-native`, `base-scheduler-selector`, `base-scheduler-iouring` and `base-scheduler-timers`. See the [published interface](https://crates.io/crates/base-scheduler/0.1.0) and [release notes](../../base-scheduler/releases.md).

The current [design](../../base-scheduler/context/design.md), [I/O guide](../../base-scheduler/context/io.md) and [interface reference](../../base-scheduler/context/interface.md) replace the old transport assumptions:

- Construct scheduler-bound stream/packet/file readers and writers explicitly; protocol objects may retain these capabilities. Handles keep their creating scheduler and receive cancellation per operation.
- Use independent readers/writers for concurrent directions. Ordinary borrowed access ends before a poll returns; owned storage stays retained through backend retirement.
- `ReceiveBuffer` provides uniquely mutable completed input and can freeze into independently retained immutable storage. No new mutable multishot interface is a prerequisite for a TLS prototype.
- Flush successful stream output, await each handle's uncancellable `close()`, and drop the handles before exclusively closing the underlying resource. Successful handle closure establishes backend retirement.
- Preserve `Send` futures, owning task handles, explicit resource limits and monotonic `Clock` timing. Routing/registration/multishot strategy remain backend details.

The HTTP prototype now imports `base-*` registry packages. Its updated file-body test borrows a bound `FileReader`, passes cancellation per read, and explicitly retires the reader before exclusive file closure. The [implementation notes](implementation.md) distinguish that evidence from the historical test below.

## Historical scheduler constraints

The initial [research readme](../../base-scheduler-research/readme.md) and [behavioral contracts](../../base-scheduler-research/context/design.md) described `SocketRead`, `SocketWrite`, `SocketClose`, `Clock`, `Spawn`, `Cancellation`, `IoBuf`, `IoBufMut`, and `BufferResult`. The following constraints record that review rather than the released interface:

- Operation context is explicit; a reusable protocol object must not retain a scheduler selected by construction or a previous operation.
- I/O futures are `Send`; resources are associated types with documented compatibility. Spawn has stronger owned `'static` requirements.
- Owned I/O returns original buffer owners on normal error/cancellation completion; dropping the future does not establish that backend access ended.
- Read/write operations are partial transfers. Completion and cleanup ordering are the caller's responsibility.
- Close/shutdown are uncancellable cleanup operations; establish completion and exclusive ownership first.
- Spawned task handles own lifetime and cancel/destroy on drop rather than detach.
- Timed policy uses the supplied monotonic `Clock`, not an unrelated wall clock.

The historical [protocol-buffer findings](../../base-scheduler-research/benchmarks/protocol-buffers/findings.md) favor bounded reusable ownership and optional sharing, while recording the then-missing generic mutable multishot lease. Their traffic is protocol-shaped synthetic framing, including an H3-shaped UDP case, not a real HTTP/TLS/QUIC implementation. They do not establish HTTP throughput or a universal zero-copy policy, nor a gap in the subsequently released `ReceiveBuffer` contract.

## Public specifications and integration candidates

These references constrain protocol compatibility or identify candidates. They do not by themselves choose the Rust API or certify a dependency.

| Reference | Relevance |
| --- | --- |
| [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) | Common semantics, methods, fields, informational responses and retry behavior. |
| [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html) | HTTP/1 syntax, framing, connection management and ambiguous-message handling. |
| [RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html) | HTTP/2 state, admission, flow control, error scope and graceful shutdown. |
| [RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html) | HTTP/3 mapping and its distinct QUIC/control/compression requirements. |
| [RFC 8441](https://www.rfc-editor.org/rfc/rfc8441.html), [RFC 9220](https://www.rfc-editor.org/rfc/rfc9220.html) | Extended CONNECT for HTTP/2 and HTTP/3; reserve compatible protocol intent and bidirectional body semantics. |
| [`http`](https://docs.rs/http/latest/http/), [`Request<B>`](https://docs.rs/http/latest/http/request/struct.Request.html) | Shared message and scalar types independent of the execution engine; arbitrary `B` admits a native Base body. |
| [`http::Uri`](https://docs.rs/http/latest/http/uri/struct.Uri.html), [`Extensions`](https://docs.rs/http/latest/http/struct.Extensions.html) | Representations relevant to optional conversion; Base retains its native request components and explicit exchange capabilities. |
| [`http::HeaderMap`](https://docs.rs/http/latest/http/header/struct.HeaderMap.html#iteration-order) | Multiple raw values are supported, but global field order is not guaranteed and there is no integrated Socketry-style field policy or trailer boundary. Relevant to adapter fidelity. |
| [`http::HeaderName`](https://docs.rs/http/latest/http/header/struct.HeaderName.html), [`HeaderValue`](https://docs.rs/http/latest/http/header/struct.HeaderValue.html) | Validated name/value primitives; names normalize to lowercase and values support opaque bytes. |
| [`http_body::Frame`](https://docs.rs/http-body/latest/http_body/struct.Frame.html) | Trailer fields are represented as a separate frame rather than a mutable tail in the initial header collection. |
| [`headers`](https://docs.rs/headers/latest/headers/) | Optional typed field parsers/encoders; distinct from the collection-wide Socketry policy and lazy interpretation model. |
| [`http_body::Body`](https://docs.rs/http-body/latest/http_body/trait.Body.html) | Data/trailer frame interoperability; scheduler/cancellation binding requires a deliberate adapter. |
| [`http-body-util`](https://docs.rs/http-body-util/latest/http_body_util/) | Existing simple body implementations and combinators; consider reuse through the interoperability adapter. |
| [`hyper::rt`](https://docs.rs/hyper/latest/hyper/rt/index.html) | Runtime abstraction reference and performance comparison context. Hyper is excluded from the protocol implementation dependency path; Base drives owned-buffer I/O through its scheduler. |
| [`tower-service::Service`](https://docs.rs/tower-service/latest/tower_service/trait.Service.html) | Existing request/response middleware interface; its call/readiness signatures need explicit context-binding analysis before adopting it as Base's native service contract. |
| [`h2`](https://docs.rs/h2/latest/h2/), [client handshake](https://docs.rs/h2/latest/h2/client/fn.handshake.html) | Independent implementation reference and potential interoperability peer. Engine reuse through an I/O adapter is excluded; focused HPACK extraction remains an option within `base-http-hpack`. |
| [`rustls::Connection`](https://docs.rs/rustls/latest/rustls/enum.Connection.html), [unbuffered API](https://docs.rs/rustls/0.23.46/rustls/unbuffered/index.html) | Recommended TLS engine for `base-tls`; unbuffered processing exposes caller-managed storage and requires an ownership/progress prototype. |

The initial body port retains MIT attribution in source and [the license](../license.md). The [implementation notes](implementation.md) record its exact source revision, scope and scheduler evidence. Other reviewed components remain design work.
