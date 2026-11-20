# HTTP/2

Accepted 2026-11-20, from a review of Async::HTTP and `protocol-http2`; not yet implemented. The open questions of the first draft were decided on 2026-11-20, and each decision is recorded in its section. The [roadmap](roadmap.md) orders the work, the [proposal](proposal.md#http2) and [duplex design](duplex.md#protocol-specific-progress-owners) describe the progress architecture, and the [client/connection taxonomy](client-connection.md) defines admission and negotiation.

## Reference implementation and its defects

This section is background for the requirements that follow and states no requirement of its own.

Async::HTTP is the model for the connection contract and its HTTP/2 integration, and `protocol-http2` for the frame, connection, stream and flow-control separation. References are to upstream main as of 2026-11-20: [async-http `b7029c7`](https://github.com/socketry/async-http/tree/b7029c722791711588354016891297df21264272) (0.105.0), [protocol-http2 `7d5a7db`](https://github.com/socketry/protocol-http2/tree/7d5a7db509e1b91783a0f57922cf16b6b8911886) (0.29.2) and [protocol-hpack `e572ccf`](https://github.com/socketry/protocol-hpack/tree/e572ccf0d291821915bc9d2d57425c7435b7483b) (1.5.1). The first draft reviewed older local checkouts, async-http `0a3cfd6`, protocol-http2 `3914bc5` and protocol-hpack `7677643`, which were 46, 28 and 1 commits behind these; every reference was then checked again at the revisions above. The review inspected source and ran no Ruby test suite, apart from a one-off timing of HPACK integer decoding and a check of truncated HPACK input, both at protocol-hpack `7677643`, whose decoder is unchanged at `e572ccf`.

The following carry over: one write path, so HPACK encoder state follows wire order; idle and closed streams derived from identifier ranges, so retained state is proportional to active streams; receive credit returned when the application reads, in Async::HTTP's [`Input#read`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/input.rb#L26-L30); RFC 7540 priorities dropped and `SETTINGS_NO_RFC7540_PRIORITIES` advertised; extended CONNECT mapped onto the same semantic `protocol` used by HTTP/1 upgrades; and the regression scenarios listed in the [source review](source-review.md#regression-scenarios-worth-porting).

The following must not carry over. Each is addressed by a requirement below, whether or not upstream has since fixed it.

| Reviewed behavior | Source | Upstream status |
| --- | --- | --- |
| Exceeding the local stream limit sends GOAWAY for the whole connection instead of refusing the stream. | [`connection.rb#L528-L536`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L528-L536) | Present. |
| Received DATA is never checked against the local window, and the connection window is replenished on arrival. | [`flow_controlled.rb#L49-L60`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/flow_controlled.rb#L49-L60), [`connection.rb#L431-L432`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L431-L432) | Present. |
| A HEADERS frame received on a known stream in a state that resets or ignores it is not decoded, desynchronizing the HPACK dynamic table. | [`stream.rb#L295-L325`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/stream.rb#L295-L325) | Present. |
| A late HEADERS frame on a stream this endpoint already closed is a connection error. | [`connection.rb#L512-L526`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L512-L526) | Fixed for locally initiated streams, which are now decoded and discarded; still a connection error for a peer-initiated stream this endpoint closed. |
| A received GOAWAY closes the connection at once, even with `NO_ERROR`. | [`connection.rb#L269-L297`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L269-L297) | Fixed: streams at or below the last identifier drain, and streams above it fail as refused. |
| Neither library sends a graceful GOAWAY; `protocol-http2` sends GOAWAY only for connection errors. | [`connection.rb#L223-L230`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L223-L230) | Present. |
| CONTINUATION is limited to eight frames, not by bytes: up to nine maximum-size frames per header block, 9 MiB with Async::HTTP's 1 MiB frames. `SETTINGS_MAX_HEADER_LIST_SIZE` defaults to unlimited and is neither advertised nor enforced. | [`continuation_frame.rb#L12-L66`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/continuation_frame.rb#L12-L66), [`settings_frame.rb#L43`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/settings_frame.rb#L43) | Present. |
| HPACK integer decoding has no length or value bound; decoding time grew quadratically with input length when measured. | [`decompressor.rb#L62-L76`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L62-L76) | Present. |
| A stream reset by the client does not cancel its server handler, which runs in its own task with stop deferred, so the stream limit does not bound handler work (CVE-2023-44487, rapid reset). | [`server.rb#L53-L83`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/server.rb#L53-L83), [`request.rb#L116-L120`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/request.rb#L116-L120) | Present. |
| Cancelling an output task for a reason other than a stream reset closes the stream with END_STREAM, so a truncated body appears complete. | [`output.rb#L129-L149`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/output.rb#L129-L149) | Fixed: `passthrough` now rescues `Async::Cancel`, so cancellation resets the stream with `INTERNAL_ERROR`. |
| Local limits apply only after the peer acknowledges SETTINGS, so a peer that never acknowledges is unlimited. | [`settings_frame.rb#L157-L229`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/settings_frame.rb#L157-L229) | Present. |
| Parsing failures that are not protocol errors end the reader without GOAWAY: an invalid `content-length` raises `ArgumentError` from [`Integer`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/request.rb#L78-L81), which [`process_headers`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/stream.rb#L71-L89) does not rescue, and the reader's [generic rescue](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/connection.rb#L123-L137) closes the connection. | [`connection.rb#L223-L230`](https://github.com/socketry/protocol-http2/blob/7d5a7db509e1b91783a0f57922cf16b6b8911886/lib/protocol/http2/connection.rb#L223-L230) | Present. |
| An HPACK integer truncated within its continuation bytes is accepted as the smaller value read so far, because [`read_integer`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L62-L76) stops at the end of input without an error; for example `0xFF 0x80` is read as index 126 rather than rejected as truncated. Other truncations raise `CompressionError`, which sends GOAWAY. | [`decompressor.rb#L62-L76`](https://github.com/socketry/protocol-hpack/blob/e572ccf0d291821915bc9d2d57425c7435b7483b/lib/protocol/hpack/decompressor.rb#L62-L76) | Present. |
| Received field validation rejects `connection`, uppercase names and unknown pseudo-fields, but passes CR, LF and NUL in values and `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade` and `te`. | [`request.rb#L56-L89`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/request.rb#L56-L89), [`stream.rb#L47-L57`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/stream.rb#L47-L57) | Present. |

## Connection contract and protocol selection

### Requirement

`base-http-protocol` factories must construct a selected *connection*, not a pair of HTTP/1 codec directions. Selection returns a closed enum over the engines compiled in, for example `ServerConnection<R, W>` with `Http1` and `Http2` variants, and the client equivalent. The client and server dispatch on that enum; adding an engine adds a variant and a feature, not a trait object or a second selection path.

`HttpProtocols` must describe the enabled engines in preference order and the plaintext policy, in addition to the existing `require_alpn`:

- ALPN offers come from the enabled engines in preference order: `h2` before `http/1.1` when both are enabled. Negotiation keeps the existing rules: an unknown or disabled identifier is rejected, and a missing identifier selects HTTP/1 only under compatibility policy and only when HTTP/1 is enabled; otherwise it is rejected.
- A plain TCP client uses HTTP/1, or HTTP/2 with explicit prior knowledge. It never sends a preface to a peer that was not configured for it.
- A plain TCP server uses a configured engine, or detects the HTTP/2 connection preface. Detection inspects at most the 24 preface bytes, decides as soon as the bytes read cannot begin the preface, and gives every inspected byte to the selected engine. Detection is bounded by the preface timeout in [default limits](#default-limits) and by the connection's cancellation.
- The HTTP/1 `Upgrade: h2c` mechanism, which RFC 9113 deprecates, is not implemented.

Errors in `base-http-server` and `base-http-client` must not name a single engine's error type. A protocol error carries the engine that produced it. Per-engine options are separate fields, as `Options::http1` is now.

M6 delivers the enum, the engine list, the plaintext policy types and the version-neutral errors and options, with HTTP/1 as the only engine. Client prior knowledge and server preface detection take effect when the `http2` engine is added in M7.

### Rationale

Fixing the connection contract first, as its own milestone before HTTP/2, with a closed enum, an engine list in preference order, a bounded plaintext policy, no h2c Upgrade and version-neutral errors, was decided on 2026-11-20. Async::HTTP defines [one connection contract](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol.rb#L13-L28) for every engine: a client connection answers `concurrency`, `viable?`, `reusable?` and `call`; a server connection runs `each`. Its plaintext [`Protocol::HTTP`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http.rb#L39-L56) peeks for the preface without consuming it. The current `HttpProtocol::client` and `server` return HTTP/1 reader/writer pairs (`crates/protocol/src/lib.rs`), which HTTP/2's single connection driver cannot satisfy. Async::HTTP's [`Protocol::HTTPS`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/https.rb#L49-L60) dispatches through the global `HANDLERS` table rather than the instance's, so configured and advertised protocols can disagree; one enabled set for both offers and selection prevents that. A closed enum keeps dispatch static, as the [taxonomy](client-connection.md#one-http-client-pool-contract) records.

### Scope

`HttpProtocols`, `HttpProtocol` and the factories in `base-http-protocol`; the error and option types and connection dispatch in `base-http-server` and `base-http-client`; plaintext and TLS establishment on both paths.

### Validation

Inspect the factory signatures for codec-pair returns and the client and server error types for engine-specific variants. Tests must cover the ALPN offer order for each enabled set, rejection of disabled identifiers, a missing identifier with and without HTTP/1 enabled, server detection given the preface or an HTTP/1 request one byte at a time, a request beginning with `P`, and early end of input during detection, with the selected engine receiving every inspected byte, and a detection stalled by the peer ending at the preface timeout. An HTTP/1-only build must still compile and pass its existing suites without the `http2` feature.

## Packages and dependencies

### Requirement

- `base-http-hpack` depends on neither `base-scheduler` nor `base-http`. Its public API owns its encoder, decoder, options, field references and errors; no upstream type appears in it as an alias or re-export. Its integer and Huffman primitives may be public for `base-http-qpack`, as a narrow module documented for that use.
- `base-http-hpack` is a native Rust port of `protocol-hpack`'s behavior, written to the Base interface rather than translated literally, with no production dependency on another HPACK implementation. Ported code and imported fixtures keep their MIT attribution and source revision. An independent Rust codec may be a development dependency, as a differential oracle in tests and fuzzing.
- `base-http2` depends on `base-http`, `base-http-hpack`, `base-message`, for interpreting sections by static-table index as [static table indices](#static-table-indices) requires, `base-endpoint` stream capabilities and `base-scheduler`. It must not depend on `h2`, Hyper, `http`, `http-body`, Tokio, or a concrete scheduler; those may be development dependencies for interoperability tests.
- `base-http-protocol`, `base-http-server` and `base-http-client` enable HTTP/2 through an `http2` feature. Without it, none of them depends on `base-http2` or `base-http-hpack`.

### Rationale

This applies the [component evaluation](components.md#recommended-direction) and [crate structure](proposal.md#3-crate-structure) to HTTP/2, and the runtime-dependency rule in the repository's agent instructions. `base-message` depends on no HTTP, scheduler or runtime crate ([its scope](https://github.com/base-rs/base-message/blob/main/design/scope.md)), so depending on it keeps that rule. The native port, with an independent codec only as a test oracle, was decided on 2026-11-20, because the decoder must deliver fields to a callback while bounding integers, strings and section size as it decodes, and reuse output storage. Of the evaluated candidates, `h2`'s codec is private and coupled to `http` types, `httlib-hpack` returns owned name and value vectors, and `fluke-hpack`'s encoder lacks Huffman output.

### Scope

The manifests and public APIs of these packages.

### Validation

Inspect `cargo tree -e normal` for each package with and without `http2`, the public API of `base-http-hpack` for upstream types, and its source for attribution of ported code and fixtures.

## Protocol core and connection driver

### Requirement

Frame decoding and encoding, connection state, stream state, flow control and HPACK state must be usable without a scheduler: given input bytes and the current time they produce events and output bytes, so fragmented, coalesced and adversarial input can be tested deterministically. A separate driver owns the transport directions.

Each connection has one driver, which:

- serializes all outgoing frames, never interleaving another frame inside a HEADERS/CONTINUATION sequence;
- keeps reading and processing control frames while any stream waits for application input, application consumption, flow-control credit or transport capacity; a full stream queue never parks the receive loop;
- awaits retirement of its transport directions before the socket is closed, as HTTP/1 does.

The client and server run connection drivers in one of two modes, chosen by the application per server and per client:

- Scoped, the default: drivers are polled within the server's or client's running operation, as HTTP/1 connections are now, without `Spawn`, so handlers and bodies may borrow from the caller. Connections interleave on whichever worker polls that operation.
- Spawned, opt-in: with the scheduler's `Spawn` and `Send + 'static` handlers and bodies, each connection's driver is its own task, so connections are scheduled independently across workers. Server-wide and client-wide supervision keep admission, cancellation and joined retirement, and retain every task handle.

M6 provides both modes for HTTP/1; HTTP/2 uses them unchanged.

Ready streams share connection output round-robin; one stream cannot monopolize output while others have data and credit. [RFC 9218](https://www.rfc-editor.org/rfc/rfc9218.html) urgency and incremental scheduling is not implemented: received PRIORITY_UPDATE frames are validated and otherwise ignored, counted in the shared frame budget, and the `priority` field reaches applications as an ordinary field. Whether to schedule by urgency is decided later from a page-load benchmark resembling a browser's requests, and applies to HTTP/3 as well.

On the server, in either mode, the connection's driver owns and polls the handler futures of its concurrent streams, bounded by the local stream limit; it does not spawn a task per stream. Handlers of one connection run concurrently, each awaiting independently and woken individually, and each has its own cancellation. In the scoped mode HTTP/2 adds no bound to the handler contract, the shared request interface of [middleware](middleware.md#shared-request-interface): no `'static` bound on handlers or bodies, and returned bodies may wrap the request body. Spawning a task per stream, for a few connections each carrying many CPU-bound streams, is not part of this requirement; whether to add it as a further opt-in is decided later by benchmark, and the design must leave room for it.

### Rationale

HTTP/2 multiplexes independent exchanges onto one transport, so the receive loop, ordered output and stream progress have different owners. These are the progress roles in the [duplex design](duplex.md#protocol-specific-progress-owners) and [proposal](proposal.md#http2). A deterministic core follows the [HTTP/1 framing requirement](proposal.md#stream-seam) and the planned QUIC core.

A sans-I/O core with one driver per connection, server handler futures polled by the connection's driver rather than spawned per stream, round-robin output with RFC 9218 priorities ignored until a page-load benchmark, and the two driver modes were decided on 2026-11-20. Polling handlers within the connection's driver gives the same concurrency as Falcon's task per request, whose fibers share one reactor thread, while keeping connection state on one core and avoiding a task allocation and cross-thread wake-ups per request. The current server and client poll every connection within one `FuturesUnordered` (`crates/server/src/lib.rs`, `crates/client/src/driver.rs`); the scoped mode keeps that shape so borrowed handlers and bodies remain usable, and the spawned mode provides parallelism across connections for applications that can meet its bounds. Either way one connection is capped at one core at a time, which matters for a few heavily multiplexed connections, such as a proxy's upstream; a handler can still spawn its own CPU-bound work.

### Scope

`base-http2` and its integration into the client and server, including the driver modes of `base-http-server` and `base-http-client`.

### Validation

Core tests run without a scheduler and deliver each test input byte by byte and fully coalesced. A test stalls one stream's consumer with a full queue while PING, SETTINGS and other streams continue. A test with two streams and limited credit shows both make progress. A test runs handlers that await concurrently on one connection and shows their total time is that of the slowest, not the sum, and that a reset cancels only its own handler. Inspect the server for a task spawned per stream. In the scoped mode, a server and a client run with a handler and a body borrowing from the caller's scope and no `Spawn` capability. In the spawned mode, connections of one server and of one client make progress on more than one worker, and shutdown joins every connection task.

## Flow control and retained memory

### Requirement

- Received DATA beyond the stream or connection receive window in effect is a `FLOW_CONTROL_ERROR`, of stream or connection scope respectively. Padding counts against the windows.
- Receive credit is returned when the application's read consumes the data, that is, by the read that hands the data over, and never on arrival. Data discarded by the endpoint, for a reset or closed stream or abandoned input, returns its connection credit immediately.
- Unread buffered data per stream never exceeds the stream window in effect, and per connection never exceeds the connection window in effect. Data the application keeps after reading it is the application's memory and is not charged to the connection. Stream admission and flow-control credit are accounted separately.
- A change to the peer's `SETTINGS_INITIAL_WINDOW_SIZE` adjusts every open stream's send window by the difference. A value above 2³¹−1, or a change that makes any window exceed 2³¹−1, is a connection `FLOW_CONTROL_ERROR`. A WINDOW_UPDATE that makes a window exceed 2³¹−1 is a `FLOW_CONTROL_ERROR`, and one with a zero increment a `PROTOCOL_ERROR`, each of the scope of the window it names.
- When this endpoint lowers its initial window, the peer may send against the previous window until it has processed the change; the receive window in effect follows the rule for pending settings under [abuse limits](#abuse-limits).
- Data on a stream whose input has completed or been abandoned is discarded within the window, and a reset is issued when policy stops the input.

### Rationale

Returning credit on the consuming read, never on arrival, with unread data bounded by the advertised windows and data kept after reading left as the application's memory, was decided on 2026-11-20: what matters is backpressure. Async::HTTP returns credit in the same place, in [`Input#read`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/input.rb#L26-L30), but `protocol-http2` neither enforces receive windows nor ties the connection window to consumption, so a peer, even a compliant one, can make the server buffer up to every stream window at once. The [proposal](proposal.md#http2) requires admission and credit to be separate limits. Window arithmetic follows [RFC 9113 §6.9](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.9), including §6.9.3 on reducing the window.

### Scope

`base-http2` input bodies, receive windows and WINDOW_UPDATE emission and receipt.

### Validation

Tests send DATA that overruns each window, retain received chunks without reading, read and keep chunks, and drop an incoming body with data buffered. Assert the error scope and code, that no WINDOW_UPDATE is sent before reads, that a read returns credit for what it handed over, and that unread buffered bytes stay within the windows in effect. Tests cover WINDOW_UPDATE overflow and zero increments on a stream and on the connection, an initial window change that overflows an open stream's window, and DATA sent against the previous window after this endpoint lowers it.

## Header blocks and field validation

### Requirement

The HPACK decoder bounds integer length and value and string length, and reports the decoded field section size and field count incrementally as it decodes, rather than after decoding a block. Truncated input, invalid Huffman padding, EOS in a string, an out-of-range index, an integer or string over its bound, and a table size update above the local limit or after the first field representation of a block are decoding errors with typed causes. HTTP/2 maps every decoding error to a `COMPRESSION_ERROR` connection error.

The HTTP/2 core:

- limits a header block, including all CONTINUATION frames, to a configured byte size, exceeding which is a connection error, and limits the CONTINUATION frames per block as [abuse limits](#abuse-limits) require; any other frame interleaved within a block is a connection `PROTOCOL_ERROR`;
- decodes every received header block, including blocks for streams it is about to reset, refuse or ignore, so the decoder's dynamic table stays synchronized;
- advertises `SETTINGS_MAX_HEADER_LIST_SIZE` and enforces it, and the fields-per-section limit, on received sections. Past either limit the decoder keeps processing the block, updating its dynamic table, without delivering further fields, so its work stays proportional to the block's bytes. The server answers such a request with 431, the client fails such a response with a stream reset, and the connection continues;
- uses the smaller of the peer's `SETTINGS_HEADER_TABLE_SIZE` and a local cap for the encoder table;
- validates received sections before exposing a request or response: pseudo-fields precede regular fields, appear at most once, and are known and permitted for the message type; names contain no bytes 0x00–0x20, 0x41–0x5A (uppercase) or 0x7F–0xFF, and no colon except the leading colon of a pseudo-field; values contain no NUL, CR or LF and do not begin or end with SP or HTAB; the connection-specific fields `connection`, `keep-alive`, `proxy-connection`, `transfer-encoding` and `upgrade`, and `te` other than `trailers`, are rejected; `content-length` is ASCII decimal digits only, repeated values agree, and the received data length must match it; ordinary requests carry `:method`, `:scheme` and a non-empty `:path`; CONNECT carries `:method` and `:authority`, with `:scheme` and `:path` omitted; extended CONNECT carries `:protocol`, `:scheme`, `:path` and `:authority` and is accepted only after this endpoint advertised `SETTINGS_ENABLE_CONNECT_PROTOCOL`; responses carry `:status` and no other pseudo-field;
- maps pseudo-fields to `RequestMetadata::{method, scheme, authority, path, protocol}` and `:status` to `ResponseMetadata::status`, sends `content-length` as resolved by `declared_length`, and resets the stream when a body contradicts a declared length, as the [implementation notes](implementation.md#semantic-core) require of HTTP/2. Base fields keep the case they were constructed with, so the encoder lowercases names on send. Outgoing messages are checked against the other rules before any frame is written, and an invalid response gets the same bodiless 500 and handler-error report as HTTP/1, without closing the connection.

Malformed messages are stream errors of type `PROTOCOL_ERROR`, as [RFC 9113 §8.1.1](https://www.rfc-editor.org/rfc/rfc9113.html#section-8.1.1) requires.

### Rationale

The reviewed HPACK decoder and CONTINUATION handling allow unbounded CPU and memory per header block, and the reviewed field validation passes CR, LF and most connection-specific fields, which a proxy forwarding to HTTP/1 would turn into smuggling. Undecoded blocks corrupt the shared compression state ([RFC 9113 §4.3](https://www.rfc-editor.org/rfc/rfc9113.html#section-4.3)), so a section over the list size cannot simply stop decoding; discarding fields while still processing the block keeps the table synchronized and lets the oversized section fail alone, as [RFC 9113 §10.5.1](https://www.rfc-editor.org/rfc/rfc9113.html#section-10.5.1) permits. An indexed reference costs constant work when the field is discarded, so the block's byte limit bounds the work. Field rules follow [RFC 9113 §8.2-§8.3](https://www.rfc-editor.org/rfc/rfc9113.html#section-8.2) and §8.5, and extended CONNECT follows [RFC 8441 §4](https://www.rfc-editor.org/rfc/rfc8441.html#section-4).

### Scope

`base-http-hpack`, and header decoding, validation and encoding in `base-http2`.

### Validation

Fuzz the HPACK decoder and the frame decoder. Import the `protocol-hpack` test cases and the [hpack-test-case](https://github.com/http2jp/hpack-test-case) corpus as conformance fixtures; record their provenance and licenses. Tests cover integers longer than the bound, oversized strings, a section over the list size and one over the field count, each followed by a request on the same connection that uses the dynamic table, a block split across CONTINUATION frames past the byte limit, an interleaved frame, a block received for a stream being refused, each field validation rule, outgoing uppercase names, and a body shorter or longer than its declared length. Record decoding time for a maximum-size block.

## Static table indices

### Requirement

Fields named by HPACK's static table are handled by index, without hashing or comparing names, in both directions:

- The HPACK decoder reports, for each decoded field, whether its name came from the static table and at which index, in addition to the name and value bytes. Names from the dynamic table or sent literally are reported as text.
- Pseudo-fields named by static-table index are mapped into `RequestMetadata` and `ResponseMetadata` by a `match` on the index; literal pseudo-field names are matched as text.
- Where `base-http2` interprets a received section into a typed [`base-message`](https://github.com/base-rs/base-message) `FieldSection`, it uses the indexed path: statically indexed fields are passed as `FieldName::Indexed` entries of a `const` table built with `static_table_entries` over HPACK's 61 entries, so they are dispatched by declaration id, and other fields as `FieldName::Text`. Which sections `base-http2` interprets this way is chosen when that part is implemented, and benchmarked then.
- The HPACK encoder finds a name's static-table index with dispatch generated at compile time, branching on the name's length and first byte before comparing the few candidates, not with a hash table.

`Field` does not retain the static index: raw `Fields` delivered to applications keep their current representation.

### Rationale

The aim is to extract common fields without comparing strings. `base-message` provides compile-time classification of a protocol's static table for this purpose and leaves each protocol crate to define its own table. Its [performance requirements](https://github.com/base-rs/base-message/blob/main/design/performance.md) record two measurements: interpreting a whole section cost 2.6 to 2.9 ns per field with static-table ids and 4.7 to 5.0 ns by name; separately, dispatching one name cost 12.2 ns through a hash table of lowercased names and 5.7 ns with a jump on length and first byte. Pseudo-fields are protocol control data outside `base-message`'s scope, so the core maps them itself.

The user decided on 2026-11-20 that the decoders keep reporting static indices, that where a protocol crate interprets a section it uses the indexed path, and that which sections `base-http2` and `base-http3` interpret by index is deferred until that part is implemented and benchmarked. The user also decided on 2026-11-20 that `Field` does not keep the index for now.

Two future opportunities, not planned, were recorded on 2026-11-20:

- `Field` could retain a well-known name's identity after decoding, independent of protocol and mapped to each protocol's static table, so that interpretation of delivered raw fields and re-encoding, such as when proxying between HTTP/2 and HTTP/3, also avoid comparing names. HPACK's and QPACK's indices differ, so the identity could not be either table's index.
- A dynamic-table entry could remember the static name index it was inserted with, so that later references to it are also dispatched by index. Encoders commonly insert repeated fields, such as `user-agent` or `cookie`, with a statically indexed name and then refer to them through the dynamic table, so on a long-lived connection most repeated fields would otherwise arrive as text.

A benchmark of forwarding and interpretation would decide whether to explore either.

### Scope

The HPACK decoder's output and the encoder's static lookup in `base-http-hpack`, and pseudo-field mapping and any section interpretation in `base-http2`.

### Validation

Tests decode each static-table name as an indexed field and as a literal, and check that both produce the same metadata and, in each section `base-http2` interprets, the same declaration. Inspect the encoder's and core's static-table lookups for hashing, and each section interpretation in `base-http2` for names passed as text when the decoder reported a static index. When a section is first interpreted by index, measure it by index against by name with `base-message`'s interpretation benchmark workload and record the result.

## Stream lifecycle, errors and cancellation

### Requirement

- Stream and connection errors stay distinct. A stream error resets one stream; only connection errors send GOAWAY and close.
- A HEADERS frame that would exceed the local stream limit is decoded and refused with `REFUSED_STREAM`, which the peer may retry.
- Frames received for a closed stream are minimally processed and discarded: header blocks are decoded, and DATA counts against the connection window and returns its credit. Closed streams are derived from identifier ranges rather than retained, so this applies to every closed stream, whether this endpoint reset it or it completed.
- Cancellation, exchange abandonment and body failure always reset the stream (`CANCEL` or `INTERNAL_ERROR`); they never send END_STREAM. Only a body that ends normally ends the stream normally.
- A stream reset by the peer fails the stream's incoming body read, and on the server cancels the handler and its response body. The server's per-stream cancellation is linked to the connection's.
- A client dropping or abandoning a response body resets the stream, unless the response has already ended. Pool admission is released exactly once, after the driver has processed the reset or normal completion.
- Interim responses are HEADERS without END_STREAM; 101 is never sent or accepted. Responses and requests carrying a protocol map to extended CONNECT as [protocol switching](protocol-switching.md) requires, including dropping HTTP/1 Upgrade offers that have no extended CONNECT mapping, such as `h2c`, so the request is sent as an ordinary request. Trailers are a final HEADERS frame with END_STREAM, carrying the body's `BodyFrame::Headers`.
- Server push is disabled: the client advertises `SETTINGS_ENABLE_PUSH` 0 and treats PUSH_PROMISE as a connection `PROTOCOL_ERROR`; the server never sends PUSH_PROMISE.
- Stream identifier exhaustion stops new admission and starts graceful shutdown.

### Rationale

These follow [RFC 9113 §5](https://www.rfc-editor.org/rfc/rfc9113.html#section-5) and the [proposal's](proposal.md#http2) separation of input completion from stream abortion. [§5.1](https://www.rfc-editor.org/rfc/rfc9113.html#section-5.1) requires minimal processing of frames received after this endpoint sends RST_STREAM and permits it for every closed stream. Dropping Upgrade offers without an extended CONNECT mapping when a request crosses to HTTP/2 was decided on 2026-11-20. The reviewed implementation sends GOAWAY for excess streams and for late HEADERS on peer-initiated streams, and leaves handlers running after a reset; until a recent fix it also reported truncated output as complete on cancellation.

### Scope

Stream state, error mapping, body adapters and handler supervision in `base-http2`, `base-http-server` and `base-http-client`.

### Validation

Tests: a request over the stream limit receives `REFUSED_STREAM` and the connection stays usable; trailers and DATA arrive after a local reset and after normal completion and are discarded, with the dynamic table and connection window intact; cancelling or failing a response body mid-transfer produces RST_STREAM and never END_STREAM; a peer reset cancels a running handler; dropping a client response body releases admission exactly once; interim and trailer forwarding; a request with an `h2c` Upgrade offer sent as an ordinary request; PUSH_PROMISE to a client. Port the Async::HTTP [`rst_stream_mid_write`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/test/async/http/protocol/http2/rst_stream_mid_write.rb), [`response_close_on_stream_error`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/test/async/http/protocol/http2/response_close_on_stream_error.rb), [`connection_close_with_active_streams`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/test/async/http/protocol/http2/connection_close_with_active_streams.rb) and [`input_close`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/test/async/http/protocol/http2/input_close.rb) scenarios.

## Abuse limits

### Requirement

Each of the following is bounded by an option with a finite default, listed under [default limits](#default-limits); exceeding a limit sends GOAWAY with `ENHANCE_YOUR_CALM` and closes the connection:

- streams reset by the peer, or reset locally because of peer behavior, that do not complete normally, including streams refused before admission (rapid reset), in a token bucket;
- PING, SETTINGS, PRIORITY and PRIORITY_UPDATE frames, empty DATA frames without END_STREAM, and unknown frame types, in one shared token bucket;
- CONTINUATION frames per header block, and settings per SETTINGS frame;
- queued control output; acknowledgments waiting for transport capacity are a bounded queue, not unbounded memory.

Until the peer acknowledges a change to a local setting, the endpoint applies the more restrictive of the acknowledged and pending values where exceeding the limit has a non-fatal response: `SETTINGS_MAX_CONCURRENT_STREAMS`, refused with `REFUSED_STREAM`, and `SETTINGS_MAX_HEADER_LIST_SIZE`, answered as header blocks require. For `SETTINGS_INITIAL_WINDOW_SIZE`, `SETTINGS_MAX_FRAME_SIZE` and `SETTINGS_HEADER_TABLE_SIZE` it applies the acknowledged value, and switches to the new value on acknowledgment. The connection preface, the peer's first SETTINGS, and each SETTINGS acknowledgment must arrive within a configured time measured with the scheduler `Clock`.

### Rationale

[CVE-2023-44487](https://nvd.nist.gov/vuln/detail/CVE-2023-44487) and the HTTP/2 CONTINUATION flood showed that ordinary limits on concurrent streams do not bound work. The reviewed implementation has none of these limits, and enforces its own limits only after acknowledgment. A peer may legitimately use the previous window, frame size or table size until it processes a change ([RFC 9113 §6.9.3](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.9.3)), so treating that use as an error would fail compliant peers; the acknowledgment deadline bounds how long the previous value lasts. WINDOW_UPDATE frames are not counted, because a fast download legitimately produces many of them. Budgets derived from nghttp2's defaults were decided on 2026-11-20. [RFC 9113 §10.5](https://www.rfc-editor.org/rfc/rfc9113.html#section-10.5).

### Scope

`base-http2` options and connection state.

### Validation

A test for each limit shows GOAWAY with `ENHANCE_YOUR_CALM` at the threshold and normal operation below it. A peer that never acknowledges SETTINGS meets the stricter stream and header list limits, and a peer sending against the previous window or frame size before acknowledging a reduction is not treated as an error. A peer that never sends the preface or SETTINGS, or never acknowledges, is closed at the deadline.

## Graceful shutdown and GOAWAY

### Requirement

- Graceful server shutdown sends GOAWAY with stream identifier 2³¹−1 and `NO_ERROR`, followed by a PING, and waits for the PING acknowledgment or the configured maximum wait, whichever comes first. It then sends GOAWAY with the last stream it processed, completes streams at or below it, and closes.
- Graceful client shutdown sends one GOAWAY with last stream identifier 0, because it accepts no peer-initiated streams, completes its open streams, and closes.
- Forced shutdown resets remaining streams and closes.
- A received GOAWAY immediately removes the connection from new admission. Streams at or below the peer's last identifier continue to completion. Locally initiated streams above it fail as unprocessed, which the client may retry on another connection under its retry policy.
- A received GOAWAY with an error code ends the connection, failing open streams with that error rather than ending their bodies normally.
- `begin_close`, graceful driver completion, forced shutdown and drop keep their meanings from the [driver lifetime design](proposal.md#driver-lifetime-and-application-handles).

### Rationale

[RFC 9113 §6.8](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.8) asks a server to allow at least one round trip for in-flight stream creation before its second GOAWAY. The PING measures that round trip; the maximum wait bounds shutdown when the peer does not answer, at the cost that a stream a slow peer opens later is refused as unprocessed and can be retried. Neither reviewed library sends a graceful GOAWAY, so shutdown truncates accepted streams without signalling which were processed. Until recently `protocol-http2` also closed everything on any received GOAWAY; upstream now drains, which matches the receiving rules here.

### Scope

`base-http2`, and server shutdown and client retirement.

### Validation

Port the Async::HTTP [`graceful_goaway`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/test/async/http/protocol/http2/graceful_goaway.rb) and [`request_failed_on_refused_stream`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/test/async/http/protocol/http2/request_failed_on_refused_stream.rb) scenarios. Test server shutdown with in-flight streams and new streams racing the first GOAWAY, a peer that never acknowledges the PING, client shutdown with open streams, and a client receiving GOAWAY while opening a stream.

## Client admission

### Requirement

An HTTP/2 connection publishes `Admission { max_concurrent, opening_credit }` with `max_concurrent` the smaller of the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` and a local cap, and `opening_credit` the remaining client stream identifiers. It updates admission when SETTINGS change and sets it to zero on GOAWAY, error or identifier exhaustion. The connection owner revalidates each use when opening the stream. A connection is viable while its driver runs and it is not draining; no idle read probe is required, because the driver reads continuously. One client pool holds HTTP/1 and HTTP/2 connections under the same contract.

Until the peer's SETTINGS arrive, the connection admits up to a small configured number of streams, by default 1, no more than the local cap, so a new connection's first request need not wait for the peer's SETTINGS. Requests that depend on the peer's settings, such as an extended CONNECT, which needs `SETTINGS_ENABLE_CONNECT_PROTOCOL`, wait for them. A stream the peer then refuses because its limit is lower is unprocessed, so the client's [retry policy](proposal.md#client) recovers it when the request's body can be reproduced and retries are enabled.

### Rationale

This is the [unified pool contract](client-connection.md#one-http-client-pool-contract), corresponding to Async::HTTP's `concurrency` and `viable?`. Stream identifiers are consumed and never restored, which is what `base-pool`'s `opening_credit` represents; completing a stream restores concurrent capacity but not an identifier. Admitting a small number of streams before the peer's SETTINGS, decided on 2026-11-20, lets a new connection's first request leave without waiting for the peer's SETTINGS, while bounding how many the peer can refuse. [RFC 9113 §3.4](https://www.rfc-editor.org/rfc/rfc9113.html#section-3.4) permits a client to send frames before receiving the server's preface, and until the peer's SETTINGS arrive its `SETTINGS_MAX_CONCURRENT_STREAMS` has the unlimited initial value of [§6.5.2](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.5.2).

### Scope

`base-http-client` and its use of `base-pool`.

### Validation

Tests: a peer lowering its stream limit below the number of active streams; GOAWAY racing acquisition; an endpoint selecting HTTP/1 and HTTP/2 on successive connections in one pool; opening credit reaching zero near identifier exhaustion; a new connection sending its first request before the peer's SETTINGS, holding a second until they arrive, and holding an extended CONNECT until it knows the peer supports it; a peer whose limit is lower than the early admission refusing a stream, which is retried; each charge released exactly once on completion, reset and abandonment.

## Default limits

### Requirement

Every limit has the default below unless an application configures another. The client uses the same values for the settings it advertises. These are starting values; M7 benchmarks may revise them, recording the change and its evidence here. Rows added after the table was decided record their own decision dates.

| Limit | Default | Reason |
| --- | --- | --- |
| `SETTINGS_MAX_CONCURRENT_STREAMS` advertised, and the client's local cap on streams per connection | 100 | RFC 9113 §6.5.2 recommends no fewer than 100. With handlers polled by the connection's driver, more streams add concurrency, not cores. |
| Initial stream receive window | 1 MiB | Avoids window-limited uploads on high bandwidth-delay paths; Hyper's and Go's server defaults. Their clients advertise larger windows: Hyper 2 MiB, Go 4 MiB. |
| Connection receive window | 1 MiB | Bounds unread data per connection to 1 MiB; Hyper's and Go's server defaults. Their clients advertise 5 MiB and 1 GiB. One stream left unread can hold the whole window, stalling other streams' uploads until it is read or discarded. |
| `SETTINGS_MAX_FRAME_SIZE` advertised | 16 KiB | Protocol default, matching a TLS record; larger frames delay other streams' frames. Hyper's default; Go advertises 1 MiB. |
| `SETTINGS_HEADER_TABLE_SIZE` advertised, and the encoder's cap on the peer's value | 4 KiB | Protocol default. Capping the encoder bounds memory per connection however large a table the peer offers. |
| `SETTINGS_MAX_HEADER_LIST_SIZE` advertised and enforced | 32 KiB | Matches HTTP/1's `max_head_bytes`. The two are measured differently: HTTP/2 charges each field 32 bytes beyond its name and value, and HTTP/1 counts wire bytes, so a head near the limit can be accepted over one version and refused over the other. |
| Fields per section | 100 | Matches HTTP/1's `max_fields`. |
| Header block bytes, across HEADERS and CONTINUATION | 64 KiB | Twice the header list limit. A field's plain literal encoding never exceeds its RFC 9113 §6.5.2 size of name plus value plus 32 bytes, but an encoder may legally Huffman-code a string into more bytes than its plain form; the margin admits that for ordinary text, while still bounding work per block. |
| CONTINUATION frames per header block | 8 | nghttp2's default; the byte cap alone admits many one-byte frames. |
| Streams reset by the peer, or locally because of peer errors (rapid reset) | Bucket of 1,000, refilled at 33 per second | nghttp2's default stream reset rate limit, which counts every RST_STREAM a server receives. Resets also cancel their handlers, so each one costs little. |
| PING, SETTINGS, PRIORITY and PRIORITY_UPDATE frames, empty DATA without END_STREAM, and unknown frame types | One shared bucket of 10,000, refilled at 330 per second | nghttp2's default "glitch" rate limit, which counts empty DATA, PRIORITY, unknown frames and other suspicious frames in one bucket. |
| Settings per SETTINGS frame | 32 | nghttp2's default. |
| Queued control frames awaiting transport capacity | 1,000 | nghttp2's default outbound queue flood limit (Go uses 10,000). |
| Connection preface and first SETTINGS from the peer, including server preface detection | 10 seconds | Bounds connections that open but never speak HTTP/2; Go's preface timeout. |
| Acknowledgment of each SETTINGS this endpoint sends | 10 seconds | Ends with `SETTINGS_TIMEOUT`, per RFC 9113 §6.5.3. |
| Maximum wait between the two GOAWAY frames of graceful shutdown | 1 second, ending earlier when the PING is acknowledged | Lets streams opened before the first GOAWAY arrive, without letting an unresponsive peer delay shutdown. |
| `SETTINGS_ENABLE_PUSH` advertised by the client | 0 | Server push is disabled. |
| `SETTINGS_NO_RFC7540_PRIORITIES` advertised | 1 | RFC 7540 priorities are dropped, as Async::HTTP does. |
| `SETTINGS_ENABLE_CONNECT_PROTOCOL` advertised by the server | 1 | Extended CONNECT, and so [protocol switching](protocol-switching.md) and WebSockets over HTTP/2, is accepted only after it is advertised; handlers that accept no protocol refuse such requests. Async::HTTP's server advertises 1. Decided on 2026-11-20. |

Reference values were checked on 2026-11-20 at [Hyper `21da6a5`](https://github.com/hyperium/hyper/blob/21da6a59db9a/src/proto/h2/server.rs) (and its [client](https://github.com/hyperium/hyper/blob/21da6a59db9a/src/proto/h2/client.rs)), [Go `x/net` `fd4c9ad`](https://github.com/golang/net/blob/fd4c9ad54137/http2/server.go) (and its [configuration defaults](https://github.com/golang/net/blob/fd4c9ad54137/http2/config.go)), [nghttp2 `7262774`](https://github.com/nghttp2/nghttp2/blob/7262774042b8/lib/nghttp2_session.h) (and its [public header](https://github.com/nghttp2/nghttp2/blob/7262774042b8/lib/includes/nghttp2/nghttp2.h), which defines the settings-per-frame limit) and [Async::HTTP `b7029c7`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2.rb). For comparison: Hyper advertises 200 streams and a 16 KiB header list; Go 250 streams and about 1 MiB of headers; Async::HTTP 128 streams, an 8 MiB stream window and 1 MiB frames.

### Rationale

The table was decided on 2026-11-20 as drafted: 100 streams, 1 MiB windows, 16 KiB frames, a 32 KiB header list, 64 KiB header blocks, nghttp2-derived abuse budgets and 10 second timeouts. Finite, documented defaults make the abuse limits effective without configuration, and tie memory per connection to a stated bound. Aligning header limits with HTTP/1 keeps behavior close across versions.

### Scope

`base-http2` options and the client and server options that expose them.

### Validation

Inspect the option defaults against this table. Tests assert the advertised SETTINGS of a default server and client, and the abuse-limit tests run at these thresholds.

## Interoperability evidence

### Requirement

Before HTTP/2 is published, the client and server must interoperate with independent implementations, at least `curl` with nghttp2 and the `h2` crate, and the server must be run against [h2spec](https://github.com/summerwind/h2spec), with every failing case listed and justified or fixed. The shared behavior suites in [`base-http-fixtures`](fixtures.md) run against HTTP/2 as well as HTTP/1.

### Rationale

Tests written from this document share its authors' reading of RFC 9113; independent peers and a conformance suite expose misreadings. Running the shared suites on both versions makes version-specific gaps visible. The [roadmap](roadmap.md) lists h2spec in M7's exit evidence.

### Scope

`base-http2`, `base-http-client` and `base-http-server` with the `http2` feature, before HTTP/2 is published.

### Validation

Record the peer versions and h2spec results with the release, and inspect the fixture suites for HTTP/2 coverage. Interoperability peers remain development dependencies only, as [packages and dependencies](#packages-and-dependencies) requires.
