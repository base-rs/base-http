# Native protocol and transport design space

Evaluated 2026-10-26. This document recommends implementation boundaries and starting points; it does not report completed ports or performance measurements. It supplements the [proposal](proposal.md) and [implementation evidence](implementation.md).

Direction updated after discussion: implement native `base-quic` and `base-http3`. The earlier recommendation to first integrate `quinn-proto` is superseded; the comparisons below remain useful for reference implementations, interoperability and focused forks.

## Recommended direction

Own the HTTP semantics, HTTP/1, HTTP/2 and HTTP/3 state machines, and scheduler integration. Introduce focused `base-http-hpack` and `base-http-qpack` packages with Base-owned interfaces. Their implementations can begin as facades over private dependencies, ports, extractions or forks. Build `base-tls` on rustls. Implement QUIC transport in `base-quic`, using rustls's QUIC interface for handshake/key support and existing transport implementations as references, peers or fork sources.

The deciding property of a dependency is whether we retain control of execution, storage and protocol behavior. An engine which consumes memory and returns work for our driver can preserve the io_uring path. Forking becomes useful when a concrete API or implementation constraint prevents that path, or a measured improvement needs changes upstream cannot accommodate. Owning a package name alone does not reduce allocations or copies.

| Component | Recommended starting point | Main reason |
| --- | --- | --- |
| `base-http` | Native port of `protocol-http` semantics | One message, field and body model for all versions. |
| `base-http1` | Native connection/framing state and head parser | Own persistence, body framing, I/O progress and HTTP/1 syntax; `httparse` remains a test oracle. |
| `base-http-hpack` | Base-owned interface over a selected private dependency, focused fork or native port; use `protocol-hpack` as a behavioral reference | Small, independently testable codec whose implementation can change without changing protocol consumers. |
| `base-http2` | Native connection/stream state with `base-http-hpack` | Own batching, flow control, cancellation and buffer lifetime. |
| `base-http-qpack` | Base-owned interface over a private dependency, focused fork or native codec, initially with dynamic-table capacity zero | Deliver a working HTTP/3 path before adding cross-stream compression dependencies. |
| `base-tls` | rustls with an unbuffered integration prototype | TLS processing can use caller-managed buffers and caller-performed I/O. |
| `base-quic` | Native transport core and scheduler driver; reuse/fork focused algorithms as appropriate | Own packet/stream state, buffers, UDP I/O and timers independently of HTTP. |
| `base-http3` | Native HTTP mapping over `base-quic`, plus `base-http-qpack` | Preserve the semantic model and explicit driver ownership. |

These are package boundaries, not a requirement for separate repositories or simultaneous implementation. Compression packages should have no scheduler, request/response or field-policy dependency. `base-tls` should have no HTTP dependency.

`base-http-client` and `base-http-server` compose these protocol engines into application APIs. Both depend directly on `base-http` and select `base-http1`, `base-http2` and `base-http3` through optional protocol features; the client also depends on `base-pool`. Their driver supervision and transport orchestration sit above the protocol-specific connection APIs. See the [crate structure and dependency graph](proposal.md#3-crate-structure).

## Compression interfaces and replaceable implementations

The compression crates own encoder/decoder types, configuration, limits, progress/results and errors tailored to the protocol drivers. They accept byte-oriented fields and permit reused output storage. Keep upstream implementation types private rather than exposing them through public aliases or re-exports. The field/message model stays independent of the selected codec.

A facade over a dependency is a valid first implementation when it satisfies the required behavior and bounds. Adapter buffering or copies must be explicit and measured. A later port or fork can replace that implementation while retaining the Base contract and conformance suite; adding a newly required public capability remains a deliberate API change. Start with one implementation per codec, without a generic backend-selection framework.

## HTTP/1: keep the framing owner native

[`httparse`](https://docs.rs/httparse/1.10.1/httparse/) parses request/response heads and includes helpers for fields and chunk sizes. It was the initial focused dependency and is now replaced by a native single-pass parser (see [the parser design](http1-parser.md)), remaining only as a differential test oracle. It does not supply our exchange lifecycle, persistence rules, body-length validation, chunked-body state or output driver; those remain Base code.

Start with borrowed parser results and deliberately choose which bytes the message retains. Preserve the ordered field sequence and supplied spelling. The earlier `String`/`Vec<u8>` field sketch is a simple owned representation, not a promise of one new allocation for every name and value on the optimized path. Keep raw storage behind the collection API while evaluating compact ownership and retained ranges.

A parser fork is justified only by a concrete parsing policy, ownership or performance need. We can make that decision with the first HTTP/1 vertical slice rather than build a parser framework in advance.

## HPACK: own a small codec with a familiar reference

The local `protocol-hpack` checkout is at `e572ccf0d291821915bc9d2d57425c7435b7483b`. Its `Context`, `Compressor`, `Decompressor` and generated Huffman machinery provide a familiar behavioral reference. The checked-in tests include RFC examples; `test/protocol/hpack/external.rb` describes optional external fixture sets and skips absent directories. Inspect and import the actual fixtures deliberately rather than claiming those external suites already pass in Rust. See the [reviewed source](https://github.com/socketry/protocol-hpack/tree/e572ccf0d291821915bc9d2d57425c7435b7483b).

| Approach | Value | Cost or limitation | Assessment |
| --- | --- | --- | --- |
| Port `protocol-hpack` | Familiar behavior, existing fixtures, direct control of storage and limits | Rust still needs checked arithmetic, incremental input handling and independent conformance evidence | Strong native implementation option; not a prerequisite to the Base interface or HTTP/2. |
| Extract the HPACK code from `h2` | Existing Rust encoder, decoder, Huffman and table implementation | Its [header representation](https://github.com/hyperium/h2/blob/master/src/hpack/header.rs) uses `http` types and protocol metadata; the [decoder](https://github.com/hyperium/h2/blob/master/src/hpack/decoder.rs) also references framing code | Credible source for a focused fork, with explicit decoupling work. This does not require an `h2` engine dependency. |
| Use or fork `httlib-hpack` | Standalone codec and separate Huffman package | Its public examples use owned name/value vectors; inspect buffer/limit behavior before choosing it | Useful candidate and comparison implementation, without an assumed performance advantage. [Documentation](https://docs.rs/httlib-hpack/0.1.3/httlib_hpack/). |
| Use or fork `fluke-hpack` | Standalone Rust fork aimed at HTTP/2 conformance | Its published documentation says the encoder lacks Huffman output | Useful reference or starting code; assess the missing encoder path. [Project documentation](https://docs.rs/crate/fluke-hpack/0.3.1). |
| Depend on the original `hpack` crate | Simple existing API | RustSec records it as unmaintained | Do not choose it solely because it has the shortest name. This is an informational maintenance finding, not a claim of a specific exploit. [Advisory](https://rustsec.org/advisories/RUSTSEC-2023-0084.html). |

Select the first implementation by trying the required Base interface against a focused dependency and the `protocol-hpack` fixtures. A private facade can get HTTP/2 moving; a native port or focused fork is equally valid where it better fits ownership, limits or performance. A port should be idiomatic Rust rather than a literal translation of temporary Ruby command hashes. Keep the encoder and decoder independent and operate on raw byte pairs. HTTP/2 prepares pseudo-fields and validates HTTP semantics outside the compression codec; pseudo-field names are not ordinary `Fields` names.

The codec API should support reused output storage, input-progress reporting, explicit limits and field delivery without requiring a temporary message or map. Exercise fragmented field blocks with the HTTP/2 consumer; whole-block buffering required by a private dependency must be bounded and its cost recorded. Preserve sensitive/never-indexed information where forwarding requires it. Decide its representation at the field/encoding boundary without coupling the codec to the whole semantic crate.

Validation needs checked integer lengths, truncated inputs, invalid Huffman padding/EOS, table updates and eviction, sensitive fields, decoded-size limits, sequential connection stories and independent encoder/decoder comparisons. For a fork, preserve source revision, license notices, original tests and a record of local changes. No source extraction has been performed by this review.

## QPACK: stage dynamic compression independently of transport

QPACK reuses HPACK's integer and Huffman primitives, but adds encoder/decoder instruction streams and dependencies between field sections and dynamic-table updates. Blocked decoding, acknowledgments and cancellation are part of its state. A single synchronous header-block call cannot describe every outcome. [RFC 9204](https://www.rfc-editor.org/rfc/rfc9204.html).

Use a common byte-pair input style for both codecs, while giving QPACK its own state machine. It should report progress or blocked field sections and accept subsequent instruction bytes; the HTTP/3 driver controls stream I/O and resumption. Neither codec needs tasks or scheduler access. Initially QPACK can reuse deliberately separated integer/Huffman modules from `base-http-hpack`; avoid another utility package until both implementations demonstrate the shared boundary.

For the first interoperable HTTP/3 slice, advertise zero dynamic-table capacity and zero blocked streams, and emit static references/literals. This is a limited compression policy, not a complete dynamic QPACK implementation. Settings and permitted encoder/decoder streams still need correct handling. Add dynamic compression as a separately tested optimization. [RFC 9204 §§3.2.3, 4.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-3.2.3).

Possible starting code includes the [`h3` QPACK module](https://github.com/hyperium/h3/blob/master/h3/src/qpack/mod.rs), whose exported encode/decode surface is stateless. Its presence does not establish complete dynamic-table support. For an existing dynamic implementation, [`ls-qpack`](https://github.com/litespeedtech/ls-qpack) and its [Rust wrapper](https://docs.rs/crate/ls-qpack-rs/0.3.1) are candidates, with C build, callback and buffer-lifetime integration to evaluate. A private dependency facade, native codec or focused extraction can implement the Base interface; use an independent implementation for comparison.

## TLS: build `base-tls` on rustls

Rustls is a suitable dependency because it leaves network I/O to its caller. Its [unbuffered API](https://docs.rs/rustls/0.23.46/rustls/unbuffered/index.html) also leaves TLS and plaintext storage with the caller. The buffered API remains useful as an integration baseline. This supports a Base driver around rustls without a Tokio TLS stream dependency.

`base-tls` should own TLS configuration, handshake/progress integration, ALPN and peer information, record buffering and shutdown. Scheduler operations own actual network transfers. The implementation must retain partially transmitted ciphertext, avoid encrypting a retried range twice, handle fragmented/coalesced records, and keep processing required TLS traffic while application reads or writes are blocked.

Prototype the unbuffered interface before settling the public stream type. Its [`process_tls_records`](https://docs.rs/rustls/0.23.46/rustls/unbuffered/struct.UnbufferedConnectionCommon.html#method.process_tls_records) takes mutable input. Prove plaintext borrowing/retention, record batching and how pending writes coexist with read-side progress. Buffer borrows must not imply that an allocation can be recycled while a submitted operation still accesses it. Use explicit limits and configuration for verification and crypto-provider selection.

The earlier [research buffer findings](../../base-scheduler-research/benchmarks/protocol-buffers/findings.md) recorded immutable completed multishot leases. The released scheduler now exposes uniquely mutable `ReceiveBuffer` chunks that can freeze into retained immutable views, alongside borrowed reads into reused parser storage. Use these [portable I/O capabilities](../../base-scheduler/context/io.md) for the TLS experiment. Multishot and registration choices remain backend details; the historical finding does not establish a missing mutable contract in the release.

Shared stream capabilities must sit below HTTP when `base-tls` consumes them. Keep `base-tls` independent of `base-http-io`; settle the smallest shared stream interface in the plain/TLS prototype and extract it only when exercised by both. HTTP-specific buffering can remain in `base-http-io`.

QUIC uses a different TLS integration from an encrypted TCP stream. Rustls has a separate [`quic` API](https://docs.rs/rustls/0.23.46/rustls/quic/index.html) for `base-quic` to use. Common trust, identity and ALPN configuration may be shared where useful; the TCP record driver should not be forced into that path.

## QUIC: native transport with existing implementations as references

Writing QUIC entails loss recovery, congestion control, pacing, stream reassembly, flow control, path validation and packet protection, in addition to handshake integration. It is a much larger independent project than the compression codecs. The selected direction is `base-quic`: keep protocol state driven by explicit datagrams/time, expose stream operations and outgoing work, and place scheduler-driven UDP/timer progress alongside that core. Rustls supplies handshake/key support. The crate remains independent of HTTP.

Existing transport cores offer useful reference behavior, independent peers and possible algorithm extractions. Reusing a whole core was the earlier scope-saving alternative; it is not the implementation assumed by the current plan.

| Candidate | Fit with Base | Main experiment or tradeoff |
| --- | --- | --- |
| `quinn-proto` | Its [endpoint](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Endpoint.html) performs no I/O; [connection methods](https://docs.rs/quinn-proto/0.11.19/quinn_proto/struct.Connection.html) take time and produce transmissions/events. It offers rustls integration without requiring Quinn's async runtime layer. | Reference, interoperability peer and possible fork source. Input ownership is specifically `BytesMut`; output uses caller-supplied `Vec<u8>`. Inspect those boundaries when extracting code. |
| ngtcp2 | Familiar from the Ruby implementation; its [programmer's guide](https://nghttp2.org/ngtcp2/programmers-guide.html) describes caller-supplied timestamps, packet processing and callbacks. | Reference and interoperability peer; FFI reuse remains a comparison option. Callback ownership and retransmission retention need explicit design. The documented helper TLS backends do not include rustls. |
| quiche | [Low-level packet API](https://docs.quic.tech/quiche/) leaves sockets and timers to the application; includes HTTP/3 and QPACK, and exposes buffer customization interfaces. | Credible integrated alternative or fork source. Its default TLS backend is BoringSSL and the current release requires a higher Rust version than this prototype. Evaluate how much of the integrated stack we want to own or extract. |
| Native `base-quic` | Complete control of layout, retransmission ownership and scheduler integration | Selected direction. Scope the transport independently and use simulation plus independent peers throughout implementation. |

The incoming `BytesMut` boundary observed in `quinn-proto` is a concrete example of an ownership constraint to evaluate when borrowing code. Native `base-quic` should accept scheduler-compatible owned storage without inheriting that specific container requirement. Establish the ordinary owned-buffer path and measure copies, allocation reuse and retransmission retention before adding provided-buffer optimizations. Compression, TLS and QUIC decisions need not all use the same reuse strategy.

The QUIC prototype must drive UDP reception/transmission, expiry and pacing through the scheduler; preserve packet metadata; route connection IDs; and retain outgoing buffers until completion. Use scheduler `Clock` observations for protocol time. Establish stream ownership and receive-credit behavior with slow consumers. Add batching or native receive optimizations after the ordinary owned-buffer path works.

## Current release and toolchain observations

Registry metadata checked on 2026-10-26 gives the historical package constraints below. This is not a successful build of the selected dependency graph; crypto-provider and transitive dependencies still require verification. The HTTP prototype now requires Rust 1.87 and uses the released scheduler and pool 0.3.0 and endpoint 0.1.0 packages; the registry-backed client is tested on macOS and Linux.

| Package | Evaluated release | Declared Rust minimum | Declared license |
| --- | --- | --- | --- |
| base-scheduler and seven companions | 0.1.0 | 1.85 | MIT |
| rustls | 0.23.46 | 1.71 | Apache-2.0 OR ISC OR MIT |
| quinn-proto | 0.11.19 | 1.85 | MIT OR Apache-2.0 |
| quiche | 0.30.0 | 1.88 | BSD-2-Clause |
| h2, as extraction reference | 0.4.20 | 1.63 | MIT |

Sources: [scheduler registry metadata](https://crates.io/api/v1/crates/base-scheduler/0.1.0) and [companion release notes](../../base-scheduler/releases.md), [rustls registry metadata](https://crates.io/api/v1/crates/rustls/0.23.46), [quinn-proto registry metadata](https://crates.io/api/v1/crates/quinn-proto/0.11.19), [quiche registry metadata](https://crates.io/api/v1/crates/quiche/0.30.0), [h2 registry metadata](https://crates.io/api/v1/crates/h2/0.4.20). All eight scheduler publications were checked against crates.io on 2026-10-26. Linked upstream source branches are reference material, not pinned fork inputs. Pin exact source revisions and preserve notices when importing code.

## Implementation order and decision evidence

The [build roadmap](roadmap.md) is the canonical sequence. Scheduler release, HTTP rename/migration, the initial semantic core and semantic streaming/interim contracts are implemented. The initial Direct transport lifecycle, native HTTP/1 client/server framing, scoped pooled application client and application server are implemented. Generic [base-pool](../../base-pool/readme.md) singleplex and multiplex contracts are also implemented independently. Remaining work starts with transient borrowed-body proxy integration, managed client tunnels, TLS and client/server negotiation, HPACK/HTTP/2 extending the same admission contract, native QUIC, then QPACK/HTTP/3. See the [client/connection taxonomy](client-connection.md). The sequence includes the early TLS storage experiment and acceptance evidence for each stage. Rich field-specific parsers remain semantic extensions described in the [implementation notes](implementation.md).

Compare compression CPU, compressed bytes and retained memory separately from I/O throughput. End-to-end comparisons need the same protocol, TLS/crypto configuration, payloads, concurrency, connection reuse, machine and memory limits. Include application consumers that retain data, early responses, cancellation during pending I/O, and shutdown. Record actual wins and regressions; neither a native implementation nor io_uring establishes an end-to-end speedup without those results.

The first implementation now adds sibling `base-http1` and `base-http-server` packages. HTTP/1 uses a native head parser over published scheduler stream capabilities, with native framing and lifecycle ownership. Real TCP and deterministic framing tests exercise the pull-body design. The separate [static hello benchmark](../benchmarks/hello/readme.md) now measures small persistent responses against Hyper/Tokio on macOS; its [results](../benchmarks/hello/results.md) delimit that workload. Compression, TLS, QUIC and Linux io_uring performance remain unmeasured here.
