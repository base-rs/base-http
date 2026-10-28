# Endpoint, transport and HTTP protocol selection

Updated 2026-10-28. The current implementation supports HTTP/1 over plain TCP and TLS. Packet endpoints, QUIC and HTTP/3 remain future work. This note separates their capabilities without making them prerequisites for HTTPS.

## Ownership and capability boundaries

An endpoint describes establishment. `ConnectEndpoint<S>::Connection` is the resource produced by that endpoint, rather than a promise that every connection implements byte-stream reads and writes. The consumer constrains the capabilities it actually needs. The initial built-in address and hostname endpoints establish TCP sockets; a TLS endpoint produces a TLS session over the connected transport.

| Resource | I/O shape | Intended consumer |
| --- | --- | --- |
| TCP socket | Ordered byte-stream reader/writer | HTTP/1 or HTTP/2, directly or through TLS |
| TLS-over-TCP session | Authenticated plaintext byte-stream directions | HTTP/1 or HTTP/2 |
| UDP socket, future | Packet reader/writer preserving message boundaries and peer metadata | Datagram applications or a QUIC driver |
| QUIC connection, future | Multiple reliable streams and optional application datagrams | HTTP/3 or other QUIC applications |

A connected UDP socket would establish a default peer, without a transport handshake or a delivery guarantee. UDP binding does not imply listening or accepting. The current TCP `BindEndpoint`/`Listener` conveniences must not become requirements for all packet endpoints. A future QUIC server needs connection-ID routing over shared UDP I/O; a logical QUIC connection does not necessarily own a separate socket. These distinctions require additional interfaces when those implementations start.

For the current byte-stream path, small `ReadableStream` and `WritableStream` capabilities live in `base_endpoint::stream`. Scheduler socket handles satisfy them through direct forwarding implementations. The HTTP/1 codec uses only plaintext reads, writes, flush and directional retirement. It does not use TLS, ALPN, socket flags, encryption configuration or endpoint establishment. No separate `base-stream` crate is introduced.

## HTTP protocol factories

`base-http-protocol` is the shared selection layer used by application clients and servers. `HttpProtocols` supplies enabled application-protocol identifiers, selects an engine from explicit plaintext policy or negotiated evidence, and returns `HttpProtocol`. That selected factory constructs the corresponding client or server codec pair.

The initial engine is HTTP/1. Its secure-stream offer is `http/1.1`. Unknown negotiated identifiers are rejected before parsing or client pool publication. Missing ALPN selects HTTP/1 only when compatibility is allowed; `require_alpn` rejects it. Plain TCP uses the explicit HTTP/1 policy. The semantic `base_http::Protocol` remains application upgrade/tunnel intent, independent of these factories.

This follows Async::HTTP: [`Endpoint#alpn_protocols`](https://github.com/socketry/async-http/blob/b6bf5b68d555e3c3caa27e3548b993e20595d7a3/lib/async/http/endpoint.rb) derives offers from the configured protocol, while [`Protocol::HTTPS`](https://github.com/socketry/async-http/blob/b6bf5b68d555e3c3caa27e3548b993e20595d7a3/lib/async/http/protocol/https.rb) interprets the selected identifier before constructing either a client or server connection. Base retains explicit futures and cleanup ownership at those boundaries.

The progress path is:

```text
HTTP policy -> ALPN offers -> endpoint/TLS handshake -> selected identifier
HTTP policy -> selected HTTP factory -> codecs and supervised connection

application bodies <-> HTTP plaintext framing <-> optional TLS <-> socket I/O
```

The high-level client currently uses a resolved `AddressEndpoint`. `Endpoint::with_tls` adds explicit trust configuration and server identity; the identity and HTTP authority are independent of the routing address. The server accepts TCP first and performs TLS handshakes within bounded connection futures, so a stalled handshake does not block further acceptance. Neither TLS nor the endpoint library chooses an HTTP handler.

## TLS progress over a socket

`RustlsConnection` owns the socket, TLS session and retained ciphertext/plaintext state. The sequential `TlsStream` interface remains available. `with_duplex` supplies independent encrypted reader/writer handles. Application reads and writes drive retained input/output futures, while the enclosing future supervises control output and joins retirement. No task is spawned or detached. Each network direction retains one scheduler capability until retirement.

The application reader first consumes authenticated plaintext from the shared TLS session. When none is available it polls the retained receiver, which reads ciphertext and processes records until plaintext is available. It then stops reading ahead until that plaintext is consumed. Incoming TLS control records therefore progress on application reads, as with sequential TLS. The writer accepts plaintext into bounded TLS buffering and directly polls the retained output future. Successful socket writes can finish in the caller without a supervisor handoff. Independently polled upload/download still provide full duplex; a paused response consumer does not prevent upload.

A stable I/O waker routes blocked transport progress to its application and supervised pollers; switching between them cannot replace a live socket registration's wake target. The pinned futures and initialized ciphertext storage are retained per connection, without allocating a future per frame. Short locks cover synchronous polling or TLS state access; none spans an asynchronous wait. The supervisor progresses read-generated control output and joins both lanes after cancellation. HTTP/2's framer will continuously drive its input direction in its independently schedulable connection task.

Flush uses an acceptance barrier to wait for the relevant ciphertext to reach the underlying transport. It does not establish peer receipt. A cancelled flush does not require replaying accepted plaintext. Successful application completion sends `close_notify`; failure or lifetime cancellation aborts pending work. Awaited completion joins both lane retirements and closes the owned socket. Dropping the enclosing future does not promise an asynchronous join.

TLS remains below HTTP: the codec sees plaintext and ordinary stream operations. Request and response body semantics do not change when encryption is enabled.

## Future packet encryption and HTTP/3

Stream TLS cannot be applied to UDP by flattening messages. DTLS would provide its own datagram security contract. QUIC instead integrates TLS handshake/key derivation with QUIC packet protection, bypassing ordinary TLS records; see [RFC 9001](https://www.rfc-editor.org/rfc/rfc9001.html#section-4). A QUIC endpoint must expose a QUIC connection to HTTP/3 while its driver uses scheduler packet capabilities underneath.

HTTP policy will supply transport-compatible offers: HTTP/1 and HTTP/2 over TLS/TCP, and `h3` over QUIC. Transport candidates are chosen before their handshakes; ALPN over an existing TCP stream cannot switch it to QUIC. HTTP/3 uses ALPN during QUIC establishment as specified in [RFC 9114 §3.2](https://www.rfc-editor.org/rfc/rfc9114.html#section-3.2). Optional QUIC application datagrams are distinct from the UDP packets carrying the connection; see [RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html).

## Evidence and remaining work

The initial HTTPS tests cover TLS 1.2/1.3, connection reuse, a stalled peer during server acceptance, absent-ALPN compatibility/strictness, failed identity verification before HTTP output, independent raw HTTP peers, and upload progress while response consumption pauses. Endpoint duplex tests cover a blocking TLS peer, backpressure, lifetime cancellation with an independently uncancelled application read, and truncation after authenticated plaintext. Existing HTTP framing, proxy and pool-lifecycle regressions remain applicable.

The implementation remains a prototype: hostname endpoint integration into the pooled client, handshake/request deadlines, wider TLS fragmentation and fault injection, idle TLS failure notification, managed client tunnels and performance measurements remain follow-up work. No performance equivalence with the plain TCP path is claimed.
