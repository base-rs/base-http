# Duplex progress and proxy experiment

Updated 2026-10-26. The experiment uses native HTTP/1 lanes and the released Direct scheduler. It validates the current pull-body contract and connection lifecycle; it does not establish performance or complete a high-level client/proxy API.

## Progress and reuse contract

Request output and response input are independent work within one HTTP/1 exchange. A final response head, or even a complete final response body, can arrive before upload production finishes. Successful response completion must not automatically cancel the upload. Expose the response promptly and continue polling both directions. If the peer refuses further upload, cancellation/error policy must preserve response readability and retire the unsynchronized connection.

Reuse requires completed local output, completed remote input, persistent framing and joined exchange work. In a proxy this applies to both connections. Output writers return persistence eligibility; they cannot establish input completion by themselves. RFC 9112 requires the server to consume the request body or close the connection, and the client to consume the response body for reuse. [RFC 9112 §9.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-9.3).

The server `Exchange` now distinguishes active input from abandoned input. Sending an early response while input remains active can preserve persistence. Closing/dropping unfinished input disables reuse. After output completion, the owner checks input completion before admitting another request. No automatic draining or detached upload task is introduced.

## Implemented experiment

`base-http1::RequestWriter` and `ResponseReader` supply the upstream client directions. They share native body framing with the server codec and retain generic bodies. The test proxy concurrently drives:

```text
upload:   downstream request body -> upstream request writer
download: upstream response reader -> downstream response writer
```

The directions are separate futures, polled together. Either error cancels the sibling signal; both futures are awaited before cleanup. Successful completion leaves the sibling running. All four I/O lanes are closed and dropped before either socket is exclusively closed.

The [three-party TCP tests](../../base-http-server/tests/proxy.rs) verify:

- A native upstream server sends 100/103 and streams response data before the downstream upload completes. Trailers survive both hops; Connection-nominated fields are removed and Host is rewritten.
- A raw upstream sends a complete fixed response before any upload data is produced. The subsequent 2 MiB upload still finishes. Both connections serve a second exchange.
- Independent 8 MiB upload/response transfers complete when downstream response consumption is delayed until upload production finishes, exercising socket backpressure.
- Upstream truncation cancels a pending upload and joins all four lane retirements, retaining the causal error.
- External cancellation while both directions wait joins all four lanes and closes both sockets.

The [deterministic codec tests](../../base-http1/tests/codec.rs) cover request framing and short writes, response fragmentation, interim sequencing, trailer delivery, bodyless responses, close-delimited input, malformed/truncated input, cancellation, abandonment and switch classification. Existing server upgrade/CONNECT tests remain in place.

These are focused protocol and lifecycle tests. The test forwarding helper handles ordinary HTTP/1 messages; it is not a production proxy policy layer. Client switch responses expose raw input, but outgoing client tunnel coordination remains unimplemented.

## Symmetric application interface

The target follows Async::HTTP: applications produce outgoing bodies and consume incoming bodies; client and server drivers transmit those bodies and retain exchange ownership.

| Role | Application supplies | Application receives | Driver responsibility |
| --- | --- | --- | --- |
| Client | Request head and readable request body | Response head and readable response body | Transmit the request while independently receiving the response. |
| Server | Response head and readable response body | Request head and readable request body | Receive the request while independently transmitting the response. |

`client.call(request, cancellation).await` completes when the final response head is available, including a final 101 switch response. The returned response body remains readable while request transmission continues. Receiving the complete response does not itself finish an active upload. The server handler receives a request head with a streaming body and can return a response before request consumption finishes.

An application can supply `BufferedBody` directly. For imperative production on either side, it supplies a `BodyReader` as the outgoing body and writes through the paired `BodyWriter` in separately supervised application work. The protocol's outgoing-body consumer and the application's producer are distinct operations. Only applications that need concurrent production have to arrange that producer work.

An ordinary proxy already has an incoming request body to supply to the upstream client. Its application-level operation should be:

```rust,ignore
// Intended application API; high-level client implementation is still pending.
// Inside an ordinary proxy handler:
let response = client.call(request, cancellation).await?;
Ok(response)
```

The client retains request transmission while the server transmits the returned response. The handler continues to return a semantic response; no mandatory response-plus-work return type is added. Upstream upload progress must continue when downstream response consumption pauses or reaches EOF. The server must also coordinate retained active request input before reuse or retirement; response serialization alone does not establish its completion.

## Protocol-specific progress owners

HTTP/1 has one active exchange per connection. Its driver supervises independent request and response directions. The existing proxy test polls those futures together; the high-level client will retain the upload work across `call` completion. These directions can run in owned tasks or as separately polled futures within an owned driver.

HTTP/2 has a shared connection receive loop which demultiplexes frames into stream state and per-stream input queues. Applications consume those queues through incoming readable bodies. Final headers resolve the corresponding client's `call`; DATA feeds its incoming body, and stream termination completes input. Stream output work pulls each outgoing readable body, observes flow-control credit and serializes frame writes. Receive progress must continue while output waits for data, credit or transport capacity.

This structure applies to both HTTP/2 clients and servers. A slow stream must not park the connection reader on that stream's application queue; advertised credit and reserved buffering must agree. Stream completion, body abandonment and connection closure retain distinct ownership and accounting. HTTP/2 remains planned; its queues and flow-control body adapter are not implemented by the HTTP/1 experiment.

## Next implementation work

Build the supervised client/exchange driver behind the `call` interface, following the [driver lifetime design](proposal.md#driver-lifetime-and-application-handles). Prototype the Rust ownership boundary using borrowed incoming request bodies as well as owned static/queue bodies. Prove that upload work survives `call` return and response EOF, and is joined before either connection's retirement. Preserve generic bodies without imposing boxing or `'static` on every body. Exact task/future storage is implementation work, not an additional application message abstraction.

Move the low-level proxy scenarios through the ordinary handler/client path, including paused response consumption and retained active request input. Then integrate the existing [base-pool `SingleplexPool`](../../base-pool/readme.md), keeping its lease charged until both directions and associated work complete or retire. The generic pool's tests do not establish HTTP exchange integration. Endpoint establishment, Expect/Continue waiting, early-final upload cancellation policy, deadlines and retry policy belong to client orchestration.
