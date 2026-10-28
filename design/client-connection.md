# Clients, connections and protocol negotiation

Updated 2026-10-28. This records the taxonomy and orchestration boundary for the [proposal](proposal.md). The scoped high-level HTTP/1 client uses the multiplex pool with capacity one per connection. Plain HTTP and rustls HTTPS client/server paths are implemented, with shared HTTP protocol factories above the TLS/endpoint layer. Additional HTTP engines remain planned. See [transport layering](transport.md) and [implementation evidence](implementation.md).

## Taxonomy

| Term | Meaning | Lifetime and responsibility |
| --- | --- | --- |
| Client | A capability an application uses to interact with a remote server. | Outlives individual connections; owns establishment policy, pooling, request execution and bounded retries. The first concrete client is endpoint-scoped. |
| Endpoint | Configuration for reaching a remote server. | Authority, route, transport, security and enabled protocol policy; it does not promise which protocol a connection will select. |
| Transport | The established communication substrate. | A plain/TLS byte stream for HTTP/1 or HTTP/2, or a QUIC connection for HTTP/3. |
| Protocol selection | The establishment step that chooses an enabled HTTP implementation. | Uses negotiated evidence or explicit configuration on both client and server paths. |
| Connection | One established transport with its selected HTTP protocol session. | Owns wire state, admission capacity, reuse/draining decisions and retirement. It can serve one or many exchanges over its lifetime. |
| Driver | The supervised progress owner for a connection. | Owns independently schedulable connection work, transport progress and cleanup. Direct HTTP/1 handles can temporarily own directional codecs; a client supervisor retains lifecycle accounting. |
| Exchange | One request and its informational/final response sequence, with both body directions. | Remains active until both directions and associated work finish or are terminated. In HTTP/2 and HTTP/3 it normally occupies one request stream. |
| Server | The application-facing accept/serve capability. | Establishes accepted transports, selects the protocol, owns connection drivers and dispatches requests to handlers. |

`Client` names the durable remote-server interface. `Connection` names the actual selected session, whether selected by negotiation or explicit configuration. An HTTP/2 connection is not one request stream; an HTTP/3 connection contains QUIC streams and is not a TCP-shaped byte stream.

These terms define responsibilities, not a new public trait hierarchy. `base-http` retains shared message/body semantics; `base-http-client` and `base-http-server` compose protocol-specific connection APIs. The existing semantic `Protocol` type expresses upgrade/tunnel intent, not the set of enabled HTTP engines or an ALPN selector.

## Establishment and selection on both sides

The client path is:

```text
Client + endpoint policy
  -> pool acquisition
       existing connection use -> revalidate and execute the request
       construction reservation -> establish transport and security
                                -> select HTTP protocol and construct Connection
                                -> register driver and publish admission
                                -> return to pool acquisition
```

The server path is:

```text
Server + listener policy
  -> accept transport under connection/handshake limits
  -> establish security and select HTTP protocol
  -> construct and supervise its Connection
  -> dispatch ordinary semantic requests to Handler
```

Client and server use the same selection rules with their respective handshake roles. Negotiation belongs before protocol-specific HTTP parsing and exchange admission, rather than inside an application handler or field interpreter. Client construction capacity is reserved before negotiation starts.

| Establishment path | Selection policy |
| --- | --- |
| TLS over TCP | Configure ALPN for enabled protocols and construct the selected HTTP/1 or HTTP/2 connection. Allow absent ALPN to select HTTP/1 only under an explicit compatibility policy. |
| Plain TCP client | Use configured HTTP/1 or explicit HTTP/2 prior knowledge. Do not send an HTTP/2 preface to an arbitrary HTTP/1 server and retry blindly. |
| Plain TCP server | Use a configured protocol, or optional bounded HTTP/2 preface detection. Preserve all inspected bytes for the selected parser. |
| QUIC | Establish QUIC with the appropriate HTTP/3 ALPN and construct an HTTP/3 connection. HTTP/3 is a separate transport path, not another selection over a TCP stream. |

Unknown, disabled or incompatible selections fail establishment. Any fallback to another transport is explicit client policy with its own attempt and cleanup accounting. Deadlines, cancellation and byte limits apply during establishment as well as during requests. HTTP/1 Upgrade and CONNECT remain exchange-level transitions with managed body I/O; this selection boundary does not detach their transports.

## One HTTP client pool contract

Use `base-pool::MultiplexPool<ConnectionHandle>` conceptually for an endpoint's compatible connections. The selected connection supplies admission after establishment:

| Selected connection | Published admission |
| --- | --- |
| HTTP/1 | `Admission::concurrent(1)` while reusable; no new use while draining or switched to a tunnel. |
| HTTP/2 | Concurrent-stream capacity constrained by peer settings and local policy; updates as settings or draining state change. |
| HTTP/3 | Concurrent-use bounds plus actual QUIC request-stream opening credit. Finishing one stream does not itself replenish peer-granted opening credit. |

A client can receive HTTP/1 on one connection and HTTP/2 on another. Both belong to the same pool contract when compatible with that client's policy. The client does not need to choose a pool type before connecting or move an unclassified connection between typed pools. A closed enum over enabled connection handles is one possible implementation; handle representation and dispatch remain prototype work.

Reserve construction capacity before dialing or handshaking. Retain that reservation through selection and driver setup, then commit the selected connection with its admission. Register the progress owner before making the handle usable. A connection that still awaits protocol readiness can initially publish zero admission. Failed/cancelled construction retains its charge until pending I/O and transport cleanup finish. Building, live and retiring connections count against the stated resource bound.

Revalidate a use at the connection owner's admission boundary. Capacity, GOAWAY or closure can change between pool selection and opening an exchange. Connection handles expose admission changes to the pool; the generic pool neither performs ALPN nor parses HTTP settings.

`SingleplexPool` remains useful for general exclusively leased resources, including resources that need not be `Sync`. Its separate contract does not require the HTTP client to use it for HTTP/1. Capacity one in the multiplex contract prevents overlapping HTTP/1 exchanges while retaining full-duplex upload/download within the admitted exchange.

## Exchange completion and reuse

`client.call(request, cancellation).await` returns when final response metadata is available, including a final HTTP/1 switch response. It does not wait for upload completion or response-body consumption. Interim responses use the exchange's existing hooks.

Transfer the pool's active use into exchange ownership. Keep it charged through request production, response consumption, trailers, half closure and any required cleanup. Response EOF alone cannot release an unfinished upload. An HTTP/1 connection becomes reusable only after both directions complete and framing is synchronized. A tunnel stops ordinary HTTP/1 admission and retains its use until duplex termination and connection retirement. HTTP/2 and HTTP/3 can retire/reset one exchange while other streams continue; driver acknowledgment determines when abandoned use accounting can be released.

The server mirrors protocol selection and exchange progress, but its accepted-connection and handler limits remain separate from outbound connection pooling. Handlers continue to receive `Request<B>` with raw `Fields` and return `Response<B>`. Optional field interpretation stays application-owned; negotiation adds no parsed-request wrapper or mandatory middleware stage.

## Implementation sequence and evidence

1. Build the supervised high-level HTTP/1 client and connection handle, with the selection boundary initially choosing configured HTTP/1. Integrate the existing multiplex pool with capacity one.
2. Add TLS/ALPN establishment to both application client and server. Exercise supported selection, absent ALPN policy, disabled selections and cancelled handshakes.
3. Add HTTP/2 connections to the same client pool contract and server dispatch. Test an endpoint yielding different protocol selections across successive connections, capacity changes and GOAWAY/open races.
4. Extend establishment to QUIC/HTTP/3 with stream-opening credit and the same semantic exchange lifecycle.

The HTTP/1 milestone must exercise construction bounds, connection reuse, cancelled waiters, upload progress after `call` returns, delayed response consumption and joined shutdown/retirement. Later protocol milestones extend those same scenarios rather than replace the application contract. See the [roadmap](roadmap.md).

## Source correspondence

At the [reviewed Async::HTTP revision](source-review.md), [`Client#make_pool`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/client.rb) constructs connections through the selected protocol factory, and [`Protocol::HTTPS`](https://github.com/socketry/async-http/blob/f8261403c5b44cf74df1308787efb5dc7263539d/lib/async/http/protocol/https.rb) selects an ALPN handler in both `client(peer)` and `server(peer)`. [`Async::Pool::Controller`](https://github.com/socketry/async-pool/blob/4539ee837207b80d7985827bde85d58887b871bc/lib/async/pool/controller.rb) admits uses according to resource concurrency. Base adopts that unified HTTP admission model while retaining its explicit construction reservations, drivers and retirement accounting.

## Initial Rust client boundary

The implemented `base-http-client` selects HTTP/1 over a resolved TCP endpoint, optionally wrapped in rustls TLS using `Endpoint::with_tls`. `HttpProtocols` supplies ALPN and selects the factory before publication. `Client::new(&scheduler, endpoint, options)` returns `(client, driver)`. Poll `driver.run(&lifetime)` concurrently with application calls and body production/consumption. `client.call(request, &request_cancellation)` returns a response whose body receives bounded owned frames; the driver keeps uploading independently. Call `begin_close` or drop all client handles to stop new admission and finish accepted work. Cancel the driver lifetime to terminate active work and await cleanup.

This first managed client rejects CONNECT and Upgrade before dialing. Bodies must live for the driver scope; transient borrowed server input requires an additional ownership design. The general tunnel, negotiation and multi-protocol contracts above remain targets, not claims that those paths are implemented. See the [working example](../crates/client/examples/client_server.rs) and [implementation evidence](implementation.md#pooled-client-implementation).

## Writable HTTP/1 experiment and task placement

The HTTP/1 client now also provides `open` / `open_request` returning an active `WritableRequest`, alongside the existing semantic pull-body `call`. `open` acquires pool admission in the caller and hands connection-bound encoder/decoder directions to the application. `write` directly drives output; `finish` completes output and reads response metadata. `split` separates the request writer from the pending response so applications can poll upload and download independently. Response bodies decode on read, without an HTTP frame queue. The connection retains its transport directions and reusable codec buffers. Short locks transfer those directions to/from direct request handles; normal completion returns them synchronously and releases admission without waiting on the supervisor. Pool and client-accounting notifications can still wake lifecycle work. Established plaintext requests require no driver polls or worker handoff; construction, queued calls, deferred synchronous closure and retirement remain supervised. Encrypted reads, writes and flushes directly poll retained I/O futures below those codecs, with stable wake routing for application and supervised pollers. The input future returns available authenticated plaintext before reading more ciphertext; control output and directional retirement remain supervised. Neither a per-write producer queue nor a per-write future allocation is required.

The [client example](../crates/client/examples/client_server.rs) demonstrates buffered, sequential writable and split duplex requests. [Writable integration tests](../crates/client/tests/writable.rs) exercise independent peers, interim responses, trailers, fixed-length failures, cancellation, abandonment, pool waiters, and large HTTP/TLS 1.2/1.3 duplex echo. [Three-party proxy tests](../crates/client/tests/proxy.rs) also forward transient borrowed downstream input through direct upstream handles under response backpressure and early response completion. Proxy forwarding must drive both directions by default. Existing `call` still uses its queued implementation; this experiment does not claim a performance result or implement HTTP/2.

Do not make a single client driver the scheduling unit for all connections. A `FuturesUnordered` inside one task permits interleaving but all child polls execute on whichever worker is polling that task. A thousand HTTP/2 connections need independently schedulable connection drivers, each owning its framer/demultiplexer and any TLS progress. Stream queues are a protocol-specific boundary; the connection receive loop must not wait for a slow application writer. The client supervisor owns connection task handles, pool admission and lifecycle cancellation, then joins connection cleanup. This permits distribution across workers without creating one OS thread per connection or one mandatory task per body.

The present borrowed-scope prototype still collects its supervised connection work within one driver; ordinary direct HTTP/1 operations run in their calling tasks. This is an implementation limitation, not the multiworker contract. The scheduler's `Spawn` requires `Send + 'static` captures. A runtime-owned client execution mode must supply owned scheduler/transport state and retain all task handles, or use a separately specified scoped executor. Borrowed bodies cannot be silently extended to static lifetimes. Direct imperative writes can keep borrowed producers in application scope because only owned frame data crosses into the active handle. Resolve this scheduling/ownership boundary before implementing the high-scale HTTP/2 client.

## Storage reuse within a connection

The direct HTTP/1 implementation keeps one empty pinned allocation for each of its opening future, request sink and response stream. These are implementation caches, separate from protocol admission and exchange identity. A connection-local factory retains their concrete types behind the existing erased handles; transport dispatch remains static and I/O continues in application futures.

A live operation exclusively owns its allocation. Dropping its handle first drops all operation state in place, then publishes only empty storage to a single-slot cache. A weak cache reference prevents idle storage from owning its connection. Overlapping returns can discard surplus empty allocations; there is no shared mutable live future. Completed empty response input releases its stream immediately, even if the application retains the response. Cancellation, deferred finalization and per-exchange completion records retain their existing ownership rules.

This trades bounded idle storage per connection and small cache bookkeeping for lower steady-state allocation traffic. It does not cache response payloads or make fresh connections allocation-free. See the [storage reuse investigation](../benchmarks/client/exchange-reuse-20261029.md) for measurements, rejected variants and limits.
