# Load balancer

Accepted 2026-11-20; not yet implemented. The open questions of the first draft were decided on 2026-11-20, and each decision is recorded in its section. They compose the [HTTP/2](http2.md), [QUIC and HTTP/3](http3.md) and [stateless QUIC routing](quic-routing.md) requirements, and the existing client and server.

## Modes and composition

### Requirement

The load balancer provides two modes, usable separately or together in one process:

- **Stateless QUIC routing**: `base-quic-router` forwards HTTP/3, or any QUIC traffic, to backend endpoints by connection identifier, with replies through the router or by direct server return, as [stateless QUIC routing](quic-routing.md) requires.
- **Stateful proxying**: the load balancer terminates TLS over TCP or QUIC, accepts requests over every enabled HTTP version through `base-http-server`, and forwards them to upstream origins through `base-http-client`, over whichever version each upstream is configured or negotiates to use.

Stateless TCP balancing, which forwards TCP packets without terminating connections, is out of scope; HTTP/1 and HTTP/2 are balanced by the stateful proxy.

When both modes share a UDP listener, the load balancer is itself one of the router's backends, with its own server identifier, entered in the router's [backend table](quic-routing.md#configuration-and-rotation) as a local backend. The router's driver still drives the shared socket, and keeps its [forwarding](quic-routing.md#forwarding), Version Negotiation and [Retry offload](quic-routing.md#optional-retry-offload) for every backend. It delivers each datagram its decision assigns to the local backend through a callback or handle the load balancer supplies, which passes the datagram to the in-process `base-quic` endpoint with the client's address, the shared address and the ECN marking, without encapsulation or a network hop; datagrams for other backends are forwarded as usual. The in-process endpoint sends its replies on the shared socket, from the shared address. Local delivery is subject to the router's steady-state rule of no allocation per datagram and at most one payload copy ([performance measurement](quic-routing.md#performance-measurement)). New connections are assigned among the configured backends, which may include the load balancer itself, by the router's ordinary weighted hashing, so the configuration chooses stateless-only, stateful-only or mixed operation per listener. The in-process endpoint takes its connection-identifier codec configuration, stateless reset key, token key and session ticket keys from its configuration value, like any other backend, and shares them with the other backends where the [QUIC requirements](http3.md) require them to be shared. The router library gains the local backend kind but still does not depend on `base-quic` or HTTP; composing the router with the in-process endpoint lives in the load balancer.

A new QUIC connection is not assigned by its TLS server name; assignment uses the listener and the connection identifier only.

### Rationale

Deployments need both: stateless routing scales and survives restarts without terminating TLS, while terminating proxies can inspect requests, choose upstreams per request and translate between HTTP versions. Treating the local endpoint as a backend lets one listener mix them with no second routing mechanism; this was decided on 2026-11-20. A local backend kind in the router, whose delivery the application supplies, was decided on 2026-11-20 after the design review: in-process delivery skips encapsulation while the router's own driver keeps forwarding, Version Negotiation and Retry offload, so the load balancer does not reimplement them, and the router stays free of any dependency on `base-quic`. This corrects the first draft, in which the load balancer drove the shared socket with the router's decision function and the router package needed no change.

Not assigning by server name follows from the decision, on 2026-11-20, that the router decodes no packets: reading the name requires decrypting the client's Initial packets, whose ClientHello can span several datagrams and would have to be buffered per connection, and Encrypted Client Hello hides it.

### Scope

The load balancer package and its use of the router, server, client and QUIC packages.

### Validation

An integration test runs one listener in mixed mode, with QUIC connections assigned to the load balancer and to a remote backend, both migrating client addresses, and HTTP/1 and HTTP/2 requests over TCP on the same host. Datagrams delivered in process carry no encapsulation header, and the in-process endpoint's replies reach the client from the shared address; Version Negotiation and Retry offload work on the mixed listener as on a router alone. Local delivery is covered by the router's allocation and copy check ([performance measurement](quic-routing.md#performance-measurement)). The router package's dependencies are checked as [stateless QUIC routing](quic-routing.md#package-boundary) requires.

## Packages

### Requirement

The proxy forwarding handler is `base-http-proxy`, a sub-crate of this workspace at `crates/proxy`. It depends on `base-http`, `base-http-client` and `base-http-server`, and must not depend on `base-quic-router`, the load balancer, or any configuration format. Its optional `orca` feature adds only the dependencies the [ORCA load reports](#orca-load-reports) section allows. Each group's upstream client can be wrapped in [middleware layers](middleware.md#load-balancer-layers) that adapt requests and responses; a layer with dependencies of its own, such as WebSocket handshake translation with SHA-1, base64 and a random source, lives in the load balancer, so `base-http-proxy` needs none of them. The load balancer is a separate repository and workspace, `base-load-balancer` as a working name, which composes `base-http-proxy`, `base-quic-router`, `base-quic`, `base-http-server` and `base-endpoint`. It is configured only programmatically: listeners, hosts and their certificates, upstream groups and policy objects are values built through its Rust interface. It defines no configuration file format and no executable; a service-configuration system, like Ruby's `async-service` for Falcon, is a separate concern that can build on that interface. This workspace does not depend on the load balancer or on `base-quic-router`.

### Rationale

The handler is useful to any application built on the server, so it belongs beside the client and server it composes; the load balancer depends on two workspaces, and placing it in either would make that workspace depend on the other's router or proxy. A programmatic interface lets applications supply their own policy objects and certificate sources without plugins, and keeps configuration languages out of the libraries. This was decided on 2026-11-20. Placing transformations, starting with WebSocket translation, in self-contained middleware layers of the load balancer rather than in the proxy core was decided on 2026-11-20 after the design review.

### Scope

The manifests of `base-http-proxy` and the load balancer, and this workspace's members.

### Validation

Inspect `cargo tree -e normal` for `base-http-proxy` with default features, and with the `orca` feature, including for SHA-1, base64 and random-number dependencies in the default build, and this workspace's manifests for dependencies on the router or the load balancer.

## Host routing

### Requirement

Each listener of the load balancer has a set of hosts, and each host names the upstream group that serves it. Every request is assigned to a group by the request's semantic `authority`, which the codecs take from the request target or `Host` over HTTP/1, and from `:authority`, or `Host` without it, over HTTP/2 and HTTP/3, before the policy object is called. The authority's host is compared case-insensitively and without its port with the listener's host names. A trailing dot is ignored as well, as in Falcon, decided on 2026-11-20.

On a TLS or QUIC connection, a request is served only when its host is configured on the listener and the certificate this connection presented is the one configured for that host. Any other request is answered with 421 Misdirected Request ([RFC 9110 §15.5.20](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.20)) and is not passed to a policy object or forwarded. This covers HTTP/2 and HTTP/3 clients that reuse one connection for several origins ([RFC 9113 §9.1.1](https://www.rfc-editor.org/rfc/rfc9113.html#section-9.1.1), [RFC 9114 §3.3](https://www.rfc-editor.org/rfc/rfc9114.html#section-3.3)), and a request whose authority differs from the server name of its handshake. On a plaintext listener, a request whose host is not configured is answered with 421.

Host names match exactly or by a wildcard in the leftmost label only, as for certificates. A listener can name a default group, which serves plaintext requests for unconfigured hosts and HTTP/1.0 requests without `Host`; without one, they receive 421. Both decided on 2026-11-20. On a TLS listener, an HTTP/1.0 request without `Host` is assigned like a traditional CONNECT, below, to the group of the host its connection was established for, decided on 2026-11-20.

A traditional CONNECT names its tunnel's target, not an origin the listener serves, so its authority is not used for host routing. It is assigned to the group of the host its connection was established for, by the TLS or QUIC server name, or to the listener's default group on a plaintext listener; without such a group it receives 421. Its target authority skips the host and certificate check and is passed to the policy object, and to the tunnel handler if the policy chooses one. An extended CONNECT names an origin and is routed like any other request.

### Rationale

A certificate chosen by server name says nothing about the origins a client later sends on the same connection. Without this check, a client could reach a host through a connection whose handshake named another, which [RFC 9110 §7.4](https://www.rfc-editor.org/rfc/rfc9110.html#section-7.4) identifies as a way to bypass filters or poison caches. 421 tells the client to retry on a new connection, so legitimate connection reuse still works. Falcon's [proxy middleware](https://github.com/socketry/falcon/blob/5107b0713fd78dbe8b08ee123391b1f4bcffdf12/lib/falcon/middleware/proxy.rb#L85-L90) likewise looks up each request's host by its authority, without its port or a trailing dot.

Assigning a traditional CONNECT by the host of its connection, with its target passed to the policy, was decided on 2026-11-20 after the design review: routing it by its target would answer every tunnel to an unconfigured authority with 421 before the policy object or a tunnel handler could see it.

### Scope

Request routing in the load balancer, and its host definitions.

### Validation

Tests: requests for two hosts on one listener reach their groups over each HTTP version; an HTTP/2 and an HTTP/3 connection whose handshake named one host and that carries a request for another receive 421, and the policy object is not called; an unconfigured host receives 421 over TLS, QUIC and plaintext, or the default group when one is configured; an HTTP/1.0 request without `Host` reaches the default group, or receives 421 without one; a wildcard host name matches one leftmost label and no more; host comparison ignores case and port; a traditional CONNECT to an unconfigured target reaches the policy of its connection's host group over TLS and QUIC, and of the default group over plaintext, with its target authority, and reaches a test tunnel handler the policy chooses.

## Proxy forwarding

### Requirement

The forwarding logic is a library handler usable in any `base-http-server`, independent of the load balancer application, so other applications can proxy without it. It:

- streams request and response bodies in both directions without buffering whole bodies, and always drives both directions, as the [duplex design](duplex.md#direct-writable-client-experiment) requires of proxy forwarding: upload continues while the response is consumed slowly, and after an early complete response. The proxy retains no copy of request data for retries; a request is retried only before its body is read, or when it is empty, as described under [retries](#policy-object);
- forwards between any downstream and upstream versions through the semantic messages, so version-specific framing and connection-specific fields never cross it; each codec removes and generates its own, as now;
- forwards trailers when both sides can carry them, and otherwise follows the receiving codec's rules, such as [HTTP/1 trailers](http1-trailers.md); forwards interim responses, including 103;
- forwards a traditional CONNECT between any versions to the member the policy object chooses, and a protocol switch only when both sides use the same mechanism: an HTTP/1 `Upgrade` to an HTTP/1 upstream, or an extended CONNECT from HTTP/2 or HTTP/3 to an HTTP/2 or HTTP/3 upstream, relying on the codecs' [protocol switching](protocol-switching.md) for status and method. Handshake fields, such as WebSocket's `Sec-WebSocket-Key` and `Sec-WebSocket-Accept`, are forwarded unchanged, and a declining response is relayed as the codecs frame it. A switch that would cross mechanisms, such as a WebSocket upgrade from an HTTP/1 client to an HTTP/2 upstream, is answered with 502 and not forwarded, unless a [middleware layer](middleware.md#load-balancer-layers) wrapping the group's client translates it, as the load balancer's WebSocket layer does. HTTP/1 `Upgrade` offers without an extended-CONNECT mapping, including `h2c`, are not switches outside HTTP/1: an HTTP/2 or HTTP/3 upstream client sends such a request as an ordinary request, as protocol switching requires;
- never dials the authority named by a CONNECT request. An application that wants the proxy to open tunnels to requested authorities supplies a tunnel handler, which the policy object can choose instead of a member; none is configured by default;
- removes forwarding fields (`Forwarded`, `X-Forwarded-*`) unless the transport peer of the downstream connection, the address the connection was received from, its TCP peer or the UDP peer of an HTTP/3 connection, is configured as a trusted forwarding peer. The trusted forwarding peers are configured separately from the trusted senders of the [PROXY protocol](#connection-information-and-the-proxy-protocol). It then adds the configured forwarding fields. By default it adds [`Forwarded`](https://www.rfc-editor.org/rfc/rfc7239.html) with `for`, `proto` and `host` for the downstream request, appending an element to a value received from a trusted peer. Addresses follow the node syntax of RFC 7239 §6 and are quoted where the field syntax of §4 requires, such as for IPv6 addresses; a peer without an IP address, such as one a PROXY protocol header reports over a Unix socket, is `for=unknown` (§6.2). `by` and the `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` fields are added only when configured;
- removes `Alt-Svc` from upstream responses, and adds the listener's own `Alt-Svc` when configured, since an upstream's alternative services describe the upstream, not the load balancer;
- applies the server's default 425 guard of [early data](http3.md#early-data-0-rtt) like any server: a request received in early data, or carrying `Early-Data`, whose method is not safe is answered with 425 Too Early and never reaches the policy object. It forwards the other requests received in early data, those with safe methods, with `Early-Data: 1`, and never removes a received `Early-Data` field ([RFC 8470 §5.1](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.1)), but only to a group configured as understanding `Early-Data`, meaning its upstreams answer 425 correctly. For any other group, which is the default, it holds the request until the downstream handshake completes and then forwards it as an ordinary request, as [RFC 8470 §6.1](https://www.rfc-editor.org/rfc/rfc8470.html#section-6.1) requires of a gateway uncertain of the origin's support. It lets the client send a request upstream in early data only if the request arrived in early data or with `Early-Data: 1`. Every forwarded request has the client's automatic resend after 425 turned off, so a 425 from the upstream reaches the policy object, which handles it as [retries](#policy-object) describe ([RFC 8470 §5.2](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.2)). Early data arrives only over HTTP/3 for now: 0-RTT over TLS over TCP is out of scope until `base-endpoint` supports it;
- propagates cancellation both ways: a downstream reset or disconnect cancels the upstream exchange, or a pending policy decision, resetting the upstream stream or retiring its HTTP/1 connection; an upstream failure resets or closes the downstream exchange once its response has started.

`Expect: 100-continue` is forwarded end to end, decided on 2026-11-20: the Expect is sent upstream, and the upstream's interim responses, 100 Continue among them, are relayed downstream through the existing interim callbacks, the upstream client's `InterimHandler` feeding the handler's `InterimResponses`, so the member that will process the body decides whether it is sent. The proxy sends no 100 of its own, and a request the policy object answers directly receives its final response without 100 ([RFC 9110 §10.1.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.1.1)). The HTTP/1 server codec's existing 100 before a 101 to a request that expected one and received none is codec sequencing, not a response of the proxy.

Without a response from the policy object, the proxy responds 502 when the upstream fails before a response head or a switch would cross mechanisms, 503 when the group has no eligible member or a decision deadline expires, and 504 when the upstream response-head deadline expires, without forwarding upstream error details, as [RFC 9110 §15.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.6) defines these statuses; decided on 2026-11-20.

### Rationale

These are the proxy properties the existing [duplex experiment](duplex.md) and [client/connection taxonomy](client-connection.md) anticipate, made available as a reusable handler. Forwarding through semantic messages is what lets one proxy translate between versions without request smuggling through version-specific fields. `Forwarded` is the default, decided on 2026-11-20, as the standard field; backends that read only `X-Forwarded-*` are served by configuration. Dropping HTTP/1 upgrade offers without an extended-CONNECT mapping across versions was decided on 2026-11-20. Forwarding protocol switches only between sides using the same mechanism, answering others with 502, and leaving WebSocket translation to a middleware layer of the load balancer were decided on 2026-11-20 after the design review, revising the earlier decision that the proxy translates WebSocket handshakes itself.

Judging forwarding fields by the actual transport peer was decided on 2026-11-20. A layer 4 balancer that sends the PROXY protocol does not author forwarding fields, so its addresses belong only among the PROXY senders; trusting it for forwarding fields would let any client behind it supply its own.

Routing CONNECT through the policy, with tunnels to requested authorities only through an application-supplied handler, was decided on 2026-11-20: a proxy that dialed requested authorities by default would be an open proxy.

RFC 8470 requires the early-data rules of intermediaries. Applying the server's 425 guard at the proxy, including to requests that arrive carrying `Early-Data` as RFC 8470 §5.1 requires, forwarding only safe-method early requests, and only to groups configured as understanding `Early-Data`, holding them until the downstream handshake completes otherwise, as RFC 8470 §6.1 requires of a gateway, and leaving 0-RTT over TLS over TCP out of scope until `base-endpoint` supports it were decided on 2026-11-20, as was turning off the client's automatic 425 resend for forwarded requests: RFC 8470 §5.2 requires an intermediary to forward a 425 to a request that carried `Early-Data`, and to wait for the handshake of the connection the request arrived on before retrying any other, which the upstream client's own handshake does not show. Upstream `Alt-Svc` would point clients at the upstream's alternative services, or at ports of the load balancer's host where nothing listens.

### Scope

The proxy handler and its integration with `base-http-client` and `base-http-server`. Forwarding tunnels and protocol switches needs managed client tunnels, CONNECT, HTTP/1 upgrades and extended CONNECT in `base-http-client`, which currently rejects all of them before dialing; they are a prerequisite. Cross-version WebSocket translation is covered by [load balancer layers](middleware.md#load-balancer-layers).

### Validation

Port the [three-party proxy tests](../crates/client/tests/proxy.rs) to every pair of downstream and upstream versions. The tests cover:

- slow response consumption with ongoing upload, and early responses;
- trailers and interim responses across versions; `Expect: 100-continue`, with the upstream's 100 relayed, no 100 downstream when the upstream sends none, and a request the policy object answers directly receiving its final response without 100;
- forwarding fields: those from an untrusted peer removed; those from a trusted TCP peer extended; those from a client behind a trusted PROXY protocol sender that is not a trusted forwarding peer removed; the default `Forwarded` value for IPv4, IPv6 and Unix-socket clients; configured `X-Forwarded-*` fields;
- removal of upstream `Alt-Svc`;
- WebSocket echo through the proxy, without layers, for every pair of versions using the same mechanism, against independent WebSocket servers, with the handshake fields forwarded unchanged; a cross-mechanism WebSocket switch answered with 502 without an upstream request; an `h2c` offer crossing to HTTP/2 sent as an ordinary request;
- a CONNECT forwarded to a member, a CONNECT with no tunnel handler that opens no connection to its authority, and one through a test tunnel handler;
- over HTTP/3, a safe-method request received in early data forwarded with `Early-Data: 1`; an unsafe-method one answered with 425 by the proxy without calling the policy object; the upstream client never resending a forwarded request after 425 itself;
- cancellation in each direction, including during a pending policy decision, and each error status.

## Upstream groups and health

### Requirement

Upstreams are grouped; each group has its members, protocol policy, connection pool bounds and policy object. The policy object configures the group's upstream clients, including their bounded retry policy ([retries](#policy-object)), and any [middleware layers](middleware.md#load-balancer-layers) wrap them. A group's protocol policy selects HTTP/1, HTTP/2 or HTTP/3 among the versions the client was built with; HTTP/3 to upstreams is configured explicitly per group, and upstream clients are built with `Alt-Svc` discovery disabled, so upstreams are never switched to HTTP/3 by `Alt-Svc` ([HTTP/3 selection](http3.md#selection-and-discovery)).

Removing or draining a member stops new exchanges to it while its in-flight exchanges complete. For stateless QUIC backends, draining uses the router's draining backend state ([routing decisions](quic-routing.md#routing-decisions)): the draining backend's existing connections keep reaching it while new connections are hashed among the others. Removing a stateless backend from the router's table entirely ends routing of its existing connections, so the load balancer removes one only after draining it. The configuration can be replaced atomically while running, as the [router's](quic-routing.md#configuration-and-rotation) can.

Health is the policy object's responsibility, decided on 2026-11-20. The proxy excludes only draining and removed members from the eligible members it supplies; the policy decides which of them are healthy, from the exchange outcomes it observes (passive checks) and from probes (active checks). For active checks, the proxy lets a policy send a probe request to a chosen member through the group's client, outside any downstream request. The built-in default policy performs both by default, decided on 2026-11-20: it excludes a member after a number of consecutive failures, such as failed connections, resets before a response head and response-head deadlines, restores it after an interval or a successful probe, and probes each member with a configured request at an interval. Its thresholds, intervals and default probe request are recorded when M11 starts.

### Rationale

Configuring the group clients through the policy object was decided on 2026-11-20 after the design review. Health and draining distinguish a load balancer from a fixed reverse proxy, and deployments judge health differently, from utilization reports to queue depth, so health belongs with the policy that already combines those signals; and replacing configuration without restart preserves long-lived connections. The user decided on 2026-11-20 that HTTP/3 upstreams are configured explicitly per group rather than discovered. Using the router's draining state is necessary because a stateless router knows a backend's connections only by the server identifier they carry; removing it loses them.

### Scope

Upstream configuration, draining, the probe facility for policies and the default policy's health checks in `base-http-proxy` and the load balancer.

### Validation

Tests: under the default policy, a member failing consecutive exchanges, and one failing its probe, is excluded and later restored; a custom policy that ignores failures keeps forwarding to the member; draining completes in-flight exchanges and starts no new ones over each version; draining a stateless QUIC backend keeps its established connections on it, including after client address changes, while new connections go to other backends; replacing the configuration during traffic drops no exchanges to unaffected members; an upstream advertising `Alt-Svc: h3` is not reached over HTTP/3 unless the group configures it.

## Policy object

### Requirement

Each request is decided by a policy object, defined by `base-http-proxy` as a trait, which an application can implement. The proxy handler and the load balancer take the policy type as a type parameter, and call it with static dispatch. Replacing the configuration swaps values of that one type; a deployment whose groups use different policies supplies an application enum over them, which implements the trait.

- **Decision.** After host routing assigns the request to a group, the policy is called before any upstream connection is chosen. It receives the request metadata, for a traditional CONNECT including its target authority, the downstream [`Peer`](#connection-information-and-the-proxy-protocol) (its address, and TLS or QUIC details where available, including whether the request arrived as early data) and the group's eligible members, which may be none. It returns one of: forward to a member, or to the application's tunnel handler for CONNECT; respond directly with a response the policy supplies, such as 503 or 429 with `Retry-After` to shed load, or a redirect to another load balancer; or reject the request. This one decision covers pre-filtering, such as refusing traffic classified as automated or shedding under overload, as well as choosing a member.
- **Asynchronous decisions and queueing.** The decision is a future, so a policy can hold requests in its own queues, such as one global queue and one per member, tracking in-flight requests or utilization, and release each when it chooses a member. A synchronous policy returns a future that is ready on its first poll, which needs no allocation. The proxy bounds the time from the request head to the decision with a configured decision deadline, and cancels the decision when the deadline expires or the downstream exchange is cancelled.
- **Reject.** A rejected request receives no response. Over HTTP/1, the server closes the connection without writing a response. By default, over HTTP/2, the stream is reset with `CANCEL`; over HTTP/3, the request stream is aborted, and reading from it is stopped, with `H3_REQUEST_CANCELLED`. A reject decision can override the action, decided on 2026-11-20: it can name another stream error code, such as `ENHANCE_YOUR_CALM` or `H3_EXCESSIVE_LOAD`, or close the whole connection, with GOAWAY over HTTP/2 and HTTP/3. A reject never uses `REFUSED_STREAM` or `H3_REQUEST_REJECTED`, which tell the client the request was not processed and invite it to retry automatically.
- **Retries.** The proxy never retries by itself. The policy configures each group's upstream clients, including their bounded retry policy: an attempt limit, by default 3, as Async::HTTP's client [`DEFAULT_RETRIES`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/client.rb#L19), within which a client retries transparently recoverable requests on the same member, as the [proposal](proposal.md#client) describes; retries can also be disabled. The automatic resend after 425 is part of that retry policy, and is turned off for every forwarded request. When an upstream attempt still fails before its response head is forwarded downstream, the proxy calls the policy again with the outcome and its facts. The first fact is whether the attempt is proven unprocessed: the upstream refused the stream with `REFUSED_STREAM` or `H3_REQUEST_REJECTED`, the stream is above the last identifier of a received GOAWAY, or the connection failed before any of the request was sent. The second is whether the request body can be replayed, which it can only while the proxy has not started reading it, or when it is empty: the proxy keeps no replay copy, as Falcon's proxy does not, decided on 2026-11-20. The policy decides again among forwarding to a member, possibly the same one, responding and rejecting, using the request's method and these facts. A re-decision that forwards again counts against the same attempt limit, so one request makes at most that many attempts across all members, and the request's deadlines still apply. Once a response head has been forwarded downstream, no attempt is retried.
- **Too Early.** A 425 from the upstream reaches the policy before anything is forwarded downstream, with two more facts: whether the request carried `Early-Data` when it was received, and whether the downstream handshake has completed. For a request that carried `Early-Data`, the policy forwards the 425 downstream. For one received in early data without the field, it forwards the 425 or retries only after the downstream handshake completes ([RFC 8470 §5.2](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.2)). The proxy enforces both rules, decided on 2026-11-20, refusing a retry decision for a request that carried `Early-Data` and holding a retry until the downstream handshake completes.
- **Member state.** For each member the proxy maintains and supplies the leading indicators it can observe itself: in-flight requests, and recent completions with their latency, status and failures, over a window the policy configures. These are counted per request, not per connection, since one HTTP/2 or HTTP/3 connection carries many requests.
- **Observations.** The policy is notified when each forwarded exchange starts and when it finishes, that is, when both directions have completed or been terminated, with its outcome. It can inspect the upstream response's metadata and trailers, so it can read utilization that backends report in-band, such as CPU load, available request slots or internal queue depth. Applications can also deliver reports obtained out of band, such as by polling or a push channel, to the policy through the proxy's API. The policy owns how it combines leading indicators with reported utilization, and which fields carry reports; `base-http-proxy` defines no load-report convention of its own beyond the optional [ORCA](#orca-load-reports) parser.
- **Built-in policies.** `base-http-proxy` provides policies implemented through the same trait, which applications can use directly or compose with their own, including the default policy, whose health checks [upstream groups and health](#upstream-groups-and-health) describe. The default policy's member selection, and which other built-ins ship first, are open decisions, made when M11 starts.

The stateless QUIC router does not use this policy object: it has no requests to inspect and keeps no state, and assigns new connections by weighted hashing. The load balancer can still feed the same utilization into the router's backend weights, by replacing its configuration ([configuration and rotation](quic-routing.md#configuration-and-rotation)); weights apply to new connections only, and established connections are unaffected.

### Rationale

Selection in practice combines a leading indicator, such as in-flight requests per member, with actual utilization reported by the backends, and deployments differ in what they report and how. The user decided on 2026-11-20 that this belongs in a policy object applications can implement, which also performs pre-filtering such as shedding or redirecting traffic. Deciding per request, with the request available, is what affinity, classification and per-request load all need. Delegating retries to the policy, with the proxy reporting the facts that make a retry safe, and allowing asynchronous decisions so policies can queue requests under deadlines, were decided on 2026-11-20. The facts follow the [retry model](proposal.md#client): processing state, semantic retryability and body reproduction are independent. A proven-unprocessed attempt can be retried whatever its method, as [RFC 9113 §8.7](https://www.rfc-editor.org/rfc/rfc9113.html#section-8.7) permits. The user decided on 2026-11-20, after the design review, that the policy configures the group clients' bounded retries, with Async::HTTP's default of 3 attempts, and that re-decisions count against the same limit, so retries are bounded by attempts as in Async::HTTP and not only by deadlines; and that the policy, not the client, handles a 425 to a forwarded request, as RFC 8470 §5.2 requires of intermediaries. Backend weights in the router's configuration, used for new connections only, were decided on 2026-11-20.

Static dispatch was a reviewer choice disclosed to the user. A type parameter keeps the request path free of dynamic dispatch, and an application enum keeps heterogeneous policies explicit. A reject that invited automatic retry would turn refused traffic into repeated traffic.

### Scope

The policy trait, retries and member state in `base-http-proxy`, and their use in the load balancer.

### Validation

A custom policy test:

- sees in-flight counts per member rise and fall across many requests on one HTTP/2 connection;
- reads a load report from upstream response trailers and from an out-of-band report, and changes its choice accordingly;
- sheds a request with 503 and `Retry-After`, redirects one, and rejects one, none of which opens an upstream connection;
- queues requests and releases them, with an expired decision deadline answered and the queued decision cancelled.

Reject tests check the default wire form over each version, an overridden code and an overridden connection close, that a reject naming `REFUSED_STREAM` or `H3_REQUEST_REJECTED` is refused, and that an independent HTTP/2 client does not automatically retry a rejected request. Retry tests check that the policy receives an unprocessed `REFUSED_STREAM`, a GOAWAY above the last identifier and a connection failure before sending as proven unprocessed, and a reset after a partial upload as not proven. They check that a body is replayable before reading and not replayable once reading has started; that a retry on another member succeeds; that no retry follows a forwarded response head; that a group's client retries a recoverable request on the same member within its attempt limit, 3 by default, and makes one attempt with retries disabled, while the policy receives only the final outcome; and that a policy re-deciding after every failure stops at the same limit across members. Too Early tests check that a safe early request is forwarded with `Early-Data: 1` only to a group configured as understanding it, and otherwise held until the downstream handshake completes and forwarded without it; that a 425 to a request that carried `Early-Data` is forwarded downstream and not retried, and that a retry of one received in early data without the field waits for the downstream handshake. Inspect the policy trait and the proxy's request path for dynamic dispatch.

## ORCA load reports

### Requirement

An optional `orca` feature of `base-http-proxy` parses in-band [ORCA](https://github.com/cncf/xds/blob/dba9d589def2cd10099a3a64887d859188c2f57a/xds/data/orca/v3/orca_load_report.proto) load reports from upstream response headers and trailers. Each report becomes a typed report, delivered to the policy with the exchange's observations. The forms follow [Envoy's parser](https://github.com/envoyproxy/envoy/blob/122129a87f6ff72a094f5a0b4d6998951b2f0409/source/common/orca/orca_parser.cc):

- `endpoint-load-metrics` with a `TEXT ` prefix: comma-separated pairs of a name and a value, separated by `=` or `:` as Envoy accepts;
- `endpoint-load-metrics` with a `JSON ` prefix: the JSON mapping of `OrcaLoadReport`;
- `endpoint-load-metrics` with a `BIN ` prefix, or the `endpoint-load-metrics-bin` field: a base64-encoded serialized `OrcaLoadReport`, with or without padding.

The parsing is expressed in the header-interpretation system, decided on 2026-11-20: the feature provides [`base-message`](https://github.com/base-rs/base-message) field types for `endpoint-load-metrics` and `endpoint-load-metrics-bin`, each parsing its whole value in `FieldType::parse`, which takes no context, combining as a singleton, and treating an invalid value as an error, `base-message`'s default; and a section declaring both that the proxy interprets over each upstream response's headers and, separately, as [trailer interpretation](trailer-interpretation.md) requires, over its trailers. A section error, from a second instance of either field or a malformed value, means that section's report is ignored and counted in the proxy's statistics rather than failing the exchange. When both fields are present and valid, `endpoint-load-metrics-bin` wins. The typed report carries CPU, memory and application utilization, requests and errors per second, and the `request_cost`, `utilization` and `named_metrics` maps. The text form names map entries with the `utilization.` and `named_metrics.` prefixes, and cannot carry `request_cost`. In the text form, as in Envoy, duplicate and unknown names, and values that are negative, infinite or not numbers, make a report malformed. The same value checks apply to the JSON and binary forms, decided on 2026-11-20, whose scalar fields the ORCA definition constrains to be non-negative, although Envoy's parser checks them only in the text form.

Reports have no ORCA-specific bounds: they are bounded only by the codecs' header and trailer section limits, by default 32 KiB and 100 fields, decided on 2026-11-20. This needs no change to `base-message`.

By default the proxy removes ORCA fields from responses it forwards downstream, with or without the feature, so a client balancing across load balancers does not see one upstream's load as the load balancer's. Out-of-band ORCA, a gRPC streaming service, is not provided; applications can deliver such reports through the out-of-band API. Without the feature, no ORCA parsing is compiled.

The feature decodes the protobuf wire format of `OrcaLoadReport` directly, since its fields are scalars and maps from strings to numbers, and adds no protobuf code generator or runtime. It may add `base-message`, unless `base-http` already provides it, a focused base64 decoder and a focused JSON parser, and no other dependency.

### Rationale

ORCA is an existing convention for in-band load reports, so a policy can use it without each backend inventing its own. Field types reuse the shared interpretation and its error reporting, rather than a parser of the proxy's own, and stay in `base-http-proxy` with their only consumer. Parsing ORCA as an optional feature was decided on 2026-11-20. Ignoring and counting malformed reports, removing ORCA fields downstream by default, and not providing out-of-band ORCA were reviewer choices disclosed to the user. Relying on the codecs' section limits, with context-free field types, singleton combination and section errors ignored and counted, was decided on 2026-11-20 after the design review; it revises the earlier decision of a 4 KiB value bound and a 64-entry bound configurable per group, which `FieldType::parse` could not receive without a change to `base-message`. The codecs' limits already bound the work and memory a report can cost.

### Scope

The `orca` feature of `base-http-proxy`: its field types and section, and their interpretation over upstream responses.

### Validation

With the `orca` feature, tests parse each form, the three `endpoint-load-metrics` prefixes and `endpoint-load-metrics-bin`, from headers and from trailers into the same typed report. They check the precedence of `endpoint-load-metrics-bin` when both fields are valid, unpadded base64, every field and map, and that malformed reports and a second instance of either field are ignored and counted, in headers and in trailers independently. ORCA fields are removed from the downstream response by default, with and without the feature. Fuzz the parsers. Inspect `cargo tree -e normal` with the feature for a protobuf runtime or code generator, and the default build for ORCA parsing code and dependencies.

## Connection information and the PROXY protocol

### Requirement

`base-http-server` makes each request's connection information available to its handler: the peer and local addresses, TLS or QUIC details where present, such as the server name, the negotiated protocol and whether the request arrived as early data, as [early data](http3.md#early-data-0-rtt) defines that flag, and what a PROXY protocol header supplied. Over HTTP/3, the peer address is the connection's current peer address when the request stream is received. Handlers currently receive no connection information, so this extends the server's handler interface, which [clients and connections](client-connection.md#exchange-completion-and-reuse) describes as receiving the request. The information is a `Peer` value, named after `protocol-http`'s [`Peer`](https://github.com/socketry/protocol-http/blob/6b9b3d84f4e6261068fac7b52e567d70eb3909e5/lib/protocol/http/peer.rb) and defined in `base-http`, passed to the handler in the `ServerContext` of the shared [request interface](middleware.md#shared-request-interface), not as part of the request or its metadata, so a forwarded request cannot carry it upstream; decided on 2026-11-20. The proxy uses it for host routing, forwarding fields and early data, and passes it to policy objects. The `Peer` implemented on 2026-11-21 reports the addresses, encryption and negotiated application protocol, and early data, which is always false until HTTP/3; the TLS server name needs an accessor in `base-endpoint`, whose 0.3 TLS connection does not expose it, and PROXY protocol details arrive with the listener wrapper.

Listeners can be configured to require the [PROXY protocol](https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt), version 1, version 2 or either, from configured trusted source addresses:

- The header is read and validated on each accepted TCP connection before the TLS handshake or HTTP/2 preface detection, within the connection's establishment deadline. A listener that requires the header never guesses whether one is present. A connection from an untrusted source, or without a valid header, is closed without reading further. The header may arrive split across reads.
- A version 1 header is a line of at most 107 bytes including its CRLF; a connection whose first 107 bytes contain no CRLF is closed. A version 2 header is its 16-byte fixed part followed by the number of bytes its length field states. The listener bounds the whole version 2 header, and closes a connection whose length field exceeds the bound. By default 536 bytes, decided on 2026-11-20, the TCP segment size the specification designs both formats to fit, and configurable up to the format's maximum of 16 + 65,535 bytes.
- Version 2 headers with a version other than 2, or a command other than `LOCAL` or `PROXY`, are closed, as are address families above 3 and transport protocols above 2, which the specification makes invalid. A `LOCAL` command, which a balancer sends for its own health checks, and a version 1 `UNKNOWN` header are accepted with the actual connection addresses, and their address information is ignored. A `PROXY` command with the stream transport reports its addresses: TCP over IPv4 or IPv6 (`\x11`, `\x21`) reports them as given, and a Unix stream socket (`\x31`) reports a peer without an IP address, since only non-stream transports are closed, decided on 2026-11-20. A `PROXY` command with an unspecified family or a transport other than stream, including UDP and Unix datagram sockets, is closed, decided on 2026-11-20.
- Type-length-value entries after the addresses are parsed within the header bound; an entry overrunning the header closes the connection. A `PP2_TYPE_CRC32C` checksum is verified, and a mismatch closes the connection. `PP2_TYPE_NOOP` entries are skipped. Every other entry, including `PP2_TYPE_AUTHORITY`, `PP2_TYPE_SSL`, `PP2_TYPE_UNIQUE_ID` and custom types such as a cloud provider's endpoint identifier, is exposed to handlers in order as its type and raw value, without interpretation.
- The header's source address becomes the peer address reported to handlers, policy objects and `Forwarded`; the actual TCP peer is retained as the address the connection was received from, and is the address that [forwarding-field trust](#proxy-forwarding) judges.
- The header is consumed at the transport level, below TLS, as an accepted-connection wrapper in `base-endpoint` alongside its TLS listener, so plain and TLS listeners use it alike and HTTP codecs never see it.

The proxy never sends the PROXY protocol to upstreams.

### Rationale

Behind a layer 4 balancer that does not preserve client addresses, such as AWS PrivateLink, or a Network Load Balancer with client IP preservation disabled, the PROXY protocol is the source of the client's address that `Forwarded`, policy decisions and logs need. Sending it to upstreams would describe a whole pooled connection that carries requests from many clients, which the specification forbids on multiplexed connections, and `Forwarded` already supplies the client's address over HTTP. Accepting it only on listeners from trusted sources, before TLS and in `base-endpoint`, and never sending it, was decided on 2026-11-20, as was exposing connection information to handlers. The bounds, `LOCAL` and `UNKNOWN` handling, the refusal to guess and the checksum rule follow the specification. Raw type-length-value entries let deployments read provider-specific entries without the library interpreting each.

### Scope

The handler interface of `base-http-server`, an accepted-connection wrapper in `base-endpoint`, and listener configuration in the load balancer. The wrapper's requirements belong in `base-endpoint`'s design documents when it is implemented.

### Validation

Tests:

- a handler observes peer, local and TLS details, and the current peer address after an HTTP/3 client migrates;
- version 1 and version 2 headers from a trusted source set the reported peer, over plain TCP and TLS, including a header split across reads;
- each of the following closes the connection before any TLS or HTTP processing: a header from an untrusted source; a missing or malformed header; a version 1 line without CRLF within 107 bytes; a version 2 length beyond the bound; an unexpected version or command; an unspecified, invalid or datagram family and transport; an overrunning entry; a checksum mismatch; a stalled header;
- `LOCAL` and `UNKNOWN` use the actual addresses, a Unix stream header reports a peer without an IP address, and entries are exposed in order;
- `Forwarded` and the policy object see the header's address.

Fuzz the header parser.

## Hosts and certificates

### Requirement

A listener serving several hostnames selects each connection's certificate by the TLS server name, through a rustls certificate resolver in the `ServerConfig` it supplies. The listener's TCP TLS and QUIC endpoints each have their own `ServerConfig`, since QUIC needs TLS 1.3 and its own ALPN, and both use the same resolver. The load balancer builds the resolver from its host definitions. The set of certificates can be replaced atomically while running, so renewed certificates apply to new handshakes without a restart. `base-endpoint` gains no generic certificate interface while rustls is its only TLS provider, and obtaining certificates, such as by ACME, is outside these libraries.

Server names match host names exactly or by a wildcard in the leftmost label, as in [host routing](#host-routing). A handshake with no server name, or naming no configured host, uses the listener's default certificate if one is configured, and otherwise fails. Both decided on 2026-11-20.

### Rationale

This follows the Ruby layering. `io-endpoint`'s [`SSLEndpoint`](https://github.com/socketry/io-endpoint/blob/21586fcb293b35b36c79ed288eb8679b262591fe/lib/io/endpoint/ssl_endpoint.rb) takes a provider-specific `SSLContext`, and Falcon's [proxy environment](https://github.com/socketry/falcon/blob/5107b0713fd78dbe8b08ee123391b1f4bcffdf12/lib/falcon/environment/proxy.rb) selects each host's context by server name. `base-endpoint` already accepts a rustls `ServerConfig`, whose resolver performs the same selection for both TLS over TCP and QUIC. This was decided on 2026-11-20.

### Scope

Certificate selection and replacement in the load balancer, and its use of `base-endpoint` and `base-quic`.

### Validation

Tests: two hostnames on one listener each receive their own certificate over TLS and QUIC; a wildcard host's certificate is selected for a name one label below it and not for one two labels below; an unknown or absent name receives the default certificate or fails; replacing a certificate during traffic applies to new handshakes and leaves established connections unaffected. Inspect `base-endpoint` for a certificate abstraction beyond the rustls configuration.

## Deadlines and limits

### Requirement

Before the load balancer is released, the client and server provide the following deadlines, measured with the scheduler `Clock`:

- establishment, including TLS and QUIC handshakes and a PROXY protocol header;
- receiving a complete downstream request head after the connection is established or its previous exchange completes;
- an upstream's response head after the request head is sent;
- body inactivity in each direction, on both the downstream and the upstream exchange: no body data or trailers transferred, while the exchange waits for the peer, for a configured interval;
- idle connections.

The proxy adds the decision deadline of the [policy object](#policy-object). Each deadline is configurable. Its default is an open decision, chosen with the client and server implementation and recorded here.

The load balancer bounds downstream connections and exchanges through the server's limits, and upstream connections through the client's pool. It bounds retained body data through the protocols' flow-control windows and the client's and server's bounded body queues, and attempts through each group's attempt limit. No queue between downstream and upstream is unbounded, including queues a policy object keeps, which the decision deadline bounds in time.

### Rationale

Deadlines remain follow-up work in the [roadmap](roadmap.md), and a load balancer without them holds resources for stalled peers indefinitely. A peer can stall before sending its head, as in slow header attacks, or in the middle of a body, so each of these phases needs a deadline. Bounding policy queues by deadlines was decided on 2026-11-20.

### Scope

`base-http-client`, `base-http-server`, `base-http-proxy` and the load balancer's configuration.

### Validation

Tests for each deadline with stalled peers: a client that sends part of a request head; a stalled upload and a stalled download, on each side; an upstream that never answers; a queued decision. Each releases its connection or stream and its pool use. A test checks that a slow downstream reader bounds the data buffered from its upstream.
