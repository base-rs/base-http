# QUIC and HTTP/3

Accepted 2026-11-20; not yet implemented. The open questions of the first draft were decided on 2026-11-20, and each decision is recorded in the Rationale of its section. They refine the QUIC and HTTP/3 parts of the [component evaluation](components.md#quic-native-transport-with-existing-implementations-as-references), [proposal](proposal.md#http3) and [transport layering](transport.md#future-packet-encryption-and-http3). Stateless routing of QUIC datagrams to these endpoints is specified separately in [QUIC routing](quic-routing.md); the HTTP/2 requirements in [HTTP/2](http2.md) that are not specific to TCP apply here as the [mapping](#mapping-from-http2) below states.

The Socketry implementations offer no HTTP/3 reference. At the reviewed [protocol-quic `a21aa21`](https://github.com/socketry/protocol-quic/tree/a21aa21a7cfc3bac6677af63edd8b1cde4c94fda), a Ruby binding over a C++ ngtcp2 wrapper, nghttp3 is packaged but unused and no stream read or write API is exposed to Ruby; Async::HTTP has no QUIC or `Alt-Svc` code. The RFCs, `quinn-proto`, quiche and ngtcp2 are the references, as the component evaluation records.

## Packages and dependencies

### Requirement

| Package | Responsibility | Production dependencies |
| --- | --- | --- |
| `base-quic-wire` | QUIC version-independent header parsing ([RFC 8999](https://www.rfc-editor.org/rfc/rfc8999.html)), variable-length integers, connection identifiers, connection-identifier generation and server-identifier encoding, the router encapsulation header that [QUIC routing](quic-routing.md#forwarding) defines, and Version Negotiation packets. | None beyond focused support. AES for encrypted identifiers is always included; other cryptographic primitives, for Retry integrity and stateless reset token derivation, are behind optional features, as [QUIC routing](quic-routing.md) requires. No TLS, scheduler or HTTP. |
| `base-quic` | QUIC v1 transport ([RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html), [RFC 9001](https://www.rfc-editor.org/rfc/rfc9001.html), [RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html)): endpoint and connection state, packet protection, streams, recovery, congestion and flow control, address validation, migration, 0-RTT, and a scheduler-driven packet and timer driver. | `base-quic-wire`, `base-scheduler`, rustls through its QUIC interface. No HTTP. |
| `base-http-qpack` | QPACK field compression ([RFC 9204](https://www.rfc-editor.org/rfc/rfc9204.html)), a native implementation behind a Base-owned interface. | `base-http-hpack` integer and Huffman primitives only. No other QPACK implementation, scheduler or `base-http`. |
| `base-http3` | HTTP/3 mapping ([RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html)): control and request streams, settings, field sections, data, trailers, errors and GOAWAY. | `base-http`, `base-http-qpack`, `base-quic`, `base-scheduler`, and [`base-message`](https://github.com/base-rs/base-message) for indexed section interpretation, as [static table indices](#http3-mapping) describe. |

`base-quic-wire` and `base-quic-router` are sub-crates of the `base-quic` workspace, at `crates/wire` and `crates/router` beside the root `base-quic` package, as the HTTP packages share this repository's workspace. Each is published separately; `base-quic` and `base-quic-router` depend on `base-quic-wire` by version and path. Sharing a workspace does not relax the dependency rules: `base-quic-wire` and `base-quic-router` must not depend on the root `base-quic` package.

`base-http-protocol`, `base-http-server` and `base-http-client` enable HTTP/3 through an `http3` feature. Without it none of them depends on these packages. No package depends on a concrete scheduler, quinn, quiche, ngtcp2, `h3`, another QPACK implementation, Hyper, `http`, `http-body` or Tokio, except as a development dependency for interoperability tests or as a differential oracle in QPACK tests and fuzzing.

### Rationale

This applies the [crate structure](proposal.md#3-crate-structure) and the runtime-dependency rule of the repository's agent instructions, adding `base-quic-wire` so that a router can share the connection-identifier format with `base-quic` without depending on TLS or transport state. A separate package, rather than a feature-gated module of `base-quic`, keeps that boundary checkable by inspecting dependencies, since features enabled anywhere in a build apply to every dependent. Keeping it in the `base-quic` workspace versions the format with the transport that issues it; the sub-crate layout was decided on 2026-11-20. QPACK is implemented natively, like HPACK, with an independent codec only as a test oracle, decided on 2026-11-20; QPACK's decoder must bound its work while decoding and report blocked sections, which a native codec over the shared primitives fits as HPACK's port does.

### Scope

The manifests and public APIs of these packages.

### Validation

Inspect `cargo tree -e normal` for each package, for `base-quic-wire` with and without its features, and for the client and server with and without `http3`.

## QUIC transport core and driver

### Requirement

- The endpoint and connection state are usable without a scheduler. Their input is a received datagram with its remote address, local address and ECN marking or its absence, and the current time; their output is datagrams to send, each with its destination and source addresses, plus timer deadlines and stream events. They never read a socket or a clock themselves.
- Input is independent of the socket that delivered the datagram. An endpoint behind a router receives forwarded datagrams carrying the client's original addresses and processes them as if received directly, and its output carries the addresses the forwarding layer needs.
- The driver uses the scheduler's packet reader and writer and `Timers`, retains each outgoing buffer until its send completes, and takes protocol time from the scheduler `Clock`. Each connection's protocol progress, and on the server the handler futures of its streams, can be scheduled independently of other connections sharing an endpoint socket, in the same execution modes as the [HTTP/2 driver](http2.md#protocol-core-and-connection-driver). Batching (`recvmmsg`/`sendmmsg`), GSO, GRO and ECN are optimizations; the correctness path does not require them.
- A server binds one socket per local address it serves directly, including each preferred address, never a wildcard address, and sends each datagram from the socket bound to its source address, or through the [forwarding layer](quic-routing.md#forwarding) for the shared address of a router. A client connects its UDP socket to the server address, or binds it to a specific address, and learns its local address through the scheduler's `SocketConnect` and `SocketLocalAddress`; each connection migration or move to a preferred address uses a new socket.
- Stream receive credit is returned when the application's read consumes the data, never on arrival, as in [HTTP/2](http2.md#flow-control-and-retained-memory); unread buffered data per stream and per connection never exceeds the advertised windows.
- Stream-opening credit (`MAX_STREAMS`) for a peer-initiated stream is returned only after the stream is closed in both directions at the QUIC layer and, on the server, after its handler and response body have completed or been dropped. Stream-opening credit, flow-control credit and retained memory are accounted separately.
- Loss detection, probe timeouts and congestion control are implemented natively from [RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html), including its pseudocode appendices, with controllers behind a small interface so further algorithms can be added. The first controller is RFC 9002's NewReno, which establishes recovery correctness against its specification; [CUBIC](https://www.rfc-editor.org/rfc/rfc9438.html) follows and is the default by the first release, decided on 2026-11-20; BBR is considered only if measurements justify it. No recovery or congestion code is copied from another implementation; quinn-proto, quiche and ngtcp2 are references and comparison peers in simulation.
- A server discards Initial packets in datagrams whose payload is smaller than 1200 bytes ([RFC 9000 §14.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-14.1)). The client pads every datagram carrying Initial packets to at least 1200 bytes, and the server pads every datagram carrying ack-eliciting Initial packets to at least 1200 bytes, as §14.1 requires of each. Each endpoint honors a configured maximum UDP payload size, so a backend can advertise the reduced `max_udp_payload_size` that [QUIC routing](quic-routing.md#forwarding) requires.

### Rationale

A deterministic core allows simulation of loss, reordering and timers, as the [roadmap](roadmap.md) requires. Taking addresses from the input rather than the socket, and generating identifiers through a shared format, are what let [stateless routing](quic-routing.md) place many endpoints behind one address, and are much harder to add later. Binding specific addresses, one socket per address, with `IP_PKTINFO` deferred until needed, was decided on 2026-11-20; a connected client socket makes its local address known without per-datagram addressing. Returning stream-opening credit only after the handler retires makes the stream limit bound concurrent handler work, which in HTTP/3 is the defense against the rapid-reset pattern that HTTP/2 also bounds with a reset budget: a peer cannot reset streams faster than their handlers stop. Native recovery from RFC 9002, NewReno first, then CUBIC as the default, and BBR only with evidence, was decided on 2026-11-20.

### Scope

`base-quic` public API, driver and configuration, and stream-credit integration in `base-http3` and `base-http-server`.

### Validation

Simulation tests drive the core with injected loss, duplication, reordering, delay and timer expiry, without a scheduler. Recovery and throughput are compared with quinn-proto or quiche under the same conditions and recorded, without acceptance criteria until targets are agreed. Inspect recovery and congestion sources for copied code. A test delivers datagrams through a forwarding adapter with rewritten socket addresses and observes the original addresses. Inspect server and client socket setup for wildcard binds, and check that every client socket is either connected to the server address or bound to a specific local address. A test resets request streams faster than their handlers finish cancelling, and shows that running handlers never exceed the advertised stream limit and that no `MAX_STREAMS` is sent before handler retirement. Tests show that a server discards an Initial in a datagram under 1200 bytes, that client Initial datagrams and server datagrams carrying ack-eliciting Initial packets are at least 1200 bytes, and that no `MAX_STREAM_DATA` or `MAX_DATA` is sent before the application reads. Interoperate through the [QUIC interop runner](https://github.com/quic-interop/quic-interop-runner) with at least quiche, ngtcp2 and quinn.

## Connection identifiers and endpoint keys

### Requirement

- Every connection identifier an endpoint issues, including those in Retry, `NEW_CONNECTION_ID` and `preferred_address`, comes from the endpoint's configured `base-quic-wire` generator. A server's generator encodes its server identifier with the [QUIC-LB codec](quic-routing.md#identifier-modes); a client's generator, by default, produces random identifiers with no server identifier. Identifiers are never zero-length.
- Issued identifier length is fixed per rotation codepoint and may differ between codepoints. An endpoint, like the [router](quic-routing.md#header-parsing-without-decryption), reads the codepoint from an identifier's first octet to learn its length, so that short headers can be parsed, routed and reset without connection state ([RFC 9000 §10.3.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.2)). Identifiers do not self-encode their length.
- Each endpoint advertises an `active_connection_id_limit` of at least 2 and supplies the peer with spare identifiers up to the peer's limit, replacing each one the peer retires, so that both sides can use a new identifier on a new path ([RFC 9000 §5.1.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.1.1), [§9.5](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.5)). The exception is a server whose codec configuration is in QUIC-LB clear-text mode: it sends no `NEW_CONNECTION_ID` frames, except one new identifier when its configuration rotates, and sends `disable_active_migration`. Encrypted mode, the default, supports migration. A server in clear-text mode offers `preferred_address` only when explicitly configured to, as [identifier modes](quic-routing.md#identifier-modes) require, decided on 2026-11-20.
- The stateless reset key, the address-validation token key used for Retry and `NEW_TOKEN`, the TLS session ticket keys and the identifier codec configurations are state of the endpoint configuration value that the application constructs, for the client or the server protocol object, never process-global. A deployment shares any of them across backends by giving each backend the same values. Each codec rotation codepoint's configuration carries its own reset key, token key and ticket keys, replaced when that codepoint's configuration changes, and every token and ticket identifies the codepoint whose key protected it, as [shared keys](quic-routing.md#shared-keys) requires. Stateless reset tokens are derived from the issued identifier and its codepoint's reset key, so that an endpoint that has lost connection state, for example after restart, can still send a valid stateless reset.
- Hash-routed stateless resets, router Retry offload and 0-RTT accepted on any backend each need the corresponding keys shared, per codepoint, across the backend pool. The documentation states the trade-off: a pool sharing a reset key lets any backend that receives a packet for an identifier it did not issue send a valid stateless reset for it ([RFC 9000 §21.11](https://www.rfc-editor.org/rfc/rfc9000.html#section-21.11)), which the router's routing of decodable identifiers to their issuing backend otherwise prevents.

### Rationale

The router's statelessness depends on every identifier a client can use decoding to the backend holding its connection ([QUIC routing](quic-routing.md#backend-cooperation)). A server must not reuse an identifier when sending to a new destination address, and a client must not reuse one when sending from a new local address ([RFC 9000 §9.5](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.5)), so migration needs spare identifiers supplied by both sides; zero-length identifiers would also prevent derived reset tokens. Keeping keys in the configuration value the application constructs, sharing them by giving each backend the same values, and keeping reset, token and ticket keys per codepoint were decided on 2026-11-20, as were an identifier length fixed per codepoint without self-encoding, and clear-text mode's behavior, which follows the QUIC-LB draft's advice not to send further clear-text identifiers ([draft-ietf-quic-load-balancers-22 §8](https://www.ietf.org/archive/id/draft-ietf-quic-load-balancers-22.html#section-8)); a separate reset key per codepoint follows its Stateless Reset Oracle consideration (§8.5 of that revision).

### Scope

Identifier generation, transport parameters, stateless reset and key configuration in `base-quic`.

### Validation

A property test checks that every identifier a server issues decodes to its configured server identifier, that issued identifiers are never zero-length, and that each has its codepoint's configured length, including with two codepoints of different lengths active at once. Tests check that each side keeps the peer supplied with spare identifiers up to its limit, and that a clear-text server sends no `NEW_CONNECTION_ID` apart from rotation and sends `disable_active_migration`. A restart test receives a short-header packet for a lost connection and validates its stateless reset, across a codepoint rotation. Inspect the public API for process-global key state.

## Address validation and handshake limits

### Requirement

- A server limits the bytes it sends to an unvalidated client address to three times those received ([RFC 9000 §8.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1)).
- A server can be configured to validate client addresses with Retry for every new connection, never, or when its handshakes in progress exceed a configured bound. Retry tokens are protected with the address-validation token key of the current codepoint, bound to the client address and the original destination identifier, and expire after a configured lifetime. The server sends the `original_destination_connection_id` and `retry_source_connection_id` transport parameters recovered from a valid Retry token, and the client validates them against the identifiers it used ([RFC 9000 §7.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.3)). The server accepts tokens issued by a router's [Retry offload](quic-routing.md#optional-retry-offload) when configured with the same token key. The client follows at most one Retry per connection attempt and validates the Retry integrity tag ([RFC 9001 §5.8](https://www.rfc-editor.org/rfc/rfc9001.html#section-5.8)).
- A token that fails validation is handled by its kind, following RFC 9000, decided on 2026-11-20. A token whose key or codepoint the server does not know cannot be authenticated as either kind and is treated as no token. A `NEW_TOKEN` token that is expired, for another address or otherwise invalid is treated as no token, so the server proceeds as for an unvalidated address, including sending Retry when its mode calls for it ([§8.1.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.3)). A Retry token that is expired, for another address or otherwise invalid closes the connection attempt with `INVALID_TOKEN` ([§8.1.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.2)), since the client will not accept a second Retry. The router, which has no Initial keys, drops such Initials instead, as [Retry offload](quic-routing.md#optional-retry-offload) records.
- A server can issue `NEW_TOKEN` tokens, distinguishable from Retry tokens, with their own lifetime and keyed like Retry tokens, per codepoint and shareable across a pool as the endpoint keys are; a client stores them per server name with its session cache and presents one on its next connection to that server, which may then skip Retry ([RFC 9000 §8.1.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.3)). Decided on 2026-11-20.
- A server bounds handshakes in progress and new handshakes per period. Above the bound, it sends Retry to an unvalidated address when Retry is enabled, and otherwise refuses the connection with `CONNECTION_REFUSED` or drops the datagram; it never closes established connections because of handshake load. These limits apply to every public socket, including a backend's own address used for [handoff](quic-routing.md#forwarding).

### Rationale

Address validation and handshake limits protect a server from spoofed and flooding handshakes ([RFC 9000 §8](https://www.rfc-editor.org/rfc/rfc9000.html#section-8), [§21.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-21.2)). `NEW_TOKEN` support on both sides, keyed like Retry tokens, was decided on 2026-11-20. Treating a failed `NEW_TOKEN` token as absent and closing on a failed Retry token are RFC 9000's recommendations; an endpoint, unlike the router, can send the `INVALID_TOKEN` close because it has the Initial keys. A backend reachable directly for handoff bypasses the router and its Retry offload, so it needs its own, as [QUIC routing](quic-routing.md#forwarding) requires.

### Scope

Address validation, tokens and handshake admission in `base-quic`, and their configuration.

### Validation

Tests: amplification stays within three times received bytes before validation; Retry in each configured mode; a `NEW_TOKEN` token presented on a later connection skips Retry, including at another backend sharing the token key; an expired `NEW_TOKEN` token, one for another address and one under an unknown key are each treated as no token, receiving Retry when Retry is enabled; an expired Retry token and one for another address each receive `INVALID_TOKEN`; a client rejects a server whose transport parameters do not match the Retry; a token from a router configured with the shared key is accepted, and one issued under a codepoint that has since been removed is treated as no token; a handshake flood above the bound receives Retry or `CONNECTION_REFUSED` while established connections continue. Run the interop runner's Retry and amplification-limit cases.

## Connection migration and preferred address

### Requirement

- A server accepts a change of the client's address, from NAT rebinding or migration, validating the new path as [RFC 9000 §9](https://www.rfc-editor.org/rfc/rfc9000.html#section-9) requires, and resets congestion and RTT state for the new path unless only the port changed ([§9.4](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.4)). It rate-limits path validation for new addresses. A server that sent `disable_active_migration` still handles NAT rebinding.
- The client migrates a connection actively, to a new local address on a new socket, when the application asks, and automatically when the current path is declared failed after a configurable number of consecutive probe timeouts. The driver holds the new socket alongside the old one while it validates the new path, uses an unused identifier issued by the peer, resets congestion and RTT state for the new path ([RFC 9000 §9.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.2), §9.4), and retires the old socket only after the switch. A client whose peer sent `disable_active_migration` does not use a new local address towards the address the server used during the handshake; this does not prevent migration to, or after reaching, a preferred address ([RFC 9000 §18.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-18.2)). A client with no unused peer identifier does not migrate.
- `preferred_address` ([RFC 9000 §9.6](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.6)) is supported on both sides. A server can be configured with a preferred IPv4 and IPv6 address; it offers them with an identifier issued through its generator and the matching reset token, and accepts the connection on both its original and preferred addresses, so clients that ignore the offer keep working. It sends non-probing packets from its original address until it has validated the client's path from its preferred address and received a non-probing packet there ([§9.6.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.6.2)).
- A client offered a preferred address of the family it is using validates that path once the handshake is confirmed and migrates to it automatically, reusing the active-migration machinery; if validation fails it stays on the original path. A client that must migrate before reaching the preferred address validates both server addresses from its new address concurrently and prefers the preferred address ([§9.6.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.6.3)).

### Rationale

Active client migration and `preferred_address` on both sides in the first release were decided on 2026-11-20, as were the client's triggers: it migrates automatically to an offered preferred address after validating it, and when the current path fails, detected by a configurable number of consecutive probe timeouts, and applications can also migrate explicitly. The default number of probe timeouts is set and recorded here in M8. `preferred_address` lets a backend behind a shared address move a connection to its own address, the [handoff](quic-routing.md#forwarding) return path, and lets clients reach a stable unicast address.

### Scope

Path validation, migration and `preferred_address` in `base-quic`, client and server.

### Validation

Migration tests move an active transfer to a new client socket, at the application's request with the old path still working, and automatically when the old path fails for the configured number of consecutive probe timeouts, and check that the new path uses a new identifier in both directions, that stream data is neither lost nor duplicated, and that the old socket is retired. Tests check that a peer's `disable_active_migration` prevents migration towards the handshake address but not to or from a preferred address, and that a server handles NAT rebinding after sending it. Preferred-address tests: a client migrates to an offered address after validating it, stays put when validation fails, migrates concurrently when its own address changes first, and a client ignoring the offer keeps using the original address; the server sends from its original address until the client's path is validated. Run the interop runner's rebinding and connection-migration cases, and interoperate with an independent client that honors the offer.

## ECN

### Requirement

- Endpoints enable ECN reporting on receive when the scheduler provides it (`base-scheduler` `0.6.0`, [packet ECN](../../base-scheduler/design/packet-ecn.md)), and report ECN counts in acknowledgments only for datagrams whose marking is known ([RFC 9000 §13.4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.4.1)). A forwarded datagram whose marking the router reports as unknown is treated like one received without ECN information.
- Marking with ECT(0) uses the per-socket `SendEcn` option only on a socket that carries one path, such as a client's connected socket, and turns marking off on that socket when the path fails ECN validation ([RFC 9000 §13.4.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.4.2)). Each new path, including one after migration or to a preferred address, is validated afresh.
- Servers on shared sockets, direct server return sockets, and replies sent through the router do not mark until per-datagram ECN on send exists, a later scheduler capability listed by [QUIC routing](quic-routing.md#scheduler-capabilities).

### Rationale

ECN lets QUIC learn of congestion without loss, but an endpoint must stop marking on a path that fails validation, and a per-socket option cannot stop it for one path among many sharing a socket. These rules are the canonical ECN rules shared with [QUIC routing](quic-routing.md) and [packet ECN](../../base-scheduler/design/packet-ecn.md), agreed after the design review on 2026-11-20. Running the interop ECN case with Base as the client only, and recording the server role as an expected failure, was decided on 2026-11-20: the runner's ECN check passes only when both sides mark.

### Scope

ECN reporting, validation and marking in `base-quic` and its driver.

### Validation

With scheduler `0.6.0`, tests over loopback check reported ECN counts, a client marking ECT(0) on a connected socket, marking turned off after simulated validation failure, and no marking on a server's shared socket. Inspect the driver for `SendEcn` on shared sockets. Run the interop runner's ECN case with Base as the client. With Base as the server the case is an expected failure, recorded with the results, because the runner requires the server to mark ECT and a Base server on a shared socket does not mark until per-datagram ECN on send exists.

## Early data (0-RTT)

### Requirement

- 0-RTT is supported on both sides. The client's session ticket cache, with the transport parameters, ALPN and HTTP/3 SETTINGS remembered for each ticket, lives in the client's protocol object; the server's ticket keys and anti-replay state live in the server's protocol object. A client sending 0-RTT complies with the remembered transport parameters and settings ([RFC 9000 §7.4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1), [RFC 9114 §7.2.4.2](https://www.rfc-editor.org/rfc/rfc9114.html#section-7.2.4.2)).
- The client sends in 0-RTT only requests that are replay-safe: by default requests with method `GET`, `HEAD` or `OPTIONS` and no body; the application can override this per request. Other requests wait for the handshake to complete.
- When the server rejects 0-RTT, the client resets the state of the streams it opened in 0-RTT ([RFC 9001 §4.6.2](https://www.rfc-editor.org/rfc/rfc9001.html#section-4.6.2)) and sends those requests again in 1-RTT on the same connection; the server never processed them. This resend is unconditional.
- A request answered with 425 (Too Early) ([RFC 8470](https://www.rfc-editor.org/rfc/rfc8470.html)) after being sent in early data is resent once the handshake completes, as RFC 8470 §5.2 recommends for user agents, at most once and not in early data. This resend is part of the client's configurable [retry policy](proposal.md#client): it is on by default and off when retries are disabled. The client never automatically resends a request carrying an `Early-Data` field, since such a request was sent in early data on a previous hop and RFC 8470 §5.2 requires intermediaries to forward its 425. A 425 that is not resent, including one to a request that was not sent in early data, is delivered to the caller. The [proxy](load-balancer.md#policy-object) disables the resend for forwarded requests; its policy forwards the 425 downstream, or retries only after the downstream handshake completes.
- Servers accept early data by default, and acceptance is configurable per server. A request counts as early when it was received in early data, before the handshake completed, or carries an `Early-Data` field ([RFC 8470 §5.1](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.1)). By default the server answers 425 to an early request whose method is not safe (GET, HEAD or OPTIONS), unless its handler opts in, mirroring the client's replay-safe rule for clients that do not follow it. Tickets that permit 0-RTT carry `max_early_data_size` 0xffffffff ([RFC 9001 §4.6.1](https://www.rfc-editor.org/rfc/rfc9001.html#section-4.6.1)). The server accepts 0-RTT only when the remembered transport parameters and settings are compatible with its current ones, and then does not reduce them. Handlers observe whether a request is early through the request's [`Peer`](load-balancer.md#connection-information-and-the-proxy-protocol), and can answer 425.
- The server bounds replay with anti-replay state kept per backend; the mechanism and its bound are recorded in M8. Behind the [stateless router](quic-routing.md#routing-decisions), 0-RTT is routed by hashing the client-chosen destination identifier, so it may reach a backend other than the one that issued the ticket: that backend accepts it only when the ticket keys are shared across the pool through its configuration, and otherwise rejects 0-RTT, and the handshake continues in 1-RTT. Because anti-replay is per backend, replay safety relies on the replay-safe request rule.

### Rationale

0-RTT support in the first QUIC/HTTP/3 release, with its state in the protocol objects the application constructs, as in `Async::HTTP::Protocol::HTTP3.new(...)`, and the replay-safe request rule, 425 and per-backend anti-replay, was decided on 2026-11-20, as were accepting early data by default with the 425 default for methods that are not safe, resending automatically after 425, and resending requests in 1-RTT after a rejected 0-RTT. Also decided on 2026-11-20: the 425 resend belongs to the configurable retry policy, never applies to a request carrying `Early-Data`, and is disabled by the proxy for forwarded requests, so that the proxy can meet RFC 8470 §5.2's rules for intermediaries, which require it to forward the 425 for a request that carried `Early-Data` and otherwise to wait for the downstream handshake before retrying; treating a received `Early-Data` field like early receipt follows RFC 8470 §5.1, which requires such requests to be rejected with 425 when they cannot be processed safely, since waiting for this connection's handshake does not make them safe. Early data arrives only over HTTP/3 for now: 0-RTT over TLS on TCP is out of scope until `base-endpoint` supports it, decided on 2026-11-20. Early data can be replayed by an attacker, so only requests that can safely run twice are sent in it. Accepting it by default saves a round trip on every resumed connection; the cost is that a handler which performs a side effect for a request it treats as safe, such as a `GET` that changes state, can run that effect twice, and that early data lacks forward secrecy against later compromise of the ticket keys. Handlers see whether a request is early, and the 425 default for methods that are not safe narrows the exposure to safe methods.

### Scope

Session tickets, 0-RTT and anti-replay in `base-quic`, early-data policy in `base-http3`, `base-http-client` and `base-http-server`.

### Validation

Tests: a resumed connection sends a `GET` in 0-RTT and the handler observes early data; a `POST` waits for the handshake unless overridden; a rejected 0-RTT resends its requests in 1-RTT exactly once; a replay of captured 0-RTT to the same backend is refused by anti-replay; a handler answers 425 to a request sent in early data and the client resends it once after the handshake, receiving the second response; with retries disabled, and for a request carrying `Early-Data: 1`, the client delivers the 425 without resending; a default server accepts early data and answers 425 to a `POST` received in it, and to a `POST` received after the handshake carrying `Early-Data: 1`, and its handler's `Peer` reports both requests as early; a server whose settings were reduced rejects 0-RTT. Behind the router, 0-RTT reaching another backend is accepted with shared ticket keys and rejected, completing in 1-RTT, without them. Run the interop runner's resumption and 0-RTT cases.

## HTTP/3 mapping

### Requirement

- `base-http3` exposes the same application shape as [`base-http2`](http2.md#protocol-core-and-connection-driver): a client connection that opens exchanges and publishes admission, and a server connection whose driver polls the handler futures of its streams, as the HTTP/2 driver does, with incoming bodies implementing `ReadableBody`. Handlers, semantic messages and bodies are unchanged, and handlers and the client implement the shared request interface of [middleware](middleware.md) over HTTP/3 as over other versions.
- Each side opens its control stream, and may open its QPACK encoder and decoder streams. A duplicate control, encoder or decoder stream (`H3_STREAM_CREATION_ERROR`), the closure of any of them (`H3_CLOSED_CRITICAL_STREAM`), a first control-stream frame other than SETTINGS (`H3_MISSING_SETTINGS`), a second SETTINGS frame or a frame forbidden on its stream (`H3_FRAME_UNEXPECTED`), a reserved HTTP/2 frame type (`H3_FRAME_UNEXPECTED`) and a reserved HTTP/2 setting (`H3_SETTINGS_ERROR`) are connection errors, as [RFC 9114 §6.2](https://www.rfc-editor.org/rfc/rfc9114.html#section-6.2), [§7](https://www.rfc-editor.org/rfc/rfc9114.html#section-7) and [RFC 9204 §4.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-4.2) specify. A peer that opens no QPACK streams is not in error. Unknown stream types, frame types and settings are ignored, within the [abuse limits](#abuse-limits).
- The first implementation advertises `SETTINGS_QPACK_MAX_TABLE_CAPACITY` 0 and `SETTINGS_QPACK_BLOCKED_STREAMS` 0, encodes with static references and literals only, and still parses and validates the peer's encoder and decoder stream instructions. Dynamic compression is added later with explicit blocked-section, acknowledgment and cancellation state.
- `SETTINGS_MAX_FIELD_SECTION_SIZE` is advertised and enforced incrementally, and the QPACK decoder bounds integers and strings, as HPACK does. Fields named by QPACK's static table are handled by index as the HTTP/2 [static table indices](http2.md#static-table-indices) require, using QPACK's 99-entry table: the QPACK decoder reports each field's static index, pseudo-fields are mapped by index, and the encoder's static lookup uses compile-time dispatch rather than hashing. Where `base-http3` interprets a section, it uses the indexed path, `FieldName::Indexed` entries of a `const` table built with `base-message`'s `static_table_entries` over QPACK's names; which sections it interprets is chosen, and benchmarked, when that part is implemented.
- A HEADERS frame whose length exceeds the header block limit is rejected from its frame header, before its payload is buffered. This is a stream error, decided on 2026-11-20; the receiver stops reading the stream with `STOP_SENDING` and resets it with `H3_EXCESSIVE_LOAD`, sending a QPACK Stream Cancellation once a dynamic table is in use ([RFC 9204 §4.4.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-4.4.2)), and the connection continues.
- Field validation and pseudo-field mapping follow the [HTTP/2 header requirements](http2.md#header-blocks-and-field-validation); `content-length` handling, interim responses, trailers, cancellation and reset behavior follow the [HTTP/2 stream lifecycle requirements](http2.md#stream-lifecycle-errors-and-cancellation); both use HTTP/3 error codes as the [mapping](#mapping-from-http2) states. Server push is disabled: the client never sends `MAX_PUSH_ID`, and treats a push stream or PUSH_PROMISE as `H3_ID_ERROR`.
- Client admission is the smaller of the concurrent-use cap and the actual QUIC bidirectional stream-opening credit. Completing a stream does not itself restore admission; new credit from the peer does.
- GOAWAY follows the [HTTP/2 shutdown requirements](http2.md#graceful-shutdown-and-goaway), with HTTP/3's identifier semantics and the substitutions in the mapping.
- Extended CONNECT ([RFC 9220](https://www.rfc-editor.org/rfc/rfc9220.html)) is accepted only after `SETTINGS_ENABLE_CONNECT_PROTOCOL` is advertised, and maps to the semantic protocol as [protocol switching](protocol-switching.md) requires. HTTP datagrams ([RFC 9297](https://www.rfc-editor.org/rfc/rfc9297.html)) and WebTransport are out of scope.

#### Mapping from HTTP/2

| HTTP/2 requirement | HTTP/3 and QUIC equivalent |
| --- | --- |
| END_STREAM | QUIC stream FIN. |
| Stream reset (`RST_STREAM`) | `RESET_STREAM` for the send direction and `STOP_SENDING` for the receive direction, with an HTTP/3 error code. |
| `CANCEL`, `INTERNAL_ERROR` | `H3_REQUEST_CANCELLED`, `H3_INTERNAL_ERROR`. |
| Malformed message, stream `PROTOCOL_ERROR` | `H3_MESSAGE_ERROR` stream error ([RFC 9114 §4.1.2](https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1.2)). |
| `COMPRESSION_ERROR` | `QPACK_DECOMPRESSION_FAILED`, `QPACK_ENCODER_STREAM_ERROR` or `QPACK_DECODER_STREAM_ERROR` ([RFC 9204 §6](https://www.rfc-editor.org/rfc/rfc9204.html#section-6)). |
| `REFUSED_STREAM` for a stream over the local limit | Not applicable: QUIC stream limits make an excess stream `STREAM_LIMIT_ERROR`. A request rejected unprocessed for another reason is reset with `H3_REQUEST_REJECTED`, which the peer may retry. |
| `ENHANCE_YOUR_CALM` | `H3_EXCESSIVE_LOAD`. |
| `FLOW_CONTROL_ERROR`, WINDOW_UPDATE | QUIC `FLOW_CONTROL_ERROR`; `MAX_STREAM_DATA` and `MAX_DATA`. HTTP/3 frames carry no padding. |
| `SETTINGS_MAX_CONCURRENT_STREAMS` | `initial_max_streams_bidi` and `MAX_STREAMS`, returned as the [transport core](#quic-transport-core-and-driver) requires. |
| `SETTINGS_MAX_HEADER_LIST_SIZE`, section over the limit | `SETTINGS_MAX_FIELD_SECTION_SIZE`; a stream error or a 431 response, without ending the connection. |
| Header block across CONTINUATION | One HEADERS frame; CONTINUATION does not exist, so its frame count limit has no equivalent. |
| SETTINGS acknowledgment and its timeout | Not applicable: HTTP/3 SETTINGS are sent once and not acknowledged. The peer's control stream and SETTINGS must arrive within the HTTP/2 preface deadline. |
| PUSH_PROMISE, `SETTINGS_ENABLE_PUSH` 0 | No `MAX_PUSH_ID` sent; a push stream or PUSH_PROMISE is `H3_ID_ERROR`. |
| Graceful GOAWAY: maximum stream identifier, one PING round trip, then the last processed | Server: GOAWAY with 2⁶²−4, then the next unprocessed client bidirectional stream identifier; client: push identifiers. The wait is one round trip by QUIC's RTT estimate, eliciting an acknowledgment with a QUIC PING when needed, at most 1 second. |
| Stream identifier exhaustion | Unreachable in practice with 62-bit identifiers; the stream-opening credit governs admission. |
| Rapid reset and frame budgets | Streams reset by the peer through `RESET_STREAM` or `STOP_SENDING` before completing; unknown and reserved frame types, empty DATA and PRIORITY_UPDATE in the shared frame budget. |

### Rationale

The semantic model and application contract are shared by every HTTP version, as the [roadmap](roadmap.md) and [taxonomy](client-connection.md#one-http-client-pool-contract) require. Zero dynamic table capacity gives an interoperable first slice without cross-stream blocking, as the [component evaluation](components.md#qpack-stage-dynamic-compression-independently-of-transport) recommends. QPACK encoder and decoder streams are optional when the dynamic table is unused ([RFC 9204 §4.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-4.2)), so their absence must not fail a connection. Decoders reporting static indices, with the choice of sections interpreted by index deferred until that part is implemented and benchmarked, was decided on 2026-11-20; the goal is to avoid comparing strings when extracting common fields. The mapping follows [RFC 9114 Appendix A](https://www.rfc-editor.org/rfc/rfc9114.html#appendix-A).

### Scope

`base-http3`, `base-http-qpack`, and their integration into the client and server.

### Validation

Run the shared [`base-http-fixtures`](fixtures.md) suites against HTTP/3. Tests cover each critical-stream and SETTINGS error, a peer opening no QPACK streams, reserved HTTP/2 frame types and settings, unknown stream and frame types, oversized field sections, an oversized HEADERS frame rejected before its payload is buffered with the connection continuing, the decoder's static indices and pseudo-fields mapped by index, a peer using a dynamic QPACK table against zero advertised capacity, a push stream to a client, graceful GOAWAY with requests racing the first frame, and admission while stream credit is exhausted. Interoperate with at least curl, quiche and ngtcp2/nghttp3 clients and servers. Fuzz the HTTP/3 frame decoder and the QPACK decoder.

## Abuse limits

### Requirement

The [HTTP/2 abuse limits](http2.md#abuse-limits) apply to the corresponding HTTP/3 events listed in the [mapping](#mapping-from-http2). QUIC additionally bounds the following. Each limit is an option with a finite default, and takes the action listed:

| Limit | Action when exceeded |
| --- | --- |
| Streams reset by the peer, HTTP/3 frame budget, queued HTTP/3 control output | Close with `H3_EXCESSIVE_LOAD`. |
| Connection identifiers from `NEW_CONNECTION_ID` beyond the advertised `active_connection_id_limit` | Close with `CONNECTION_ID_LIMIT_ERROR` ([RFC 9000 §5.1.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.1.1)). |
| Identifiers awaiting acknowledged retirement, at least twice `active_connection_id_limit` | Close with `CONNECTION_ID_LIMIT_ERROR` ([RFC 9000 §5.1.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.1.2)). |
| Path validations for new peer addresses, and PATH_RESPONSE frames awaiting capacity | Rate-limit: excess probes and challenges are dropped; the connection continues. |
| Packets that fail authentication | Drop and count; close with `AEAD_LIMIT_REACHED` only at the integrity limit of [RFC 9001 §6.6](https://www.rfc-editor.org/rfc/rfc9001.html#section-6.6). |
| Bytes sent before address validation, handshakes in progress and new handshakes | As [address validation](#address-validation-and-handshake-limits) requires: stop sending, Retry, refuse or drop; never close an established connection. |
| Other QUIC control frames queued for transport capacity | Close with the QUIC error recorded with the limit in M8. |

Defaults follow the [HTTP/2 default limits](http2.md#default-limits) where they correspond, decided on 2026-11-20: 100 bidirectional streams (`initial_max_streams_bidi`), 1 MiB stream and connection receive windows, a 32 KiB `SETTINGS_MAX_FIELD_SECTION_SIZE`, a 64 KiB HEADERS frame, 100 fields per section, 32 settings per SETTINGS frame, a 10 second deadline for the peer's control stream and SETTINGS, the same reset and frame buckets, 1,000 queued HTTP/3 control frames awaiting transport capacity, a 1 second maximum wait between the two GOAWAY frames of graceful shutdown, and `SETTINGS_ENABLE_CONNECT_PROTOCOL` 1 advertised by the server; QPACK table capacity and blocked streams are 0. QUIC-specific defaults, such as the idle timeout, unidirectional streams, `active_connection_id_limit`, acknowledgment delay, handshake bounds and the path-validation rate, are set and recorded here in M8. The default for unidirectional streams (`initial_max_streams_uni`) is at least 3, decided on 2026-11-20, as [RFC 9114 §6.2](https://www.rfc-editor.org/rfc/rfc9114.html#section-6.2) recommends so that the control and QPACK streams are not blocked.

### Rationale

The defaults that correspond to HTTP/2's follow them, as decided on 2026-11-20, including the server advertising extended CONNECT, which was decided for HTTP/2 the same day. Each HTTP/2 attack has an HTTP/3 equivalent, and QUIC adds attacks on connection identifiers, migration and amplification. Packets that fail authentication and new-path probes can come from off-path attackers who know a connection identifier, so they are dropped rather than allowed to end the connection.

### Scope

`base-quic` and `base-http3` options and state.

### Validation

Tests assert the advertised SETTINGS and transport parameters of a default server and client against these defaults. A test for each limit shows its action at the threshold and normal operation below it, including a flood of forged short-header packets and of probes from new addresses that leaves the connection open.

## Selection and discovery

### Requirement

HTTP/3 is established over QUIC with ALPN `h3`; it is never selected over a TCP stream. A client uses HTTP/3 only when configured for it or when it has an `Alt-Svc` record ([RFC 7838](https://www.rfc-editor.org/rfc/rfc7838.html)) for the origin under its configured policy. A server can be configured to advertise `Alt-Svc` on its HTTP/1 and HTTP/2 responses.

The client's `Alt-Svc` cache and its memory of QUIC failures per origin live in the client's protocol object. The cache honors `ma`, with its default of 24 hours, and `clear`, and holds a configured maximum number of origins. A connection to an alternative must present a certificate valid for the origin ([RFC 7838 §2.1](https://www.rfc-editor.org/rfc/rfc7838.html#section-2.1)).

When a client may use HTTP/3 for an origin and its policy permits TCP as well, it races the two with a delay: it starts the QUIC attempt, starts TCP and TLS if QUIC has not completed its handshake within a configured delay, 300 ms by default, uses whichever completes first, and cancels the other. Each attempt holds its own construction reservation until its cleanup completes, as the [taxonomy](client-connection.md#establishment-and-selection-on-both-sides) requires, so a race can briefly occupy two. A QUIC attempt that fails or times out is remembered for the origin for a configured period, during which the client uses TCP alone; a QUIC attempt that merely loses the race to TCP is not remembered. A client configured for HTTP/3 only never falls back.

### Rationale

[Transport layering](transport.md#future-packet-encryption-and-http3) chooses the transport before its handshake. The delayed race with a 300 ms default, the remembered failure and the HTTP/3-only mode without fallback were decided on 2026-11-20; the delay resembles [Happy Eyeballs](https://www.rfc-editor.org/rfc/rfc8305.html)' Connection Attempt Delay, whose recommended default is 250 ms. Remembering only actual failures or timeouts, in the client's protocol object with its `Alt-Svc` cache, was decided on 2026-11-20 after the design review; a QUIC handshake slower than TCP is not evidence that QUIC is blocked.

### Scope

`base-http-protocol`, and client and server establishment.

### Validation

Tests: a client configured for HTTP/3 and one discovering it through `Alt-Svc`; `ma` expiry, `clear` and the cache bound; an alternative presenting a certificate not valid for the origin is rejected; with UDP dropped, TCP starts after the delay and wins, the QUIC attempt is cancelled and cleaned up, and later requests skip QUIC during the remembered period; with QUIC slower than TCP but working, TCP wins and QUIC is still tried for the next connection; with both working, QUIC wins and the TCP attempt either never starts or is cancelled; pool construction accounting stays within its bound throughout; a client configured for HTTP/3 only never opens TCP.

## Scheduler capabilities

### Requirement

`base-quic` uses `base-scheduler` packet, timer and clock capabilities for all I/O and time, and never reaches around them. The first release needs no per-datagram addressing: servers bind one socket per address and clients use connected or specifically bound sockets, as the [transport core](#quic-transport-core-and-driver) requires, so each socket's local address is implicit. Receiving a datagram's destination address and choosing a sent datagram's source address (`IP_PKTINFO`/`IPV6_RECVPKTINFO`), which a wildcard-bound socket on a multihomed host would need, are deferred; ECN follows the [ECN](#ecn) requirements. [QUIC routing](quic-routing.md#scheduler-capabilities) lists when each further capability, including address reuse, per-datagram ECN on send, batched I/O, GSO and GRO, is added to `base-scheduler`; performance capabilities are raised when QUIC performance work starts, with measurements.

### Rationale

Keeping all I/O behind scheduler capabilities is what the [proposal](proposal.md#scheduler-contract) requires. The released `base-scheduler` `0.5.0` packet writer takes only a destination address and its receive result reports only the sender, so per-datagram addressing is unavailable; binding specific addresses, with `IP_PKTINFO` deferred until needed, was decided on 2026-11-20.

### Scope

The `base-quic` driver and its use of `base-scheduler`.

### Validation

Inspect `base-quic` for socket calls and clock reads outside scheduler capabilities. Run the driver tests with the released scheduler capabilities only, apart from ECN reporting once `0.6.0` is available.
