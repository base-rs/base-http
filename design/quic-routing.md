# Stateless QUIC routing

Accepted 2026-11-20; not yet implemented. The open questions of the first draft were decided on 2026-11-20, and each decision is recorded in its section. The [QUIC and HTTP/3 requirements](http3.md) cover the endpoints behind the router.

## Purpose

Several QUIC endpoints, on one host or many, must be reachable through one address and port, with every datagram of a connection reaching the endpoint that holds its state, even after the client's address changes. A router achieves this statelessly when the endpoints encode their identity into the connection identifiers they issue, and the router decodes it from each datagram's header. The router then never terminates TLS, removes packet protection, or parses HTTP, and keeps no per-connection state, so it can be restarted or replicated freely.

The format follows the IETF QUIC-LB work, [*QUIC-LB: Generating Routable QUIC Connection IDs*](https://datatracker.ietf.org/doc/draft-ietf-quic-load-balancers/): a three-bit configuration rotation codepoint in the first octet, then a server identifier and nonce, either in the clear or encrypted with a key the router and endpoints share. The current reference is [draft-ietf-quic-load-balancers-22](https://datatracker.ietf.org/doc/html/draft-ietf-quic-load-balancers-22) of 23 October 2026; section numbers below refer to it, and the revision implemented is pinned when M10 starts, as [identifier modes](#identifier-modes) requires. The reviewed [kurocha/protocol-quic `e80bac4`](https://github.com/kurocha/protocol-quic/tree/e80bac4d5d73f4c3bb76847150d0e4b7b8267882) `Dispatcher` instead maps each issued identifier to a connection in a table, maintained in [`Dispatcher.cpp#L44-L62`](https://github.com/kurocha/protocol-quic/blob/e80bac4d5d73f4c3bb76847150d0e4b7b8267882/source/Protocol/QUIC/Dispatcher.cpp#L44-L62) and consulted in [`Dispatcher.cpp#L104-L122`](https://github.com/kurocha/protocol-quic/blob/e80bac4d5d73f4c3bb76847150d0e4b7b8267882/source/Protocol/QUIC/Dispatcher.cpp#L104-L122), which requires the dispatcher and connections to share a process; it remains a reference for identifier lifecycle within one endpoint.

TCP needs no equivalent: a TCP connection keeps one address and port pair for its lifetime, so ordinary layer 4 balancing, by hashing or `SO_REUSEPORT`, keeps it on one backend. Base provides no stateless TCP balancer, decided on 2026-11-20, and no proxy routing TLS by Server Name Indication without terminating it. HTTP/1 and HTTP/2 are balanced by the stateful proxy in the [load balancer](load-balancer.md), which can share a process with this router.

The identifier format ships with `base-quic` from its first slice, so endpoints need no change when the router arrives; the router itself follows HTTP/3, as milestone M10 of the [roadmap](roadmap.md), decided on 2026-11-20. Its decision logic can be built earlier.

## Package boundary

### Requirement

The router is `base-quic-router`, a sub-crate of the `base-quic` workspace at `crates/router`, beside `crates/wire`. It depends on `base-quic-wire` and `base-scheduler`, and must not depend on rustls or another TLS implementation, `base-quic`, any HTTP package, or a concrete scheduler. Its routing decision is usable without a scheduler: given a datagram's bytes and addresses, it returns a decision. A separate driver performs packet I/O. A backend can be [local](#forwarding), delivered in process through a handle the application supplies, so an application such as the [load balancer](load-balancer.md#modes-and-composition) can run its own endpoint behind the router without the router depending on `base-quic`. The router is configured programmatically; an executable or configuration format built on it is outside this package, as for the [load balancer](load-balancer.md#packages).

The shared contract between router and endpoints lives in `base-quic-wire`:

- the server-identifier codec and its configuration per rotation codepoint: server identifier length, nonce length, the length of any server-use bytes and therefore the total identifier length, mode and key;
- the encapsulation header of [forwarding](#forwarding);
- when [Retry offload](#optional-retry-offload) is used, the address-validation token format and the Retry packet construction.

`base-quic` uses the same codec to generate its identifiers and the same token format to validate tokens. Keys are values in the configurations the application constructs, as [shared keys](#shared-keys) requires.

### Rationale

Not depending on TLS or transport state is what makes the router stateless and independently deployable. A single codec, header and token format in a package both depend on prevents the router and endpoints from disagreeing about them.

### Scope

`base-quic-router` and `base-quic-wire` manifests and public APIs, and identifier generation and token validation in `base-quic`.

### Validation

Inspect `cargo tree -e normal` for `base-quic-router`, and its public API for `base-quic` types, including in the local backend handle. Decision tests run without a scheduler. Inspect `base-quic` and `base-quic-router` for copies of the codec, header or token encoding outside `base-quic-wire`.

## Identifier modes

### Requirement

The codec implements QUIC-LB's clear-text mode and its encrypted modes (§5.4): single-pass AES-128-ECB when the server identifier and nonce together are 16 bytes, and the four-pass scheme otherwise. Server identifiers are at least one octet and nonces at least four, and the two sum to at most 19 octets for QUIC v1 and v2 (§5.3). Each codepoint's configuration fixes its identifier length, which may differ between codepoints; identifiers do not encode their length in the first octet (§3.3), whose five bits after the codepoint are chosen with no observable relationship to identifiers issued before. Encrypted is the default mode; clear-text is available by configuration for debugging and trusted networks. A deployment using both modes gives them different server identifier allocations (§8.7).

AES is always part of `base-quic-wire`, from a focused implementation that adds no TLS implementation, with no feature to remove it, decided on 2026-11-20. The configuration constructor that takes no mode builds an encrypted configuration; clear-text mode is chosen explicitly.

In clear-text mode a `base-quic` server follows the draft's advice for identifiers without a key (§8): it sends no `NEW_CONNECTION_ID` frames, apart from the single replacement identifier the draft permits on configuration rotation. It sends the `disable_active_migration` transport parameter, as the draft advises for tenants assigned clear-text identifiers (§8.4) and for servers without a configuration (§3.2). It offers `preferred_address`, for [handoff](#forwarding), only when explicitly configured to, decided on 2026-11-20. That offer departs from §8's advice to issue clear-text identifiers only in the server's Initial packets: it carries one more identifier showing the same server identifier, and the backend's own address identifies it in any case. Encrypted mode supports `NEW_CONNECTION_ID`, active migration and `preferred_address` without that configuration.

The QUIC-LB revision implemented is the latest draft, or the RFC if published, when M10 starts, and is recorded here then. Until then the reference is draft 22.

### Rationale

In clear-text mode every identifier a backend issues shows the same server identifier, so an observer can tell that a client's paths before and after migration reach the same server; with few connections per server that can link them, which RFC 9000 §9.5 seeks to prevent. Clients migrate actively from the first release. Encrypted identifiers avoid this, so encrypted is the default, decided on 2026-11-20. Clear-text mode following the draft, so that it never suggests unlinkable identifiers, was decided on 2026-11-20, as was the opt-in `preferred_address` offer, recorded above as a deviation. A length fixed per codepoint, with no length self-description, was decided on 2026-11-20: reading the codepoint gives the length to the router and to endpoints, which is all [header parsing](#header-parsing-without-decryption) and stateless reset derivation need, and self-description serves only hardware offload (§3.3). Always including AES, decided on 2026-11-20, means no build can silently fall back to clear-text.

### Scope

The `base-quic-wire` codec and its configuration in `base-quic` and `base-quic-router`, and identifier issuing in `base-quic`.

### Validation

Check both modes against the draft's test vectors (Appendix B). A property test shows that encrypted identifiers issued by one backend show no common bytes after the first octet beyond what chance predicts, and that clear-text nonces show no observable relationship. Tests reject configurations outside the length limits; a test issues identifiers under two codepoints of different lengths and parses short headers carrying each. A clear-text server configured with `preferred_address` offers it, and one not so configured does not. A clear-text server test observes no `NEW_CONNECTION_ID` frames outside rotation and `disable_active_migration` in its transport parameters; an encrypted server test observes both identifier issuing and a client migration. Inspect `cargo tree -e normal` of `base-quic-wire` for TLS implementations.

## Configuration and rotation

### Requirement

The router and `base-quic` endpoints each accept a configuration that can be replaced atomically while running: the codec configurations, each under its own rotation codepoint, with the keys [shared keys](#shared-keys) lists; the codepoint used for issuing new identifiers; and for the router, the backend table, the secret that keys its [hashing](#routing-decisions), and, with [Retry offload](#optional-retry-offload), the key for its Retry nonces. At most seven codepoints are configured; codepoint `0b111` is reserved by the draft (§3.1, §3.2) and no configuration uses it, and the router treats identifiers carrying it as undecodable. Datagrams already being processed complete under the configuration they started with.

The backend table maps each server identifier to a backend, with its kind, weight and state. A remote backend has an encapsulation address and return path; a local backend has the in-process delivery handle of [forwarding](#forwarding). The weight is a positive number that sets the backend's share of new connections assigned by hashing; it has no effect on identifiers that decode. The states are:

- **Active.** Its identifiers decode and route to it, and it takes part in hashing with its weight.
- **Draining.** Its identifiers still decode and route to it, but it is excluded from hashing for new connections.
- Removing a backend entirely makes its identifiers undecodable.

Keys rotate by adding a configuration under a new codepoint, moving backends to issue with it, and removing the old one once its counts reach zero, as below. When a `base-quic` endpoint's issuing codepoint changes, it issues replacement identifiers under the new codepoint to every active connection with `NEW_CONNECTION_ID` and retires the old ones with Retire Prior To (§3.1). It raises Retire Prior To one identifier at a time across the batch, decided on 2026-11-20, the first of the two mitigations in §6.2, so peers do not lose paths. Neither side needs to restart. A configuration should reach routers before backends (§3.1); how configurations reach each machine, by files or a control plane, is the application's responsibility, and the libraries define no distribution protocol and no configuration file format.

An application removes a draining backend, or a codepoint no backend issues with any longer, only when the counts each backend reports under [backend cooperation](#backend-cooperation) reach zero: for a backend, its live connections; for a codepoint, the live connections and outstanding identifiers under it on every backend. Removing a codepoint also invalidates the tokens and tickets it protected, whose clients then complete Retry or a full handshake.

### Rationale

QUIC-LB's rotation codepoints exist so that several configurations can coexist during rotation, and the draft requires servers to move their connections to the new configuration, which is what lets the old one be removed while long-lived connections continue. Leaving distribution to the application, decided on 2026-11-20, keeps the libraries independent of any deployment's control plane. A draining state lets the [load balancer](load-balancer.md) drain a stateless QUIC backend without breaking its connections, which removal alone would do. A stateless router cannot know when a backend's or a codepoint's connections have ended, so removal waits on the counts the backends report, decided on 2026-11-20. Weights carried by each backend entry, applying to new connections only and changed by replacing the configuration, were decided on 2026-11-20; they let a deployment, or the load balancer's policy, shift new connections without moving established ones.

### Scope

Configuration types and replacement in `base-quic-router` and `base-quic`, and identifier replacement in `base-quic`.

### Validation

A test rotates the key while connections are active: each connection receives identifiers under the new codepoint and retires the old ones, the reported counts for the old codepoint reach zero, new connections use the new codepoint, and removing the old configuration afterwards affects no connection. A test replaces the backend table, including a weight change, during forwarding without dropping datagrams for unaffected backends or moving established connections. A draining test shows a draining backend's connections continuing while new handshakes hash to other backends, its live-connection count reaching zero once they close, and a removed backend's identifiers treated as undecodable. Tests reject a configuration using codepoint `0b111`, more than seven codepoints, or a backend weight that is not positive.

## Shared keys

### Requirement

The stateless reset key, the address-validation token key for Retry and `NEW_TOKEN` tokens, the session ticket keys, and the codec configuration are state of the endpoint and protocol configuration values the application constructs, never global. A deployment shares them across backends by giving each backend the same values. Each codepoint's configuration carries its own stateless reset key, token key and ticket keys, replaced when that codepoint's configuration changes (§8.5). Tokens and tickets identify the codepoint whose key protected them, so they remain valid during rotation until that codepoint is removed. The router holds a codepoint's token key only when Retry offload is configured, and never holds reset or ticket keys.

Three features require these keys shared per codepoint across the pool, and each is unavailable without that sharing:

- hash-routed stateless resets, under [routing decisions](#routing-decisions), require a shared reset key, since the backend receiving a hash-routed packet did not issue its identifier;
- [Retry offload](#optional-retry-offload) requires a token key shared by the router and backends;
- accepting 0-RTT on a backend other than the one that issued the ticket requires shared ticket keys; otherwise that backend rejects 0-RTT and the handshake continues in 1-RTT.

With a shared reset key, any backend can produce a valid stateless reset for any identifier under that codepoint. [RFC 9000 §10.3.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.2) and [§21.11](https://www.rfc-editor.org/rfc/rfc9000.html#section-21.11) warn that a packet routed to an instance with the same key but no state for the connection then resets it. Deterministic routing of decodable identifiers avoids this for live connections (§8.5); the documentation states the remaining exposure: with hash routing of undecodable short headers enabled, a live backend absent from a router's table, through misconfiguration or a router replica whose configuration has not yet been replaced, has its connections reset by their own packets or by anyone replaying an observed identifier. Configurations must therefore drain a backend, and remove it only when its reported counts reach zero, as [configuration and rotation](#configuration-and-rotation) requires. A shared key also lets the compromise of one backend reset or forge tokens for the whole pool, and a shared ticket key lets it decrypt early data and resume sessions for the pool. A backend never reuses a nonce under the same codepoint key (§8.6); the router's Retry identifiers follow [Retry offload](#optional-retry-offload).

### Rationale

Keys belong to the configuration values the application constructs, and sharing them is a deployment choice, decided on 2026-11-20. Scoping reset keys to codepoints follows QUIC-LB §8.5, so that rotating a codepoint also replaces keys whose identifiers may have been seen before.

### Scope

Key configuration in `base-quic`, `base-http3` and `base-quic-router`.

### Validation

Tests: two backends sharing a reset key, one resetting a connection of the other when hash routing is enabled, and the client accepting it; without sharing, the client ignores the reset and waits for its idle timeout. A token and a ticket issued under one codepoint validate after a new codepoint is added and fail after the old one is removed. Inspect for global or process-wide key state.

## Header parsing without decryption

### Requirement

The router reads only fields that [RFC 8999](https://www.rfc-editor.org/rfc/rfc8999.html) makes version-independent: the header form bit, and for long headers the version and destination and source connection identifiers with their lengths; for short headers the destination identifier. A short header does not state its identifier length, so the router reads the first octet, takes the codepoint from it, and decodes the following server identifier and nonce, `1 + server identifier length + nonce length` octets for that codepoint; trailing server-use octets are ignored. A long-header identifier shorter than that for its codepoint is undecodable (§4.1). Each codepoint's total identifier length is fixed for all backends and may differ between codepoints, as [identifier modes](#identifier-modes) requires, so the router and endpoints learn the length from the codepoint, and endpoints can parse short headers and derive stateless reset tokens without state (RFC 9000 §10.3.2). A short header whose codepoint is not configured has no known length.

For QUIC v1 and v2 ([RFC 9369](https://www.rfc-editor.org/rfc/rfc9369.html)) long headers it may also read the packet type, which is not protected, and, with Retry offload configured, the token field of Initial packets. It never removes header or packet protection, and never reads a payload. Datagrams too short to contain the fields it needs are dropped. Coalesced packets share one destination identifier (RFC 9000 §12.2) and are routed by the first packet's.

### Rationale

These fields are visible without keys, and the destination identifier is the only stable routing key across client address changes.

### Scope

`base-quic-wire` header parsing and `base-quic-router` decisions.

### Validation

Fuzz the header parser. Tests cover v1, v2 and unknown versions, both header forms, codepoints with different identifier lengths, identifiers with server-use octets, long-header identifiers too short for their codepoint, truncated datagrams and coalesced packets. Inspect the router for any use of packet keys.

## Routing decisions

### Requirement

For each datagram the router decides one of: forward to a backend, send a Version Negotiation packet, send a Retry when [Retry offload](#optional-retry-offload) is configured, or drop. Decoding comes before hashing:

- A long-header packet with version 0, a Version Negotiation packet, is dropped ([RFC 8999 §6](https://www.rfc-editor.org/rfc/rfc8999.html#section-6)).
- A long-header packet with another version not configured gets a Version Negotiation packet listing the configured versions, subject to a rate limit, when its datagram is at least the smallest size that can start a connection in a configured version, 1200 bytes for v1 and v2 ([RFC 9000 §5.2.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.2.2), [§14.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-14.1)). Smaller datagrams are dropped. The configured versions must be ones every backend supports.
- Otherwise the router first tries to decode every destination identifier, in long and short headers alike, with the configured codepoints. It is routable when its codepoint is configured, it is long enough for that codepoint, and its server identifier maps to an active or draining backend; it is then forwarded to that backend, whatever the client's address.
- A long-header packet whose identifier cannot be routed, such as most identifiers a client chooses for its Initial and 0-RTT packets, is forwarded to a backend chosen by weighted rendezvous hashing of the destination identifier over the active backends, with each backend's weight, keyed with the secret from the router's configuration. Repeated and coalesced packets with the same identifier therefore reach the same backend.
- A short-header packet whose identifier cannot be routed is dropped and counted by default, so junk is never forwarded to backends; a client whose connection no longer exists then waits for its idle timeout. The router can be configured to route such packets by the same hash instead, so a backend can answer with a stateless reset and the client fails fast, which requires the reset keys of [shared keys](#shared-keys). A backend can derive a reset token only for an identifier under a codepoint it has configured, since the codepoint gives the identifier's length (RFC 9000 §10.3.2), so packets under other codepoints still go unanswered.

A client-chosen identifier that happens to decode to an active or draining backend routes there by decoding, consistently for all of that client's packets with it; the fraction that does so is the fraction of the server identifier space that is allocated, which a configuration can reduce by over-provisioning that space (§5.1).

Hashing the destination identifier, not the address and port 4-tuple, deviates from the draft's baseline fallback algorithm, which must take only the 4-tuple (§4.3.1). The implementation and its documentation record this deviation.

The documentation states the exposure to targeted routing ([RFC 9000 §21.13](https://www.rfc-editor.org/rfc/rfc9000.html#section-21.13)): the keyed hash prevents a client from predicting which backend a chosen identifier hashes to, but a client can still choose an identifier that decodes to a specific backend, trivially in clear-text mode, where server identifiers are visible, and in encrypted mode only by reusing an identifier it was issued. It can therefore concentrate handshakes on one backend; [Retry offload](#optional-retry-offload) and each backend's [handshake limits](http3.md#address-validation-and-handshake-limits) bound the effect.

The router keeps no state per connection, flow or client address. Its state is its configuration, the codec keys, hashing secret and backend table, together with the counters of its Version Negotiation rate limit and Retry trigger, which are bounded, independent of the number of connections and kept separately by each replica. Replacing the configuration takes effect for subsequent datagrams. Adding an active backend, changing a weight, or making one draining, moves only the unroutable identifiers whose hash changes to or from it; removing a backend also makes its own identifiers undecodable. A client still using its own identifier during a handshake, before it has received the server's first Initial, whose hash changes reaches a different backend, and that handshake fails or restarts; the same happens while router replicas hold different configurations during a rollout. Packets that never carry a server-issued identifier, such as a client's stateless resets, cannot be routed and are dropped or hashed; ICMP errors are not delivered on the router's UDP sockets as datagrams and are not forwarded (§6.1).

### Rationale

Once an endpoint has issued its own identifiers, routing depends only on them, so address changes from NAT rebinding or migration need no router involvement ([RFC 9000 §9](https://www.rfc-editor.org/rfc/rfc9000.html#section-9)). Decoding before hashing is what lets a backend's or the router's Retry, whose source identifier the client then uses as its destination in long headers, reach the backend it names. Before the server has chosen an identifier, the client's choice is the only key stable across that client's packets, even if its address changes during the handshake; this is why it is hashed rather than the 4-tuple, and 0-RTT is routed by that hash, so it may reach a backend other than the one that issued the ticket, decided on 2026-11-20. Rendezvous hashing limits disruption when backends change during handshakes. Keying it with a configured secret, so that the client-chosen value does not let a client predict its backend, and documenting what decoding still allows, were decided on 2026-11-20, as was weighting it per backend. Dropping undecodable short-header packets by default, decided on 2026-11-20, keeps junk traffic off the backends at the cost of slower failure for clients whose connection was lost. Version Negotiation needs no connection state ([RFC 9000 §6](https://www.rfc-editor.org/rfc/rfc9000.html#section-6)).

### Scope

`base-quic-router` decisions and configuration.

### Validation

Property tests: every identifier a configured generator issues routes to its backend, active or draining; a client address change does not change the decision; an Initial whose client-chosen identifier decodes to a backend routes there, and one that does not is hashed; an Initial carrying a Retry source identifier routes to the backend it names. Tests for hashing stability across repeated Initial and 0-RTT packets, the movement caused by adding, draining, reweighting and removing a backend, shares of hashed identifiers across backends of different weights that follow the weights within a statistical tolerance, and different hash secrets assigning the same identifiers differently, Version Negotiation for unknown versions and its rate limit, version-0 packets dropped, and undecodable short-header identifiers dropped by default and, when so configured, routed by hash to a backend whose stateless reset the client accepts with shared reset keys. Inspect the documentation for the targeted-routing exposure. Measure router memory with increasing numbers of connections and show it does not grow.

## Backend cooperation

### Requirement

Each backend endpoint issues every connection identifier, including identifiers in Retry, `preferred_address` and `NEW_CONNECTION_ID`, through the shared codec with its own server identifier, and accepts identifiers on the client's Initial and 0-RTT packets that it did not issue. A backend issuing Retry chooses a source identifier that routes back to itself. A backend that receives a short-header packet for an identifier it does not know may send a stateless reset, with the token derived as the [QUIC requirements](http3.md#connection-identifiers-and-endpoint-keys) specify and the key of [shared keys](#shared-keys).

Each `base-quic` endpoint reports, per codepoint and in total, its live connections, its outstanding identifiers (issued and not yet retired or ended with their connection), and the nonce space it has used, so that applications can decide removal as [configuration and rotation](#configuration-and-rotation) requires and can rotate before exhaustion (§8.6). When a backend has exhausted the nonce space of a codepoint, it stops opening new connections and issuing identifiers under that codepoint and switches to another configured codepoint if one is available; with none, it opens no new connections and issues no further identifiers, while its established connections continue with the identifiers they hold. In that case it drops the Initial packets of new connections, decided on 2026-11-20. A refusal with `CONNECTION_REFUSED` would be possible, since it could reuse the client's chosen identifier rather than issue one, but the backend should not be receiving new connections at all: the application marks an exhausted backend draining, as the reported counts allow, so hashing stops assigning it new connections. It never issues codepoint `0b111`, the draft's fallback for exhaustion (§8.6, §3.2), whose short headers this router drops by default.

0-RTT follows the [QUIC and HTTP/3 requirements](http3.md#early-data-0-rtt). Behind the router, a backend accepts 0-RTT on a ticket another backend issued only when the pool shares ticket keys; otherwise it rejects 0-RTT and completes the handshake in 1-RTT. Anti-replay state is per backend, so replay safety behind the router relies on clients sending only replay-safe requests in early data.

### Rationale

The router's statelessness depends on every identifier a client can use being decodable to the backend holding its connection. The counts, and the behavior on exhaustion that never falls back to undecodable identifiers, were decided on 2026-11-20. 0-RTT behind the router, with ticket keys shared through the configuration and per-backend anti-replay, was decided on 2026-11-20.

### Scope

Connection identifier generation, counts and nonce exhaustion, Retry and 0-RTT acceptance in `base-quic` and `base-http3`.

### Validation

An integration test with two or more `base-quic` endpoints behind the router: handshakes, a Retry, a client address change mid-transfer, and the client switching to a new identifier from `NEW_CONNECTION_ID` all keep the connection on one backend. A resumption test sends 0-RTT that hashes to a backend other than the ticket's issuer: with shared ticket keys it is accepted, and without them it is rejected and the connection completes in 1-RTT. Count tests check live connections, outstanding identifiers and nonce use per codepoint and in total as connections open, rotate, retire identifiers and close. An exhaustion test with a small nonce space shows a backend switching to another configured codepoint, and with none opening no new connections and issuing nothing, including nothing under `0b111`, while established connections continue.

## Forwarding

### Requirement

Forwarding preserves what the backend needs to treat the datagram as received directly: the payload unchanged, the client's address and port, the address the client sent to, and the ECN marking.

Inbound, the router forwards to a remote backend with a Base encapsulation over UDP, and delivers to a local backend in process:

- The router sends each forwarded datagram to its backend with a small, versioned header carrying the client's address and port, the shared address and port the client sent to, and the ECN marking, or that it is unknown while the scheduler cannot report it, followed by the unchanged payload. The header and its encoding live in `base-quic-wire`, so `base-quic`'s receive adapter and the router share them.
- A backend accepts encapsulated datagrams only from configured router addresses, and the encapsulation port must not be reachable from clients, so nobody else can inject datagrams with a forged client address. The encapsulation socket binds only a private address, never a public or wildcard one, and is separate from any public QUIC socket the backend has for handoff. The encapsulation carries no authentication tag. A private address is an IPv4 loopback or RFC 1918 address, or an IPv6 loopback, unique local (`fc00::/7`) or link-local address. `base-quic`'s receive adapter refuses to bind the encapsulation socket to any other address unless the configuration explicitly allows that address, a specific address or range and never a wildcard address, for internal networks using other ranges such as `100.64.0.0/10`; decided on 2026-11-20.
- The router never fragments. The configuration states the MTU between router and backends and the largest client datagram the router accepts; constructing a configuration fails unless the internal MTU fits that datagram with the header. Larger client datagrams, and any datagram that would exceed the internal MTU with the header, are dropped and counted. Backends advertise a `max_udp_payload_size` no larger than the accepted client datagram size, so clients send datagrams that fit once the handshake has delivered transport parameters; client Initial datagrams, sent before them, must already fit the accepted size.
- A **local** backend is delivered by calling the handle the application supplies with the payload, the client's address and port, the shared address and the ECN marking, without encapsulation or a network hop. The router supplies a send handle for its replies on the shared socket, from the shared address. The router keeps forwarding to remote backends, Version Negotiation and Retry offload on the same socket, and local backends take part in decoding, weights, states and hashing like remote ones. The handle's types belong to `base-quic-router` and `base-quic-wire`, not `base-quic`, and the [performance](#performance-measurement) rule on allocation and copies applies to local delivery as to forwarding.

A remote backend's replies take one of the following return paths, configured per backend; a local backend replies through the router's send handle:

- **Through the router.** The backend sends each reply to the router with the same header naming the client as destination and the shared address as source. The router checks that the source is one of its shared addresses, removes the header and sends the payload to the client from that address. Replies carry everything needed to send them, so the return path is stateless as well, and the router never parses the QUIC packet inside a reply. The router accepts replies only from configured backend addresses, so it is not an open relay, and backends limit their outgoing datagram size so a reply with the header fits the internal MTU.
- **Direct server return.** The backend sends each reply straight to the client, with the shared address as its source; the router never sees it. The backend sends from a socket bound to the shared address and port, so every reply has the shared address as its source with no per-datagram addressing; the socket only sends, since inbound datagrams arrive encapsulated at the backend's own address. Several backend processes on one host each bind the shared address and port with address reuse; workers within one process share one socket. Replies carry no encapsulation, so their size follows the backend's ordinary path to the client.
- **Handoff.** The backend offers its own publicly reachable address as `preferred_address`, as [QUIC and HTTP/3](http3.md#connection-migration-and-preferred-address) permits. The handshake passes through the router with replies by either path above; a client that migrates then reaches the backend directly in both directions, and the router sees none of its traffic. A client that ignores the offer stays on the shared address, routed as usual. This needs each backend's own address reachable from clients, but no shared address on backends. Anyone can then reach that address directly, including with new handshakes that bypass the router and any Retry it offloads, so a backend offering handoff runs its own [address validation (Retry) and handshake rate limits](http3.md#address-validation-and-handshake-limits) and the [QUIC abuse limits](http3.md#abuse-limits) on its public socket, as any public QUIC server does. Only clients that act on the offer leave the router; it is a complement to the other paths, not a replacement.

Direct server return has deployment prerequisites that the documentation must state, and that the libraries cannot establish themselves: the shared address is configured on each backend so that it may be used as a source, typically on the loopback interface with ARP replies for it suppressed (on Linux, `arp_ignore` 1 and `arp_announce` 2), so that only the router answers for it on the network, with the IPv6 equivalent for Neighbor Discovery stated once the M10 tests establish it; a direct server return backend does not share a host with a router serving the same shared address and port, since both sockets would bind that address and port and the host could deliver inbound datagrams to the backend's socket, so a backend on the router's host replies through the router or is a local backend; and the network between backends and clients must accept datagrams with the shared address as their source, which anti-spoofing filters and many cloud networks prevent. Backends using it must still drop encapsulated datagrams from unconfigured addresses, since a forged client address would make them send replies to a victim, within QUIC's anti-amplification limit before address validation.

The documentation states the threat model of the unauthenticated encapsulation: the network between routers and backends carries no untrusted hosts, or prevents them from spoofing router and backend source addresses. A host that can send with those source addresses can make a backend answer a forged handshake towards any address, within QUIC's anti-amplification limit and bypassing the router's Retry offload, and can make the router send arbitrary payloads from a shared address to any destination. It cannot forge or alter the contents of established connections, whose packets QUIC authenticates; a copied genuine packet with a rewritten client address is handled by QUIC's path validation, as without a router, and one with a false ECN marking can at most affect congestion control, as [RFC 9000 §21.10](https://www.rfc-editor.org/rfc/rfc9000.html#section-21.10) describes. The threat model also states the targeted-routing exposure of [routing decisions](#routing-decisions).

Same-host steering with `SO_REUSEPORT` and eBPF is a possible later mode; it uses the same identifier codec and backend input contract, and is configured, not fixed by the decision logic.

The driver uses the scheduler's packet capabilities and batched I/O when they are available.

### Rationale

The backends' [input contract](http3.md#quic-transport-core-and-driver) takes original addresses from the datagram rather than the socket, which is what makes forwarding transparent to the transport. A Base encapsulation with replies through the router, chosen on 2026-11-20, works across hosts with no kernel, routing or address configuration on backends, at the cost of carrying reply traffic through the router. Carrying the client's address in every reply keeps the return path free of per-connection state. Handoff, also chosen that day, removes the router from established connections entirely where backends can be reached directly, without spoofed source addresses. Direct server return, added the same day, removes reply traffic from the router, usually most of the bytes, where the deployment controls its network; using the same inbound encapsulation for both keeps the backends' receive path the same and needs no kernel tunnel on them. Leaving the encapsulation unauthenticated and binding it only to private addresses was decided on 2026-11-20; the threat model makes the trust that decision places in the internal network explicit. The local backend kind was decided on 2026-11-20 so that the [load balancer's mixed mode](load-balancer.md#modes-and-composition) runs its own endpoint behind the router through the router's own driver, keeping forwarding, Version Negotiation and Retry offload in one place, while the router still does not depend on `base-quic`. Clients may pad their first datagrams beyond 1200 bytes before they learn any transport parameter, so the internal MTU must be checked against the client datagrams accepted, not only against `max_udp_payload_size`.

### Scope

The `base-quic-router` driver and configuration, and the matching receive adapter for `base-quic`.

### Validation

An integration test in which a backend observes the client's original addresses, and replies reach the client from the shared address. Tests show that the receive adapter refuses to bind the encapsulation socket to a public or wildcard address unless the configuration explicitly allows it, that encapsulated datagrams from an unconfigured address are dropped by backends and router alike, that a reply naming a source that is not a shared address is dropped, that a configuration whose internal MTU cannot fit the accepted client datagram with the header is rejected, and that an oversized datagram is dropped and counted, not fragmented. A direct server return test, over IPv4 and IPv6, in a network namespace with the shared address on the backend's loopback, shows replies reaching the client from the shared address without passing through the router, from two backend processes on one host each bound to the shared address, and that a client address change keeps working. A local-backend test delivers datagrams to an in-process endpoint with the client's addresses and ECN marking and no encapsulation header, with its replies reaching the client from the shared address while other datagrams on the same socket are forwarded to a remote backend. A test shows the receive adapter refusing a public or wildcard encapsulation address, and the documentation is inspected for the threat model and direct server return prerequisites. A handoff test shows a backend answering a handshake sent directly to its public address with Retry when so configured, and a client completing its handshake through the router, migrating to the backend's preferred address and continuing with no datagrams through the router, while a client ignoring the offer stays routed. Fuzz the encapsulation header decoder. Throughput and latency follow [performance measurement](#performance-measurement).

## Scheduler capabilities

### Requirement

The router and `base-quic` use `base-scheduler` packet capabilities and never bypass them. The first release needs no per-datagram addressing: the router and every endpoint bind specific addresses, one socket per address, so each socket's local address is implicit; the encapsulation carries the address a client sent to; and direct server return sends from a socket bound to the shared address. Several processes binding the same address and port need address reuse; whether the scheduler's existing `SocketReuseAddress` (`SO_REUSEADDR`) provides it for UDP on each supported platform, or `SO_REUSEPORT` must be added, is established when M10 starts.

ECN follows one rule across the QUIC documents. Endpoints and the router enable ECN reporting on receive, planned for `base-scheduler` `0.6.0` by its [packet ECN](../../base-scheduler/design/packet-ecn.md) design. Marking with ECT(0) uses the per-socket `SendEcn` option only on a socket that carries one path, such as a client's connected socket, and turns it off when that path fails ECN validation ([RFC 9000 §13.4.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.4.2)). Servers on shared sockets, direct server return sockets and replies through the router do not mark until per-datagram ECN on send exists. The router copies each received datagram's marking into the encapsulation, or reports it as unknown before `0.6.0`.

The following become `base-scheduler` capabilities when a deployment needs them, each specified and tested in that project's design notes, and supported by its backends or reported as unsupported, before the router or QUIC depend on it:

- sending a datagram with an explicit source address (`IP_PKTINFO`/`IPV6_PKTINFO` on `sendmsg`), and receiving a datagram's destination address (`IP_PKTINFO`/`IPV6_RECVPKTINFO`), for endpoints bound to a wildcard address on hosts with several addresses, and for routers or backends serving more shared addresses than one socket each allows;
- socket options permitting a source address that is not configured locally (`IP_FREEBIND`, `IP_TRANSPARENT`), for deployments that do not place the shared address on backends;
- per-datagram ECN marking on send, so that servers on shared sockets and direct server return sockets can mark per path, and the router's return path can apply the marking the backend chose;
- batched receive and send, and UDP GSO and GRO, as performance capabilities.

### Rationale

Binding the shared address on each backend, decided on 2026-11-20, gives direct server return the right source address without per-datagram addressing, so the first release needs no new addressing capability. QUIC requires replies from the address a client used, which a wildcard-bound socket can guarantee only with per-datagram addressing; binding specific addresses avoids that until it is needed. RFC 9000 requires an endpoint to stop marking on a path that fails ECN validation, which a socket-wide option can do only when the socket carries that one path. Adding capabilities to the scheduler, rather than bypassing it, keeps all I/O behind its capabilities as the [proposal](proposal.md#scheduler-contract) requires.

### Scope

`base-scheduler` packet interfaces and backends, and their use in `base-quic` and `base-quic-router`.

### Validation

Inspect `base-quic` and `base-quic-router` for socket calls outside scheduler capabilities, and for `SendEcn` on sockets that carry more than one path. A test fails ECN validation on a client path and observes that the client stops marking. The direct server return tests above run with released scheduler capabilities only, apart from any address reuse M10 adds. The scheduler's conformance tests cover each added capability on every backend that supports it.

## Performance measurement

### Requirement

Router performance is measured with named `base-benchmark` workloads: forwarded datagrams per second per core, and latency added by the router at the median and 99th percentile, for 1200- and 1350-byte datagrams, in both directions, on matched hardware and build settings recorded with the results. The baseline is a minimal UDP relay on the same scheduler and hardware, which receives and resends datagrams without routing or encapsulation, measured when M10 starts. Throughput and latency targets and regression criteria are agreed relative to that baseline then, and recorded here; until they are, results are recorded without acceptance criteria.

In the steady state the router performs no allocation per datagram, and copies a payload at most once, into or out of the encapsulation or into local delivery, on every path: inbound forwarding, replies through the router, local delivery and its replies, Version Negotiation and Retry.

### Rationale

Targets need a baseline and a comparable environment to be meaningful, decided on 2026-11-20. A relay on the same scheduler separates the router's own cost from the scheduler's packet I/O. The allocation and copy bounds were decided on 2026-11-20 as a structural requirement that applies now, independent of the throughput and latency targets, and they apply to local delivery as the local backend decision requires.

### Scope

`base-quic-router` benchmarks, and its driver and local delivery paths.

### Validation

The benchmark definitions, baseline results and agreed targets are recorded with the repository's benchmarks; reviews compare new results with them under the recorded conditions. Independently of the targets, a test runs steady forwarding on each path above under a counting allocator and observes no allocation per datagram after warm-up, and inspection of the forwarding paths, with a test that counts payload copies through an instrumented buffer, shows at most one copy per datagram.

## Optional Retry offload

### Requirement

When configured with the token keys of [shared keys](#shared-keys), the router answers Initial packets with Retry, to validate client addresses under load, without TLS. Retry and token construction need only AEAD primitives, behind a `base-quic-wire` feature that adds no TLS implementation, which `base-quic-router`'s Retry offload feature enables: the Retry integrity tag uses the fixed key and nonce of [RFC 9001 §5.8](https://www.rfc-editor.org/rfc/rfc9001.html#section-5.8) for QUIC v1 and of [RFC 9369 §3.3.3](https://www.rfc-editor.org/rfc/rfc9369.html#section-3.3.3) for v2. Without that configuration the router never sends Retry.

- The router answers only Initial packets in datagrams of at least 1200 bytes, and drops smaller ones, which servers must discard ([RFC 9000 §14.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-14.1)).
- The router validates the token field of each Initial with the token key of the codepoint the token names. An Initial with a valid token, from Retry or `NEW_TOKEN`, for the client's address is forwarded as [routing decisions](#routing-decisions) requires. An Initial carrying a token that authenticates as a Retry token but fails validation, because it has expired or names another address, is dropped and counted, since a client processes only one Retry per connection attempt. A token that does not authenticate, including one whose key or codepoint the router does not have, cannot be recognized as a Retry token and is treated as no token, as is a `NEW_TOKEN` token that fails validation ([RFC 9000 §8.1.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.3)). For an Initial with no valid token the router answers with Retry; when it does so is configured, either for every such Initial or, by default, only above a configured rate of them, so ordinary traffic pays no extra round trip until a flood begins, decided on 2026-11-20. Below that rate such Initials are forwarded.
- The Retry source identifier encodes the server identifier of the backend the router would otherwise have forwarded the Initial to, so the client's next Initial routes there by decoding. Its nonce lies in a range reserved for router-issued identifiers, from which backends never issue, and is derived by a keyed hash, under the Retry nonce key of the router's configuration, of the client's original destination identifier and the client's address and port. Every replica sharing the configuration, and every retransmission of the same Initial, therefore produces the same identifier, and distinct attempts collide only by chance. The router's range is the nonces whose most significant bit is set, and backends issue only nonces with it clear, decided on 2026-11-20. The reservation applies to codepoints of configurations with Retry offload enabled; under them, a backend's nonce space, and the nonce space its counts report, is halved. The configuration sizes the nonce length, and the documentation states the bound: with `n` bits of router nonce space, the chance that any two of `k` Retry attempts naming one backend within a token lifetime share an identifier is about `k^2 / 2^(n+1)`, and such a collision can deliver one client's second Initial to the other's pending handshake, which then fails.
- The token carries the original destination identifier, the client's address and port, an expiry and its codepoint, protected by the token key, so the backend validates it and sends `original_destination_connection_id` and `retry_source_connection_id` without state. The backend chooses its own source identifier in its first Initial and never uses the router's Retry identifier as a connection identifier.

### Rationale

Address validation at the router protects backends from spoofed handshakes while preserving statelessness ([RFC 9000 §8.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1)). A stateless Retry needs everything the backend must later prove carried in the token ([RFC 9000 §7.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.3), [§17.2.5](https://www.rfc-editor.org/rfc/rfc9000.html#section-17.2.5)). QUIC-LB forbids a server to reuse a nonce under a configuration's key (§8.6) but does not cover identifiers issued by a load balancer. A reserved range keeps router-issued identifiers from colliding with backend-issued ones, which would deliver an Initial to an existing connection, and a nonce derived by a keyed hash, decided on 2026-11-20, lets stateless replicas agree without coordination; a stateless router cannot guarantee that distinct attempts never collide, so the bound is stated instead. Dropping and counting Retry tokens that fail validation, and treating tokens that do not authenticate as no token, were decided on 2026-11-20. Dropping deviates from [RFC 9000 §8.1.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.2), under which a server SHOULD close the connection with `INVALID_TOKEN`: the router never removes packet protection, so it does not construct protected Initial packets; the deviation is recorded in the implementation and its documentation. Offload depends on the shared token keys decided on 2026-11-20.

### Scope

`base-quic-router`, `base-quic-wire` and token validation in `base-quic`.

### Validation

Integration tests with Retry offload enabled, for v1 and v2, under both trigger policies, the rate policy sending no Retry below its rate and Retry above it: the client's second Initial reaches the backend the Retry identifier names, and the connection's transport parameters validate; an expired Retry token and one for another address are dropped and counted, while a token under an unknown key or codepoint, a malformed token and a failing `NEW_TOKEN` token are treated as no token under each trigger policy; an Initial in a datagram smaller than 1200 bytes gets no Retry; and the router stays stateless. Property tests show router-issued nonces never fall in the backends' range, and two router instances with the same configuration producing the same Retry identifier for the same original identifier and client address, and different ones when either differs. Inspect the documentation for the collision bound and the §8.1.2 deviation. A dependency check shows the feature adds no TLS implementation.
