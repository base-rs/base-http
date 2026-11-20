# Protocol switching across versions

Accepted 2026-11-20; not yet implemented. These requirements extend the managed upgrades and CONNECT described in the [implementation notes](implementation.md#http1-behavior) to [HTTP/2](http2.md) and [HTTP/3](http3.md). The [roadmap](roadmap.md) schedules them for HTTP/1 and HTTP/2 [in M7](roadmap.md#m7-add-hpack-http2-and-multiplex-admission), including the HTTP/1 codec changes, and for HTTP/3 [in M9](roadmap.md#m9-add-qpack-and-http3).

## Requirement

The semantic `protocol` of a request asks for the exchange to switch to that protocol; on a final response it means the switch was accepted. On the wire this is an HTTP/1 `Upgrade`, or an HTTP/2 or HTTP/3 extended CONNECT ([RFC 8441](https://www.rfc-editor.org/rfc/rfc8441.html), [RFC 9220](https://www.rfc-editor.org/rfc/rfc9220.html)). Each codec maps it to and from its own version's wire form, so the same handler, and a middleware layer translating between versions, work over every version. Traditional CONNECT, without a protocol, keeps its existing meaning.

An HTTP/1 `Upgrade` is an optional offer that the server may ignore, whereas an extended CONNECT succeeds only as a tunnel. Only tokens with a defined extended-CONNECT mapping cross between them. The mapped set is a list owned by the HTTP/2 and HTTP/3 codecs, initially `websocket` ([RFC 8441 §5](https://www.rfc-editor.org/rfc/rfc8441.html#section-5)). Other offers, including connection-level ones such as `h2c`, are not switch requests outside HTTP/1.

Server codecs, receiving requests:

- A request carrying a protocol is identified as a switch by its protocol, not its method. Server codecs deliver the method as received, `GET` or another method over HTTP/1 and `CONNECT` over HTTP/2 and HTTP/3, so handlers test `protocol`.
- The HTTP/1 server reports a protocol for a single offered token on a request other than CONNECT, as now; with several offers it reports none and retains the offers privately, as now.
- An HTTP/1 authority-form CONNECT is delivered as traditional CONNECT without a protocol, even if it carries `Upgrade`. This changes the current server, which reports a single offer's protocol whatever the method. An Async::HTTP client sending its extended CONNECT in that form receives a plain tunnel. The HTTP/1 client below never sends that form.
- Such a CONNECT retains no upgrade offers, decided on 2026-11-20, so a 101 response to it is refused as unsolicited, as a 101 to a request without `Upgrade` is now.

Server codecs, sending final responses:

- A final response carrying the request's protocol, with status 101 or any 2xx, accepts the switch. Over HTTP/1 it is sent as 101 with `Upgrade` and `Connection: upgrade`. Over HTTP/2 and HTTP/3, which never send 101, a 101 is sent as 200 and any other 2xx is sent unchanged.
- A final response carrying a protocol is refused before any of it is sent, as the codec refuses other invalid final responses, when:
  - its status is neither 101 nor 2xx;
  - the request carried no protocol;
  - the protocol differs from the request's, or over HTTP/1 was not among the offered tokens.
- Over HTTP/2 and HTTP/3, a 2xx final response without a protocol, to a request carrying a protocol, is refused the same way. On the wire, any 2xx to an extended CONNECT establishes the tunnel, so sending one would report a switch that did not happen. This covers a proxy relaying an HTTP/1 upstream's ordinary 200 to a declined upgrade.
- Over HTTP/1, a non-101 final response to a request carrying a protocol is an ordinary response that declines the offer. It is framed by its status, as now.

Client codecs, sending requests:

- The HTTP/1 client sends a request carrying a protocol with `Upgrade` and `Connection: upgrade`, and never as authority-form CONNECT. A request whose method is `CONNECT` is sent as `GET` with its path; one without a path fails before any of it is sent. A request with any other method keeps that method.
- The HTTP/2 and HTTP/3 clients send a request as `:method CONNECT` with `:protocol`, `:scheme`, `:path` and `:authority` when it carries a protocol and either:
  - its method is `CONNECT`, which already makes it an extended CONNECT; or
  - its method is `GET` and its protocol is in the mapped set.
- Such a request is sent only after the peer advertised `SETTINGS_ENABLE_CONNECT_PROTOCOL`. Such a request waits for the peer's SETTINGS, even when [HTTP/2 client admission](http2.md#client-admission) admits other requests earlier.
- A request whose method is not `CONNECT` and whose protocol is not in the mapped set is sent as an ordinary request with its method and no `:protocol`. Its response is ordinary.
- A request with a mapped protocol and a method other than `GET` or `CONNECT`, an extended CONNECT without a path, or one to a peer that did not advertise extended CONNECT fails before any of it is sent. The error identifies the request as unsent. Whether the client then picks another connection or version is application or retry policy; selecting connections by extended-CONNECT support is not decided.

Client codecs, reading responses:

- The client classifies a response by the request it actually sent, not by the semantic method.
- HTTP/1: a request sent as `GET` with `Upgrade` switches only on 101 with the offered token, which is reported as the response's protocol, as now. Any other final response, including 2xx, is an ordinary response, framed by its status, without a protocol or tunnel. Only a request sent as traditional CONNECT becomes a tunnel on any 2xx.
- HTTP/2 and HTTP/3: a request sent as extended CONNECT switches on any 2xx, and the response reports the request's protocol. A request sent without `:protocol` never switches.

Handshake fields particular to a protocol, such as WebSocket's `Sec-WebSocket-Key` and `Sec-WebSocket-Accept`, are not codec concerns; codecs pass them as ordinary fields. The [proxy](load-balancer.md#proxy-forwarding) core forwards a protocol switch only when both sides use the same mechanism, HTTP/1 `Upgrade` or extended CONNECT, and otherwise responds 502. Translating a handshake between the mechanisms is the job of a middleware layer, such as the load balancer's [WebSocket layer](middleware.md#load-balancer-layers). `Connection` and `Upgrade` are connection-specific and never sent over HTTP/2 or HTTP/3 ([RFC 8441 §5](https://www.rfc-editor.org/rfc/rfc8441.html#section-5)).

## Rationale

References are to async-http [`b7029c7`](https://github.com/socketry/async-http/tree/b7029c722791711588354016891297df21264272), the revision the [HTTP/2 requirements](http2.md#reference-implementation-and-its-defects) cite. The first draft cited the older local checkout `0a3cfd6`; the cited behavior is the same at both.

Async::HTTP maps the semantic protocol to a version's success status on the HTTP/1 server path. [`Server#each`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http1/server.rb#L102-L104) sends 101 for any response with a body and a protocol. Its HTTP/2 client sets the protocol on a 200 ([`response.rb#L67-L70`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/response.rb#L67-L70)).

The reviewed code lacks the opposite mappings:

- Its HTTP/2 server sends the response's status unchanged ([`request.rb#L159-L161`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/request.rb#L159-L161)), so a 101 from an HTTP/1 upstream reaches the client, which [RFC 9113 §8.6](https://www.rfc-editor.org/rfc/rfc9113.html#section-8.6) forbids.
- Its HTTP/1 client sends an extended CONNECT as authority-form CONNECT with `Upgrade`, losing the path: it uses the authority as the target for any CONNECT ([`client.rb#L44-L50`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http1/client.rb#L44-L50)) and then writes the upgrade ([`#L55-L59`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http1/client.rb#L55-L59)).
- Its HTTP/2 client sends an HTTP/1 upgrade as `:method GET` with `:protocol` ([`response.rb#L249-L251`](https://github.com/socketry/async-http/blob/b7029c722791711588354016891297df21264272/lib/async/http/protocol/http2/response.rb#L249-L251)). [RFC 8441 §4](https://www.rfc-editor.org/rfc/rfc8441.html#section-4) defines `:protocol` only as a modification of CONNECT, so [RFC 9113 §8.3](https://www.rfc-editor.org/rfc/rfc9113.html#section-8.3) makes that request malformed.

These forms interoperate between Async::HTTP peers because both ends accept them. The user decided on 2026-11-20 to adopt the approach with every direction mapped. The user also decided on 2026-11-20 that over HTTP/2 and HTTP/3 a handler's 101 is sent as 200 and any other 2xx unchanged, and that an HTTP/1 authority-form CONNECT carrying `Upgrade` is delivered as traditional CONNECT without a protocol, so Base's server does not reproduce the Async::HTTP client's lossy form.

The user also decided on 2026-11-20 that HTTP/1 Upgrade offers are optional offers. A token with a defined extended-CONNECT mapping, `websocket`, maps to extended CONNECT; other offers, including `h2c`, are dropped when a request crosses to HTTP/2 or HTTP/3, which then send it as an ordinary request. Without this, `curl --http2` against a plain `http` URL, which sends `GET` with `Upgrade: h2c`, would become a tunnel request through a proxy to an HTTP/2 upstream. [HTTP/2](http2.md#connection-contract-and-protocol-selection) does not implement `h2c`.

An optional HTTP/1 offer can be declined with an ordinary 200, but over HTTP/2 and HTTP/3 a 200 is the acceptance. A proxy or middleware layer mapping between versions would otherwise turn a declined upgrade into a falsely reported switch. Refusing a 2xx without a protocol in the HTTP/2 and HTTP/3 server codecs keeps the wire meaning consistent with the semantic response. Classifying HTTP/1 client responses by the method actually sent keeps a 200 to a mapped `GET` from being read as a successful CONNECT tunnel. The current reader classifies by the semantic method ([`client.rs`](../crates/http1/src/client.rs), `response_body`).

The user decided on 2026-11-20 that translating handshake fields between versions is not part of the proxy core but a self-contained middleware layer, as [middleware](middleware.md#load-balancer-layers) describes, and that the proxy core otherwise forwards only same-mechanism switches and responds 502. The codec mappings above are what let such a layer, and handlers, treat a switch uniformly across versions.

Base's HTTP/1 codec currently differs from these requirements in these ways:

- The client sends a CONNECT request carrying a protocol as authority-form CONNECT with `Upgrade`, the form criticized above ([`client.rs`](../crates/http1/src/client.rs), request head), and refuses a CONNECT request with a path ("CONNECT cannot have a path").
- The server reports a single offered token as the protocol of any request, including an authority-form CONNECT ([`wire.rs`](../crates/http1/src/wire.rs), request head parsing), and retains the offers, so it also permits a 101 to that CONNECT ([`output.rs`](../crates/http1/src/output.rs), "unsolicited upgrade").
- The server refuses a protocol on a 2xx response to a request other than CONNECT ("protocol without upgrade status"), and on a successful CONNECT response ("protocol on traditional CONNECT").
- The client classifies responses by the semantic method, as noted above.

So it needs the changes above. They are deliberate codec mappings between the semantic model and each version's wire form, not the incidental method rewriting during parsing that the [proposal](proposal.md#port-the-native-semantic-model) excludes.

## Scope

Request reading and writing and response writing and reading, in `base-http1`, `base-http2` and `base-http3`, for requests and responses carrying a protocol or answering one. The unsent-request error of `base-http-client`.

## Validation

For every pair of client and server versions, a handler that accepts a protocol switch with 200 and one that accepts it with 101 both complete a duplex echo, with the client observing the protocol on its response.

Tests check the wire forms:

- HTTP/1 `GET` with the path and `Upgrade` for a request built as CONNECT with a protocol, and refusal before sending of one without a path;
- HTTP/2 and HTTP/3 `:method CONNECT` with `:protocol` for a request built as `GET` with `websocket`;
- an ordinary HTTP/2 or HTTP/3 `GET` without `:protocol` for a request carrying `h2c`, as received from an HTTP/1 client offering `Upgrade: h2c`;
- no 101 on HTTP/2 or HTTP/3: a handler's 101 is sent as 200, and a handler's 201 with the protocol is sent as 201;
- refusal before sending when the peer did not advertise extended CONNECT or the method is another, with the unsent error;
- an HTTP/2 extended CONNECT on a new connection waits for the peer's SETTINGS while an ordinary request admitted before them is sent.

An HTTP/1 authority-form CONNECT carrying `Upgrade: websocket` reaches the handler as CONNECT without a protocol, and a successful response to it is a plain tunnel; a 101 to it is refused.

Tests check refusal of a protocol on a non-success final status, on a response to a request without one, and of a different protocol. They check refusal by the HTTP/2 and HTTP/3 servers of a 2xx without a protocol to an extended CONNECT.

A declined upgrade is tested: an HTTP/1 server answers a mapped `GET` with an ordinary 200 and a body, and the HTTP/1 client delivers that body by its framing, without a protocol or tunnel. Through the proxy, with or without the WebSocket layer, that response does not reach an HTTP/2 or HTTP/3 client as a 2xx.

Interoperate with an independent HTTP/2 peer enforcing RFC 8441, an independent HTTP/3 peer supporting RFC 9220, and an independent HTTP/1 WebSocket peer.
