# HTTP/1 trailers

## Trailers are not known in advance

### Requirement

Whether a message will have trailers is not known until its body ends. `ReadableBody::may_have_trailers` is the only input to that decision: it defaults to true, and a body returns false only when it can promise that it produces no trailer frame, as static bodies do. There is no separate API for declaring that a streamed body has no trailers. The `Trailer` field is never used to drop trailers, to refuse a message, or to decide that a body has no trailers.

On HTTP/1.1, an outgoing body that may have trailers uses chunked framing, even when its length is known, so its trailers are preserved. An outgoing body that promises no trailers but produces them anyway, with a fixed length, is refused with "trailers require chunked framing".

### Rationale

The user decided this model. The `Trailer` field is only a SHOULD hint ([RFC 9110 §6.6.2](https://www.rfc-editor.org/rfc/rfc9110#section-6.6.2)), and protocols such as gRPC send trailers without announcing them. A sender that dropped or refused trailers based on the field, or a body API that had to predict them, would lose real trailers.

### Scope

Outgoing framing in `RequestWriter` (`crates/http1/src/client.rs`) and `ResponseWriter::write_response` (`crates/http1/src/output.rs`), and the `ReadableBody::may_have_trailers` contract in `base-http`.

### Validation

Inspect both framing plans: `may_have_trailers` selects chunked framing on HTTP/1.1, and a `Trailer` field never causes trailers to be dropped or a message to be refused. `known_length_body_trailers_select_chunked_framing_and_stay_out_of_metadata` in `crates/http1/tests/codec/main.rs` covers HTTP/1.1 chunked framing for a known length. `unannounced_trailers_on_an_http_11_fixed_body_are_rejected` in `crates/http1/tests/codec/output.rs` covers the broken promise.

## HTTP/1.0 never has trailers

### Requirement

HTTP/1.0 has no chunked framing, so `base-http1` drops trailers on HTTP/1.0 output rather than refusing them. On HTTP/1.0, `may_have_trailers` never selects chunked framing or causes a refusal, and a `Trailer` field is sent as an ordinary field. Trailer frames the body produces are validated as they would be on HTTP/1.1, so a body is accepted or refused the same way over either version. They are then omitted, and the message's data is sent unchanged:

- An HTTP/1.0 request body of known length is sent with `Content-Length`. A request body of unknown length is refused before any output with "HTTP/1.0 request requires a body length", because an HTTP/1.0 request cannot be close-delimited. This holds for `RequestWriter::write_request` and for imperative output through `begin_request` and `write_frame`.
- A response to an HTTP/1.0 request is sent with its length when it is known and is close-delimited otherwise. A 205 response declares `Content-Length: 0`. Dropping the trailers completes the response normally, so a persistent connection stays reusable.

A raw `Transfer-Encoding` is still refused on HTTP/1.0, with "HTTP/1.0 transfer-encoding". Tunnel output after `101` or CONNECT still refuses trailer frames.

### Rationale

The user decided that HTTP/1.0 discards trailers. HTTP semantics let trailers be discarded ([RFC 9110 §6.5.1](https://www.rfc-editor.org/rfc/rfc9110#section-6.5.1)), as an intermediary must do when it forwards a message to an HTTP/1.0 recipient. `BodyReader` always reports `may_have_trailers`, so refusing such bodies meant no streamed request could be sent over HTTP/1.0, and a handler could not serve the same body to HTTP/1.0 and HTTP/1.1 clients.

### Scope

`RequestWriter` in `crates/http1/src/client.rs`, `ResponseWriter::write_response` and the shared body encoder in `crates/http1/src/output.rs`, and through them `base-http-client` and `base-http-server`.

### Validation

`crates/http1/tests/codec/client.rs` covers a known-length request body whose trailers are dropped through both `write_request` and `write_frame`, a `Trailer` field sent as an ordinary field, and the refusal of an unknown-length request body before any output. `crates/http1/tests/codec/output.rs` covers fixed-length, close-delimited and 205 responses whose trailers are dropped, a `Trailer` field, the validation of dropped trailers, and the HTTP/1.1 refusal. `http10_trailers_are_dropped` in `crates/server/tests/end_to_end.rs` checks that a persistent HTTP/1.0 connection serves a further request after a response whose trailers were dropped. The fixtures' trailer cases run over HTTP/1.0 in `crates/client/tests/protocol.rs` and check, in both directions, that no trailer arrives and the data does; see [test fixtures](fixtures.md). A close-delimited response looks the same to a client whether the server dropped its trailers or failed after its data, so the end-to-end server test, not the response case, shows that the response completed.

## Incoming bodies report trailers only for chunked framing

### Requirement

An incoming request or response body reports `may_have_trailers()` as true only when it uses chunked framing, which only HTTP/1.1 allows. A body framed by `Content-Length` or by closing the connection, a tunnel, and every HTTP/1.0 body report false and never yield a trailer frame, whether or not the message carries a `Trailer` field. This holds for `RequestReader` on the server and for `ResponseReader` on the client, and so for the bodies `base-http-server` hands to handlers and `base-http-client` returns.

### Rationale

Only the chunked coding can carry a trailer section ([RFC 9112 §7.1.2](https://www.rfc-editor.org/rfc/rfc9112#section-7.1.2)). Reporting false for other framing lets a consumer that forwards the body choose fixed-length framing, instead of chunked framing for trailers that cannot arrive.

### Scope

`IncomingBody::may_have_trailers` in `crates/http1/src/input.rs`, which `RequestReader` and `ResponseReader` share. HTTP/1.0 messages that use `Transfer-Encoding` are refused when read.

### Validation

`only_chunked_request_bodies_may_have_trailers` in `crates/http1/tests/codec/input.rs` and `only_chunked_response_bodies_may_have_trailers` in `crates/http1/tests/codec/client.rs` read HTTP/1.0 and HTTP/1.1 bodies with each framing, each announcing a `Trailer` field. They check what `may_have_trailers` reports and whether a trailer frame arrives.
