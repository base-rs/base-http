# HTTP/1 trailers

## Trailers are not known in advance

### Requirement

Whether a message will have trailers is not known until its body ends. `ReadableBody::may_have_trailers` is the only input to that decision: it defaults to true, and a body returns false only when it can promise that it produces no trailer frame, as static bodies do. There is no separate API for declaring that a streamed body has no trailers. The `Trailer` field is never used to choose framing, to drop trailers, to refuse a message, or to decide whether a body has trailers; it is sent and received as an ordinary field.

Requests and responses frame a body that may have trailers differently on HTTP/1.1:

- **Responses** keep trailers. A response body that may have trailers uses chunked framing, even when its length is known. A response body that promises no trailers but produces them anyway, with a fixed length, is refused with "trailers require chunked framing". Bodies that cannot have trailers, such as `BufferedBody`, `EmptyBody` and an incoming body received with `Content-Length` or close-delimited and then forwarded, report false and keep `Content-Length`. Responses that never carry a body (to HEAD, and 204 and 304) take no framing at all, whatever the body reports. Response framing follows the version of the request it answers.
- **Requests** prefer fixed framing. A request body of known length, whether from `ReadableBody::length` or from the `content_length` given to `begin_request`, is sent with `Content-Length` whatever `may_have_trailers` reports. Trailer frames it then produces are validated as they would be under chunked framing and dropped, and the request completes normally. A request body of unknown length is chunked and keeps its trailers.

### Rationale

The user decided this model. The `Trailer` field is only a SHOULD hint ([RFC 9110 §6.6.2](https://www.rfc-editor.org/rfc/rfc9110#section-6.6.2)), and protocols such as gRPC send trailers without announcing them. A sender that dropped or refused trailers based on the field, or a body API that had to predict them, would lose real trailers.

The asymmetry is also the user's decision. Request trailers are rare, while a client MUST NOT send `Transfer-Encoding` unless it knows the server handles HTTP/1.1 ([RFC 9112 §6.1](https://www.rfc-editor.org/rfc/rfc9112#section-6.1)), and servers that require a length answer a chunked upload with 411 Length Required. Chunking every streamed upload would also give bodyless requests such as GET a `Transfer-Encoding`. Discarding trailers is permitted ([RFC 9110 §6.5.1](https://www.rfc-editor.org/rfc/rfc9110#section-6.5.1)). A response answers a client that has already said which version it speaks, so chunked framing is safe there, and response trailers (gRPC status, digests) are the common case worth preserving.


### Scope

Outgoing framing in `RequestWriter` (`crates/http1/src/client.rs`) and `ResponseWriter::write_response` (`crates/http1/src/output.rs`), and the `ReadableBody::may_have_trailers` contract in `base-http`.

### Validation

Inspect both framing plans: `may_have_trailers` selects chunked framing for HTTP/1.1 responses, a known length selects `Content-Length` for requests, and a `Trailer` field never causes trailers to be dropped or a message to be refused. `known_length_body_trailers_select_chunked_framing_and_stay_out_of_metadata` in `crates/http1/tests/codec/main.rs` covers HTTP/1.1 chunked response framing for a known length, and `bodyless_responses_skip_the_trailer_rule` in `crates/http1/tests/codec/output.rs` covers HEAD, 204 and 304. `http_11_requests_of_known_length_drop_their_trailers` in `crates/http1/tests/codec/client.rs` covers a known-length request through `write_request` and through `begin_request` with a `Trailer` field, the lane staying reusable, validation of the dropped trailers, and an unknown-length request keeping them; `client_requests_frame_streams_and_static_trailers` in `crates/http1/tests/codec/main.rs` does the same for whole requests. `fixed_length_uploads_drop_their_trailers_and_stay_reusable` in `crates/client/tests/writable/main.rs` checks it through `base-http-client`. `unannounced_trailers_on_an_http_11_fixed_body_are_rejected` in `crates/http1/tests/codec/output.rs` covers the broken promise. `reset_content_and_transfer_encoding_select_their_framing` in the same file and `http_10_requests_send_known_length_bodies_and_drop_their_trailers` in `crates/http1/tests/codec/client.rs` check that a `Trailer` field leaves a body without trailers on fixed-length framing.

## HTTP/1.0 never has trailers

### Requirement

HTTP/1.0 has no chunked framing, so `base-http1` drops trailers on HTTP/1.0 output rather than refusing them. On HTTP/1.0, `may_have_trailers` never selects chunked framing or causes a refusal, and a `Trailer` field is sent as an ordinary field. Trailer frames the body produces are validated as they would be on HTTP/1.1, so trailer fields are validated the same way over either version. They are then omitted, and the message's data is sent unchanged:

- An HTTP/1.0 request body of known length is sent with `Content-Length`, as on HTTP/1.1. A request body of unknown length is refused before any output with "HTTP/1.0 request requires a body length", because an HTTP/1.0 request cannot be close-delimited. This holds for `RequestWriter::write_request` and for imperative output through `begin_request` and `write_frame`.
- A response to an HTTP/1.0 request is sent with its length when it is known and is close-delimited otherwise. A 205 response declares `Content-Length: 0`. Dropping the trailers completes the response normally, so a persistent connection stays reusable.

A raw `Transfer-Encoding` is still refused on HTTP/1.0, with "HTTP/1.0 transfer-encoding". Tunnel output after a successful CONNECT is raw bytes rather than an HTTP/1.0 body, so it still refuses trailer frames.

### Rationale

The user decided that HTTP/1.0 discards trailers. HTTP semantics let trailers be discarded ([RFC 9110 §6.5.1](https://www.rfc-editor.org/rfc/rfc9110#section-6.5.1)), as an intermediary must do when it forwards a message to an HTTP/1.0 recipient. `BodyReader` always reports `may_have_trailers`, so refusing such bodies meant no streamed request could be sent over HTTP/1.0, and a handler could not serve the same body to HTTP/1.0 and HTTP/1.1 clients.

### Scope

`RequestWriter` in `crates/http1/src/client.rs`, `ResponseWriter::write_response` and the shared body encoder in `crates/http1/src/output.rs`, and through them `base-http-client` and `base-http-server`.

### Validation

`crates/http1/tests/codec/client.rs` covers a known-length request body whose trailers are dropped through both `write_request` and `write_frame`, a `Trailer` field sent as an ordinary field, and the refusal of an unknown-length request body before any output. `crates/http1/tests/codec/output.rs` covers fixed-length and close-delimited responses whose trailers are dropped, a `Trailer` field, the framing of a 205 whose body may have trailers, and the validation of dropped trailers. Inspect `ResponseWriter::write_response` for the tunnel exception: it enables discarding only when the response is not a tunnel. `http10_trailers_are_dropped` in `crates/server/tests/end_to_end.rs` checks that a persistent HTTP/1.0 connection serves a further request after a response whose trailers were dropped. The fixtures' trailer cases run over HTTP/1.0 in `crates/client/tests/protocol.rs` and check, in both directions, that no trailer arrives and the data does; see [test fixtures](fixtures.md). A close-delimited response looks the same to a client whether the server dropped its trailers or failed after its data, so the end-to-end server test, not the response case, shows that the response completed.

## Incoming bodies report trailers only for chunked framing

### Requirement

An incoming request or response body reports `may_have_trailers()` as true only when it uses chunked framing, which only HTTP/1.1 allows. A body framed by `Content-Length` or by closing the connection, a tunnel, and every HTTP/1.0 body report false and never yield a trailer frame, whether or not the message carries a `Trailer` field. This holds for `RequestReader` on the server and for `ResponseReader` on the client, and so for the bodies `base-http-server` hands to handlers and `base-http-client` returns.

### Rationale

Only the chunked coding can carry a trailer section ([RFC 9112 §7.1.2](https://www.rfc-editor.org/rfc/rfc9112#section-7.1.2)). Reporting false for other framing lets a consumer that forwards the body choose fixed-length framing, instead of chunked framing for trailers that cannot arrive.

### Scope

`IncomingBody::may_have_trailers` in `crates/http1/src/input.rs`, which `RequestReader` and `ResponseReader` share. An HTTP/1.0 message that uses `Transfer-Encoding` has faulty framing ([RFC 9112 §6.1](https://www.rfc-editor.org/rfc/rfc9112#section-6.1)), so it is refused when read and the connection is not reused. Received trailers are yielded as a body frame and never merged into the message's header section.

### Validation

`only_chunked_request_bodies_may_have_trailers` in `crates/http1/tests/codec/input.rs` and `only_chunked_response_bodies_may_have_trailers` in `crates/http1/tests/codec/client.rs` read HTTP/1.0 and HTTP/1.1 bodies with each framing, each announcing a `Trailer` field. They check what `may_have_trailers` reports, whether a trailer frame arrives, and that trailers stay out of the header fields. `fused_request_facts_reject_ambiguous_framing_and_invalid_lines` and `client_rejects_malformed_and_ambiguous_response_heads` in `crates/http1/tests/codec/main.rs` refuse HTTP/1.0 `Transfer-Encoding` and check the lane is no longer reusable. `crates/client/tests/client/call.rs` checks the flag on bodies the client returns. The remaining cases follow by inspection: `IncomingBody::may_have_trailers` also excludes tunnels, `base-http-server` passes the incoming body to handlers unchanged, and `base-http-client` copies the flag from it in `crates/client/src/connection.rs` and `crates/client/src/writable.rs`.
