# HTTP/1.0 trailers

## Dropping trailers on HTTP/1.0

### Requirement

HTTP/1.0 has no chunked framing, so `base-http1` drops trailers on HTTP/1.0 output rather than refusing them. A body's `may_have_trailers` never selects chunked framing or causes a refusal on HTTP/1.0, and trailer frames the body produces are validated as they would be on HTTP/1.1, then omitted. The message's data is sent unchanged:

- An HTTP/1.0 request body of known length is sent with `Content-Length`. A request body of unknown length is refused before any output with "HTTP/1.0 request requires a body length", because an HTTP/1.0 request cannot be close-delimited. This holds for `RequestWriter::write_request` and for imperative output through `begin_request` and `write_frame`.
- A response to an HTTP/1.0 request is sent with its length when it is known and is close-delimited otherwise. Dropping its trailers completes the response normally, so a persistent connection stays reusable.

A raw `Trailer` field announces a trailer section that HTTP/1.0 cannot send, so it is still refused with "trailers require HTTP/1.1 chunked framing", as is a raw `Transfer-Encoding`. On HTTP/1.1, `may_have_trailers` still selects chunked framing, and trailer frames from a body framed with a fixed length are still refused.

### Rationale

The user decided that HTTP/1.0 discards trailers, as HTTP semantics permit a recipient to do ([RFC 9110 §6.5](https://www.rfc-editor.org/rfc/rfc9110#section-6.5)). `BodyReader` always reports `may_have_trailers`, so refusing such bodies meant no streamed request could be sent over HTTP/1.0, and a handler could not serve the same body to HTTP/1.0 and HTTP/1.1 clients. Raw framing controls are written deliberately, so they stay strict.

### Scope

`RequestWriter` in `crates/http1/src/client.rs`, `ResponseWriter::write_response` and the shared body encoder in `crates/http1/src/output.rs`, and through them `base-http-client` and `base-http-server`.

### Validation

`crates/http1/tests/codec/client.rs` covers a known-length request body whose trailers are dropped through both `write_request` and `write_frame`, and the refusal of an unknown-length request body and of a raw `Trailer` field before any output. `crates/http1/tests/codec/output.rs` covers fixed-length and close-delimited responses whose trailers are dropped, the validation of dropped trailers, and the HTTP/1.1 refusal. `http10_trailers_are_dropped` in `crates/server/tests/end_to_end.rs` checks that a persistent HTTP/1.0 connection serves a further request after a response whose trailers were dropped. The fixtures' trailer cases run over HTTP/1.0 in `crates/client/tests/protocol.rs` and check, in both directions, that no trailer arrives and the data does; see [test fixtures](fixtures.md). A close-delimited response looks the same to a client whether the server dropped its trailers or failed after its data, so the end-to-end server test, not the response case, shows that the response completed.
