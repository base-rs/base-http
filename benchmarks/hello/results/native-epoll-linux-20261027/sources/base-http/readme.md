# Base HTTP

A runtime-independent HTTP semantic layer, porting Socketry's `protocol-http` model to Rust. The package is unpublished and its API is a prototype. It depends on the released `base-scheduler` interface; the Direct backend is a development dependency.

The crate provides `Request<B>`, `Response<B>`, `Method`, `Status`, `Version`, application `Protocol` intent, ordered `Fields`, and native bodies in `base_http::body`. Message construction and body mapping perform no I/O. The sibling [base-http1](../base-http1/readme.md) and [base-http-server](../base-http-server/readme.md) packages implement native HTTP/1 client/server framing and application serving over Base scheduler sockets.

The [single-file client/server example](../base-http-server/examples/client_server.rs) runs both peers on loopback using the Direct scheduler, streams a POST body through an echo handler and joins connection cleanup. Run it from this directory with `cargo run --locked --manifest-path ../base-http-server/Cargo.toml --example client_server`. The client uses the current HTTP/1 request/response primitives.

```rust
use base_http::{Method, Request, Response, Status, body::BufferedBody};

let request = Request::new(Method::POST, "/submit", b"hello".to_vec())
    .map_body(BufferedBody::from);
assert_eq!(request.method, Method::POST);
assert_eq!(request.path.as_deref(), Some("/submit"));

let (head, body) = request.into_parts();
let mut response = Response::new(Status::OK, body);
response.fields.add("Content-Type", "text/plain")?;
response.fields.mark_trailers();
response.fields.add("x-checksum", "example")?;
assert_eq!(response.fields.headers().len(), 1);
assert_eq!(response.fields.trailers().len(), 1);
assert_eq!(head.method, Method::POST);
# Ok::<(), base_http::FieldError>(())
```

`Fields` preserves ordered raw `pairs`, original name spelling and byte-valued content. A single optional marker divides initial fields from trailers. Raw inspection uses `&self`; interpreted lookup uses `&mut self` to lazily build the policy index without interior mutability. The index combines comma lists and cookies, keeps `set-cookie` separate, and enforces singleton/trailer policies. Indexing leaves raw pairs intact. Field-specific date, range, authentication and structured-value parsing helpers remain follow-up work.

`Request` and `Response` expose a public `head` and `body`; dereferencing exposes head members directly, such as `request.fields`. `into_parts`, `from_parts`, `map_body` and `with_body` preserve head ownership across body-type changes. Bodies need not implement `Clone` or a body trait merely to be carried or mapped. Method safety/idempotence does not by itself authorize retries; processing state and body reproduction remain separate.

```rust
use base_http::body::{BufferedBody, HeadBody, ReadableBody};

let mut body = BufferedBody::from(b"hello".to_vec());
assert!(body.is_ready());
assert_eq!(body.length(), Some(5));
assert_eq!(body.read_chunk(), Some(b"hello".to_vec()));
assert_eq!(body.length(), Some(5));
assert!(body.rewind());

let head = HeadBody::for_body(&mut body);
assert!(head.is_empty());
assert_eq!(head.length(), Some(5));
```

`ReadableBody::read` takes cancellation and returns a `Send` future. Memory bodies need no scheduler. I/O-backed bodies can retain explicitly constructed scheduler-bound readers; asynchronous retirement remains with the resource/driver owner. `ReadableBody::close` signals abandonment and does not prove retirement. Readiness, exhaustion, total representation length and replayability have separate meanings. See the [implementation evidence](design/implementation.md).

`ReadableBody` combines asynchronous reads with readiness, exhaustion, total representation length, replay and closure. `WritableBody` describes producer operations: whole-chunk writes, flushing, normal/error closure and abandonment. The concrete `BodyReader` and `BodyWriter` handles implement those traits. `BodyWriter` also exposes these operations as inherent methods.

`BodyReader::bounded(max_bytes, max_chunks)` returns independently movable reader/writer halves, with backpressure under both limits. Normal write closure drains accepted chunks before EOF; failure discards queued data and preserves the cause. Reader abandonment wakes blocked writes; dropping unfinished output reports failure. Empty chunks count toward chunk limits, and oversized chunks fail immediately. Limits measure exposed bytes; a chunk owner may retain additional storage.

```rust
use base_http::body::{BodyReader, BodyError, ReadableBody};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, zip};

let (mut reader, mut writer) = BodyReader::bounded(1024, 16);
let producer = async {
    writer.write(b"hello".to_vec(), &Cancellation::never()).await?;
    writer.close_write(None);
    Ok::<_, BodyError>(())
};
let consumer = async {
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.read(&Cancellation::never()).await? {
        bytes.extend(chunk);
    }
    Ok::<_, BodyError>(bytes)
};
let (sent, received) = block_on(zip(producer, consumer));
sent?;
assert_eq!(received?, b"hello");
# Ok::<(), BodyError>(())
```

There is one pull-body abstraction. Custom bodies can read incoming input while producing outgoing chunks. Protocol drivers read bodies directly in their existing future; an extra producer task is unnecessary. Imperative producers can use the bounded pair and explicitly supervised concurrent work. No public callable-stream abstraction is required.

`Request::read` and `Response::read` delegate to their body and invoke `ReadableBody::finish(&mut Fields)` at EOF. This moves received trailers into the message's existing ordered collection after its marker, avoiding shared mutable heads and separate trailer frames. Implementations make `finish` idempotent. Callers reading `body.read` directly perform this final step themselves. Outbound drivers invoke it before emitting trailers. A producer that has known length and will generate trailers must declare the trailer boundary or `Trailer` field before transmission so HTTP/1 selects chunked framing.

`InterimResponses<H>` supplies per-exchange send/receive hooks using the same `ResponseHead`. Its handler can be a protocol writer or async observer. It validates interim status codes, excludes final `101`, and stops delivery after `finish` or a failed/abandoned started send. Drivers call `finish` when committing or receiving the final head; message heads remain ordinary cloneable values.

```rust
use base_http::{InterimResponses, ResponseHead, Status};
use base_scheduler::Cancellation;
use std::convert::Infallible;

let mut interim = InterimResponses::new(|head: ResponseHead| async move {
    assert_eq!(head.status, Status::EARLY_HINTS);
    Ok::<_, Infallible>(())
});
futures_lite::future::block_on(interim.send_interim_response(
    ResponseHead::new(Status::EARLY_HINTS), &Cancellation::never(),
))?;
interim.finish();
# Ok::<(), base_http::InterimError<Infallible>>(())
```

The native HTTP/1 server transmits interim heads and supports managed `101` and successful CONNECT exchanges using these bodies. HTTP ownership continues through lane retirement and socket closure. See the [source review](design/source-review.md#bidirectional-bodies-upgrades-and-interim-responses).

The stack uses native protocol engines over scheduler I/O capabilities. The prototype has real TCP tests against independent raw peers, including full-duplex proxy forwarding and reuse after an early response (see the [experiment](design/duplex.md)). The sibling [base-pool](../base-pool/readme.md) implements generic singleplex and multiplex pools; HTTP/1 client orchestration, HTTP pool integration and TLS remain future work. Linux io_uring is the target performance backend; end-to-end comparisons against Hyper/Tokio will begin with HTTP/1. The [proposal](design/proposal.md) defines package boundaries, the [roadmap](design/roadmap.md) orders streaming bodies and transport ownership through HTTP/1, TLS, HTTP/2, QUIC and HTTP/3, and the [component evaluation](design/components.md) describes parsing/compression/TLS implementation choices. The [source review](design/source-review.md) records provenance and reference behavior.

Run `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings`. The crate declares Rust 1.85. No concrete scheduler backend, `http`, `http-body`, Hyper or protocol engine is a production dependency.

The separate [static hello benchmark](benchmarks/hello/readme.md) compares the current HTTP/1 server with Hyper/Tokio using matched responses and persistent connections. Its [macOS results](benchmarks/hello/results.md) include native readiness, Tokio and Direct backends, configuration and raw measurements. They do not measure Linux io_uring.
