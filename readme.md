# Base HTTP

A runtime-independent HTTP semantic layer, porting Socketry's `protocol-http` model to Rust. The package is unpublished and its API is a prototype. It depends on the released `base-scheduler` interface; the Direct backend is a development dependency.

The crate provides `Request<B>`, `Response<B>`, `Method`, `Status`, `Version`, application `Protocol` intent, ordered `Fields`, and native bodies in `base_http::body`. Message construction and body mapping perform no I/O. The sibling [base-http1](crates/http1/readme.md), [base-http-client](crates/client/readme.md) and [base-http-server](crates/server/readme.md) packages implement native HTTP/1 framing, pooled client calls and application serving over Base scheduler sockets.

The [pooled client/server example](crates/client/examples/client_server.rs) sends two POST requests through one endpoint pool to a local echo server, then joins driver and transport cleanup. Run `cargo run --locked --manifest-path crates/client/Cargo.toml --example client_server`. Add `--features rustls` and `-- --https` to run the pooled example with TLS. The [low-level example](crates/server/examples/client_server.rs) demonstrates direct ownership of the HTTP/1 lanes.

The [client/connection taxonomy](design/client-connection.md) distinguishes a durable remote-server client from an established protocol session. The first high-level client selects HTTP/1 over resolved plain TCP or rustls TLS and uses `base-pool::MultiplexPool` with capacity one per connection. `Client::new` returns a cloneable request handle and an explicit scoped driver; `call` returns final response metadata while upload continues. Shared protocol factories supply and interpret ALPN above the codec. HTTP/2/3 and managed client tunnels remain follow-up work; see [transport layering](design/transport.md).

```rust
use base_http::{Fields, HttpHeaders, Method, Request, Response, Status, body::BufferedBody};

let request = Request::new(Method::POST, "/submit", b"hello".to_vec())
    .map_body(BufferedBody::from);
let (metadata, body) = request.into_parts();
let mut response = Response::new(Status::OK, body);
response.fields.add("Content-Type", "text/plain")?;

// Interpret fields explicitly. A received body Headers frame can contribute
// trailers to this object without changing the message's initial fields.
let mut headers = HttpHeaders::new(response.fields.clone());
headers.append_trailers(Fields::try_from_pairs([("x-checksum", "example")])?)?;
assert_eq!(headers.headers().len(), 1);
assert_eq!(headers.trailers().len(), 1);
assert_eq!(response.fields.len(), 1);
assert_eq!(metadata.method, Method::POST);
# Ok::<(), base_http::FieldError>(())
```

`Fields` stores ordered raw `pairs` with token names and opaque values, preserving original name spelling, duplicate pairs and byte-valued content. It has no trailer marker, combination policy or cached index. `HttpHeaders::new(fields)` provides explicit HTTP interpretation: lazy lookup combines lists and cookies, preserves separate `set-cookie` values, and checks singleton/trailer rules. `headers::Policy` supports application overrides; other protocols can interpret the same raw type independently. `append_trailers(fields)` accepts an explicit trailing section, including an empty one. `into_parts()` recovers the initial and trailing sections without losing provenance. Protocol codecs independently enforce mandatory wire restrictions. Field-specific date, range, authentication and structured-value parsing helpers remain follow-up work.

`Request` and `Response` expose public `metadata` and `body` members. Their metadata types are `RequestMetadata` and `ResponseMetadata`; dereferencing exposes members such as `request.fields`. `into_parts`, `from_parts`, `map_body` and `with_body` preserve metadata ownership across body-type changes. Bodies need not implement `Clone` or a body trait merely to be carried or mapped. `content_length` retains a declared representation length, including for HEAD/304 responses with no readable data. `persistent: Option<bool>` expresses an outgoing reuse preference; received hop-specific reuse decisions remain with the protocol exchange. Method safety/idempotence does not by itself authorize retries; processing state and body reproduction remain separate.

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

`ReadableBody::read` takes cancellation and returns a `Send` future yielding an owned `BodyFrame<Data>`. Its variants are `Data { data, end }`, `Headers { fields, end }`, and `End`; `is_end()` handles both payload-associated and separate completion. Initial fields stay in the message metadata; a body `Headers` frame contains trailers. No data or further headers may follow trailing headers, even when completion is observed separately. An empty data payload is ordinary data unless `end` is true. After completion, reads return `End`. Memory bodies need no scheduler. I/O-backed bodies can retain explicitly constructed scheduler-bound readers; asynchronous retirement remains with the resource/driver owner. `ReadableBody::close` signals abandonment and does not prove retirement. Readiness, exhaustion, total representation length and replayability have separate meanings. See the [implementation evidence](design/implementation.md).

`ReadableBody` combines asynchronous reads with readiness, exhaustion, total representation length, replay and closure. `WritableBody` describes producer operations: `write_frame`, convenience data writes, flushing, normal/error closure and abandonment. The concrete `BodyReader` and `BodyWriter` handles implement those traits. `BodyWriter` also exposes these operations as inherent methods.

`BodyReader::bounded(max_bytes, max_chunks)` returns independently movable reader/writer halves, with backpressure under both limits. Normal write closure drains accepted frames and marks the last queued payload as final; failure discards queued data and preserves the cause. Reader abandonment wakes blocked writes; dropping unfinished output reports failure. Every frame, including empty data and headers, counts toward the item limit. Data bytes and field name/value bytes count toward the byte limit; oversized frames fail immediately. Limits measure exposed bytes; a chunk owner may retain additional storage.

```rust
use base_http::body::{BodyFrame, BodyReader, BodyError, ReadableBody};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, zip};

let (mut reader, mut writer) = BodyReader::bounded(1024, 16);
let producer = async {
    writer.write_frame(
        BodyFrame::Data { data: b"hello".to_vec(), end: true },
        &Cancellation::never(),
    ).await?;
    Ok::<_, BodyError>(())
};
let consumer = async {
    let mut bytes = Vec::new();
    loop {
        let frame = reader.read(&Cancellation::never()).await?;
        let end = frame.is_end();
        match frame {
            BodyFrame::Data { data, .. } => bytes.extend(data),
            BodyFrame::Headers { fields, .. } => {
                // Forward fields, inspect them, or explicitly collect them.
                assert!(fields.is_empty());
            }
            BodyFrame::End => {}
        }
        if end { break; }
    }
    Ok::<_, BodyError>(bytes)
};
let (sent, received) = block_on(zip(producer, consumer));
sent?;
assert_eq!(received?, b"hello");
# Ok::<(), BodyError>(())
```

There is one pull-body abstraction. Custom bodies can read incoming input while producing outgoing chunks. Protocol drivers read bodies directly in their existing future; an extra producer task is unnecessary. Imperative producers can use the bounded pair and explicitly supervised concurrent work. No public callable-stream abstraction is required.

`Request::read` and `Response::read` return the body's owned frames without mutating the initial fields. Forward a frame with `writer.write_frame(frame, cancellation)` or explicitly collect received fields with `headers.append_trailers(fields)` on an explicit `HttpHeaders` interpreter. There is no `ReadableBody::finish` hook. A final data or header frame avoids another read just to discover completion; `End` handles empty bodies and completion learned later. `BodyFrame::map_data` changes the data owner while preserving fields and completion. Bodies select their own data type, so forwarding does not require a common allocation, cloning, boxing or copying payload bytes.

Outgoing HTTP/1 drivers consume frames directly and serialize received header frames as trailers without merging them into the initial fields. `ReadableBody::may_have_trailers()` conservatively defaults to true, allowing HTTP/1.1 to choose chunked framing before any reads, even with a known data length. Static bodies return false. A false result promises no trailing headers. HTTP/1.0 has no trailer representation and rejects actual header frames. Known lengths remain validated when data is chunked. Drivers retain ownership of submitted buffers until transport access ends; receipt of a terminal body frame does not itself establish backend retirement.

`InterimResponses<H>` supplies per-exchange send/receive hooks using the same `ResponseMetadata`. Its handler can be a protocol writer or async observer. It validates interim status codes, excludes final `101`, and stops delivery after `finish` or a failed/abandoned started send. Drivers call `finish` when committing or receiving the final metadata; message metadata remain ordinary cloneable values.

```rust
use base_http::{InterimResponses, ResponseMetadata, Status};
use base_scheduler::Cancellation;
use std::convert::Infallible;

let mut interim = InterimResponses::new(|head: ResponseMetadata| async move {
    assert_eq!(head.status, Status::EARLY_HINTS);
    Ok::<_, Infallible>(())
});
futures_lite::future::block_on(interim.send_interim_response(
    ResponseMetadata::new(Status::EARLY_HINTS), &Cancellation::never(),
))?;
interim.finish();
# Ok::<(), base_http::InterimError<Infallible>>(())
```

The native HTTP/1 server transmits interim heads and supports managed `101` and successful CONNECT exchanges using these bodies. HTTP ownership continues through lane retirement and socket closure. See the [source review](design/source-review.md#bidirectional-bodies-upgrades-and-interim-responses).

The stack uses native protocol engines over scheduler I/O capabilities. The prototype has real TCP tests against independent raw peers, including full-duplex proxy forwarding and reuse after an early response (see the [experiment](design/duplex.md)). The HTTP/1 client integrates the released [base-pool](../base-pool/readme.md); HTTP/1 HTTPS is also implemented; forwarding transient borrowed server input through a longer-lived client remains follow-up work. Linux io_uring is the target performance backend. The [proposal](design/proposal.md) defines package boundaries, the [roadmap](design/roadmap.md) orders streaming bodies and transport ownership through HTTP/1, TLS, HTTP/2, QUIC and HTTP/3, and the [component evaluation](design/components.md) describes parsing/compression/TLS implementation choices. The [source review](design/source-review.md) records provenance and reference behavior.

Run `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings`. The crate declares Rust 1.87. No concrete scheduler backend, `http`, `http-body`, Hyper or protocol engine is a production dependency.

The [performance notes](performance.md) collect the cost model, latest HTTP/HTTPS client comparisons, optimization history, benchmark evidence and reproduction commands, including variation between identical builds.

The separate [static hello benchmark](benchmarks/hello/readme.md) compares the current HTTP/1 server with Hyper/Tokio using matched responses and persistent connections. Its [macOS results](benchmarks/hello/results.md) include native readiness, Tokio and Direct backends, configuration and raw measurements. They do not measure Linux io_uring.

The [echo workload comparison](benchmarks/echo/results.md) isolates scheduler and socket behavior with useful per-connection work, validated payloads, and client-placement controls. The [overhead notes](overhead.md) distinguish those results from the synthetic self-waking tests.

## Contributing

Please open an issue or pull request on [GitHub](https://github.com/base-rs/base-http).

### Agent Context

Run `cargo bake agent:context:install` to install shared context and skills. Use `cargo bake agent:context:list` and `cargo bake agent:context:show` to discover guidance. Follow `agents.md` and the relevant skills under `.agents/skills/`.
