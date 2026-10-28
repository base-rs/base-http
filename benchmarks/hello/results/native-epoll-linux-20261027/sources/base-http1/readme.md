# Base HTTP/1

An unpublished native HTTP/1 codec over published Base scheduler stream capabilities. `httparse` handles request/response head syntax privately; this crate owns framing, read-ahead, persistence and body lifecycle.

`RequestReader<R>::read_request` returns a `Request<IncomingBody<'_, R>>` and an `Exchange`. The body borrows the input lane. `ResponseWriter<W>` owns an independent output lane, sends interim responses and writes a generic `Response<B>` where `B: ReadableBody`. Returning the incoming body gives streaming echo without an extra task or dynamic dispatch.

`Exchange` retains immutable framing decisions: version, HEAD/CONNECT classification, persistence, Expect/Continue and any upgrade offers. The application owns the complete request head. Its mutations cannot change the captured decisions. Use `exchange.version()` when constructing the interim writer; the earlier prototype's `Exchange::head()` snapshot accessor has been removed.

The initial implementation supports HTTP/1.0 and HTTP/1.1 requests, fixed and chunked input, request trailers, fixed/chunked/close-delimited output, response trailers, HEAD/bodyless responses, interim heads, persistence, and managed 101/CONNECT duplex bodies. It rejects conflicting framing and enforces configurable head/field/chunk-line limits. An early final response permits reuse when the active upload subsequently completes; abandoned input disables reuse. Only chunked transfer coding is implemented; upgrade selection currently supports one unversioned token.

`RequestWriter<W>` and `ResponseReader<R>` provide independent client directions. Drive request output and response input concurrently, delivering interim heads through `InterimResponses`. The reader uses the matching request head for HEAD/CONNECT semantics and shares the native input body decoder. Request output supports fixed/chunked framing and trailers; response input also supports close-delimited bodies. HTTP/1.0 request bodies require a length. These primitives hide no dialing, pooling, task creation or Expect/Continue wait policy. Switch responses expose raw input and disable HTTP reuse; managed bidirectional client switch coordination remains future work.

A writer's successful persistence result applies to its output lane only. Finish both directions, check input reuse, and join all exchange work before admitting another request. A complete early response must not automatically cancel a still-active upload. An error can cancel its sibling direction, but both must still be awaited. See the [duplex experiment](../base-http/design/duplex.md).

Bodies use `base-http` semantics. Use `request.read(&cancellation)` to append received trailers to the message's ordered `Fields` at EOF. Direct body readers call `body.finish(&mut fields)` themselves. Known-length outgoing bodies that generate trailers must declare a boundary or `Trailer` field before the response starts.

The caller owns the socket: flush successful output, await both lane closures, drop the handles, then close the socket exclusively. A failed or dropped started operation prevents reuse. Cancellation is distinct from backend retirement. `base-http-server` implements this ownership sequence and bounded listener orchestration.

Run `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings`. Deterministic tests cover fragmentation, partial writes and framing failures. Real socket integration tests live in the sibling `base-http-server` package. Rust 1.85 is the declared minimum. High-level client orchestration, managed client tunnels, pooling, TLS, fuzzing and other runtime-backend validation remain follow-up work.
