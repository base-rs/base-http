# Base HTTP Server

An unpublished application server using native `base-http1`, `base-http` semantics and published Base scheduler capabilities. The server owns and joins its connection futures; it requires no hidden tasks, boxed bodies or concrete runtime.

Handlers are generic over incoming body and interim-response hook types. The associated `Body<B>` selects the outgoing representation. For a streaming echo, it is simply `B`:

```rust
use base_http::{InterimHandler, InterimResponses, Request, Response, Status, body::ReadableBody};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use std::convert::Infallible;

struct Echo;
impl Handler for Echo {
    type Error = Infallible;
    type Body<B: ReadableBody> = B;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<B>, Infallible> {
        Ok(Response::new(Status::OK, request.body))
    }
}
```

Static handlers can use `BufferedBody`; imperative producers can return `BodyReader` and supervise their `BodyWriter` producer explicitly. There is one pull-body model. A handler may send interim heads while processing input. Returning a readable body that owns input also supports managed CONNECT and upgrade exchanges.

`serve_connection` consumes one accepted socket. `serve` consumes an already-bound listener and enforces `Options::max_connections`. Both retain the scheduler until reader/writer retirement and exclusive socket closure complete. For shutdown, cancel the supplied signal and await the serving future. Listener shutdown terminates active work and joins cleanup; timed draining is future policy.

Run `cargo run --example hello`, then `curl -v http://127.0.0.1:8080/`. Use `-- --once` to serve one connection and exit; `BASE_HTTP_ADDRESS` overrides the bind address. The example uses the released Direct scheduler as a development dependency. Real TCP tests use an independent raw HTTP peer and cover streaming, persistence, interim responses, trailers, managed duplex bodies, cancellation, errors and listener limits.

For both peers in one file, run `cargo run --locked --example client_server`. The [complete example](examples/client_server.rs) binds an ephemeral loopback port, starts a streaming echo handler, sends a POST using `RequestWriter` and reads its response using `ResponseReader`. Request writing and response reading run concurrently. It makes one exchange, retires both I/O lanes, closes the sockets and shuts down the Direct scheduler. It uses the current low-level client APIs; high-level client orchestration remains planned.

The [proxy tests](tests/proxy.rs) drive low-level HTTP/1 request output and response input concurrently against native and independent raw upstreams. They check streaming before upload completion, interim/trailer forwarding, complete early responses, large transfers, cancellation/error retirement and reuse of both connections. The planned high-level client returns on final response headers and retains ongoing request transmission in its driver. An ordinary proxy handler can then pass the incoming request to the client and return its response. This client orchestration is not implemented yet; see the [symmetric application design](../base-http/design/duplex.md#symmetric-application-interface).

Run `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings`. Rust 1.85 is the declared minimum. The initial server supports HTTP/1 only. TLS, deadlines, HTTP/2/3, HTTP/1 client orchestration and pools remain future work. A response can start while its request body is still active. Reuse requires completed input and output; abandoning unread input closes the connection. The server does not silently drain uploads. Parse failures receive best-effort 400/431; application or late body failures close the connection. No Linux io_uring throughput result is claimed.
