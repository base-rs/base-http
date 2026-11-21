# Base HTTP Server

An application server using native `base-http1`, `base-http` semantics and published Base scheduler capabilities. The server owns and joins its connections, whether they run within its own future or as tasks of a scope its caller passes; it requires no hidden tasks, boxed bodies or concrete runtime.

Applications implement `base-http`'s shared request interface, `Handler`, for `ServerContext`, which the server passes with each request. The context carries the connection's `Peer`, its addresses and, over TLS, the negotiated application protocol, and the hook for sending interim responses, which the server writes while it continues to poll the handler. The same interface is implemented by clients, so middleware generic over the context wraps either. Handlers are generic over the incoming body; the associated `Body<B>` selects the outgoing representation. For a streaming echo, it is simply `B`:

```rust
use base_http::{Request, Response, Status, body::ReadableBody};
use base_http_server::{Handler, ServerContext};
use base_scheduler::Cancellation;
use std::convert::Infallible;

struct Echo;
impl<'h> Handler<'h, ServerContext> for Echo {
    type Error = Infallible;
    type Body<B: ReadableBody + 'h> = B;

    async fn call<B: ReadableBody + 'h>(
        &self,
        request: Request<B>,
        _: &mut ServerContext,
        _: &Cancellation,
    ) -> Result<Response<B>, Infallible> {
        Ok(Response::new(Status::OK, request.body))
    }
}
```

A request body is an independent value: a handler can pass it to work that outlives the call, such as a client forwarding it upstream. The connection reads the next request once that body has ended or been dropped, and takes its input back when it is cancelled or fails.

Static handlers can use `BufferedBody`; imperative producers can return `BodyReader` and supervise their `BodyWriter` producer explicitly. There is one pull-body model. A handler may send interim heads while processing input. Returning a readable body that owns input also supports managed CONNECT and upgrade exchanges.

`Server::new(&handler, endpoint, &options)` serves an `Endpoint`: `Endpoint::new(listener)` takes an already-bound, listening socket, and `with_tls(config)`, with the `rustls` feature, encrypts every connection, setting ALPN from `Options::protocols`. `server.run(placement, &cancellation)` enforces `Options::max_connections`, and its first argument decides where connections run: the scheduler, `&scheduler`, polls them within the returned future, and a scope of the scheduler's scoped spawn runs each as its own task, so connections progress on more than one worker. Spawned connections borrow the handler and options, so create them before entering the scope; a spawned connection that panics is counted as a failed connection. `serve_connection` and `serve_tls_connection` consume one accepted connection and serve it with `Options::protocols`'s plaintext or negotiated engine. `run` disables Nagle's algorithm on each accepted socket unless `Options::tcp_nodelay` is false, so small streamed frames are not delayed waiting for acknowledgements. This is a best-effort hint: a socket that rejects it is still served. `serve_connection` leaves the options of a socket the caller accepted unchanged. Each retains the scheduler until reader/writer retirement and exclusive socket closure complete. For shutdown, cancel the supplied signal and await the serving future. Listener shutdown closes the listener, then terminates active work and joins cleanup, including spawned connections; timed draining is future policy. Transient accept failures do not stop a listener: per-peer failures such as `ECONNABORTED` are retried, backing off after repeated failures, and resource exhaustion such as `EMFILE` pauses acceptance for 100 ms on the scheduler's `Clock`. They are counted in `Statistics::accept_errors`. Other accept failures, including scheduler closure, end serving with an error.

Run `cargo run --example hello`, then `curl -v http://127.0.0.1:8080/`. Use `-- --once` to serve one connection and exit, or `-- --spawned` to run each connection as its own task; `BASE_HTTP_ADDRESS` overrides the bind address. The example uses the released Native scheduler as a development dependency. Real TCP tests use an independent raw HTTP peer and cover streaming, persistence, interim responses, trailers, managed duplex bodies, cancellation, errors and listener limits.

For both peers in one file, run `cargo run --locked --example client_server`. The [complete example](examples/client_server.rs) binds an ephemeral loopback port, starts a streaming echo handler, sends a POST using `RequestWriter` and reads its response using `ResponseReader`. Request writing and response reading run concurrently. It makes one exchange, retires both I/O lanes, closes the sockets and shuts down the Native scheduler. It uses the current low-level client APIs; high-level client orchestration remains planned.

The [proxy tests](tests/proxy.rs) drive low-level HTTP/1 request output and response input concurrently against native and independent raw upstreams. They check streaming before upload completion, interim/trailer forwarding, complete early responses, large transfers, cancellation/error retirement and reuse of both connections. The planned high-level client returns on final response headers and retains ongoing request transmission in its driver. An ordinary proxy handler can then pass the incoming request to the client and return its response. This client orchestration is not implemented yet; see the [symmetric application design](../../design/duplex.md#symmetric-application-interface).

Run `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings`. Rust 1.92 is the declared minimum. The initial server supports HTTP/1 only. Deadlines and HTTP/2/3 remain future work; the sibling `base-http-client` implements pooled HTTP/1 calls. A response can start while its request body is still active. Reuse requires completed input and output; abandoning unread input closes the connection. The server does not silently drain uploads. Parse failures receive best-effort 400/431. A handler error receives a best-effort bodiless 400 when reading the request failed, or 500 otherwise, with `Connection: close`, and serving that connection reports it as `Error::Handler`; a final response which HTTP/1 cannot send, such as an informational status, conflicting framing, a `Content-Length` field its body contradicts or an unsolicited upgrade, receives a bodiless 500 with `Connection: close` and is reported as `Error::Response`; a body failure after the response has started closes the connection. No Linux io_uring throughput result is claimed.

Requests and responses expose `.metadata` (`RequestMetadata` / `ResponseMetadata`) and raw ordered `.fields`. HTTP/1 has already extracted framing and connection controls before the handler runs. Body trailers remain owned `Headers` frames; an application can forward them or explicitly accumulate them in `HttpHeaders`.

Enable `rustls` for `serve_tls`, which accepts an owned TCP listener and explicit server configuration. TLS handshakes share the bounded connection supervisor, allowing acceptance to continue while a peer stalls. `Options::protocols` supplies ALPN offers; `base-http-protocol` selects the HTTP engine before codec construction. `serve_tls_connection` accepts an already authenticated rustls connection. HTTP/1 framing remains independent of encryption. Absent ALPN is allowed by default; `require_alpn` makes selection strict.
