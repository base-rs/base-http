# `base-http`

A runtime-independent HTTP semantic layer for Base, porting Socketry's `protocol-http` model to Rust. It defines requests, responses, fields and bodies without performing I/O, while sibling packages implement HTTP/1, pooled clients and servers over Base scheduler sockets.

## Usage

Add `base-http` for the semantic types:

```sh
cargo add base-http
```

Add `base-http-client` or `base-http-server` for the HTTP/1 client and server, and enable the `rustls` feature of either for TLS.

Messages carry public metadata and an arbitrary body owner, and body mapping preserves the metadata:

```rust
use base_http::{Method, Request, Response, Status, body::BufferedBody};

let request = Request::new(Method::POST, "/submit", b"hello".to_vec())
    .map_body(BufferedBody::from);
let (metadata, body) = request.into_parts();
let mut response = Response::new(Status::OK, body);
response.fields.add("Content-Type", "text/plain")?;
assert_eq!(metadata.method, Method::POST);
# Ok::<(), base_http::FieldError>(())
```

The [pooled client/server example](crates/client/examples/client_server.rs) sends POST requests through one endpoint pool to a local echo server. Run it with `cargo run --locked --manifest-path crates/client/Cargo.toml --example client_server`, adding `--features rustls` and `-- --https` for TLS. The [low-level example](crates/server/examples/client_server.rs) demonstrates direct ownership of the HTTP/1 lanes.

## Releasing

Prepare a release with `cargo bake cargo:version:patch` (or `minor`, `major`, or `bump --version X.Y.Z`), commit it with a `Release: vVERSION` trailer, then run `cargo bake cargo:release` and open a pull request. After review and merge, GitHub Actions publishes the release when the configured `crates-io` environment approves it, then creates or updates the matching GitHub Release from `releases.md`. Follow the shared [Releasing skill](https://github.com/base-rs/base-project/blob/main/context/releasing.md) for the standard process.

## Releases

<!-- bake-readme:releases:start -->

See [releases.md](releases.md) for the full release history.

### v0.2.0

- `base-http-client` and `base-http-server` disable Nagle's algorithm (`TCP_NODELAY`) on each TCP connection they establish or accept, so small writes such as streamed frames on long-lived duplex exchanges are not delayed by Nagle's algorithm interacting with delayed acknowledgements. Set the new `Options::tcp_nodelay` to `false` to leave the socket options unchanged. Setting the option is best effort and does not fail a connection. This is a breaking change: `Options` in both crates gains the public `tcp_nodelay` field, so struct literals need it or `..Default::default()`, and `Client::new`, `serve` and `serve_tls` now require the scheduler to implement `SocketSetOption<TcpNoDelay>`, which every Base scheduler backend provides. `serve_connection` leaves the options of a caller-accepted socket unchanged.
- `base-http1` incoming body reads are cancellation safe. Dropping a pending read, or cancelling its wait, loses no input, so a later read resumes at the same position. Previously any later read failed with "previous body read failed or was abandoned". A read that fails for another reason still fails every later read, now with "previous body read failed".

### v0.1.0

- First release of `base-http`, `base-http1`, `base-http-protocol`, `base-http-client` and `base-http-server`. `base-http` defines runtime-independent HTTP semantics: requests and responses, methods, statuses, versions, request targets, ordered fields with header interpretation and combination policies, interim responses, and buffered and streaming bodies. Constructing messages and mapping bodies performs no I/O.
- `base-http1` implements native HTTP/1.0 and HTTP/1.1 framing over Base scheduler streams. It parses heads with a hand-written parser and handles fixed-length, chunked and close-delimited bodies, trailers and interim responses. The server side also manages `101` upgrades and CONNECT. `base-http-protocol` selects the protocol for a connection, including through ALPN.
- `base-http-client` provides a cloneable client with an explicit driver that pools HTTP/1 connections through `base-pool`, over plain TCP or, with the `rustls` feature, TLS. `base-http-server` serves connections and listeners with a connection limit, over plain TCP or, with the `rustls` feature, TLS.
- Errors report each cause once: an error either describes its cause in its message or returns it from `source`. A connection that fails to open is `base_http_client::Error::Connect`, which carries the endpoint address.
- Requires Rust 1.87, `base-scheduler 0.3`, `base-pool 0.3` and `base-endpoint 0.1`.

<!-- bake-readme:releases:end -->

## See Also

- [`base-http1`](https://docs.rs/base-http1) implements native HTTP/1 framing, [`base-http-protocol`](https://docs.rs/base-http-protocol) selects the protocol for a connection, [`base-http-client`](https://docs.rs/base-http-client) provides pooled client calls and [`base-http-server`](https://docs.rs/base-http-server) serves applications.
- The [design documents](design/) record package boundaries, the roadmap and implementation evidence, and the [performance notes](performance.md) collect benchmark results and reproduction commands.
- [`base-scheduler`](https://docs.rs/base-scheduler) defines the runtime-independent scheduler and socket capabilities these crates use.
- [`base-pool`](https://docs.rs/base-pool) provides the connection pool behind the client.
- [`base-endpoint`](https://docs.rs/base-endpoint) binds and connects endpoints through scheduler capabilities, including TLS.

## Contributing

Please open an issue or pull request on [GitHub](https://github.com/base-rs/base-http).
