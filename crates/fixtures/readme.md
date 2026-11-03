# Base HTTP Fixtures

Shared test fixtures and behaviour suites for Base HTTP, following Socketry's `sus-fixtures-async-http` and the `AProtocol` and `AReadableBody` shared contexts of `async-http` and `protocol-http`. This crate is unpublished; the workspace's crates use it as a development dependency.

[`ServerContext`] serves a handler on an OS-selected loopback port and runs a test beside the server and a connected client, then tears both down in order within a bounded time:

```rust
use base_http::{Method, Status, body::EmptyBody};
use base_http_fixtures::{ServerContext, body::payload};
use base_scheduler::Cancellation;

ServerContext::new().run(async |context| {
    let request = context.request(Method::GET, "/", EmptyBody);
    let mut response = context
        .client()
        .call(request, &Cancellation::never())
        .await
        .unwrap();
    assert_eq!(response.status, Status::OK);
    assert_eq!(payload(&mut response.body).await, b"Hello World!");
});
```

[`ClientContext`] runs a client of any endpoint, such as a [`RawServer`]. Raw peers in [`raw`] speak HTTP/1 over blocking `std::net` sockets, so they can misbehave in ways the Base server cannot: responding then closing, stalling after a head, truncating a body, or closing or writing to an idle connection. [`socket`] waits until a peer's kernel has acknowledged what was sent, so a test observes a close or unsolicited input without sleeping.

[`protocol_tests!`] and [`protocol_trailer_tests!`] generate the shared HTTP protocol cases for a runner, and [`readable_body_tests!`] generates the `ReadableBody` contract cases for a body type.
