# Test fixtures

Shared test support lives in the unpublished `base-http-fixtures` crate (`crates/fixtures`), modelled on Socketry's `sus-fixtures-async-http` `ServerContext`, `async-http`'s `AProtocol` shared context and `protocol-http`'s `AReadableBody`.

## Packaging and `unsafe` confinement

### Requirement

`base-http-fixtures` sets `publish = false` and is only a development dependency of the workspace's crates, declared by path without a version, so it never appears in a published manifest. The published crates keep `#![forbid(unsafe_code)]`. The fixtures crate denies `unsafe` code except in its `socket` module, which allows it for the kernel queries behind `wait_acknowledged` and gives each `unsafe` block a `SAFETY` comment.

### Rationale

Tests in several crates need the same server context, raw peers and behaviour suites, and a socket query that `std` does not expose. Keeping them in one unpublished crate removes duplicated support without adding a public API or `unsafe` code to the published crates. It replaces the earlier `base-http-test-socket` crate.

### Scope

`crates/fixtures/Cargo.toml`, the development dependencies of `base-http`, `base-http-server` and `base-http-client`, and the crate attributes of every crate in the workspace.

### Validation

Inspect each manifest: the fixtures crate is unpublished and every dependency on it is a path-only `[dev-dependencies]` entry. Search the workspace for `unsafe`; it occurs only in `crates/fixtures/src/socket.rs`.

## Server context teardown

### Requirement

`ServerContext::run` binds its listener before serving and before the test starts, so the client never races the server's start. It runs the test beside the server and the client's driver, then tears down in order: the client stops accepting calls and retires its connections, then the server stops, closing its listener and joining its connections. The test and teardown are each bounded by a configurable time; exceeding either fails the test, and a forced teardown cancels the client and server and still joins them. Joining after a forced teardown relies on the client, server and handlers observing cancellation, and on raw peers' I/O timeouts; it has no second deadline. A panic in the test propagates after teardown, ahead of any server failure. A test also fails when the server fails or a client connection remains after teardown. `ClientContext` gives a client of another endpoint the same bounds and teardown.

### Rationale

Rust has no asynchronous destructor, so a fixture which leaves teardown to the test leaks connections or hangs the test binary. Bounding every phase makes a stalled implementation fail quickly and reportably, as `sus` timeouts do.

### Scope

`ServerContext`, `ClientContext` and `Context` in `crates/fixtures/src/context.rs`.

### Validation

`crates/fixtures/tests/context.rs` exercises a test panic, a test timeout, a forced teardown, a server failure and a raw peer panic, and checks each is reported.

## Independent, bounded raw peers

### Requirement

Raw peers use blocking `std::net` sockets and no Base encoder, decoder or scheduler. Every read, write, accept, latch wait and acknowledgement wait is bounded by a timeout shorter than the default test timeout, so a stalled peer reports its own failure, and an accepted socket is explicitly made blocking, since it inherits the listener's nonblocking mode on some platforms. Tests sequence peers with latches or acknowledgement, not sleeps. A canned peer which closes a connection or writes outside an exchange waits until the other side's kernel has acknowledged it, using `socket::wait_acknowledged`, before continuing or signalling the test. Linux and macOS query the kernel; on other platforms the wait is a 50 ms sleep, which is documented on the peer helpers. Raw peers never block a scheduler thread: `Context::raw` runs one on its own thread.

### Rationale

Peers that share the implementation's codec can hide a shared bug. Unbounded blocking or sleeps make tests hang or flake under load.

### Scope

`crates/fixtures/src/raw.rs`, `crates/fixtures/src/socket.rs`, `crates/fixtures/src/latch.rs` and tests built on them.

### Validation

Inspect peer construction for timeouts and the explicit blocking reset. Search tests built on the fixtures for `thread::sleep`. `crates/fixtures/tests/acknowledgement.rs` shows, on Linux and macOS, that a bounded wait expires while a peer leaves data unread, and that `wait_acknowledged` returns once the peer reads the data and the FIN. A loopback peer acknowledges a FIN as soon as it arrives, so no test can hold the window in which only the FIN is unacknowledged. Linux counts the FIN in the same queue as the data, but the separate connection-state check macOS needs is justified by inspection, not by a test.

## Behaviour suites

### Requirement

`protocol_tests!` and `protocol_trailer_tests!` generate the shared protocol cases for a runner chosen by the caller, and `readable_body_tests!` generates the `ReadableBody` contract cases for a body type. Every case is also a standalone public function, so a partial implementation can call a subset. The protocol suite runs between the Base client and server over HTTP/1.1, over HTTP/1.0, where the trailer cases check that trailers are dropped and the data arrives intact, and over TLS with the `rustls` feature. The body suite runs against each body type `base-http` provides. Each assertion is shown to fail against an implementation that breaks it.

### Rationale

Each new engine or body type should be checked against one shared definition of the contract rather than bespoke tests. A suite whose assertions cannot fail checks nothing.

### Scope

`crates/fixtures/src/protocol.rs`, `crates/fixtures/src/body.rs`, `crates/client/tests/protocol.rs` and `tests/body/contract.rs`.

### Validation

Check that the suites are applied as stated. `crates/fixtures/tests/protocol.rs` runs every protocol case against an application sabotaged on that case's route and requires the failure message of the assertion the case exists for, and runs the trailer cases over HTTP/1.0 against an application which reports a dropped request trailer or sends the response trailer as data; `crates/fixtures/tests/body.rs` runs every body assertion against a body with the corresponding fault and requires the expected failure.
