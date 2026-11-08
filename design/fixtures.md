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

## Spawned server context

### Requirement

`ServerContext::run_spawned` creates a two-worker Native scheduler, binds the listener, then spawns the server's public entry point, `base_http_server::serve` or `serve_tls`, as one task through `base_scheduler::Spawn`, so the server runs on the scheduler's workers in parallel with a client driven from the test's thread. It takes ownership of the application and owns the scheduler for the whole run; the test receives the same `Context<'_, NativeScheduler, H>` as `run`, with the application as `&H`. Any sharing between the test and the server's task, such as an `Arc`, stays inside the fixture and is not part of its public API. The fixture relies only on the server running on the scheduler's workers. It adds nothing to `base-http-server`'s public API and does not choose how connections are spread across tasks or workers. The application must be `Handler + Send + Sync + 'static`, as a server which spawned its own connection tasks would need.

`run_spawned` has the guarantees of `run`: the listener is bound before the test starts, the test is bounded, the client retires its connections before the server stops, a server error fails the test, a server panic is resumed with its original payload, and a test failure is reported before any server failure, which is printed to stderr first. After the test, the server is cancelled and joined for at most the teardown timeout, and fails the test if it overruns. That timeout is measured on its own thread rather than by the scheduler's clock, so a server occupying every worker, such as a handler which blocks, still fails on time. Native's stall rescue, on by default since scheduler 0.5, also lets the scheduler's clock bound the join, but it can be turned off with `stall_rescue: false`, and Native then strands a timer behind the busy worker. The fixture keeps the own-thread deadline so it does not depend on rescue; it can be removed once Native fires timers behind a busy worker in every configuration. The test's timeout and the client's teardown, in `supervise` and `bounded`, are still measured by the scheduler's clock, so they are bounded only while the scheduler's timers can run. The scheduler is then shut down, and Native drains its tasks, so the call does not return until an overrunning server's current poll returns. A poll which never returns cannot be preempted and hangs the test after the failure is reported, so teardown is bounded only for servers whose polls return. The scoped `run` and `run_on` are unchanged.

### Rationale

`run` polls the server, the client's driver and the test in one `block_on` on the test's thread. That makes results reproducible, but nothing runs in parallel, so cross-thread wake-up bugs and races in two-way streaming cannot show up. The user decided that the server's connection and task layout is an implementation detail. Relying only on the public entry points, with the bounds an internally spawning server would need, means that a later move to per-connection tasks inside `serve` does not break the fixtures. The user decided to keep `Arc` and the spawned server's handle out of the public API: a context which owns its scheduler and application can join the server itself, so no forgotten shutdown or drop path exists for a test to get wrong.

### Scope

`ServerContext::run_spawned` and its private spawning, joining and scheduler helpers in `crates/fixtures/src/context.rs`.

### Validation

Inspect the private spawn: its task calls only `serve` or `serve_tls`, and the change leaves `crates/server/src` untouched. Check that `crates/fixtures/src/lib.rs` exports no `Arc`-based or spawned-server type. `crates/fixtures/tests/spawned.rs` covers the following, and checks how each is reported:

- a spawned server serving a client and a raw peer;
- an application shared with the server;
- a test panic;
- a server failure;
- a server panic, resumed with its payload;
- a test failure reported before a server failure or panic.

The unit test in `crates/fixtures/src/context/tests.rs` runs a server on a one-worker scheduler with stall rescue turned off, whose handler blocks that worker, so Native strands the scheduler clock's timers behind it, and shows that the join still fails within its timeout.

## Independent, bounded raw peers

### Requirement

Raw peers use blocking `std::net` sockets and no Base encoder, decoder or scheduler. Every read, write, accept, latch wait and acknowledgement wait is bounded by a timeout shorter than the default test timeout, so a stalled peer reports its own failure, and an accepted socket is explicitly made blocking, since it inherits the listener's nonblocking mode on some platforms. Tests sequence peers with latches or acknowledgement, not sleeps. A canned peer which closes a connection or writes outside an exchange waits until the other side's kernel has acknowledged it, using `socket::wait_acknowledged`, before continuing or signalling the test. Linux and macOS query the kernel; on other platforms the wait is a 50 ms sleep, which is documented on the peer helpers. Raw peers never block a scheduler thread: `Context::raw` runs one on its own thread. `RawPeer::split` divides a peer into a reader, which keeps the peer's buffered input, as a test shows by buffering input before the split, and a writer cloned from its socket with `TcpStream::try_clone`. Each half can be used from its own thread, so one peer can upload and download at the same time.

### Rationale

Peers that share the implementation's codec can hide a shared bug. Unbounded blocking or sleeps make tests hang or flake under load.

### Scope

`crates/fixtures/src/raw.rs`, `crates/fixtures/src/socket.rs`, `crates/fixtures/src/latch.rs` and tests built on them.

### Validation

Inspect peer construction for timeouts and the explicit blocking reset. Search tests built on the fixtures for `thread::sleep`. `crates/fixtures/tests/acknowledgement.rs` shows, on Linux and macOS, that a bounded wait expires while a peer leaves data unread, and that `wait_acknowledged` returns once the peer reads the data and the FIN. A loopback peer acknowledges a FIN as soon as it arrives, so no test can hold the window in which only the FIN is unacknowledged. Linux counts the FIN in the same queue as the data, but the separate connection-state check macOS needs is justified by inspection, not by a test. `crates/fixtures/tests/raw.rs` streams `DUPLEX_BODY` up and down at the same time through a split peer. It also shows that a sequential upload on one thread stalls until its write timeout; that upload declares 1 GiB, far more than any socket buffers, so the stall does not depend on the host's buffer sizes.

## Behaviour suites

### Requirement

`protocol_tests!` and `protocol_trailer_tests!` generate the shared protocol cases for a runner chosen by the caller, and `readable_body_tests!` generates the `ReadableBody` contract cases for a body type. Every case is also a standalone public function, so a partial implementation can call a subset. The protocol suite includes two two-way streaming cases, `duplex_stream` and `duplex_chunked_stream`. In each, the client streams a request body while it reads the streamed echo at the same time, each direction `DUPLEX_BODY` long, and the upload never runs more than `DUPLEX_WINDOW`, a few chunks, ahead of the echo received. The exchange can therefore finish only if the client delivers the response while still sending the request and the server echoes the request while still receiving it, whatever the sockets buffer. The request declares its length, so it is sent with `Content-Length` over either version, as [HTTP/1 trailers](http1-trailers.md) requires for a request of known length, even though a streamed body may have trailers. In `duplex_stream` the server echoes the incoming body, whose known length and lack of trailers give the response `Content-Length`, and the case checks that the client's response body has the request's length and no trailers. In `duplex_chunked_stream` it echoes the body with no length and `may_have_trailers` true, so over HTTP/1.1 the response is chunked, and the case checks that the client's response body may have trailers and has no length; over HTTP/1.0 it checks that the echo is close-delimited. The protocol suite runs between the Base client and server over HTTP/1.1, over HTTP/1.0, where the trailer cases check that trailers are dropped and the data arrives intact, and over TLS with the `rustls` feature. Each of these runs in two modes: scoped, with `run`, and spawned, with `run_spawned`, where the server runs on Native workers and the test drives the client from its own thread. The body suite runs against each body type `base-http` provides. Each assertion is shown to fail against an implementation that breaks it.

### Rationale

Each new engine or body type should be checked against one shared definition of the contract rather than bespoke tests. A suite whose assertions cannot fail checks nothing.

### Scope

`crates/fixtures/src/protocol.rs`, `crates/fixtures/src/body.rs`, `crates/client/tests/protocol.rs` and `tests/body/contract.rs`.

### Validation

Check that the suites are applied as stated. `crates/fixtures/tests/protocol.rs` runs every protocol case against an application sabotaged on that case's route and requires the failure message of the assertion the case exists for, and runs the trailer cases over HTTP/1.0 against an application which reports a dropped request trailer or sends the response trailer as data. The sabotaged `/duplex-chunked` route echoes with the request's length, so `duplex_chunked_stream` fails on the echo's framing over HTTP/1.1 and, in `duplex_chunked_stream_http10`, over HTTP/1.0. `duplex_stream_reframed` and `duplex_stream_reframed_http10` answer `/duplex` with an echo of unknown length which may have trailers, and `duplex_stream` fails on the echo's framing over each version. `crates/fixtures/tests/body.rs` runs every body assertion against a body with the corresponding fault and requires the expected failure. `the_duplex_stream_stalls_against_a_buffering_server` in `crates/fixtures/tests/protocol.rs` shows that `duplex_stream` cannot pass against a server which reads the whole request before replying; `duplex_chunked_stream` shares its windowed exchange. `a_sequential_duplex_stream_stalls` shows that a client which uploads a 1 GiB request before reading the echo stalls: it samples the bytes written 250 ms and 500 ms after the call starts and requires that the upload made no progress between them, so the window is not what makes a sequential exchange fail, and a client which keeps uploading does not pass.
