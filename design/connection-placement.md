# Servers, endpoints and connection placement

Proposed 2026-11-21; not yet accepted or implemented.

## Requirement

### Server and endpoint

`base-http-server` provides a `Server` value, constructed from a handler, a server endpoint and options, and run until cancelled, as Async::HTTP's `Server.new(app, endpoint)` and `server.run` are:

```rust
let endpoint = Endpoint::new(listener).with_tls(config);
let server = Server::new(&handler, endpoint, options);
let statistics = server.run(&scheduler, &cancellation).await?;
```

- The endpoint decides whether connections are encrypted. `Endpoint::new` takes a bound, listening socket; `with_tls`, with the `rustls` feature, takes the `rustls::ServerConfig` that `serve_tls` takes now and configures HTTP's ALPN on it as `serve_tls` does. Nothing else in the server's API names TLS.
- `Server::run` replaces `serve` and `serve_tls`, keeping their behaviour: accept-failure handling, `Options::max_connections`, the shutdown sequence and its error precedence, and the `Statistics` it returns.
- `serve_connection` and `serve_tls_connection` remain for code that accepts its own connections.

### Connection placement

The first argument of `Server::run` and of `Client::new` decides where connections run, on both sides alike:

- The scheduler, `&scheduler`: the scoped mode, as now. Connections are supervised within the calling future, interleaving on whichever worker polls it.
- A scope of `base-scheduler`'s scoped spawn, `scope`: the spawned mode. Each connection runs as its own task of that scope, so connections progress on more than one worker. On the server a connection is one accepted socket, served until it closes; on the client it is one connection's driver, from construction to retirement.

```rust
scheduler.scope(async |scope| {
    let (client, driver) = Client::new(scope, endpoint, client_options)?;
    join(server.run(scope, &cancellation), driver.run(&cancellation)).await
});
```

In both modes, the server's or client's supervision keeps owning admission, cancellation and joined retirement: a spawned connection counts against `max_connections` or the pool as an interleaved one does, cancellation reaches it the same way, and `run` returns only after every connection it started has retired. The direct HTTP/1 client path keeps running in its calling tasks, and the client's queued calls and deferred closing stay in its driver. A spawned connection that panics is counted as a failed connection, as a connection that fails is.

Both kinds of argument implement one `base-http` trait, which supplies the scheduler, through `base-scheduler`'s `Scope::scheduler` for a scope, and places a connection's future: polling it in place for the scheduler, spawning it into the scope otherwise.

### Lifetimes

A spawned connection may borrow only what outlives the scope: the scheduler, and a handler, client or other state created before `scheduler.scope` is entered. A handler created inside the scope's root cannot be borrowed by spawned connections, because the root may return while they still run. The documentation shows this with the program shape above, the handler created before the scope.

### Efficiency

- The scoped mode costs what `serve` and `serve_tls` cost now: choosing the mode is static, with no allocation, dynamic dispatch or per-request work added.
- The endpoint's choice between plain and encrypted connections is made once per accepted connection, entering fully typed connection code; reads and writes do not branch on it.
- The spawned mode adds only its per-connection task: one task allocation, the scope's per-task accounting and the task's output storage. It adds nothing per request.

## Rationale

The user asked on 2026-11-21 for an interface as simple as Async::HTTP's, in which a caller does not need to know whether a server is encrypted to run it, nor learn a separate entry point for each combination of encryption and connection placement. The client already takes its encryption from its `Endpoint`. Choosing placement by argument keeps one rule for both sides: pass the scheduler to keep connections in the calling task, or the scope to give each its own task. The [clients and connections](client-connection.md#writable-http1-experiment-and-task-placement) record and the [roadmap](roadmap.md#m6-version-neutral-connections-and-schedulable-drivers) decide that the spawned mode is opt-in per client or server and built on the scoped spawn; this document decides how it is chosen. The user accepted on 2026-11-21 that a scope exposes its scheduler, and that the server's public entry points change in this breaking release.

## Scope

- `base-http-server`: `Server`, its `Endpoint`, and the replacement of `serve` and `serve_tls`.
- `base-http-client`: `Client::new`'s first argument and its driver's placement of connection drivers.
- The placement trait in `base-http`, and `Scope::scheduler` in `base-scheduler`.
- The fixtures, examples, tests and benchmarks that call these entry points.

## Validation

- Inspect the public server API for any entry point naming TLS or placement other than the endpoint's `with_tls` and the single-connection functions.
- Run the existing server and client suites in both modes, through a placement parameter in the fixtures, with unchanged behaviour.
- Check, in a test, that the connections of one spawned server and one spawned client progress on more than one worker, and that `run` returns only after every spawned connection has retired, including after cancellation and a panicking handler.
- Inspect `Server::run` for the scheduler argument to confirm that the scoped mode allocates nothing and dispatches statically, and rerun the hello and client benchmarks in both modes against the M5 baseline, `main` at `9fdc82e`, on matched hardware, as the roadmap's M6 exit evidence requires; record and justify any regression.
