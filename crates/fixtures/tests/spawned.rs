// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A spawned server runs on the scheduler's workers beside a client driven from the test's thread, shuts down within its bound, and reports each way it can fail, including when its handle is dropped.
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, Status, body::EmptyBody,
    body::ReadableBody,
};
use base_http_fixtures::{
    Hello, Latch, ServerContext, body::payload, protocol::App, shared_native,
};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::{
    convert::Infallible,
    io::{Read, Write},
    net::TcpStream,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Duration,
};

fn failing() -> ServerContext {
    ServerContext::new().server_options(base_http_server::Options {
        max_connections: 0,
        ..Default::default()
    })
}

async fn hello(
    context: &base_http_fixtures::Context<'_, impl base_http_fixtures::Scheduler, Arc<Hello>>,
) {
    let mut response = context
        .client()
        .call(
            context.request(Method::GET, "/", EmptyBody),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(response.status, Status::OK);
    assert_eq!(payload(&mut response.body).await, b"Hello World!");
}

#[test]
fn a_spawned_server_serves_several_clients_and_shuts_down() {
    shared_native(|scheduler| {
        let server = ServerContext::new().spawn(scheduler);
        let _: &Arc<Hello> = server.handler();
        assert_eq!(server.endpoint().authority, server.address().to_string());
        server.run(async |context| hello(context).await);
        let output = server.run(async |context| {
            hello(context).await;
            context
                .raw(|mut peer| {
                    peer.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
                    let (head, _) = peer.read_message();
                    head.status()
                })
                .await
        });
        assert_eq!(output, 200);
        let statistics = block_on(server.shutdown());
        assert_eq!(statistics.requests, 3);
    });
}

#[test]
fn a_spawned_run_shares_the_application_with_the_server() {
    ServerContext::new()
        .handler(App::new())
        .run_spawned(async |context| {
            let app: &App = context.handler();
            base_http_fixtures::protocol::response_trailer(context).await;
            assert!(app.response_released().is_released());
        });
}

#[test]
#[should_panic(expected = "the test failed")]
fn a_test_panic_propagates_from_a_spawned_run() {
    ServerContext::new().run_spawned(async |_| panic!("the test failed"));
}

#[test]
#[should_panic(expected = "server failed")]
fn a_spawned_server_failure_fails_the_test() {
    failing().run_spawned(async |_| {});
}

// The test's own failure is reported ahead of the server failure it may have caused.
#[test]
#[should_panic(expected = "the test failed first")]
fn a_test_panic_is_reported_before_a_spawned_server_failure() {
    failing().run_spawned(async |_| panic!("the test failed first"));
}

/// Panics in every call.
struct Panicking;

impl Handler for Panicking {
    type Error = Infallible;
    type Body<B: ReadableBody> = EmptyBody;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<EmptyBody>, Infallible> {
        panic!("the handler panicked")
    }
}

// A request the server cannot answer, sent without a client so the test does not wait for a response.
fn request(server: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(server).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    stream
}

#[test]
#[should_panic(expected = "the handler panicked")]
fn shutdown_resumes_a_server_panic() {
    shared_native(|scheduler| {
        let server = ServerContext::new().handler(Panicking).spawn(scheduler);
        let mut stream = request(server.address());
        // The server's task ends when the handler panics, closing the connection.
        let _ = stream.read(&mut [0; 1]);
        block_on(server.shutdown());
    });
}

/// Blocks its worker in every call until released, so the server cannot observe cancellation.
struct Blocking {
    started: Latch,
    release: Latch,
}

impl Handler for Blocking {
    type Error = Infallible;
    type Body<B: ReadableBody> = EmptyBody;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<EmptyBody>, Infallible> {
        self.started.release();
        self.release.wait_blocking();
        Ok(Response::new(Status::OK, EmptyBody))
    }
}

#[test]
fn a_server_which_does_not_stop_fails_its_shutdown() {
    let (started, release) = (Latch::new(), Latch::new());
    shared_native(|scheduler| {
        let server = ServerContext::new()
            .handler(Blocking {
                started: started.clone(),
                release: release.clone(),
            })
            .teardown_timeout(Duration::from_millis(50))
            .spawn(scheduler);
        let _stream = request(server.address());
        started.wait_blocking();
        let failure = catch_unwind(AssertUnwindSafe(|| block_on(server.shutdown())))
            .expect_err("a stalled shutdown fails");
        release.release();
        let message = failure.downcast_ref::<String>().unwrap();
        assert!(
            message.contains("did not stop within 50ms"),
            "unexpected failure: {message}"
        );
    });
}

#[test]
#[should_panic(expected = "dropped without shutdown")]
fn dropping_a_spawned_server_without_shutdown_fails() {
    shared_native(|scheduler| drop(ServerContext::new().spawn(scheduler)));
}

#[test]
#[should_panic(expected = "server failed")]
fn dropping_a_failed_spawned_server_reports_its_failure() {
    shared_native(|scheduler| drop(failing().spawn(scheduler)));
}

// While a test unwinds, the dropped server is still cancelled and joined, and the test's own panic is reported.
#[test]
#[should_panic(expected = "the test failed")]
fn a_spawned_server_dropped_during_a_panic_is_stopped() {
    shared_native(|scheduler| {
        let _server = ServerContext::new().spawn(scheduler);
        panic!("the test failed");
    });
}

#[test]
#[should_panic(expected = "the test failed")]
fn a_failed_spawned_server_dropped_during_a_panic_is_reported() {
    shared_native(|scheduler| {
        let _server = failing().spawn(scheduler);
        panic!("the test failed");
    });
}

// A server panic caused by the test is reported after the test's own failure.
#[test]
#[should_panic(expected = "the test failed first")]
fn a_test_panic_is_reported_before_a_spawned_server_panic() {
    ServerContext::new()
        .handler(Panicking)
        .run_spawned(async |context| {
            context
                .raw(|mut peer| {
                    peer.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
                    // The server's task ends when the handler panics, closing the connection.
                    let _ = peer.read(&mut [0; 1]);
                })
                .await;
            panic!("the test failed first");
        });
}
