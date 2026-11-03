// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A spawned server runs on the scheduler's workers beside a client driven from the test's thread, and reports each way it can fail, in order.
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, Status, body::EmptyBody,
    body::ReadableBody,
};
use base_http_fixtures::{Context, Hello, ServerContext, body::payload, protocol::App};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use base_scheduler_native::NativeScheduler;
use std::{convert::Infallible, io::Read};

fn failing() -> ServerContext {
    ServerContext::new().server_options(base_http_server::Options {
        max_connections: 0,
        ..Default::default()
    })
}

#[test]
fn a_spawned_server_serves_a_client_and_a_raw_peer() {
    ServerContext::new().run_spawned(async |context: &Context<'_, NativeScheduler, Hello>| {
        let _: &Hello = context.handler();
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
        let status = context
            .raw(|mut peer| {
                peer.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
                let (head, _) = peer.read_message();
                head.status()
            })
            .await;
        assert_eq!(status, 200);
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

// Sent without a client, so the test does not wait for a response.
async fn panic_the_server<H>(context: &Context<'_, NativeScheduler, H>) {
    context
        .raw(|mut peer| {
            peer.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
            // The server's task ends when the handler panics, closing the connection.
            let _ = peer.read(&mut [0; 1]);
        })
        .await;
}

#[test]
#[should_panic(expected = "the handler panicked")]
fn a_spawned_server_panic_is_resumed() {
    ServerContext::new()
        .handler(Panicking)
        .run_spawned(async |context| panic_the_server(context).await);
}

// A server panic caused by the test is reported after the test's own failure.
#[test]
#[should_panic(expected = "the test failed first")]
fn a_test_panic_is_reported_before_a_spawned_server_panic() {
    ServerContext::new()
        .handler(Panicking)
        .run_spawned(async |context| {
            panic_the_server(context).await;
            panic!("the test failed first");
        });
}
