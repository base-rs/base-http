// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A server context serves, connects, bounds and tears down a test, and reports each way it can fail.
use base_http::{Method, Status, body::EmptyBody};
use base_http_fixtures::{
    ClientContext, Hello, Latch, RawServer, ServerContext, TEARDOWN_TIMEOUT, TEST_TIMEOUT,
    body::payload, native,
};
use base_scheduler::Cancellation;
use std::time::Duration;

#[test]
fn the_default_context_serves_hello_to_its_client_and_a_raw_peer() {
    let output = ServerContext::default()
        .server_options(base_http_server::Options::default())
        .client_options(base_http_client::Options::default())
        .timeout(TEST_TIMEOUT)
        .teardown_timeout(TEARDOWN_TIMEOUT)
        .run(async |context| {
            let _: &Hello = context.handler();
            let _ = context.scheduler();
            assert_eq!(context.endpoint().authority, context.address().to_string());
            assert_eq!(context.url(), format!("http://{}", context.address()));
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

            context
                .raw(|mut peer| {
                    peer.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
                    let (head, body) = peer.read_message();
                    assert_eq!(head.status(), 200);
                    assert_eq!(body.data, b"Hello World!");
                    42
                })
                .await
        });
    assert_eq!(output, 42);
}

#[test]
#[should_panic(expected = "the test failed")]
fn a_test_panic_propagates_after_teardown() {
    ServerContext::new().run(async |_| panic!("the test failed"));
}

#[test]
#[should_panic(expected = "test exceeded")]
fn a_test_exceeding_its_timeout_fails() {
    ServerContext::new()
        .timeout(Duration::from_millis(10))
        .run(async |_| {
            let _ = Latch::new().wait(&Cancellation::never()).await;
        });
}

// A test which returns an unfinished response body keeps its connection busy, so teardown cannot complete.
#[test]
#[should_panic(expected = "teardown exceeded")]
fn a_teardown_exceeding_its_timeout_is_forced_and_fails() {
    ServerContext::new()
        .teardown_timeout(Duration::from_millis(50))
        .run(async |context| {
            context
                .client()
                .call(
                    context.request(Method::GET, "/", EmptyBody),
                    &Cancellation::never(),
                )
                .await
                .unwrap()
        });
}

#[test]
#[should_panic(expected = "server failed")]
fn a_server_failure_fails_the_test() {
    ServerContext::new()
        .server_options(base_http_server::Options {
            max_connections: 0,
            ..Default::default()
        })
        .run(async |_| {});
}

#[test]
fn a_client_context_runs_against_a_raw_server() {
    let server = RawServer::respond_then_close("HTTP/1.1 204 No Content\r\n\r\n");
    let status = ClientContext::new(server.endpoint())
        .options(base_http_client::Options::default())
        .timeout(TEST_TIMEOUT)
        .teardown_timeout(TEARDOWN_TIMEOUT)
        .run(async |client| {
            let request = base_http::Request::new(Method::GET, "/", EmptyBody);
            client
                .call(request, &Cancellation::never())
                .await
                .unwrap()
                .status
        });
    assert_eq!(status, Status::NO_CONTENT);
    server.join();
}

#[test]
#[should_panic(expected = "scheduled work failed")]
fn native_shuts_down_before_propagating_a_panic() {
    native(|_| panic!("scheduled work failed"));
}

#[test]
#[should_panic(expected = "the raw peer failed")]
fn a_raw_peer_panic_propagates() {
    ServerContext::new().run(async |context| {
        context.raw(|_| panic!("the raw peer failed")).await;
    });
}

// The test's own failure is reported ahead of the server failure it may have caused.
#[test]
#[should_panic(expected = "the test failed first")]
fn a_test_panic_is_reported_before_a_server_failure() {
    ServerContext::new()
        .server_options(base_http_server::Options {
            max_connections: 0,
            ..Default::default()
        })
        .run(async |_| panic!("the test failed first"));
}
