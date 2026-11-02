// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Pooled connections the server closes or writes to while idle are retired before reuse, and the request runs on a fresh connection.
use base_http::{Method, Request, RequestMetadata, Status, body::EmptyBody};
use base_http_client::{Client, Options};
use base_http_fixtures::{ClientContext, Latch, RawServer, body::payload};
use base_scheduler::Cancellation;

const FIRST: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst";
const NEXT: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nnext";

// One pooled connection, so the second request must either reuse the idle connection or retire it first.
fn client(server: &RawServer) -> ClientContext {
    ClientContext::new(server.endpoint()).options(Options {
        pool: base_pool::Options {
            max_resources: 1,
            ..Default::default()
        },
        ..Default::default()
    })
}

// A raw server which closes the first connection while it is idle, as an idle keep-alive timeout does.
fn close(idled: Latch) -> RawServer {
    RawServer::close_while_idle(FIRST, NEXT, idled)
}

// A raw server which writes outside any exchange, keeping the connection open for the client to retire.
fn unsolicited(idled: Latch) -> RawServer {
    RawServer::write_while_idle(FIRST, "HTTP/1.1 408 Request Timeout\r\n\r\n", NEXT, idled)
}

async fn first(client: &Client<'_>) {
    let mut response = client
        .call(
            Request::new(Method::GET, "/first", EmptyBody),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(payload(&mut response.body).await, b"first");
}

fn calls_after(serve: fn(Latch) -> RawServer) {
    let idled = Latch::new();
    let server = serve(idled.clone());
    client(&server).run(async |client| {
        first(client).await;
        idled.wait(&Cancellation::never()).await.unwrap();
        let mut response = client
            .call(
                Request::new(Method::GET, "/next", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(payload(&mut response.body).await, b"next");
    });
    server.join();
}

fn opens_after(serve: fn(Latch) -> RawServer) {
    let idled = Latch::new();
    let server = serve(idled.clone());
    client(&server).run(async |client| {
        first(client).await;
        idled.wait(&Cancellation::never()).await.unwrap();
        let never = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.path = Some("/next".into());
        metadata.content_length = Some(0);
        let request = client.open_request(metadata, &never).await.unwrap();
        let mut response = request.finish(&never).await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"next");
    });
    server.join();
}

#[test]
fn a_call_after_the_server_closes_an_idle_connection_uses_a_fresh_one() {
    calls_after(close);
}

#[test]
fn an_open_after_the_server_closes_an_idle_connection_uses_a_fresh_one() {
    opens_after(close);
}

#[test]
fn a_call_after_unsolicited_idle_input_uses_a_fresh_connection() {
    calls_after(unsolicited);
}

#[test]
fn an_open_after_unsolicited_idle_input_uses_a_fresh_connection() {
    opens_after(unsolicited);
}

// A new connection is used without probing, so a server that responds and
// closes on accept costs one connection rather than a replacement per probe.
#[test]
fn a_new_connection_is_used_without_probing() {
    let server = RawServer::spawn(|listener| {
        let mut peer = listener.accept();
        peer.write(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        peer.shutdown_write();
        assert_eq!(peer.read_head().line, "GET /first HTTP/1.1");
        peer.expect_closed();
        listener.expect_no_connection();
    });
    client(&server).run(async |client| {
        let response = client
            .call(
                Request::new(Method::GET, "/first", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(response.status, Status::SERVICE_UNAVAILABLE);
    });
    server.join();
}
