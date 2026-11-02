// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Pooled connections the server closes or writes to while idle are retired before reuse, and the request runs on a fresh connection.
use super::{accept, head, payload, peer, remainder, run_endpoint};
use base_http::{Method, Request, RequestMetadata, body::EmptyBody};
use base_http_client::{Client, Endpoint, Options};
use base_scheduler::Cancellation;
use std::{
    io::{BufReader, Write},
    net::{SocketAddr, TcpStream},
    sync::mpsc,
    thread,
};

/// Serve `/first` on one connection, apply `idle` to it, then serve `/next` on a second connection. A connection `idle` returns must then have been closed by the client without further input.
fn serve(
    idle: fn(BufReader<TcpStream>) -> Option<BufReader<TcpStream>>,
) -> (SocketAddr, mpsc::Receiver<()>, thread::JoinHandle<()>) {
    let (idled, wait) = mpsc::channel();
    let (address, peer) = peer(move |listener| {
        let mut stream = accept(&listener);
        // Send each write at once: otherwise Nagle's algorithm can hold idle input until the client acknowledges the response, after it has probed.
        stream.get_ref().set_nodelay(true).unwrap();
        assert!(head(&mut stream).starts_with("GET /first HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
            .unwrap();
        let retired = idle(stream);
        // Loopback can deliver asynchronously (on macOS through the input thread), and a probe only observes what has arrived.
        thread::sleep(std::time::Duration::from_millis(20));
        idled.send(()).unwrap();
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET /next HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nnext")
            .unwrap();
        assert!(remainder(&mut stream).is_empty());
        if let Some(mut retired) = retired {
            assert!(remainder(&mut retired).is_empty());
        }
    });
    (address, wait, peer)
}

// Close the idle connection, as an idle keep-alive timeout does.
fn close(stream: BufReader<TcpStream>) -> Option<BufReader<TcpStream>> {
    drop(stream);
    None
}

// Send bytes outside any exchange, keeping the connection open for the client to retire.
fn unsolicited(mut stream: BufReader<TcpStream>) -> Option<BufReader<TcpStream>> {
    stream
        .get_mut()
        .write_all(b"HTTP/1.1 408 Request Timeout\r\n\r\n")
        .unwrap();
    Some(stream)
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

fn calls_after(idle: fn(BufReader<TcpStream>) -> Option<BufReader<TcpStream>>) {
    let (address, idled, peer) = serve(idle);
    run_endpoint(Endpoint::new(address), Options::default(), async |client| {
        first(&client).await;
        idled.recv().unwrap();
        let mut response = client
            .call(
                Request::new(Method::GET, "/next", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(payload(&mut response.body).await, b"next");
        client.begin_close();
    });
    peer.join().unwrap();
}

fn opens_after(idle: fn(BufReader<TcpStream>) -> Option<BufReader<TcpStream>>) {
    let (address, idled, peer) = serve(idle);
    run_endpoint(Endpoint::new(address), Options::default(), async |client| {
        first(&client).await;
        idled.recv().unwrap();
        let never = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.path = Some("/next".into());
        metadata.content_length = Some(0);
        let request = client.open_request(metadata, &never).await.unwrap();
        let mut response = request.finish(&never).await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"next");
        drop(response);
        client.begin_close();
    });
    peer.join().unwrap();
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
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        stream
            .get_mut()
            .shutdown(std::net::Shutdown::Write)
            .unwrap();
        assert!(head(&mut stream).starts_with("GET /first HTTP/1.1\r\n"));
        assert!(remainder(&mut stream).is_empty());
        listener.set_nonblocking(true).unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    });
    run_endpoint(Endpoint::new(address), Options::default(), async |client| {
        let response = client
            .call(
                Request::new(Method::GET, "/first", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(response.status, base_http::Status::SERVICE_UNAVAILABLE);
        drop(response);
        client.begin_close();
    });
    peer.join().unwrap();
}
