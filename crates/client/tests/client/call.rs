// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Tracked, accept, chain, head, payload, peer, remainder, run_endpoint, unconnected};
use base_http::{
    Method, Protocol, Request, Scheme,
    body::{BodyError, EmptyBody, ReadableBody},
};
use base_http_client::{Endpoint, Error, Options};
use base_scheduler::Cancellation;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[test]
fn rejected_calls_close_their_bodies_without_dialing() {
    unconnected(async |client| {
        let closed = Arc::new(AtomicUsize::new(0));
        let request = || Request::new(Method::GET, "/", Tracked(closed.clone()));
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let error = client.call(request(), &cancelled).await.err().unwrap();
        assert!(matches!(error, Error::Cancelled));
        assert_eq!(closed.load(Ordering::Relaxed), 1);

        let never = Cancellation::never();
        let connect = Request::new(Method::CONNECT, "/", Tracked(closed.clone()));
        let mut protocol = request();
        protocol.metadata.protocol = Some(Protocol::new("websocket").unwrap());
        let mut upgrade = request();
        upgrade.metadata.fields.add("upgrade", "websocket").unwrap();
        for tunnel in [connect, protocol, upgrade] {
            let error = client.call(tunnel, &never).await.err().unwrap();
            assert_eq!(
                chain(&error),
                ["unsupported HTTP client operation: managed client tunnels and upgrades"]
            );
        }
        assert_eq!(closed.load(Ordering::Relaxed), 4);

        let mut encrypted = request();
        encrypted.metadata.scheme = Some(Scheme::from("HTTPS"));
        let error = client.call(encrypted, &never).await.err().unwrap();
        assert_eq!(
            chain(&error),
            ["unsupported HTTP client operation: request scheme does not match the endpoint"]
        );
        assert_eq!(closed.load(Ordering::Relaxed), 5);
        assert_eq!(client.pool_stats().total(), 0);
    });
}

#[test]
fn calls_skip_interim_responses_and_report_response_framing() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET /fixed HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(
                b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfixed",
            )
            .unwrap();
        assert!(head(&mut stream).starts_with("GET /chunked HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\nchunked\r\n0\r\n\r\n",
            )
            .unwrap();
        assert!(remainder(&mut stream).is_empty());
    });
    run_endpoint(Endpoint::new(address), Options::default(), async |client| {
        let never = Cancellation::never();
        let mut request = Request::new(Method::GET, "/fixed", EmptyBody);
        // An explicit scheme matching the endpoint is accepted.
        request.metadata.scheme = Some(Scheme::from("HTTP"));
        let mut response = client.call(request, &never).await.unwrap();
        assert_eq!(response.metadata.status, base_http::Status::OK);
        assert_eq!(response.body.length(), Some(5));
        assert!(!response.body.may_have_trailers());
        assert_eq!(payload(&mut response.body).await, b"fixed");

        let mut response = client
            .call(Request::new(Method::GET, "/chunked", EmptyBody), &never)
            .await
            .unwrap();
        assert_eq!(response.body.length(), None);
        assert!(response.body.may_have_trailers());
        assert_eq!(payload(&mut response.body).await, b"chunked");
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn closing_a_response_body_abandons_its_connection() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\npart")
            .unwrap();
        // The unread remainder cannot be skipped, so the client disconnects.
        assert!(remainder(&mut stream).is_empty());
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET /next HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nnext")
            .unwrap();
        assert!(remainder(&mut stream).is_empty());
    });
    run_endpoint(Endpoint::new(address), Options::default(), async |client| {
        let never = Cancellation::never();
        let mut response = client
            .call(Request::new(Method::GET, "/", EmptyBody), &never)
            .await
            .unwrap();
        response.body.close(Some(BodyError::Abandoned));
        assert!(response.body.read(&never).await.is_err());
        let mut response = client
            .call(Request::new(Method::GET, "/next", EmptyBody), &never)
            .await
            .unwrap();
        assert_eq!(payload(&mut response.body).await, b"next");
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn truncated_response_bodies_report_the_protocol_failure() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\npart")
            .unwrap();
    });
    run_endpoint(Endpoint::new(address), Options::default(), async |client| {
        let never = Cancellation::never();
        let mut response = client
            .call(Request::new(Method::GET, "/", EmptyBody), &never)
            .await
            .unwrap();
        // A failure discards queued frames, so the prefix may not be observed.
        let error = loop {
            match response.body.read(&never).await {
                Ok(frame) => assert!(!frame.is_end()),
                Err(error) => break error,
            }
        };
        assert_eq!(chain(&error), ["body failed", "truncated HTTP/1 message"]);
        let source = std::error::Error::source(&error).unwrap();
        assert!(matches!(
            source.downcast_ref::<Error>(),
            Some(Error::Protocol(_))
        ));
        client.begin_close();
    });
    peer.join().unwrap();
}
