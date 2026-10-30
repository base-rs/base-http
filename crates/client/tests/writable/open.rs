// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{accept, chain, head, payload, peer, remainder, run, run_endpoint, unconnected};
use base_http::{
    Method, RequestMetadata,
    body::{BodyFrame, ReadableBody, WritableBody},
};
use base_http_client::{Endpoint, Error, Options};
use base_scheduler::Cancellation;
use std::io::{Read, Write};

fn empty_get(path: &str) -> RequestMetadata {
    let mut metadata = RequestMetadata::new(Method::GET);
    metadata.path = Some(path.into());
    metadata.content_length = Some(0);
    metadata
}

#[test]
fn rejected_opens_do_not_dial() {
    unconnected(async |client| {
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let error = client
            .open(Method::GET, "/", &cancelled)
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::Cancelled));
        let never = Cancellation::never();
        let error = client
            .open(Method::CONNECT, "/", &never)
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            Error::Unsupported("managed client tunnels and upgrades")
        ));
        assert_eq!(client.pool_stats().total(), 0);
    });
}

#[test]
fn opens_beyond_the_request_limit_or_after_closing_are_rejected() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET /first HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
            .unwrap();
        assert!(remainder(&mut stream).is_empty());
    });
    let options = Options {
        max_requests: 1,
        ..Default::default()
    };
    run_endpoint(Endpoint::new(address), options, async |client| {
        let never = Cancellation::never();
        let first = client
            .open_request(empty_get("/first"), &never)
            .await
            .unwrap();
        let error = client
            .open(Method::GET, "/busy", &never)
            .await
            .err()
            .unwrap();
        assert_eq!(chain(&error), ["HTTP client request limit reached"]);
        client.begin_close();
        let error = client
            .open(Method::GET, "/closed", &never)
            .await
            .err()
            .unwrap();
        assert_eq!(chain(&error), ["HTTP client is closed"]);
        let mut response = first.finish(&never).await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"first");
    });
    peer.join().unwrap();
}

#[test]
fn invalid_request_heads_fail_open_and_retire_the_connection() {
    let (address, peer) = peer(|listener| {
        // Planning fails before any request bytes are written.
        let mut stream = accept(&listener);
        assert!(remainder(&mut stream).is_empty());
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET /valid HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
        assert!(remainder(&mut stream).is_empty());
    });
    run(address, async |client| {
        let never = Cancellation::never();
        let mut metadata = empty_get("/invalid");
        metadata.fields.add("host", "elsewhere.example").unwrap();
        let error = client.open_request(metadata, &never).await.err().unwrap();
        assert!(matches!(error, Error::Protocol(_)));
        assert_eq!(
            chain(&error),
            ["invalid HTTP/1 message: authority differs from Host"]
        );
        let request = client
            .open_request(empty_get("/valid"), &never)
            .await
            .unwrap();
        let response = request.finish(&never).await.unwrap();
        assert_eq!(response.metadata.status, base_http::Status::NO_CONTENT);
        client.begin_close();
    });
    peer.join().unwrap();
}

// Exercise the writer only through the generic body interface, as a body forwarder would.
async fn flush_then_end<W: WritableBody<Data = Vec<u8>>>(
    writer: &mut W,
    cancellation: &Cancellation,
) where
    W::Error: std::fmt::Debug,
{
    writer.flush(cancellation).await.unwrap();
    writer
        .write_frame(BodyFrame::End, cancellation)
        .await
        .unwrap();
}

#[test]
fn request_output_flushes_and_rejects_writes_after_its_final_frame() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        let fields = head(&mut stream).to_ascii_lowercase();
        assert!(fields.starts_with("post /upload http/1.1\r\n"));
        assert!(fields.contains("transfer-encoding: chunked\r\n"));
        let mut body = [0; 14];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(&body, b"4\r\ndata\r\n0\r\n\r\n");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone")
            .unwrap();
        assert!(remainder(&mut stream).is_empty());
    });
    run(address, async |client| {
        let never = Cancellation::never();
        let mut request = client.open(Method::POST, "/upload", &never).await.unwrap();
        request.write(b"data", &never).await.unwrap();
        request.flush(&never).await.unwrap();
        let (mut writer, response) = request.split();
        flush_then_end(&mut writer, &never).await;
        let error = writer.write(b"late", &never).await.unwrap_err();
        assert_eq!(chain(&error), ["HTTP client is closed"]);
        // Closing ended output neither fails nor cancels the exchange.
        writer.close_write(Some(Error::Busy));
        writer.flush(&never).await.unwrap();
        writer.finish(&never).await.unwrap();
        let mut response = response.await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"done");
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn aborted_request_output_cancels_the_exchange() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("POST /aborted HTTP/1.1\r\n"));
        // The client disconnects without final chunked framing.
        assert!(remainder(&mut stream).is_empty());
    });
    run(address, async |client| {
        let never = Cancellation::never();
        let request = client.open(Method::POST, "/aborted", &never).await.unwrap();
        let (mut writer, response) = request.split();
        WritableBody::abort(&mut writer);
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            writer.flush(&cancelled).await,
            Err(Error::Cancelled)
        ));
        assert!(matches!(writer.flush(&never).await, Err(Error::Cancelled)));
        // Aborted output is already ended, so a later failure is not recorded.
        writer.close_write(Some(Error::Busy));
        assert!(matches!(writer.flush(&never).await, Err(Error::Cancelled)));
        assert!(matches!(response.await, Err(Error::Cancelled)));
        drop(writer);
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn reads_after_a_failed_response_read_report_closed() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\npart")
            .unwrap();
    });
    run(address, async |client| {
        let never = Cancellation::never();
        let request = client.open_request(empty_get("/"), &never).await.unwrap();
        let mut response = request.finish(&never).await.unwrap();
        let mut received = Vec::new();
        let error = loop {
            match response.body.read(&never).await {
                Ok(frame) => {
                    assert!(!frame.is_end());
                    if let BodyFrame::Data { data, .. } = frame {
                        received.extend_from_slice(&data);
                    }
                }
                Err(error) => break error,
            }
        };
        assert_eq!(received, b"part");
        assert_eq!(chain(&error), ["truncated HTTP/1 message"]);
        let error = response.body.read(&never).await.unwrap_err();
        assert_eq!(chain(&error), ["HTTP client is closed"]);
        client.begin_close();
    });
    peer.join().unwrap();
}
