// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Raw peers read and write HTTP/1 independently of Base, and the canned servers misbehave as documented.
use base_http::{
    Method, Request, Status,
    body::{EmptyBody, ReadableBody},
};
use base_http_fixtures::{
    ClientContext, Latch, RawPeer, RawServer, ServerContext,
    body::{self, payload},
    error_chain,
    protocol::{App, DUPLEX_BODY, DUPLEX_CHUNK, duplex_data},
    raw::{self, Head},
};
use base_scheduler::Cancellation;
use std::{
    io::{self, BufRead, Cursor, Read, Write},
    thread,
    time::Duration,
};

fn get() -> Request<EmptyBody> {
    Request::new(Method::GET, "/", EmptyBody)
}

#[test]
fn raw_peers_exchange_chunked_messages_with_trailers() {
    let server = RawServer::spawn(|listener| {
        let mut peer = listener.accept();
        let (head, body) = peer.read_message();
        assert_eq!(head.line, "POST /upload HTTP/1.1");
        assert!(!head.is_response());
        assert_eq!(head.values("x-repeat").collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(body.data, b"Hello World");
        assert_eq!(body.trailers, [("etag".to_owned(), "abcd".to_owned())]);
        peer.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;name=value\r\nHello\r\n6\r\n World\r\n");
        peer.close_write_acknowledged();
        peer.expect_closed();
        listener.expect_no_connection();
    });
    let mut peer = RawPeer::connect(server.address());
    peer.trickle(
        "POST /upload HTTP/1.1\r\nX-Repeat: a\r\nX-Repeat: b\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    peer.write("5\r\nHello\r\n6\r\n World\r\n0\r\netag: abcd\r\n\r\n");
    assert_eq!(peer.read_line(), "HTTP/1.1 200 OK");
    let head = Head {
        line: "HTTP/1.1 200 OK".to_owned(),
        fields: vec![(
            "Transfer-Encoding".to_owned(),
            peer.read_line()["Transfer-Encoding: ".len()..].to_owned(),
        )],
    };
    assert_eq!(peer.read_line(), "");
    assert_eq!(head.status(), 200);
    assert_eq!(head.get("missing"), None);
    assert_eq!(peer.read_chunked_prefix(11), b"Hello World");
    assert!(peer.fill_buf().unwrap().is_empty());
    peer.consume(0);
    assert_eq!(peer.read(&mut [0; 1]).unwrap(), 0);
    peer.get_mut().set_nodelay(true).unwrap();
    peer.shutdown_write();
    server.join();
}

#[test]
fn bodies_follow_their_framing() {
    let parse = |input: &str| {
        let mut input = Cursor::new(input.as_bytes().to_vec());
        let head = raw::read_head(&mut input);
        let body = raw::read_body(&mut input, &head);
        (head, body.data)
    };
    assert_eq!(parse("HTTP/1.1 204 No Content\r\n\r\nignored").1, b"");
    assert_eq!(
        parse("HTTP/1.0 200 OK\r\n\r\nuntil close").1,
        b"until close"
    );
    assert_eq!(parse("GET / HTTP/1.1\r\n\r\nignored").1, b"");
    assert_eq!(
        parse("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokay").1,
        b"ok"
    );
    let mut input = Cursor::new(b"4\r\nabcd\r\n".to_vec());
    assert_eq!(raw::read_chunked_prefix(&mut input, 4), b"abcd");
    assert_eq!(raw::read_line(&mut Cursor::new(b"line\r\n")), "line");
}

#[test]
#[should_panic(expected = "not a response head")]
fn a_request_head_has_no_status() {
    Head {
        line: "GET / HTTP/1.1".to_owned(),
        fields: Vec::new(),
    }
    .status();
}

#[test]
#[should_panic(expected = "unexpected input before close")]
fn unexpected_input_before_close_fails() {
    let server = RawServer::spawn(|listener| {
        listener.accept().write("surprise");
    });
    let mut peer = RawPeer::connect(server.address());
    server.join();
    peer.expect_closed();
}

#[test]
#[should_panic(expected = "the raw server failed")]
fn a_raw_server_panic_propagates_on_join() {
    RawServer::spawn(|_| panic!("the raw server failed")).join();
}

#[test]
fn a_truncated_body_fails_the_client_read() {
    let server = RawServer::truncate_body(10, b"short");
    ClientContext::new(server.endpoint()).run(async |client| {
        let mut response = client.call(get(), &Cancellation::never()).await.unwrap();
        let error = body::collect(&mut response.body, &Cancellation::never())
            .await
            .unwrap_err();
        assert_eq!(
            error_chain(&error),
            ["body failed", "truncated HTTP/1 message"]
        );
    });
    server.join();
}

#[test]
fn a_stalled_body_waits_for_its_release() {
    let release = Latch::new();
    let server = RawServer::stall_after_head(
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
        release.clone(),
    );
    ClientContext::new(server.endpoint()).run(async |client| {
        let mut response = client.call(get(), &Cancellation::never()).await.unwrap();
        assert_eq!(response.status, Status::OK);
        assert!(!response.body.is_ready());
        release.release();
        assert!(
            body::collect(&mut response.body, &Cancellation::never())
                .await
                .is_err()
        );
    });
    server.join();
}

#[test]
fn respond_then_close_serves_one_response() {
    let server = RawServer::respond_then_close("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    ClientContext::new(server.endpoint()).run(async |client| {
        let mut response = client.call(get(), &Cancellation::never()).await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"ok");
    });
    server.join();
}

// Input a peer buffered before the split is read through the reader, not lost.
#[test]
fn a_split_raw_peer_keeps_its_buffered_input() {
    let sent = Latch::new();
    let server = RawServer::spawn({
        let sent = sent.clone();
        move |listener| {
            let mut peer = listener.accept();
            peer.write("first\r\nsecond\r\n");
            peer.close_write_acknowledged();
            sent.release();
            peer.expect_closed();
        }
    });
    let mut peer = RawPeer::connect(server.address());
    // Both lines have arrived in one write, so reading the first buffers the second, and a reader which lost the buffer would read EOF.
    sent.wait_blocking();
    assert_eq!(peer.read_line(), "first");
    let (mut reader, mut writer) = peer.split();
    assert_eq!(raw::read_line(&mut reader), "second");
    assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
    writer.shutdown_write();
    server.join();
}

fn upload_head(length: u64) -> String {
    format!(
        "POST /duplex HTTP/1.1\r\nHost: localhost\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
    )
}

// The halves of a split peer upload and download at the same time from two threads. Done sequentially on one thread, the upload stalls once the unread echo fills the buffers, as `a_sequential_raw_upload_stalls_under_backpressure` shows.
#[test]
fn a_split_raw_peer_uploads_and_downloads_at_once() {
    ServerContext::new()
        .handler(App::new())
        .run(async |context| {
            context
                .raw(|peer| {
                    let (mut reader, mut writer) = peer.split();
                    writer.get_mut().set_nodelay(true).unwrap();
                    let uploading = thread::spawn(move || {
                        writer.write(upload_head(DUPLEX_BODY));
                        let mut offset = 0;
                        while offset < DUPLEX_BODY {
                            writer.write(duplex_data(offset, DUPLEX_CHUNK));
                            offset += DUPLEX_CHUNK as u64;
                        }
                        writer.shutdown_write();
                    });
                    let head = reader.read_head();
                    assert_eq!(head.status(), 200);
                    let body = reader.read_body(&head);
                    assert!(
                        body.data.len() as u64 == DUPLEX_BODY
                            && body.data == duplex_data(0, body.data.len()),
                        "the echoed stream"
                    );
                    // The server closes the connection after the exchange.
                    assert!(reader.fill_buf().unwrap().is_empty());
                    reader.consume(0);
                    assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
                    uploading.join().unwrap();
                })
                .await;
        });
}

// Writing the whole upload before reading the echo stalls: the server stops reading once its echo is not read, so the writes cannot finish. The declared upload is far more than any socket buffers, so it stalls whatever the host's buffer sizes.
#[test]
fn a_sequential_raw_upload_stalls_under_backpressure() {
    const DECLARED: u64 = 1 << 30;
    ServerContext::new()
        .handler(App::new())
        .run(async |context| {
            context
                .raw(|mut peer| {
                    peer.get_mut()
                        .set_write_timeout(Some(Duration::from_millis(500)))
                        .unwrap();
                    peer.write(upload_head(DECLARED));
                    let mut written = 0;
                    let error = loop {
                        assert!(written < DECLARED, "the upload did not stall");
                        if let Err(error) = peer
                            .get_mut()
                            .write_all(&duplex_data(written, DUPLEX_CHUNK))
                        {
                            break error;
                        }
                        written += DUPLEX_CHUNK as u64;
                    };
                    assert!(
                        matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ),
                        "unexpected failure: {error}"
                    );
                })
                .await;
        });
}
