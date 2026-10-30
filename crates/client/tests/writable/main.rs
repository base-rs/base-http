// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[path = "../support/mod.rs"]
mod support;

mod open;

use base_http::{
    Fields, Method, Request, RequestMetadata, Scheme,
    body::{BodyFrame, EmptyBody, ReadableBody, WritableBody},
};
use base_http_client::{Client, Endpoint, Error, Options};
use base_scheduler::Cancellation;
use base_scheduler_direct::DirectScheduler;
use futures_lite::future::{block_on, poll_once, zip};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::Duration,
};
use support::{chain, head, payload, peer, remainder, run_endpoint, unconnected};

fn accept(listener: &TcpListener) -> BufReader<TcpStream> {
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    BufReader::new(stream)
}
fn chunks(stream: &mut impl BufRead) -> (Vec<u8>, String) {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        stream.read_line(&mut line).unwrap();
        let size = usize::from_str_radix(line.trim(), 16).unwrap();
        if size == 0 {
            return (body, head(stream));
        }
        let start = body.len();
        body.resize(start + size, 0);
        stream.read_exact(&mut body[start..]).unwrap();
        let mut crlf = [0; 2];
        stream.read_exact(&mut crlf).unwrap();
        assert_eq!(crlf, *b"\r\n");
    }
}
fn run(address: SocketAddr, application: impl for<'scope> AsyncFnOnce(Client<'scope>)) {
    run_endpoint(Endpoint::new(address), Options::default(), application);
}

#[test]
fn sequential_writes_flush_without_reading_and_share_pool_with_call() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("POST /upload HTTP/1.1\r\n"));
        let (bytes, trailers) = chunks(&mut stream);
        assert_eq!(bytes, b"Hello World");
        assert!(trailers.contains("x-upload: done"));
        stream.get_mut().write_all(b"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\nx-result: done\r\n\r\n").unwrap();
        assert!(head(&mut stream).starts_with("GET /next HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
        assert!(head(&mut stream).starts_with("POST /fixed HTTP/1.1\r\n"));
        let mut body = [0; 3];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(body, *b"abc");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        assert!(head(&mut stream).starts_with("HEAD /head HTTP/1.1\r\n"));
        assert!(chunks(&mut stream).0.is_empty());
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n")
            .unwrap();
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut request = client.open(Method::POST, "/upload", &cancel).await.unwrap();
        request.write(b"Hello ", &cancel).await.unwrap();
        request.write(b"World", &cancel).await.unwrap();
        let mut fields = Fields::new();
        fields.add("x-upload", "done").unwrap();
        request
            .write_frame(BodyFrame::Headers { fields, end: true }, &cancel)
            .await
            .unwrap();
        let mut response = request.finish(&cancel).await.unwrap();
        assert_eq!(response.status.as_u16(), 200);
        assert!(
            matches!(response.read(&cancel).await.unwrap(), BodyFrame::Data { data, .. } if data == b"ok")
        );
        assert!(
            matches!(response.read(&cancel).await.unwrap(), BodyFrame::Headers { fields, end: true } if fields.contains("x-result"))
        );
        let response = client
            .call(Request::new(Method::GET, "/next", EmptyBody), &cancel)
            .await
            .unwrap();
        assert!(response.body.is_empty());
        drop(response);
        let mut metadata = RequestMetadata::new(Method::POST);
        metadata.path = Some("/fixed".into());
        metadata.content_length = Some(3);
        let mut request = client.open_request(metadata, &cancel).await.unwrap();
        request.write(b"abc", &cancel).await.unwrap();
        assert!(request.finish(&cancel).await.unwrap().body.is_empty());
        let request = client.open(Method::HEAD, "/head", &cancel).await.unwrap();
        let response = request.finish(&cancel).await.unwrap();
        assert_eq!(response.body.length(), Some(999));
        assert!(response.body.is_empty());
        drop(response);
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn early_empty_response_does_not_finish_upload_or_release_pool_use() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        let (body, _) = chunks(&mut stream);
        assert_eq!(body, vec![b'x'; 1024 * 1024]);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let (mut output, response) = client
            .open(Method::POST, "/", &cancel)
            .await
            .unwrap()
            .split();
        let response = response.await.unwrap();
        assert!(response.body.is_empty());
        drop(response);
        assert_eq!(client.pool_stats().uses, 1);
        output
            .write(vec![b'x'; 1024 * 1024], &cancel)
            .await
            .unwrap();
        output.finish(&cancel).await.unwrap();
        let response = client
            .call(Request::new(Method::GET, "/", EmptyBody), &cancel)
            .await
            .unwrap();
        assert!(response.body.is_empty());
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn partial_input_and_unfinished_output_both_retire_connections() {
    let (address, peer) = peer(|listener| {
        let mut first = accept(&listener);
        head(&mut first);
        let mut byte = [0];
        assert_eq!(first.read(&mut byte).unwrap(), 0);
        let mut second = accept(&listener);
        head(&mut second);
        chunks(&mut second);
        second
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\npartial")
            .unwrap();
        assert_eq!(second.read(&mut byte).unwrap(), 0);
        let mut third = accept(&listener);
        head(&mut third);
        third
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        drop(client.open(Method::POST, "/", &cancel).await.unwrap());
        let request = client.open(Method::POST, "/", &cancel).await.unwrap();
        let mut response = request.finish(&cancel).await.unwrap();
        assert!(matches!(
            response.read(&cancel).await.unwrap(),
            BodyFrame::Data { end: false, .. }
        ));
        drop(response);
        assert!(
            client
                .call(Request::new(Method::GET, "/", EmptyBody), &cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn fixed_length_errors_retire_instead_of_replaying_partial_writes() {
    for (bytes, length) in [(b"ab".as_slice(), 3), (b"abcd".as_slice(), 3)] {
        let (address, peer) = peer(move |listener| {
            let mut stream = accept(&listener);
            head(&mut stream);
            let mut actual = Vec::new();
            stream.read_to_end(&mut actual).unwrap();
            assert!(actual.len() <= 2);
            let mut stream = accept(&listener);
            head(&mut stream);
            stream
                .get_mut()
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .unwrap();
        });
        run(address, async |client| {
            let cancel = Cancellation::never();
            let mut metadata = RequestMetadata::new(Method::POST);
            metadata.path = Some("/".into());
            metadata.content_length = Some(length);
            let mut request = client.open_request(metadata, &cancel).await.unwrap();
            let result = request.write(bytes, &cancel).await;
            if result.is_ok() {
                assert!(request.finish(&cancel).await.is_err());
            } else {
                drop(request);
            }
            assert!(
                client
                    .call(Request::new(Method::GET, "/", EmptyBody), &cancel)
                    .await
                    .unwrap()
                    .body
                    .is_empty()
            );
            client.begin_close();
        });
        peer.join().unwrap();
    }
}

#[test]
fn request_schemes_match_the_endpoint_case_insensitively() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET / HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.scheme = Some(Scheme::from("HTTPS"));
        metadata.path = Some("/".into());
        assert!(matches!(
            client.open_request(metadata, &cancel).await,
            Err(Error::Unsupported(
                "request scheme does not match the endpoint"
            ))
        ));
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.scheme = Some(Scheme::from("HTTP"));
        metadata.path = Some("/".into());
        metadata.content_length = Some(0);
        let request = client.open_request(metadata, &cancel).await.unwrap();
        let mut response = request.finish(&cancel).await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"ok");
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn normal_close_write_finalizes_without_a_body_queue() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        assert_eq!(chunks(&mut stream).0, b"hello");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let (mut output, response) = client
            .open(Method::POST, "/", &cancel)
            .await
            .unwrap()
            .split();
        WritableBody::write(&mut output, b"hello".to_vec(), &cancel)
            .await
            .unwrap();
        WritableBody::close_write(&mut output, None);
        output.flush(&cancel).await.unwrap();
        output.flush(&cancel).await.unwrap();
        output.finish(&cancel).await.unwrap();
        let mut response = response.await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"ok");
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn deferred_close_survives_dropped_and_cancelled_completion_waits() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        assert_eq!(chunks(&mut stream).0, b"hello");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let (mut output, response) = client
            .open(Method::POST, "/", &cancel)
            .await
            .unwrap()
            .split();
        output.write(b"hello", &cancel).await.unwrap();
        output.close_write(None);
        // No supervisor poll occurs between enqueueing finalization and these
        // waits. Losing either wait must leave the final result observable.
        let mut flushing = Box::pin(output.flush(&cancel));
        assert!(poll_once(&mut flushing).await.is_none());
        drop(flushing);
        let stopped = Cancellation::new();
        let mut flushing = Box::pin(output.flush(&stopped));
        assert!(poll_once(&mut flushing).await.is_none());
        stopped.cancel();
        assert!(matches!(flushing.await, Err(Error::Cancelled)));
        output.flush(&cancel).await.unwrap();
        output.flush(&cancel).await.unwrap();
        assert_eq!(client.pool_stats().uses, 1);
        output.finish(&cancel).await.unwrap();
        assert!(response.await.unwrap().body.is_empty());
        assert_eq!(client.pool_stats().uses, 0);
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn deferred_close_preserves_framing_errors_and_retires_the_connection() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"x");
        let mut replacement = accept(&listener);
        assert!(head(&mut replacement).starts_with("GET /next HTTP/1.1\r\n"));
        replacement
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::POST);
        metadata.path = Some("/".into());
        metadata.content_length = Some(2);
        let (mut output, response) = client
            .open_request(metadata, &cancel)
            .await
            .unwrap()
            .split();
        output.write(b"x", &cancel).await.unwrap();
        output.close_write(None);
        assert!(matches!(
            output.flush(&cancel).await,
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            output.flush(&cancel).await,
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            output.finish(&cancel).await,
            Err(Error::Protocol(_))
        ));
        drop(response);
        assert!(
            client
                .open(Method::GET, "/next", &cancel)
                .await
                .unwrap()
                .finish(&cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn old_completed_handles_cannot_cancel_the_next_exchange() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("GET /first HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
        assert!(head(&mut stream).starts_with("POST /next HTTP/1.1\r\n"));
        assert_eq!(chunks(&mut stream).0, b"kept");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::new();
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.path = Some("/first".into());
        metadata.content_length = Some(0);
        let (mut old_output, pending) = client
            .open_request(metadata, &cancel)
            .await
            .unwrap()
            .split();
        old_output
            .write_frame(BodyFrame::End, &cancel)
            .await
            .unwrap();
        let old_response = pending.await.unwrap();
        assert!(old_response.body.is_empty());
        assert_eq!(client.pool_stats().uses, 0);
        let mut next = client
            .open(Method::POST, "/next", &Cancellation::never())
            .await
            .unwrap();
        assert_eq!(client.pool_stats().uses, 1);
        old_output.abort();
        cancel.cancel();
        drop(old_output);
        drop(old_response);
        next.write(b"kept", &Cancellation::never()).await.unwrap();
        assert!(
            next.finish(&Cancellation::never())
                .await
                .unwrap()
                .body
                .is_empty()
        );
        assert_eq!(client.pool_stats().uses, 0);
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn completed_adapters_release_observers_while_old_handles_remain_alive() {
    use std::sync::Arc;
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        for index in 0..6 {
            assert!(head(&mut stream).starts_with("GET / HTTP/1.1\r\n"));
            let response: &[u8] = if index % 2 == 0 {
                b"HTTP/1.1 204 No Content\r\n\r\n"
            } else {
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\nx-done: yes\r\n\r\n"
            };
            stream.get_mut().write_all(response).unwrap();
        }
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut old_handles = Vec::new();
        for index in 0..6 {
            let capture = Arc::new(());
            let weak = Arc::downgrade(&capture);
            let observer = move |_: base_http::ResponseMetadata| {
                drop(capture.clone());
                std::future::ready(Ok::<_, std::convert::Infallible>(()))
            };
            let mut metadata = RequestMetadata::new(Method::GET);
            metadata.path = Some("/".into());
            metadata.content_length = Some(0);
            let (mut output, pending) = client
                .open_request_with_interim(metadata, observer, &cancel)
                .await
                .unwrap()
                .split();
            output.write_frame(BodyFrame::End, &cancel).await.unwrap();
            let mut response = pending.await.unwrap();
            let expected: &[u8] = if index % 2 == 0 { b"" } else { b"ok" };
            assert_eq!(payload(&mut response.body).await, expected);
            assert!(
                weak.upgrade().is_none(),
                "cached storage retains no observer"
            );
            assert_eq!(client.pool_stats().uses, 0);
            old_handles.push((output, response));
        }
        // Idle caches must not keep completed uses alive during shutdown.
        client.begin_close();
        drop(old_handles);
    });
    peer.join().unwrap();
}

#[test]
fn aborting_output_wakes_a_response_blocked_in_an_interim_observer() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 103 Early Hints\r\n\r\n")
            .unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        let mut replacement = accept(&listener);
        head(&mut replacement);
        replacement
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let started = Arc::new(AtomicBool::new(false));
        let observer_started = started.clone();
        let cancel = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::POST);
        metadata.path = Some("/".into());
        let (mut output, pending) = client
            .open_request_with_interim(
                metadata,
                move |_: base_http::ResponseMetadata| {
                    observer_started.store(true, Ordering::SeqCst);
                    std::future::pending::<Result<(), std::convert::Infallible>>()
                },
                &cancel,
            )
            .await
            .unwrap()
            .split();
        let (response, ()) = zip(pending, async {
            std::future::poll_fn(|_| {
                if started.load(Ordering::SeqCst) {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            })
            .await;
            // The observer itself never completes. Only exchange cancellation
            // can wake the other direction and let the joined futures finish.
            output.abort();
            drop(output);
        })
        .await;
        assert!(matches!(response, Err(Error::Cancelled)));
        assert!(
            client
                .call(Request::get("/next"), &cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn cancellation_wakes_direct_input_and_retains_cleanup_accounting() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        chunks(&mut stream);
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::new();
        let (output, response) = client
            .open(Method::POST, "/", &cancel)
            .await
            .unwrap()
            .split();
        output.finish(&Cancellation::never()).await.unwrap();
        let mut waiting = Box::pin(response);
        assert!(poll_once(&mut waiting).await.is_none());
        cancel.cancel();
        assert!(matches!(waiting.await, Err(Error::Cancelled)));
        assert!(
            client
                .call(
                    Request::new(Method::GET, "/", EmptyBody),
                    &Cancellation::never()
                )
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn dropping_a_pending_direct_write_retires_the_connection() {
    let (ready, wait) = std::sync::mpsc::channel();
    let (address, peer) = peer(move |listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        wait.recv_timeout(Duration::from_secs(10)).unwrap();
        let mut actual = Vec::new();
        stream.read_to_end(&mut actual).unwrap();
        assert!(actual.len() < 16 * 1024 * 1024);
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async move |client| {
        let cancel = Cancellation::never();
        let mut request = client.open(Method::POST, "/", &cancel).await.unwrap();
        let mut writing = Box::pin(request.write(vec![0; 16 * 1024 * 1024], &cancel));
        assert!(poll_once(&mut writing).await.is_none());
        drop(writing);
        drop(request);
        ready.send(()).unwrap();
        assert!(
            client
                .call(Request::new(Method::GET, "/", EmptyBody), &cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn precancelled_writes_leave_output_usable() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        assert_eq!(chunks(&mut stream).0, b"ok");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut request = client.open(Method::POST, "/", &cancel).await.unwrap();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            request.write(b"discard", &cancelled).await,
            Err(Error::Cancelled)
        ));
        request.write(b"ok", &cancel).await.unwrap();
        assert!(request.finish(&cancel).await.unwrap().body.is_empty());
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn direct_writes_reach_peer_before_finish_and_observe_interims() {
    let (acknowledged, ack) = futures_channel::oneshot::channel();
    let (address, peer) = peer(move |listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        let mut chunk = [0; 10];
        stream.read_exact(&mut chunk).unwrap();
        assert_eq!(&chunk, b"5\r\nhello\r\n");
        acknowledged.send(()).unwrap();
        assert!(chunks(&mut stream).0.is_empty());
        stream
            .get_mut()
            .write_all(
                b"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n",
            )
            .unwrap();
    });
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let interims = observed.clone();
    run(address, async move |client| {
        let cancel = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::POST);
        metadata.path = Some("/".into());
        let mut request = client
            .open_request_with_interim(
                metadata,
                move |metadata: base_http::ResponseMetadata| {
                    interims.lock().unwrap().push(metadata.status.as_u16());
                    async { Ok::<_, std::convert::Infallible>(()) }
                },
                &cancel,
            )
            .await
            .unwrap();
        request.write(b"hello", &cancel).await.unwrap();
        ack.await.unwrap(); // The peer sees bytes before finalization or response reads.
        assert!(request.finish(&cancel).await.unwrap().body.is_empty());
        client.begin_close();
    });
    assert_eq!(*observed.lock().unwrap(), vec![103]);
    peer.join().unwrap();
}

#[test]
fn cancelled_and_dropped_open_waiters_do_not_abandon_active_exchange() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        assert_eq!(chunks(&mut stream).0, b"kept");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut request = client.open(Method::POST, "/active", &cancel).await.unwrap();
        let stopped = Cancellation::new();
        let mut waiting = Box::pin(client.open(Method::POST, "/cancelled", &stopped));
        assert!(poll_once(&mut waiting).await.is_none());
        assert_eq!(client.pool_stats().waiters, 1);
        stopped.cancel();
        assert!(matches!(waiting.await, Err(Error::Cancelled)));
        let mut waiting = Box::pin(client.open(Method::POST, "/dropped", &cancel));
        assert!(poll_once(&mut waiting).await.is_none());
        drop(waiting);
        assert_eq!(client.pool_stats().waiters, 0);
        assert_eq!(client.pool_stats().uses, 1);
        request.write(b"kept", &cancel).await.unwrap();
        assert!(request.finish(&cancel).await.unwrap().body.is_empty());
        assert!(
            client
                .call(Request::new(Method::GET, "/reused", EmptyBody), &cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

struct Echo;
impl base_http_server::Handler for Echo {
    type Error = std::convert::Infallible;
    type Body<B: ReadableBody> = B;
    async fn call<B: ReadableBody, I: base_http::InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut base_http::InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<base_http::Response<B>, Self::Error> {
        Ok(base_http::Response::new(
            base_http::Status::OK,
            request.body,
        ))
    }
}

// Both directions exceed socket/TLS buffers. The echo peer cannot consume the
// whole upload without concurrently making progress on response consumption.
fn native_echo(tls: Option<bool>) {
    use base_endpoint::{AddressEndpoint, BindEndpoint, BindOptions};
    use base_scheduler::{Clock, SocketLocalAddress};
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        scheduler
            .timeout(Duration::from_secs(15), async {
                let listener = AddressEndpoint::new("127.0.0.1:0".parse().unwrap())
                    .bind(&scheduler, BindOptions::default(), &Cancellation::never())
                    .await
                    .unwrap()
                    .remove(0);
                let address = scheduler.socket_local_address(&listener).unwrap();
                let endpoint = Endpoint::new(address);
                #[cfg(not(feature = "rustls"))]
                assert!(tls.is_none());
                #[cfg(feature = "rustls")]
                let (endpoint, server_config) = if let Some(tls12) = tls {
                    use rustls::{
                        ClientConfig, RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer,
                    };
                    use std::sync::Arc;
                    let identity =
                        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                    let cert = identity.cert.der().clone();
                    let mut roots = RootCertStore::empty();
                    roots.add(cert.clone()).unwrap();
                    let versions = if tls12 {
                        &[&rustls::version::TLS12][..]
                    } else {
                        &[&rustls::version::TLS13][..]
                    };
                    let client = ClientConfig::builder_with_protocol_versions(versions)
                        .with_root_certificates(roots)
                        .with_no_client_auth();
                    let server = ServerConfig::builder_with_protocol_versions(versions)
                        .with_no_client_auth()
                        .with_single_cert(
                            vec![cert],
                            PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
                        )
                        .unwrap();
                    (
                        endpoint.with_tls(Arc::new(client), "localhost"),
                        Some(Arc::new(server)),
                    )
                } else {
                    (endpoint, None)
                };
                let stop = Cancellation::new();
                let lifetime = Cancellation::new();
                let (client, driver) = Client::new(
                    &scheduler,
                    endpoint,
                    Options {
                        pool: base_pool::Options {
                            max_resources: 1,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
                let server = async {
                    #[cfg(feature = "rustls")]
                    if let Some(config) = server_config {
                        return base_http_server::serve_tls(
                            &scheduler,
                            listener,
                            config,
                            &Echo,
                            &Default::default(),
                            &stop,
                        )
                        .await;
                    }
                    base_http_server::serve(&scheduler, listener, &Echo, &Default::default(), &stop)
                        .await
                };
                let application = async {
                    for _ in 0..2 {
                        let (mut output, response) = client
                            .open(Method::POST, "/echo", &lifetime)
                            .await
                            .unwrap()
                            .split();
                        let upload = async {
                            for _ in 0..512 {
                                output.write(vec![b'x'; 16384], &lifetime).await.unwrap();
                            }
                            let mut fields = Fields::new();
                            fields.add("x-checksum", "done").unwrap();
                            output
                                .write_frame(BodyFrame::Headers { fields, end: true }, &lifetime)
                                .await
                                .unwrap();
                            output.finish(&lifetime).await.unwrap();
                        };
                        let download = async {
                            let mut response = response.await.unwrap();
                            assert_eq!(response.status, base_http::Status::OK);
                            let mut length = 0;
                            let mut trailers = false;
                            loop {
                                let frame = response.read(&lifetime).await.unwrap();
                                let end = frame.is_end();
                                match frame {
                                    BodyFrame::Data { data, .. } => {
                                        assert!(data.iter().all(|byte| *byte == b'x'));
                                        length += data.len();
                                    }
                                    BodyFrame::Headers { fields, .. } => {
                                        trailers = fields.contains("x-checksum");
                                    }
                                    BodyFrame::End => {}
                                }
                                if end {
                                    break;
                                }
                            }
                            assert_eq!(length, 8 * 1024 * 1024);
                            assert!(trailers);
                        };
                        zip(upload, download).await;
                    }
                    client.begin_close();
                };
                let client_work = async {
                    let (driven, ()) = zip(driver.run(&lifetime), application).await;
                    stop.cancel();
                    driven.unwrap();
                    assert_eq!(client.pool_stats().total(), 0);
                };
                let (served, ()) = zip(server, client_work).await;
                let stats = served.unwrap();
                assert_eq!(stats.connections, 1);
                assert_eq!(stats.requests, 2);
                assert_eq!(stats.failed_connections, 0);
            })
            .await
            .unwrap();
        scheduler.shutdown().await;
    });
}

#[test]
fn direct_http_split_echo_under_backpressure() {
    native_echo(None);
}
#[cfg(feature = "rustls")]
#[test]
fn direct_https_split_echo_under_backpressure() {
    for tls12 in [false, true] {
        native_echo(Some(tls12));
    }
}

fn paused_input_peer(stream: impl Read + Write, uploaded: futures_channel::oneshot::Sender<()>) {
    let mut stream = BufReader::new(stream);
    assert!(
        head(&mut stream)
            .to_ascii_lowercase()
            .contains("content-length: 8388608\r\n")
    );
    stream
        .get_mut()
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 65536\r\n\r\n")
        .unwrap();
    stream.get_mut().write_all(&vec![b'r'; 65536]).unwrap();
    stream.get_mut().flush().unwrap();
    let mut upload = vec![0; 8 * 1024 * 1024];
    stream.read_exact(&mut upload).unwrap();
    assert!(upload.iter().all(|byte| *byte == b'u'));
    uploaded.send(()).unwrap();
    let mut byte = [0];
    assert_eq!(stream.read(&mut byte).unwrap(), 0);
}
async fn paused_input_application(
    client: Client<'_>,
    complete: futures_channel::oneshot::Receiver<()>,
) {
    let cancel = Cancellation::never();
    let mut metadata = RequestMetadata::new(Method::POST);
    metadata.path = Some("/upload".into());
    metadata.content_length = Some(8 * 1024 * 1024);
    let (mut output, response) = client
        .open_request(metadata, &cancel)
        .await
        .unwrap()
        .split();
    let mut response = response.await.unwrap();
    // Neither polling nor dropping the response body is needed for upload.
    for _ in 0..512 {
        output.write(vec![b'u'; 16384], &cancel).await.unwrap();
    }
    output.finish(&cancel).await.unwrap();
    complete.await.unwrap();
    assert_eq!(payload(&mut response.body).await, vec![b'r'; 65536]);
    client.begin_close();
}
#[test]
fn direct_upload_progresses_while_response_consumption_is_paused() {
    let (uploaded, complete) = futures_channel::oneshot::channel();
    let (address, peer) = peer(move |listener| {
        let stream = accept(&listener).into_inner();
        paused_input_peer(stream, uploaded);
    });
    run(address, async move |client| {
        paused_input_application(client, complete).await
    });
    peer.join().unwrap();
}
#[cfg(feature = "rustls")]
#[test]
fn direct_https_upload_progresses_while_response_consumption_is_paused() {
    use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
    use std::sync::Arc;
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = identity.cert.der().clone();
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert],
            PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
        )
        .unwrap();
    let (uploaded, complete) = futures_channel::oneshot::channel();
    let (address, peer) = peer(move |listener| {
        let socket = accept(&listener).into_inner();
        let stream = rustls::StreamOwned::new(
            rustls::ServerConnection::new(Arc::new(server)).unwrap(),
            socket,
        );
        paused_input_peer(stream, uploaded);
    });
    run_endpoint(
        Endpoint::new(address).with_tls(Arc::new(client), "localhost"),
        Options::default(),
        async move |client| {
            paused_input_application(client, complete).await;
        },
    );
    peer.join().unwrap();
}

#[test]
fn dropping_a_cold_open_retires_construction_before_replacement() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        assert!(head(&mut stream).starts_with("POST /replacement HTTP/1.1\r\n"));
        assert!(chunks(&mut stream).0.is_empty());
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let mut opening = Box::pin(client.open(Method::POST, "/dropped", &cancel));
        // No connection work is polled in between this first poll and drop.
        assert!(poll_once(&mut opening).await.is_none());
        assert_eq!(client.pool_stats().building, 1);
        drop(opening);
        assert!(
            client
                .open(Method::POST, "/replacement", &cancel)
                .await
                .unwrap()
                .finish(&cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn producer_error_is_observable_and_retires_direct_output() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    run(address, async |client| {
        let cancel = Cancellation::never();
        let (mut output, response) = client
            .open(Method::POST, "/", &cancel)
            .await
            .unwrap()
            .split();
        output.close_write(Some(Error::Unsupported("producer failed")));
        assert!(matches!(
            output.flush(&cancel).await,
            Err(Error::Unsupported("producer failed"))
        ));
        assert!(matches!(
            output.finish(&cancel).await,
            Err(Error::Unsupported("producer failed"))
        ));
        assert!(matches!(response.await, Err(Error::Cancelled)));
        assert!(
            client
                .call(Request::new(Method::GET, "/", EmptyBody), &cancel)
                .await
                .unwrap()
                .body
                .is_empty()
        );
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn dropped_input_reads_preserve_decoder_state_but_cancelled_reads_retire() {
    for cancelled in [false, true] {
        let (proceed, wait) = std::sync::mpsc::channel();
        let (address, peer) = peer(move |listener| {
            let mut stream = accept(&listener);
            head(&mut stream);
            chunks(&mut stream);
            stream
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n")
                .unwrap();
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
            if cancelled {
                let mut byte = [0];
                assert_eq!(stream.read(&mut byte).unwrap(), 0);
                stream = accept(&listener);
            } else {
                stream.get_mut().write_all(b"ok").unwrap();
            }
            head(&mut stream);
            stream
                .get_mut()
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .unwrap();
        });
        run(address, async move |client| {
            let cancel = Cancellation::never();
            let request = client.open(Method::POST, "/", &cancel).await.unwrap();
            let mut response = request.finish(&cancel).await.unwrap();
            let stopped = Cancellation::new();
            stopped.cancel();
            assert!(matches!(
                response.read(&stopped).await,
                Err(Error::Cancelled)
            ));
            let reading = Cancellation::new();
            let mut pending = Box::pin(response.read(&reading));
            assert!(poll_once(&mut pending).await.is_none());
            if cancelled {
                reading.cancel();
                assert!(matches!(pending.await, Err(Error::Cancelled)));
                proceed.send(()).unwrap();
            } else {
                drop(pending);
                proceed.send(()).unwrap();
                assert_eq!(payload(&mut response.body).await, b"ok");
            }
            drop(response);
            assert!(
                client
                    .call(Request::new(Method::GET, "/next", EmptyBody), &cancel)
                    .await
                    .unwrap()
                    .body
                    .is_empty()
            );
            client.begin_close();
        });
        peer.join().unwrap();
    }
}

#[test]
fn completed_direct_response_releases_use_before_next_driver_poll() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        for _ in 0..3 {
            head(&mut stream);
            stream
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .unwrap();
        }
    });
    run(address, async |client| {
        for _ in 0..3 {
            let cancel = Cancellation::new();
            let mut metadata = RequestMetadata::new(Method::GET);
            metadata.path = Some("/".into());
            metadata.content_length = Some(0);
            let request = client.open_request(metadata, &cancel).await.unwrap();
            let mut response = request.finish(&cancel).await.unwrap();
            assert_eq!(payload(&mut response.body).await, b"ok");
            assert_eq!(client.pool_stats().uses, 0);
            // A completed request's cancellation no longer owns the idle session.
            cancel.cancel();
        }
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn established_http1_runs_without_polling_client_driver() {
    use std::future::{Future, poll_fn};

    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        for index in 0..5 {
            assert!(head(&mut stream).starts_with(&format!("GET /{index} HTTP/1.1\r\n")));
            // Send framing and payload together so the retained reader also
            // has to preserve its read-ahead buffer across body reads.
            stream.get_mut().write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\nx-result: done\r\n\r\n").unwrap();
        }
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        let lifetime = Cancellation::new();
        let watchdog = lifetime.clone();
        let (done, wait) = std::sync::mpsc::channel();
        let timer = thread::spawn(move || {
            if wait.recv_timeout(Duration::from_secs(10)).is_err() {
                watchdog.cancel();
            }
        });
        let (client, driver) = Client::new(
            &scheduler,
            Endpoint::new(address),
            Options {
                pool: base_pool::Options {
                    max_resources: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        let mut driven = Box::pin(driver.run(&lifetime));
        {
            // Only connection establishment and this queued warmup call are
            // driven by the supervisor.
            let mut warmup = Box::pin(async {
                let mut response = client
                    .call(Request::new(Method::GET, "/0", EmptyBody), &lifetime)
                    .await
                    .unwrap();
                assert_eq!(payload(&mut response.body).await, b"ok");
            });
            poll_fn(|cx| {
                assert!(driven.as_mut().poll(cx).is_pending());
                warmup.as_mut().poll(cx)
            })
            .await;
        }
        // Complete warmup's queued accounting before freezing the driver.
        assert!(poll_once(&mut driven).await.is_none());
        assert_eq!(client.pool_stats().uses, 0);
        for index in 1..5 {
            let mut metadata = RequestMetadata::new(Method::GET);
            metadata.path = Some(format!("/{index}").into());
            metadata.content_length = Some(0);
            let request = client.open_request(metadata, &lifetime).await.unwrap();
            let mut response = request.finish(&lifetime).await.unwrap();
            assert_eq!(payload(&mut response.body).await, b"ok");
            assert_eq!(client.pool_stats().uses, 0);
        }
        client.begin_close();
        driven.await.unwrap();
        assert_eq!(client.pool_stats().total(), 0);
        let _ = done.send(());
        timer.join().unwrap();
        scheduler.shutdown().await;
    });
    peer.join().unwrap();
}
