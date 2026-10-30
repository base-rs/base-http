// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[path = "../support/mod.rs"]
mod support;

mod call;
mod connection;
mod options;

use base_http::{
    Method, Request,
    body::{BodyFrame, BufferedBody, EmptyBody, ReadableBody},
};
use base_http_client::{Client, Endpoint, Options};
use base_scheduler::Cancellation;
use base_scheduler_direct::DirectScheduler;
use futures_lite::future::{block_on, zip};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::Duration,
};
use support::{chain, head, payload, peer, remainder, run_endpoint, unconnected};

fn accept(listener: &TcpListener) -> BufReader<TcpStream> {
    listener.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out accepting client"
                );
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    BufReader::new(stream)
}
fn run<'scope>(
    scheduler: &'scope DirectScheduler,
    address: SocketAddr,
    options: Options,
    application: impl AsyncFnOnce(Client<'scope>),
) {
    block_on(async {
        let (client, driver) = Client::new(scheduler, Endpoint::new(address), options).unwrap();
        let lifetime = Cancellation::new();
        let watchdog = lifetime.clone();
        let (done, wait) = std::sync::mpsc::channel::<()>();
        let timer = thread::spawn(move || {
            if wait.recv_timeout(Duration::from_secs(15)).is_err() {
                watchdog.cancel();
            }
        });
        let (result, ()) = zip(driver.run(&lifetime), application(client)).await;
        let _ = done.send(());
        timer.join().unwrap();
        result.unwrap();
        scheduler.shutdown().await;
    });
}

#[test]
fn post_then_get_reuses_one_connection_with_heterogeneous_bodies() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        let fields = head(&mut stream);
        assert!(fields.starts_with("POST /upload HTTP/1.1\r\n"), "{fields}");
        assert!(
            fields
                .to_ascii_lowercase()
                .contains("content-length: 11\r\n")
        );
        let mut body = [0; 11];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(&body, b"Hello World");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none")
            .unwrap();
        assert!(head(&mut stream).starts_with("GET /next HTTP/1.1\r\n"));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo")
            .unwrap();
        let mut eof = [0];
        assert_eq!(stream.read(&mut eof).unwrap(), 0);
    });
    // The borrowed bytes are local to this scope, rather than static storage.
    let message = String::from("Hello World");
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        let request = Request::new(
            Method::POST,
            "/upload",
            BufferedBody::from(message.as_bytes()),
        );
        let mut response = client.call(request, &Cancellation::never()).await.unwrap();
        assert_eq!(payload(&mut response.body).await, b"one");
        let mut response = client
            .call(
                Request::new(Method::GET, "/next", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(payload(&mut response.body).await, b"two");
        assert_eq!(client.pool_stats().live, 1);
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn response_finishes_before_upload_and_use_remains_charged() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        let fields = head(&mut stream);
        assert!(
            fields
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked")
        );
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            .unwrap();
        // Upload is only produced after the application consumes this response.
        let mut upload = Vec::new();
        loop {
            let mut length = String::new();
            stream.read_line(&mut length).unwrap();
            let length = usize::from_str_radix(length.trim(), 16).unwrap();
            if length == 0 {
                let mut end = String::new();
                stream.read_line(&mut end).unwrap();
                assert_eq!(end, "\r\n");
                break;
            }
            let mut bytes = vec![0; length + 2];
            stream.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes[length..], b"\r\n");
            upload.extend_from_slice(&bytes[..length]);
        }
        assert_eq!(upload, b"late upload");
        assert!(head(&mut stream).starts_with("GET /next "));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        let (body, mut producer) = base_http::body::BodyReader::bounded(64, 2);
        let mut response = client
            .call(
                Request::new(Method::POST, "/upload", body),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(payload(&mut response.body).await, b"OK");
        drop(response);
        assert_eq!(
            client.pool_stats().uses,
            1,
            "unfinished upload retains the active use"
        );
        producer
            .write(b"late upload".to_vec(), &Cancellation::never())
            .await
            .unwrap();
        producer.close_write(None);
        let mut response = client
            .call(
                Request::new(Method::GET, "/next", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert!(payload(&mut response.body).await.is_empty());
        assert_eq!(client.pool_stats().live, 1);
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn abandoned_partial_response_disconnects_and_replaces_connection() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\npartial")
            .unwrap();
        let mut eof = [0];
        assert_eq!(
            stream.read(&mut eof).unwrap(),
            0,
            "abandoned response must disconnect without draining"
        );
        let mut replacement = accept(&listener);
        assert!(head(&mut replacement).starts_with("GET /replacement "));
        replacement
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        let response = client
            .call(
                Request::new(Method::GET, "/partial", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        drop(response);
        let mut response = client
            .call(
                Request::new(Method::GET, "/replacement", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(payload(&mut response.body).await, b"OK");
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn interim_responses_and_trailers_preserve_order() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream.get_mut().write_all(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nDigest: example\r\n\r\n").unwrap();
        assert!(head(&mut stream).starts_with("GET /next "));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = observed.clone();
        let mut response = client
            .call_with_interim(
                Request::new(Method::GET, "/", EmptyBody),
                move |metadata: base_http::ResponseMetadata| {
                    observer.lock().unwrap().push(metadata.status);
                    async { Ok::<_, std::convert::Infallible>(()) }
                },
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[base_http::Status::CONTINUE, base_http::Status::EARLY_HINTS]
        );
        let mut headers = base_http::HttpHeaders::new(response.fields.clone());
        let mut bytes = Vec::new();
        loop {
            let frame = response.read(&Cancellation::never()).await.unwrap();
            let end = frame.is_end();
            match frame {
                BodyFrame::Data { data, .. } => bytes.extend_from_slice(&data),
                BodyFrame::Headers { fields, .. } => headers.append_trailers(fields).unwrap(),
                BodyFrame::End => (),
            }
            if end {
                break;
            }
        }
        assert_eq!(bytes, b"hello");
        let (_, trailers) = headers.into_parts();
        assert!(trailers.unwrap().contains("digest"));
        let mut response = client
            .call(
                Request::new(Method::GET, "/next", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert!(payload(&mut response.body).await.is_empty());
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn cancelling_a_waiting_call_disconnects_and_allows_replacement() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        let mut eof = [0];
        assert_eq!(stream.read(&mut eof).unwrap(), 0);
        let mut next = accept(&listener);
        head(&mut next);
        next.get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        // Keep the call pending while the driver establishes its connection.
        let cancelled = Cancellation::new();
        let work = client.call(Request::new(Method::GET, "/waiting", EmptyBody), &cancelled);
        let mut work = std::pin::pin!(work);
        assert!(
            futures_lite::future::poll_once(work.as_mut())
                .await
                .is_none()
        );
        // Let the driver establish the connection before cancellation.
        while client.pool_stats().uses == 0 {
            futures_lite::future::yield_now().await;
        }
        cancelled.cancel();
        assert!(matches!(
            work.await,
            Err(base_http_client::Error::Cancelled)
        ));
        let mut response = client
            .call(
                Request::new(Method::GET, "/next", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert!(payload(&mut response.body).await.is_empty());
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn cancelled_pool_waiter_does_not_cancel_occupied_connection() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nkept")
            .unwrap();
        assert!(head(&mut stream).starts_with("GET /third "));
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let mut options = Options::default();
    options.pool.max_resources = 1;
    run(&scheduler, address, options, async |client| {
        let mut response = client
            .call(
                Request::new(Method::GET, "/first", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        let cancelled = Cancellation::new();
        let mut waiting = std::pin::pin!(
            client.call(Request::new(Method::GET, "/second", EmptyBody), &cancelled)
        );
        assert!(
            futures_lite::future::poll_once(waiting.as_mut())
                .await
                .is_none()
        );
        while client.pool_stats().waiters == 0 {
            futures_lite::future::yield_now().await;
        }
        cancelled.cancel();
        assert!(matches!(
            waiting.await,
            Err(base_http_client::Error::Cancelled)
        ));
        assert_eq!(payload(&mut response.body).await, b"kept");
        let mut response = client
            .call(
                Request::new(Method::GET, "/third", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert!(payload(&mut response.body).await.is_empty());
        client.begin_close();
    });
    peer.join().unwrap();
}

struct Stalled;
impl ReadableBody for Stalled {
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    fn close(&mut self, _: Option<Self::Error>) {}
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        std::future::pending().await
    }
}
#[test]
fn lifetime_cancellation_stops_an_uncooperative_upload_and_joins_cleanup() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        let mut eof = [0];
        assert_eq!(stream.read(&mut eof).unwrap(), 0);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        let lifetime = Cancellation::new();
        let (client, driver) =
            Client::new(&scheduler, Endpoint::new(address), Options::default()).unwrap();
        let (result, ()) = zip(driver.run(&lifetime), async {
            let response = client
                .call(
                    Request::new(Method::POST, "/", Stalled),
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            drop(response);
            lifetime.cancel();
        })
        .await;
        result.unwrap();
        let stats = client.pool_stats();
        assert_eq!(
            (stats.building, stats.live, stats.retiring, stats.uses),
            (0, 0, 0, 0)
        );
        scheduler.shutdown().await;
    });
    peer.join().unwrap();
}

#[test]
fn upload_keeps_progressing_while_response_queue_is_full() {
    const SIZE: usize = 1024 * 1024;
    let (uploaded, confirmation) = futures_channel::oneshot::channel();
    let (address, peer) = peer(move |listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        let mut output = stream.get_ref().try_clone().unwrap();
        let sending = thread::spawn(move || {
            write!(output, "HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\n\r\n").unwrap();
            output.write_all(&vec![b'r'; SIZE]).unwrap();
        });
        let mut received = 0;
        loop {
            let mut line = String::new();
            stream.read_line(&mut line).unwrap();
            let size = usize::from_str_radix(line.trim(), 16).unwrap();
            if size == 0 {
                let mut line = String::new();
                stream.read_line(&mut line).unwrap();
                break;
            }
            let mut bytes = vec![0; size + 2];
            stream.read_exact(&mut bytes).unwrap();
            assert!(bytes[..size].iter().all(|byte| *byte == b'u'));
            received += size;
        }
        assert_eq!(received, SIZE);
        uploaded.send(()).unwrap();
        sending.join().unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let options = Options {
        response_buffer_frames: 1,
        ..Options::default()
    };
    run(&scheduler, address, options, async |client| {
        let (body, mut producer) = base_http::body::BodyReader::bounded(16 * 1024, 1);
        let mut response = client
            .call(
                Request::new(Method::POST, "/", body),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        for _ in 0..SIZE / (16 * 1024) {
            producer
                .write(vec![b'u'; 16 * 1024], &Cancellation::never())
                .await
                .unwrap();
        }
        producer.close_write(None);
        confirmation.await.unwrap();
        // The peer received the entire upload before response consumption began.
        let bytes = payload(&mut response.body).await;
        assert_eq!(bytes.len(), SIZE);
        assert!(bytes.iter().all(|byte| *byte == b'r'));
        client.begin_close();
    });
    peer.join().unwrap();
}

struct Tracked(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl ReadableBody for Tracked {
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    fn close(&mut self, _: Option<Self::Error>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        Ok(BodyFrame::End)
    }
}
#[test]
fn busy_and_closed_clients_close_rejected_bodies() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nkept")
            .unwrap();
        let mut eof = [0];
        assert_eq!(stream.read(&mut eof).unwrap(), 0);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let options = Options {
        max_requests: 1,
        ..Options::default()
    };
    run(&scheduler, address, options, async |client| {
        let mut response = client
            .call(
                Request::new(Method::GET, "/first", EmptyBody),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        let closed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        assert!(matches!(
            client
                .call(
                    Request::new(Method::GET, "/busy", Tracked(closed.clone())),
                    &Cancellation::never()
                )
                .await,
            Err(base_http_client::Error::Busy)
        ));
        assert_eq!(closed.load(std::sync::atomic::Ordering::Relaxed), 1);
        client.begin_close();
        assert!(matches!(
            client
                .call(
                    Request::new(Method::GET, "/closed", Tracked(closed.clone())),
                    &Cancellation::never()
                )
                .await,
            Err(base_http_client::Error::Closed)
        ));
        assert_eq!(closed.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(payload(&mut response.body).await, b"kept");
    });
    peer.join().unwrap();
}

#[test]
fn dropping_driver_wakes_queued_calls_and_rejects_new_submissions() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        let (client, driver) = Client::new(
            &scheduler,
            Endpoint::new("127.0.0.1:1".parse().unwrap()),
            Options::default(),
        )
        .unwrap();
        let never = Cancellation::never();
        let mut call =
            std::pin::pin!(client.call(Request::new(Method::GET, "/", EmptyBody), &never));
        assert!(
            futures_lite::future::poll_once(call.as_mut())
                .await
                .is_none()
        );
        drop(driver);
        assert!(matches!(call.await, Err(base_http_client::Error::Closed)));
        assert!(matches!(
            client
                .call(Request::new(Method::GET, "/", EmptyBody), &never)
                .await,
            Err(base_http_client::Error::Closed)
        ));
        scheduler.shutdown().await;
    });
}

#[test]
fn dropped_call_retires_an_exchange_waiting_for_final_metadata() {
    let (sent, started) = futures_channel::oneshot::channel();
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        head(&mut stream);
        sent.send(()).unwrap();
        let mut eof = [0];
        assert_eq!(stream.read(&mut eof).unwrap(), 0);
        let mut stream = accept(&listener);
        head(&mut stream);
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        let never = Cancellation::never();
        let mut call =
            Box::pin(client.call(Request::new(Method::GET, "/dropped", EmptyBody), &never));
        assert!(
            futures_lite::future::poll_once(call.as_mut())
                .await
                .is_none()
        );
        started.await.unwrap();
        drop(call);
        let mut response = client
            .call(Request::new(Method::GET, "/replacement", EmptyBody), &never)
            .await
            .unwrap();
        assert!(payload(&mut response.body).await.is_empty());
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn known_empty_responses_are_ready_and_reusable_without_body_reads() {
    let (address, peer) = peer(|listener| {
        let mut stream = accept(&listener);
        for _ in 0..2 {
            head(&mut stream);
            stream
                .get_mut()
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .unwrap();
        }
        let mut eof = [0];
        assert_eq!(stream.read(&mut eof).unwrap(), 0);
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let mut options = Options::default();
    options.pool.max_resources = 1;
    run(&scheduler, address, options, async |client| {
        for _ in 0..2 {
            let response = client
                .call(
                    Request::new(Method::GET, "/", EmptyBody),
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            assert!(response.body.is_empty());
            assert!(response.body.is_ready());
            drop(response);
        }
        client.begin_close();
    });
    peer.join().unwrap();
}

#[test]
fn failed_construction_releases_capacity_and_preserves_client_admission() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let mut options = Options::default();
    options.pool.max_resources = 1;
    // TCP destination port zero cannot designate a listening endpoint.
    run(
        &scheduler,
        "127.0.0.1:0".parse().unwrap(),
        options,
        async |client| {
            for _ in 0..2 {
                let closed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                assert!(
                    client
                        .call(
                            Request::new(Method::GET, "/", Tracked(closed.clone())),
                            &Cancellation::never()
                        )
                        .await
                        .is_err()
                );
                assert_eq!(closed.load(std::sync::atomic::Ordering::Relaxed), 1);
                let stats = client.pool_stats();
                assert_eq!(
                    (stats.building, stats.live, stats.retiring, stats.uses),
                    (0, 0, 0, 0)
                );
                assert!(!stats.closed);
            }
            client.begin_close();
        },
    );
}

#[test]
fn failed_connection_reports_connect_with_the_address() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    // TCP destination port zero cannot designate a listening endpoint.
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    run(&scheduler, address, Options::default(), async |client| {
        let result = client
            .call(
                Request::new(Method::GET, "/", EmptyBody),
                &Cancellation::never(),
            )
            .await;
        match result {
            Err(base_http_client::Error::Connect {
                address: failed, ..
            }) => assert_eq!(failed, address),
            Err(error) => panic!("expected a connect error, got {error:?}"),
            Ok(_) => panic!("expected a connect error"),
        }
        assert_eq!(client.pool_stats().total(), 0);
        client.begin_close();
    });
}
