// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    HttpHeaders, InterimHandler, InterimResponses, Method, Request, RequestMetadata, Response,
    ResponseMetadata, Status,
    body::{BodyFrame, BodyReader, BufferedBody, ReadableBody},
};
use base_http_server::{Error, Handler};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::{
    io::{self, Read, Write},
    net::Shutdown,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

mod support;
use support::*;

#[derive(Default)]
struct Collect {
    seen: Arc<Mutex<Vec<(RequestMetadata, HttpHeaders)>>>,
}
impl Handler for Collect {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        mut request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        if request.fields.contains("expect") {
            interim
                .send_interim_response(ResponseMetadata::new(Status::CONTINUE), cancellation)
                .await
                .map_err(io::Error::other)?;
        }
        if request.path.as_deref() == Some("/hints") {
            let mut hint = ResponseMetadata::new(Status::EARLY_HINTS);
            hint.fields
                .add("Link", "</style.css>; rel=preload")
                .unwrap();
            interim
                .send_interim_response(hint, cancellation)
                .await
                .map_err(io::Error::other)?;
        }
        let mut headers = HttpHeaders::new(request.fields.clone());
        let mut data = Vec::new();
        loop {
            let frame = request.read(cancellation).await.map_err(io::Error::other)?;
            let end = frame.is_end();
            match frame {
                BodyFrame::Data { data: chunk, .. } => data.extend_from_slice(chunk.as_ref()),
                BodyFrame::Headers { fields, .. } => {
                    headers.append_trailers(fields).map_err(io::Error::other)?
                }
                BodyFrame::End => {}
            }
            if end {
                break;
            }
        }
        if data.is_empty() {
            data.extend_from_slice(b"hello");
        }
        let status = match request.path.as_deref() {
            Some("/204") => Status::NO_CONTENT,
            Some("/304") => Status::NOT_MODIFIED,
            _ => Status::OK,
        };
        self.seen.lock().unwrap().push((request.metadata, headers));
        let mut response = Response::new(status, BufferedBody::from(data));
        if status == Status::OK {
            response.fields.add("Set-Cookie", "a=1").unwrap();
            response.fields.add("Set-Cookie", "b=2").unwrap();
        }
        Ok(response)
    }
}

#[test]
fn fixed_bodies_and_pipelining_preserve_boundaries() {
    let (served, ()) = connection(Collect::default(), |mut peer, _| {
        peer.get_mut().write_all(b"POST /one HTTP/1.1\r\nHost: example\r\nContent-Length: 5\r\n\r\nfirstGET /two HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n").unwrap();
        let first = head(&mut peer);
        assert_eq!(first.status, 200);
        assert_eq!(
            first
                .fields
                .iter()
                .filter(|(n, _)| n.eq_ignore_ascii_case("set-cookie"))
                .count(),
            2
        );
        assert_eq!(body(&mut peer, &first).0, b"first");
        let second = head(&mut peer);
        assert_eq!(body(&mut peer, &second).0, b"hello");
        assert_eq!(second.get("connection"), Some("close"));
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert_eq!(served.unwrap(), 2);
}

#[test]
fn fragmented_chunked_upload_delivers_trailers_in_message_fields() {
    let handler = Collect::default();
    let seen = handler.seen.clone();
    let (served, ()) = connection_options(
        handler,
        base_http1::Options {
            read_size: 1,
            ..Default::default()
        },
        |mut peer, _| {
            let wire = b"POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\nTrailer: x-checksum\r\nConnection: close\r\n\r\n3; foo=\"a;b\"\r\nabc\r\n2\r\nde\r\n0\r\nx-checksum: yes\r\n\r\n";
            for byte in wire {
                peer.get_mut().write_all(&[*byte]).unwrap();
            }
            let response = head(&mut peer);
            assert_eq!(body(&mut peer, &response).0, b"abcde");
        },
    );
    assert_eq!(served.unwrap(), 1);
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].1.trailers().len(), 1);
    assert_eq!(seen[0].1.trailers()[0].name(), "x-checksum");
    assert!(seen[0].1.headers().iter().all(|f| f.name() != "x-checksum"));
}

#[test]
fn interim_continue_is_flushed_before_upload_and_hints_precede_final() {
    let (served, ()) = connection(Collect::default(), |mut peer, _| {
        peer.get_mut().write_all(b"POST /hints HTTP/1.1\r\nHost: example\r\nExpect: 100-continue\r\nContent-Length: 4\r\nConnection: close\r\n\r\n").unwrap();
        assert_eq!(head(&mut peer).status, 100);
        let hints = head(&mut peer);
        assert_eq!(hints.status, 103);
        assert!(hints.get("link").is_some());
        peer.get_mut().write_all(b"data").unwrap();
        let final_head = head(&mut peer);
        assert_eq!(final_head.status, 200);
        assert_eq!(body(&mut peer, &final_head).0, b"data");
    });
    assert_eq!(served.unwrap(), 1);
}

#[test]
fn head_and_bodyless_statuses_do_not_emit_representation_bytes() {
    let (served, ()) = connection(Collect::default(), |mut peer, _| {
        peer.get_mut().write_all(b"HEAD / HTTP/1.1\r\nHost: example\r\n\r\nGET /204 HTTP/1.1\r\nHost: example\r\n\r\nGET /304 HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n").unwrap();
        let first = head(&mut peer);
        assert_eq!(first.get("content-length"), Some("5"));
        let second = head(&mut peer);
        assert_eq!(second.status, 204);
        assert_eq!(second.get("content-length"), None);
        let third = head(&mut peer);
        assert_eq!(third.status, 304);
        assert_eq!(third.get("content-length"), Some("5"));
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert_eq!(served.unwrap(), 3);
}

struct Echo;
impl Handler for Echo {
    type Error = io::Error;
    type Body<B: ReadableBody> = B;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<B>, io::Error> {
        let status = if request.method == Method::CONNECT {
            Status::CREATED
        } else if request.protocol.is_some() {
            Status::SWITCHING_PROTOCOLS
        } else {
            Status::OK
        };
        let mut response = Response::new(status, request.body);
        response.protocol = request.metadata.protocol;
        Ok(response)
    }
}

#[test]
fn pull_echo_streams_before_upload_completion_and_forwards_trailers() {
    let (served, ()) = connection(Echo, |mut peer, _| {
        peer.get_mut().write_all(b"POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n").unwrap();
        let response = head(&mut peer);
        assert_eq!(response.get("transfer-encoding"), Some("chunked"));
        assert_eq!(chunked_prefix(&mut peer, 3), b"one");
        // The first response chunk arrived while the request remained open.
        peer.get_mut()
            .write_all(b"3\r\ntwo\r\n0\r\nx-result: complete\r\n\r\n")
            .unwrap();
        let remaining = body(&mut peer, &response);
        assert_eq!(remaining.0, b"two");
        assert_eq!(remaining.1, ["x-result: complete"]);
        assert_ne!(response.get("connection"), Some("close"));
        peer.get_mut()
            .write_all(b"GET /again HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
            .unwrap();
        let second = head(&mut peer);
        assert_eq!(second.status, 200);
        assert_eq!(body(&mut peer, &second).0, b"");
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert_eq!(served.unwrap(), 2);
}

fn tunnel(request: &'static [u8], status: u16) {
    let (served, ()) = connection(Echo, move |mut peer, _| {
        // Input coalesced with the handshake must survive parser read-ahead.
        let mut bytes = request.to_vec();
        bytes.extend_from_slice(b"early");
        peer.get_mut().write_all(&bytes).unwrap();
        let response = head(&mut peer);
        assert_eq!(response.status, status);
        assert_eq!(response.get("content-length"), None);
        assert_eq!(response.get("transfer-encoding"), None);
        let mut early = [0; 5];
        peer.read_exact(&mut early).unwrap();
        assert_eq!(&early, b"early");
        for bytes in [
            b"one".as_slice(),
            b"two",
            b"GET /still-raw HTTP/1.1\r\n\r\n",
        ] {
            peer.get_mut().write_all(bytes).unwrap();
            let mut reply = vec![0; bytes.len()];
            peer.read_exact(&mut reply).unwrap();
            assert_eq!(reply, bytes);
        }
        peer.get_mut().shutdown(Shutdown::Write).unwrap();
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert_eq!(served.unwrap(), 1);
}

#[test]
fn connect_accepts_non_200_success_and_keeps_managed_duplex_body() {
    tunnel(
        b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n",
        201,
    );
}

#[test]
fn upgrade_keeps_managed_duplex_body_and_buffered_input() {
    tunnel(
        b"GET / HTTP/1.1\r\nHost: example\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\n",
        101,
    );
}

struct Producer {
    body: Mutex<Option<BodyReader>>,
}
impl Handler for Producer {
    type Error = io::Error;
    type Body<B: ReadableBody> = BodyReader;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BodyReader>, io::Error> {
        Ok(Response::new(
            Status::OK,
            self.body.lock().unwrap().take().unwrap(),
        ))
    }
}

#[test]
fn imperative_producer_streams_unknown_length_chunks_without_driver_tasks() {
    let (reader, mut writer) = BodyReader::bounded(3, 1);
    let producer = std::thread::spawn(move || {
        block_on(async {
            writer
                .write(b"one".to_vec(), &Cancellation::never())
                .await
                .unwrap();
            writer
                .write(b"two".to_vec(), &Cancellation::never())
                .await
                .unwrap();
            writer.close_write(None);
        })
    });
    let (served, ()) = connection(
        Producer {
            body: Mutex::new(Some(reader)),
        },
        |mut peer, _| {
            peer.get_mut()
                .write_all(b"GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
                .unwrap();
            let response = head(&mut peer);
            assert_eq!(response.get("transfer-encoding"), Some("chunked"));
            assert_eq!(body(&mut peer, &response).0, b"onetwo");
        },
    );
    producer.join().unwrap();
    assert_eq!(served.unwrap(), 1);
}

#[test]
fn http10_unknown_body_is_close_delimited() {
    let (reader, mut writer) = BodyReader::bounded(8, 1);
    block_on(writer.write(b"data".to_vec(), &Cancellation::never())).unwrap();
    writer.close_write(None);
    let (served, ()) = connection(
        Producer {
            body: Mutex::new(Some(reader)),
        },
        |mut peer, _| {
            peer.get_mut().write_all(b"GET / HTTP/1.0\r\n\r\n").unwrap();
            let response = head(&mut peer);
            assert_eq!(response.get("transfer-encoding"), None);
            assert_eq!(response.get("content-length"), None);
            assert_eq!(body(&mut peer, &response).0, b"data");
        },
    );
    assert_eq!(served.unwrap(), 1);
}

#[test]
fn ambiguous_or_invalid_heads_are_rejected_without_admitting_a_handler() {
    for wire in [
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1,1\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n",
        "GET / HTTP/1.1\r\n\r\n",
        "GET / HTTP/1.1\nHost: x\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\n Folded: bad\r\n\r\n",
        "GET / HTTP/1.1\r\nHost : x\r\n\r\n",
    ] {
        let handler = Collect::default();
        let seen = handler.seen.clone();
        let (served, ()) = connection(handler, move |mut peer, _| {
            peer.get_mut().write_all(wire.as_bytes()).unwrap();
            assert_eq!(head(&mut peer).status, 400, "{wire:?}");
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        });
        assert!(served.is_err(), "{wire:?}");
        assert!(seen.lock().unwrap().is_empty());
    }
}

#[test]
fn truncated_and_invalid_chunked_bodies_fail_without_a_successful_response() {
    for wire in [
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nab",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nZ\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1;=x\r\na\r\n0\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nContent-Length: 1\r\n\r\n",
    ] {
        let (served, ()) = connection(Collect::default(), move |mut peer, _| {
            peer.get_mut().write_all(wire.as_bytes()).unwrap();
            peer.get_mut().shutdown(Shutdown::Write).unwrap();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        });
        assert!(served.is_err(), "{wire:?}");
    }
}

#[test]
fn limits_are_enforced_before_handler_admission() {
    let (served, ()) = connection_options(
        Collect::default(),
        base_http1::Options {
            max_head_bytes: 32,
            ..Default::default()
        },
        |mut peer, _| {
            peer.get_mut()
                .write_all(b"GET / HTTP/1.1\r\nHost: too-long.example\r\n\r\n")
                .unwrap();
            assert_eq!(head(&mut peer).status, 431);
        },
    );
    assert!(matches!(
        served,
        Err(Error::Protocol(base_http1::Error::Limit(_)))
    ));
}

#[test]
fn cancellation_of_pending_output_retires_lanes_and_wakes_producer() {
    let (reader, mut writer) = BodyReader::bounded(8, 1);
    let (served, ()) = connection(
        Producer {
            body: Mutex::new(Some(reader)),
        },
        |mut peer, cancellation| {
            peer.get_mut()
                .write_all(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n")
                .unwrap();
            assert_eq!(head(&mut peer).status, 200);
            cancellation.cancel();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
    );
    assert!(matches!(served, Err(Error::Cancelled)));
    assert!(block_on(writer.write(b"late".to_vec(), &Cancellation::never())).is_err());
}

struct Never(Arc<AtomicBool>);
impl Handler for Never {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        self.0.store(true, Ordering::Release);
        std::future::pending().await
    }
}

#[test]
fn cancellation_interrupts_a_handler_that_ignores_its_signal() {
    let started = Arc::new(AtomicBool::new(false));
    let handler = Never(started.clone());
    let (served, ()) = connection(handler, move |mut peer, cancellation| {
        peer.get_mut()
            .write_all(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n")
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !started.load(Ordering::Acquire) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        cancellation.cancel();
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert!(matches!(served, Err(Error::Cancelled)));
}

struct PendingRead(Arc<AtomicBool>);
impl Handler for PendingRead {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        mut request: Request<B>,
        _: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        use std::{
            future::{Future, poll_fn},
            pin::pin,
        };
        loop {
            let mut read = pin!(request.read(cancellation));
            let chunk = poll_fn(|cx| {
                let poll = read.as_mut().poll(cx);
                if poll.is_pending() {
                    self.0.store(true, Ordering::Release);
                }
                poll
            })
            .await
            .map_err(io::Error::other)?;
            if chunk.is_end() {
                return Ok(Response::new(Status::OK, BufferedBody::default()));
            }
        }
    }
}

#[test]
fn cancellation_abandons_a_pending_request_read_and_joins_backend_retirement() {
    let pending = Arc::new(AtomicBool::new(false));
    let handler = PendingRead(pending.clone());
    let (served, ()) = connection(handler, move |mut peer, cancellation| {
        peer.get_mut()
            .write_all(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc")
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !pending.load(Ordering::Acquire) {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        cancellation.cancel();
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert!(matches!(served, Err(Error::Cancelled)));
}

struct Count {
    started: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}
impl Handler for Count {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(n, Ordering::SeqCst);
        struct Guard<'a>(&'a AtomicUsize);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _guard = Guard(&self.active);
        cancellation.cancelled().await;
        Err(io::Error::other("cancelled"))
    }
}

#[test]
fn listener_bounds_connections_and_joins_them_on_shutdown() {
    let scheduler = scheduler();
    block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer_cancel = cancellation.clone();
        let handler = Count {
            started: Arc::default(),
            active: Arc::default(),
            maximum: Arc::default(),
        };
        let started = handler.started.clone();
        let peer = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                let mut peers = Vec::new();
                for _ in 0..4 {
                    let mut peer = connect(address);
                    peer.get_mut()
                        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                        .unwrap();
                    peers.push(peer);
                }
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while started.load(Ordering::SeqCst) < 2 {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
                peer_cancel.cancel();
                for peer in peers.iter_mut().take(2) {
                    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
                }
            });
            peer_cancel.cancel();
            result
        });
        let options = base_http_server::Options {
            max_connections: 2,
            ..Default::default()
        };
        let statistics =
            base_http_server::serve(&scheduler, listener, &handler, &options, &cancellation)
                .await
                .unwrap();
        peer.join().unwrap().unwrap();
        assert_eq!(statistics.connections, 2);
        assert_eq!(handler.maximum.load(Ordering::SeqCst), 2);
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
    });
    block_on(scheduler.shutdown());
}

#[test]
fn listener_isolates_bad_peers_and_counts_completed_requests_before_cancellation() {
    let scheduler = scheduler();
    block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer_cancel = cancellation.clone();
        let pending = Arc::new(AtomicBool::new(false));
        let peer_pending = pending.clone();
        let peer = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                let mut bad = connect(address);
                bad.get_mut().write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
                assert_eq!(head(&mut bad).status, 400);
                assert_eq!(bad.read(&mut [0]).unwrap(), 0);
                let mut good = connect(address);
                good.get_mut()
                    .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                    .unwrap();
                let response = head(&mut good);
                assert_eq!(body(&mut good, &response).0, b"hello");
                good.get_mut()
                    .write_all(b"GET /pending HTTP/1.1\r\nHost: x\r\n\r\n")
                    .unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !peer_pending.load(Ordering::Acquire) {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
                // The first completed exchange stays counted when the second cancels.
                peer_cancel.cancel();
                assert_eq!(good.read(&mut [0]).unwrap(), 0);
            });
            peer_cancel.cancel();
            result
        });
        let statistics = base_http_server::serve(
            &scheduler,
            listener,
            &OneThenPending { pending },
            &Default::default(),
            &cancellation,
        )
        .await
        .unwrap();
        peer.join().unwrap().unwrap();
        assert_eq!(statistics.connections, 2);
        assert_eq!(statistics.requests, 1);
        assert_eq!(statistics.failed_connections, 1);
    });
    block_on(scheduler.shutdown());
}

struct OneThenPending {
    pending: Arc<AtomicBool>,
}

#[test]
fn listener_accepts_new_peers_while_previous_connections_retire() {
    let scheduler = scheduler();
    block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer_cancel = cancellation.clone();
        let peer = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                for n in 0..40 {
                    let mut peer = connect(address);
                    let valid = n % 2 == 0;
                    let request = if valid {
                        b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".as_slice()
                    } else {
                        b"GET / HTTP/1.1\r\n\r\n"
                    };
                    peer.get_mut().write_all(request).unwrap();
                    let response = head(&mut peer);
                    assert_eq!(response.status, if valid { 200 } else { 400 }, "peer {n}");
                    let _ = body(&mut peer, &response);
                    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
                }
            });
            peer_cancel.cancel();
            result
        });
        let statistics = base_http_server::serve(
            &scheduler,
            listener,
            &Collect::default(),
            &Default::default(),
            &cancellation,
        )
        .await
        .unwrap();
        peer.join().unwrap().unwrap();
        assert_eq!(statistics.connections, 40);
        assert_eq!(statistics.requests, 20);
        assert_eq!(statistics.failed_connections, 20);
    });
    block_on(scheduler.shutdown());
}
impl Handler for OneThenPending {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        if request.path.as_deref() == Some("/pending") {
            self.pending.store(true, Ordering::Release);
            std::future::pending().await
        } else {
            Ok(Response::new(
                Status::OK,
                BufferedBody::from(b"hello".to_vec()),
            ))
        }
    }
}

struct Early;
impl Handler for Early {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        Ok(Response::new(
            Status::FORBIDDEN,
            BufferedBody::from(b"denied".to_vec()),
        ))
    }
}

#[test]
fn early_final_response_does_not_wait_for_an_unread_upload() {
    let (served, ()) = connection(Early, |mut peer, _| {
        peer.get_mut().write_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 100000\r\n\r\n").unwrap();
        let response = head(&mut peer);
        assert_eq!(response.status, 403);
        assert_eq!(response.get("connection"), Some("close"));
        assert_eq!(body(&mut peer, &response).0, b"denied");
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    assert_eq!(served.unwrap(), 1);
}

#[test]
fn rejected_connect_and_upgrade_remain_ordinary_http_exchanges() {
    for wire in [
        b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n".as_slice(),
        b"GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\n",
    ] {
        let (served, ()) = connection(Early, move |mut peer, _| {
            peer.get_mut().write_all(wire).unwrap();
            let first = head(&mut peer);
            assert_eq!(first.status, 403);
            assert_eq!(body(&mut peer, &first).0, b"denied");
            peer.get_mut()
                .write_all(b"GET /next HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .unwrap();
            let second = head(&mut peer);
            assert_eq!(second.status, 403);
            assert_eq!(body(&mut peer, &second).0, b"denied");
        });
        assert_eq!(served.unwrap(), 2);
    }
}

#[test]
fn large_payload_crosses_many_transport_reads_and_writes_without_changing_bytes() {
    let expected: Vec<u8> = (0..1_000_003).map(|i| (i % 251) as u8).collect();
    let (served, ()) = connection(Collect::default(), move |mut peer, _| {
        write!(
            peer.get_mut(),
            "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            expected.len()
        )
        .unwrap();
        peer.get_mut().write_all(&expected).unwrap();
        let response = head(&mut peer);
        assert_eq!(body(&mut peer, &response).0, expected);
    });
    assert_eq!(served.unwrap(), 1);
}

#[test]
fn partial_chunk_delimiter_does_not_hold_back_an_echo_response() {
    let (served, ()) = connection(Echo, |mut peer, _| {
        // Read-ahead ends partway through the next chunk delimiter. IncomingBody
        // can report ready even though decoding its next chunk needs more input.
        peer.get_mut().write_all(b"POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\none\r").unwrap();
        let response = head(&mut peer);
        assert_eq!(chunked_prefix(&mut peer, 3), b"one");
        peer.get_mut()
            .write_all(b"\n3\r\ntwo\r\n0\r\n\r\n")
            .unwrap();
        assert_eq!(body(&mut peer, &response).0, b"two");
    });
    assert_eq!(served.unwrap(), 1);
}
