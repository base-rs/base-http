// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A deliberately small protocol-driver experiment, not a gateway policy API.
//! Upload and download futures remain separately owned and polled until joined.

mod support;
use support::*;

use base_http::{
    Fields, InterimHandler, InterimResponses, Request, RequestHead, Response, ResponseHead, Status,
    body::ReadableBody,
};
use base_http_server::Handler;
use base_http1::{Error, RequestReader, RequestWriter, ResponseReader, ResponseWriter};
use base_scheduler::{
    Cancellation, SocketAccept, SocketClose, SocketConnect, SocketOpen, SocketStreamReading,
    SocketStreamWriting, SocketType,
};
use base_scheduler_direct::{DirectScheduler, DirectSocket};
use futures_lite::future::{block_on, zip};
use std::{
    future::Future,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

// This experiment forwards ordinary HTTP/1 messages only. Upgrade/CONNECT need
// a client-side switch coordinator before they can use this forwarding loop.
fn forward_fields(fields: &mut Fields) {
    let nominated: Vec<String> = fields
        .headers()
        .iter()
        .filter(|f| f.name().eq_ignore_ascii_case("connection"))
        .flat_map(|f| std::str::from_utf8(f.value()).unwrap().split(','))
        .map(|name| name.trim().to_owned())
        .collect();
    for name in nominated.iter().map(String::as_str).chain([
        "connection",
        "keep-alive",
        "proxy-connection",
        "te",
        "transfer-encoding",
        "upgrade",
    ]) {
        fields.remove(name);
    }
}

async fn supervised<T>(
    future: impl Future<Output = Result<T, Error>>,
    cancellation: &Cancellation,
) -> Result<T, Error> {
    let result = future.await;
    if result.is_err() {
        cancellation.cancel();
    }
    result
}

struct ProxyResult {
    result: Result<u64, Error>,
    retired_lanes: usize,
}

async fn proxy_connection(
    scheduler: &DirectScheduler,
    downstream: DirectSocket,
    upstream_address: SocketAddr,
    cancellation: &Cancellation,
) -> ProxyResult {
    let upstream = scheduler
        .socket_open(
            upstream_address.into(),
            SocketType::Stream,
            None,
            cancellation,
        )
        .await
        .unwrap();
    scheduler
        .socket_connect(&upstream, upstream_address, cancellation)
        .await
        .unwrap();
    let options = base_http1::Options {
        read_size: 1024,
        ..Default::default()
    };
    let mut input = RequestReader::new(
        scheduler.socket_stream_reader(&downstream).unwrap(),
        options.clone(),
    )
    .unwrap();
    let mut output = ResponseWriter::new(scheduler.socket_stream_writer(&downstream).unwrap());
    let mut upload = RequestWriter::new(scheduler.socket_stream_writer(&upstream).unwrap());
    let mut download =
        ResponseReader::new(scheduler.socket_stream_reader(&upstream).unwrap(), options).unwrap();
    let result = async {
        let mut count = 0;
        loop {
            let Some((mut request, exchange)) = input.read_request(cancellation).await? else {
                return Ok(count);
            };
            forward_fields(&mut request.head.fields);
            request.head.fields.remove("host");
            request.head.authority = Some(upstream_address.to_string());
            let upstream_head = request.head.clone();
            let lanes = cancellation.child();
            let send = supervised(upload.write_request(&mut request, &lanes), &lanes);
            let receive = supervised(
                async {
                    let mut interim = InterimResponses::new(output.interim(exchange.version()));
                    let mut response = download
                        .read_response(&upstream_head, &mut interim, &lanes)
                        .await?;
                    forward_fields(&mut response.head.fields);
                    response.head.version = None;
                    output
                        .write_response(&exchange, &mut response, &lanes)
                        .await
                },
                &lanes,
            );
            // Neither successful completion cancels the other direction. An
            // error cancels its sibling, but both futures are still awaited.
            let (sent, received) = zip(send, receive).await;
            drop(request);
            let (sent, received) = match (sent, received) {
                (Ok(sent), Ok(received)) => (sent, received),
                (Err(Error::Cancelled), Err(error)) => return Err(error),
                (Err(error), _) | (_, Err(error)) => return Err(error),
            };
            count += 1;
            // All four lanes must be synchronized before another admission.
            if !sent || !received || !input.is_reusable() || !download.is_reusable() {
                return Ok(count);
            }
        }
    }
    .await;
    let ((a, b), (c, d)) = zip(
        zip(input.close(), output.close()),
        zip(upload.close(), download.close()),
    )
    .await;
    let retired_lanes = [a, b, c, d]
        .into_iter()
        .map(|result| {
            result.unwrap();
            1
        })
        .sum();
    drop((input, output, upload, download));
    let (a, b) = zip(
        scheduler.socket_close(downstream),
        scheduler.socket_close(upstream),
    )
    .await;
    a.unwrap();
    b.unwrap();
    ProxyResult {
        result,
        retired_lanes,
    }
}

fn with_scheduler<T>(run: impl FnOnce(&DirectScheduler) -> T) -> T {
    let scheduler = scheduler();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&scheduler)));
    block_on(scheduler.shutdown());
    result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn checked_thread<T: Send + 'static>(
    cancellation: &Cancellation,
    run: impl FnOnce() -> T + Send + 'static,
) -> std::thread::JoinHandle<std::thread::Result<T>> {
    let cancellation = cancellation.clone();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        if result.is_err() {
            cancellation.cancel();
        }
        result
    })
}

fn joined<T>(thread: std::thread::JoinHandle<std::thread::Result<T>>) -> T {
    thread
        .join()
        .unwrap()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn raw_upstream(
    upstream: impl FnOnce(BufReader<TcpStream>) + Send + 'static,
    peer: impl FnOnce(BufReader<TcpStream>, Cancellation) + Send + 'static,
) -> ProxyResult {
    with_scheduler(|scheduler| {
        block_on(async {
            let (listener, address) = listener(scheduler).await;
            let upstream_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let upstream_address = upstream_listener.local_addr().unwrap();
            let cancellation = Cancellation::new();
            let server = checked_thread(&cancellation, move || {
                let (socket, _) = upstream_listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                upstream(BufReader::new(socket));
            });
            let peer_cancel = cancellation.clone();
            let client = checked_thread(&cancellation, move || peer(connect(address), peer_cancel));
            let (socket, _) = scheduler
                .socket_accept(&listener, &cancellation)
                .await
                .unwrap();
            scheduler.socket_close(listener).await.unwrap();
            let result = proxy_connection(scheduler, socket, upstream_address, &cancellation).await;
            joined(server);
            joined(client);
            result
        })
    })
}

fn request_head(peer: &mut impl BufRead) -> (String, Head) {
    let mut line = String::new();
    peer.read_line(&mut line).unwrap();
    assert!(line.ends_with("HTTP/1.1\r\n"), "{line:?}");
    let start = line.clone();
    let mut fields = Vec::new();
    loop {
        line.clear();
        peer.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        assert!(line.ends_with("\r\n"));
        let (name, value) = line[..line.len() - 2].split_once(':').unwrap();
        fields.push((name.to_owned(), value.trim().to_owned()));
    }
    (start, Head { status: 0, fields })
}

struct Echo {
    seen: Arc<Mutex<Vec<RequestHead>>>,
}
impl Handler for Echo {
    type Error = io::Error;
    type Body<B: ReadableBody> = B;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<B>, io::Error> {
        self.seen.lock().unwrap().push(request.head.clone());
        if request.fields.contains("expect") {
            for status in [Status::CONTINUE, Status::EARLY_HINTS] {
                interim
                    .send_interim_response(ResponseHead::new(status), cancellation)
                    .await
                    .map_err(io::Error::other)?;
            }
        }
        let mut response = Response::new(Status::OK, request.body);
        response.fields.add("Connection", "x-private").unwrap();
        response.fields.add("x-private", "remove-me").unwrap();
        Ok(response)
    }
}

#[test]
fn native_proxy_streams_both_directions_with_interims_trailers_and_connection_reuse() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let (proxy, served, upstream_address) = with_scheduler(|scheduler| {
        block_on(async {
            let (listener, address) = listener(scheduler).await;
            let (upstream_listener, upstream_address) = support::listener(scheduler).await;
            let cancellation = Cancellation::new();
            let client = checked_thread(&cancellation, move || {
                let mut peer = connect(address);
                peer.get_mut().write_all(b"POST /echo HTTP/1.1\r\nHost: original\r\nConnection: x-secret\r\nx-secret: remove-me\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\nTrailer: x-result\r\n\r\n").unwrap();
                assert_eq!(head(&mut peer).status, 100);
                assert_eq!(head(&mut peer).status, 103);
                let response = head(&mut peer);
                assert_eq!(response.status, 200);
                assert!(response.get("connection").is_none());
                assert!(response.get("x-private").is_none());
                peer.get_mut().write_all(b"3\r\none\r\n").unwrap();
                // Wait for response data before producing the rest of the request.
                assert_eq!(chunked_prefix(&mut peer, 3), b"one");
                peer.get_mut()
                    .write_all(b"3\r\ntwo\r\n0\r\nx-result: complete\r\n\r\n")
                    .unwrap();
                let (data, trailers) = body(&mut peer, &response);
                assert_eq!(data, b"two");
                assert_eq!(trailers, ["x-result: complete"]);
                peer.get_mut()
                    .write_all(
                        b"GET /again HTTP/1.1\r\nHost: original\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                let response = head(&mut peer);
                assert_eq!(response.status, 200);
                assert_eq!(response.get("connection"), Some("close"));
                assert!(body(&mut peer, &response).0.is_empty());
                assert_eq!(peer.read(&mut [0]).unwrap(), 0);
            });
            let handler = Echo { seen: capture };
            let server = async {
                let (socket, _) = scheduler
                    .socket_accept(&upstream_listener, &cancellation)
                    .await
                    .unwrap();
                scheduler.socket_close(upstream_listener).await.unwrap();
                base_http_server::serve_connection(
                    scheduler,
                    socket,
                    &handler,
                    &Default::default(),
                    &cancellation,
                )
                .await
            };
            let proxy = async {
                let (socket, _) = scheduler
                    .socket_accept(&listener, &cancellation)
                    .await
                    .unwrap();
                scheduler.socket_close(listener).await.unwrap();
                proxy_connection(scheduler, socket, upstream_address, &cancellation).await
            };
            let (served, proxy) = zip(server, proxy).await;
            joined(client);
            (proxy, served, upstream_address)
        })
    });
    assert_eq!(proxy.result.unwrap(), 2);
    assert_eq!(proxy.retired_lanes, 4);
    assert_eq!(served.unwrap(), 2);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for request in seen.iter() {
        assert_eq!(
            request.authority.as_deref(),
            Some(upstream_address.to_string().as_str())
        );
        assert!(!request.fields.contains("x-secret"));
    }
}

#[test]
fn complete_early_response_does_not_stop_upload_or_prevent_reuse() {
    const LENGTH: usize = 2 * 1024 * 1024;
    let result = raw_upstream(
        |mut peer| {
            let (start, request) = request_head(&mut peer);
            assert!(start.starts_with("POST /early "));
            assert_eq!(
                request.get("content-length"),
                Some(LENGTH.to_string().as_str())
            );
            peer.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nearly")
                .unwrap();
            let (data, _) = body(&mut peer, &request);
            assert_eq!(data, vec![b'u'; LENGTH]);
            let (start, _) = request_head(&mut peer);
            assert!(start.starts_with("GET /again "));
            peer.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone")
                .unwrap();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
        |mut peer, _| {
            write!(
                peer.get_mut(),
                "POST /early HTTP/1.1\r\nHost: original\r\nContent-Length: {LENGTH}\r\n\r\n"
            )
            .unwrap();
            let response = head(&mut peer);
            assert_eq!(response.status, 200);
            assert!(response.get("connection").is_none());
            assert_eq!(body(&mut peer, &response).0, b"early");
            // No upload byte is produced until the final response is complete.
            peer.get_mut().write_all(&vec![b'u'; LENGTH]).unwrap();
            peer.get_mut()
                .write_all(b"GET /again HTTP/1.1\r\nHost: original\r\nConnection: close\r\n\r\n")
                .unwrap();
            let response = head(&mut peer);
            assert_eq!(body(&mut peer, &response).0, b"done");
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
    );
    assert_eq!(result.result.unwrap(), 2);
    assert_eq!(result.retired_lanes, 4);
}

#[test]
fn downstream_response_backpressure_does_not_stop_independent_upload_progress() {
    const LENGTH: usize = 8 * 1024 * 1024;
    let result = raw_upstream(
        |mut peer| {
            let (_, request) = request_head(&mut peer);
            // Independent upstream directions: write a response larger than socket
            // buffers while also consuming the upload. A body-driven upload pump
            // would stall once the proxy's downstream response write blocks.
            let mut writer = peer.get_ref().try_clone().unwrap();
            let response = std::thread::spawn(move || {
                write!(
                    writer,
                    "HTTP/1.1 200 OK\r\nContent-Length: {LENGTH}\r\n\r\n"
                )
                .unwrap();
                writer.write_all(&vec![b'r'; LENGTH]).unwrap();
            });
            assert_eq!(body(&mut peer, &request).0, vec![b'u'; LENGTH]);
            response.join().unwrap();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
        |mut peer, _| {
            write!(peer.get_mut(), "POST /pressure HTTP/1.1\r\nHost: original\r\nConnection: close\r\nContent-Length: {LENGTH}\r\n\r\n").unwrap();
            peer.get_mut().write_all(&vec![b'u'; LENGTH]).unwrap();
            // Delay all response consumption until the upload has been produced.
            let response = head(&mut peer);
            assert_eq!(body(&mut peer, &response).0, vec![b'r'; LENGTH]);
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
    );
    assert_eq!(result.result.unwrap(), 1);
    assert_eq!(result.retired_lanes, 4);
}

#[test]
fn truncated_upstream_response_cancels_pending_upload_and_joins_all_lanes() {
    let result = raw_upstream(
        |mut peer| {
            request_head(&mut peer);
            peer.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nx")
                .unwrap();
            peer.get_ref().shutdown(Shutdown::Write).unwrap();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
        |mut peer, _| {
            peer.get_mut()
                .write_all(b"POST / HTTP/1.1\r\nHost: original\r\nContent-Length: 100\r\n\r\n")
                .unwrap();
            assert_eq!(head(&mut peer).status, 200);
            let mut data = Vec::new();
            peer.read_to_end(&mut data).unwrap();
            assert_eq!(data, b"x");
        },
    );
    // The response writer wraps upstream body errors with their causal chain.
    assert!(
        matches!(result.result, Err(Error::Body(_))),
        "{:?}",
        result.result
    );
    assert_eq!(result.retired_lanes, 4);
}

#[test]
fn cancellation_with_both_proxy_directions_pending_joins_all_lanes() {
    let result = raw_upstream(
        |mut peer| {
            request_head(&mut peer);
            peer.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .unwrap();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
        |mut peer, cancellation| {
            peer.get_mut()
                .write_all(b"POST / HTTP/1.1\r\nHost: original\r\nContent-Length: 100\r\n\r\n")
                .unwrap();
            assert_eq!(head(&mut peer).status, 200);
            cancellation.cancel();
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        },
    );
    assert!(
        matches!(result.result, Err(Error::Cancelled)),
        "{:?}",
        result.result
    );
    assert_eq!(result.retired_lanes, 4);
}
