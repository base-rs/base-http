// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_server::{Error, Handler, Options};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::{
    io::{self, ErrorKind, Read, Write},
    net::TcpStream,
};

mod support;
use support::*;

struct Hello;
impl Handler for Hello {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        _: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        Ok(Response::new(
            Status::OK,
            BufferedBody::from(b"hello".to_vec()),
        ))
    }
}

#[test]
fn invalid_listener_options_are_rejected_and_the_listener_closed() {
    let invalid_http1 = Options {
        http1: base_http1::Options {
            max_head_bytes: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let zero_connections = Options {
        max_connections: 0,
        ..Default::default()
    };
    for (options, message) in [
        (
            invalid_http1,
            "invalid HTTP/1 message: invalid HTTP/1 limits",
        ),
        (
            zero_connections,
            "invalid HTTP/1 message: zero connection limit",
        ),
    ] {
        let scheduler = scheduler();
        block_on(async {
            let (listener, address) = listener(&scheduler).await;
            let served = base_http_server::serve(
                &scheduler,
                listener,
                &Hello,
                &options,
                &Cancellation::new(),
            )
            .await;
            let error = served.unwrap_err();
            assert!(matches!(error, Error::Protocol(_)), "{message}");
            assert_eq!(error.to_string(), message);
            let refused = TcpStream::connect(address).unwrap_err();
            assert_eq!(refused.kind(), ErrorKind::ConnectionRefused, "{message}");
        });
        block_on(scheduler.shutdown());
    }
}

#[test]
fn listener_at_its_connection_limit_accepts_again_after_a_connection_completes() {
    let scheduler = scheduler();
    block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer_cancel = cancellation.clone();
        let peer = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                for _ in 0..3 {
                    let mut peer = connect(address);
                    peer.get_mut()
                        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                        .unwrap();
                    let response = head(&mut peer);
                    assert_eq!(response.status, 200);
                    assert_eq!(body(&mut peer, &response).0, b"hello");
                    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
                }
            });
            peer_cancel.cancel();
            result
        });
        let options = Options {
            max_connections: 1,
            ..Default::default()
        };
        let statistics =
            base_http_server::serve(&scheduler, listener, &Hello, &options, &cancellation)
                .await
                .unwrap();
        peer.join().unwrap().unwrap();
        assert_eq!(statistics.connections, 3);
        assert_eq!(statistics.requests, 3);
        assert_eq!(statistics.failed_connections, 0);
    });
    block_on(scheduler.shutdown());
}
