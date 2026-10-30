// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::support::{Input, failing_input, input, is_invalid, is_io};
use base_http::{
    InterimHandler, InterimResponses, Method, RequestMetadata, ResponseMetadata, Version,
    body::{BodyFrame, ReadableBody},
};
use base_http1::{Error, Options, ResponseReader};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::io;

fn observer() -> InterimResponses<impl InterimHandler> {
    InterimResponses::new(|_: ResponseMetadata| {
        std::future::ready(Ok::<(), std::convert::Infallible>(()))
    })
}

fn get() -> RequestMetadata {
    RequestMetadata::new(Method::GET)
}

fn read_status(
    reader: &mut ResponseReader<Input>,
    request: &RequestMetadata,
) -> Result<u16, Error> {
    block_on(async {
        let never = Cancellation::never();
        let mut response = reader
            .read_response(request, &mut observer(), &never)
            .await?;
        while !response.body.read(&never).await?.is_end() {}
        Ok(response.status.as_u16())
    })
}

#[test]
fn an_http_10_request_persists_only_when_it_asks_to() {
    let mut keep_alive_field = get();
    keep_alive_field.version = Some(Version::Http10);
    keep_alive_field
        .fields
        .add("Connection", "keep-alive")
        .unwrap();
    let mut keep_alive_preference = get();
    keep_alive_preference.version = Some(Version::Http10);
    keep_alive_preference.persistent = Some(true);
    let mut plain = get();
    plain.version = Some(Version::Http10);
    for (request, reusable) in [
        (keep_alive_field, true),
        (keep_alive_preference, true),
        (plain, false),
    ] {
        let mut reader = ResponseReader::new(
            input(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", usize::MAX),
            Options::default(),
        )
        .unwrap();
        assert_eq!(read_status(&mut reader, &request).unwrap(), 200);
        assert_eq!(reader.is_reusable(), reusable);
    }
}

#[test]
fn a_response_head_beyond_the_head_limit_is_rejected() {
    let options = Options {
        max_head_bytes: 16,
        ..Options::default()
    };
    let mut reader = ResponseReader::new(
        input(b"HTTP/1.1 200 OK\r\nServer: example\r\n\r\n", 4),
        options,
    )
    .unwrap();
    let result = read_status(&mut reader, &get());
    assert!(matches!(result, Err(Error::Limit("message head"))));
    assert!(!reader.is_reusable());
}

#[test]
fn ambiguous_or_malformed_response_controls_are_rejected() {
    for (bytes, reason) in [
        (
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"[..],
            "duplicate singleton field",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep alive\r\n\r\n",
            "invalid connection option",
        ),
    ] {
        let bytes: &'static [u8] = bytes;
        let mut reader = ResponseReader::new(input(bytes, usize::MAX), Options::default()).unwrap();
        assert!(is_invalid(read_status(&mut reader, &get()), reason), "{bytes:?}");
    }
}

#[test]
fn input_failures_are_reported_for_heads_and_tunnels() {
    let mut reader =
        ResponseReader::new(failing_input(b"HTTP/1.1 200 OK\r\n"), Options::default()).unwrap();
    assert!(is_io(
        read_status(&mut reader, &get()),
        io::ErrorKind::ConnectionReset
    ));

    let mut connect = RequestMetadata::new(Method::CONNECT);
    connect.authority = Some("example:443".into());
    let mut reader = ResponseReader::new(
        failing_input(b"HTTP/1.1 200 OK\r\n\r\nraw"),
        Options::default(),
    )
    .unwrap();
    block_on(async {
        let never = Cancellation::never();
        let mut response = reader
            .read_response(&connect, &mut observer(), &never)
            .await
            .unwrap();
        let BodyFrame::Data { data, .. } = response.body.read(&never).await.unwrap() else {
            panic!("expected raw data");
        };
        assert_eq!(data, b"raw");
        assert!(is_io(
            response.body.read(&never).await,
            io::ErrorKind::ConnectionReset
        ));
    });
}
