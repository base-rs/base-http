// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::support::{failing_input, input, is_invalid, is_io, reader};
use base_http::{
    Method,
    body::{BodyFrame, ReadableBody},
};
use base_http1::{Error, Options, RequestReader};
use base_scheduler::Cancellation;
use futures_lite::future::block_on;
use std::io;

#[test]
fn limits_that_cannot_hold_a_minimal_message_are_rejected() {
    let limits = [
        Options {
            max_head_bytes: 3,
            ..Options::default()
        },
        Options {
            max_fields: 0,
            ..Options::default()
        },
        Options {
            max_chunk_line_bytes: 1,
            ..Options::default()
        },
        Options {
            read_size: 0,
            ..Options::default()
        },
    ];
    for options in limits {
        assert!(is_invalid(options.validate(), "invalid HTTP/1 limits"));
        assert!(is_invalid(
            RequestReader::new(input(b"", 1), options),
            "invalid HTTP/1 limits"
        ));
    }
    assert!(Options::default().validate().is_ok());
}

#[test]
fn standard_and_extension_methods_are_preserved() {
    for (bytes, method) in [
        (&b"DELETE / HTTP/1.1\r\nHost: x\r\n\r\n"[..], Method::DELETE),
        (b"TRACE / HTTP/1.1\r\nHost: x\r\n\r\n", Method::TRACE),
        (b"PATCH / HTTP/1.1\r\nHost: x\r\n\r\n", Method::PATCH),
        (b"QUERY / HTTP/1.1\r\nHost: x\r\n\r\n", Method::QUERY),
        (
            b"PURGE / HTTP/1.1\r\nHost: x\r\n\r\n",
            Method::new("PURGE").unwrap(),
        ),
    ] {
        let bytes: &'static [u8] = bytes;
        let mut input = reader(bytes, usize::MAX);
        let (request, _) = block_on(input.read_request(&Cancellation::never()))
            .unwrap()
            .unwrap();
        assert_eq!(request.method, method);
    }
}

#[test]
fn an_absolute_target_without_a_path_requests_the_root() {
    let mut input = reader(
        b"GET http://example.com HTTP/1.1\r\nHost: example.com\r\n\r\n",
        usize::MAX,
    );
    let (request, _) = block_on(input.read_request(&Cancellation::never()))
        .unwrap()
        .unwrap();
    assert_eq!(request.scheme.as_deref(), Some("http"));
    assert_eq!(request.authority.as_deref(), Some("example.com"));
    assert_eq!(request.path.as_deref(), Some("/"));
}

#[test]
fn input_failures_while_reading_a_head_prevent_reuse() {
    for bytes in [&b""[..], b"GET / HTTP/1.1\r\n"] {
        let mut input = RequestReader::new(failing_input(bytes), Options::default()).unwrap();
        let result = block_on(input.read_request(&Cancellation::never()));
        assert!(is_io(result, io::ErrorKind::ConnectionReset));
        assert!(!input.is_reusable());
    }
}

fn body_result(bytes: &'static [u8], step: usize, options: Options) -> Result<Vec<u8>, Error> {
    let input = crate::support::Input {
        bytes,
        step,
        fail: false,
    };
    body_result_from(RequestReader::new(input, options).unwrap())
}

fn body_result_from(mut input: RequestReader<crate::support::Input>) -> Result<Vec<u8>, Error> {
    block_on(async {
        let never = Cancellation::never();
        let (mut request, _) = input.read_request(&never).await?.unwrap();
        let mut body = Vec::new();
        loop {
            match request.body.read(&never).await? {
                BodyFrame::Data { data, end } => {
                    body.extend_from_slice(&data);
                    if end {
                        return Ok(body);
                    }
                }
                BodyFrame::Headers { .. } | BodyFrame::End => return Ok(body),
            }
        }
    })
}

#[test]
fn input_failures_while_reading_a_body_are_reported() {
    for bytes in [
        &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nab"[..],
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab",
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5",
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na",
    ] {
        let bytes: &'static [u8] = bytes;
        let input = RequestReader::new(failing_input(bytes), Options::default()).unwrap();
        assert!(is_io(
            body_result_from(input),
            io::ErrorKind::ConnectionReset
        ));
    }
}

#[test]
fn a_chunk_without_its_delimiter_is_invalid_at_every_fragment_size() {
    // Other bytes, a bare LF, and a CR followed by anything but LF.
    for bytes in [
        &b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\naXY0\r\n\r\n"[..],
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\n0\r\n\r\n",
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\rX0\r\n\r\n",
    ] {
        let bytes: &'static [u8] = bytes;
        for step in 1..=bytes.len() {
            assert!(
                is_invalid(
                    body_result(bytes, step, Options::default()),
                    "missing chunk delimiter"
                ),
                "{bytes:?} at step {step}"
            );
        }
    }
    // A delimiter that is merely incomplete still waits for more input.
    const SPLIT: &[u8] =
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\n\r\n";
    for step in 1..=SPLIT.len() {
        assert_eq!(body_result(SPLIT, step, Options::default()).unwrap(), b"a");
    }
}

#[test]
fn chunked_input_ending_early_is_truncated() {
    for bytes in [
        &b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r"[..],
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n",
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n",
    ] {
        let bytes: &'static [u8] = bytes;
        let result = body_result(bytes, usize::MAX, Options::default());
        assert!(matches!(result, Err(Error::Truncated)), "{bytes:?}");
    }
}

#[test]
fn chunk_lines_and_trailers_respect_their_limits() {
    let options = Options {
        max_chunk_line_bytes: 4,
        ..Options::default()
    };
    // The limit applies whether the line ends within one read or is still incomplete.
    for step in [1, usize::MAX] {
        let result = body_result(
            b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1;ab\r\na\r\n0\r\n\r\n",
            step,
            options.clone(),
        );
        assert!(
            matches!(result, Err(Error::Limit("line length"))),
            "step {step}"
        );
    }

    let options = Options {
        max_fields: 2,
        ..Options::default()
    };
    let result = body_result(
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\na: 1\r\nb: 2\r\nc: 3\r\n\r\n",
        usize::MAX,
        options,
    );
    assert!(matches!(result, Err(Error::Limit("trailer count"))));
}

#[test]
fn a_failed_body_read_cannot_be_retried() {
    let mut input = reader(
        b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\naX\r\n0\r\n\r\n",
        usize::MAX,
    );
    block_on(async {
        let never = Cancellation::never();
        let (mut request, _) = input.read_request(&never).await.unwrap().unwrap();
        assert!(matches!(
            request.body.read(&never).await,
            Ok(BodyFrame::Data { .. })
        ));
        assert!(is_invalid(
            request.body.read(&never).await,
            "missing chunk delimiter"
        ));
        assert!(is_invalid(
            request.body.read(&never).await,
            "previous body read failed or was abandoned"
        ));
    });
    assert!(!input.is_reusable());
}

#[test]
fn a_cancelled_body_read_consumes_no_input() {
    let mut input = reader(
        b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nab",
        usize::MAX,
    );
    block_on(async {
        let (mut request, _) = input
            .read_request(&Cancellation::never())
            .await
            .unwrap()
            .unwrap();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            request.body.read(&cancelled).await,
            Err(Error::Cancelled)
        ));
        let BodyFrame::Data { data, end } =
            request.body.read(&Cancellation::never()).await.unwrap()
        else {
            panic!("expected data");
        };
        assert_eq!((data.as_slice(), end), (&b"ab"[..], true));
    });
    assert!(input.is_reusable());
}
