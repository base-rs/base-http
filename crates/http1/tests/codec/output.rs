// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::support::{exchange, failing_output, is_invalid, is_io, output, unflushable_output};
use base_http::{
    Fields, InterimHandler, Protocol, Response, ResponseMetadata, Status, Version,
    body::{BodyFrame, BufferedBody, ReadableBody},
};
use base_http1::Error;
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, yield_now};
use std::{collections::VecDeque, io};

const GET: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n";
const GET_10: &[u8] = b"GET / HTTP/1.0\r\n\r\n";
const UPGRADE: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\n";

/// A scripted body. `length` and `trailers` are its hints, and `suspend` makes its first read wait once.
struct Script {
    frames: VecDeque<Result<BodyFrame<Vec<u8>>, io::Error>>,
    length: Option<u64>,
    trailers: bool,
    ready: bool,
    suspend: bool,
    cancel: Option<Cancellation>,
}

impl Script {
    fn new(frames: impl IntoIterator<Item = Result<BodyFrame<Vec<u8>>, io::Error>>) -> Self {
        Self {
            frames: frames.into_iter().collect(),
            length: None,
            trailers: true,
            ready: true,
            suspend: false,
            cancel: None,
        }
    }
}

impl ReadableBody for Script {
    type Data = Vec<u8>;
    type Error = io::Error;
    fn may_have_trailers(&self) -> bool {
        self.trailers
    }
    fn is_ready(&self) -> bool {
        self.ready
    }
    fn length(&self) -> Option<u64> {
        self.length
    }
    fn close(&mut self, _: Option<io::Error>) {}
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, io::Error> {
        if std::mem::take(&mut self.suspend) {
            yield_now().await;
        }
        // Cancellation can arrive while the body is producing a frame.
        if let Some(cancellation) = &self.cancel {
            cancellation.cancel();
        }
        self.frames.pop_front().unwrap_or(Ok(BodyFrame::End))
    }
}

fn data(bytes: &[u8]) -> Result<BodyFrame<Vec<u8>>, io::Error> {
    Ok(BodyFrame::Data {
        data: bytes.to_vec(),
        end: false,
    })
}

fn write<B: ReadableBody>(
    request: &'static [u8],
    response: &mut Response<B>,
) -> Result<String, Error> {
    let (mut output, bytes) = output();
    block_on(output.write_response(&exchange(request), response, &Cancellation::never()))?;
    Ok(String::from_utf8(bytes.lock().unwrap().clone()).unwrap())
}

#[test]
fn rejection_closes_output_once() {
    let (mut output, bytes) = output();
    let never = Cancellation::never();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        block_on(output.reject(Status::BAD_REQUEST, &cancelled)),
        Err(Error::Cancelled)
    ));
    assert!(output.is_usable());
    block_on(output.reject(Status::BAD_REQUEST, &never)).unwrap();
    assert_eq!(
        *bytes.lock().unwrap(),
        b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    assert!(!output.is_usable());
    assert!(is_invalid(
        block_on(output.reject(Status::BAD_REQUEST, &never)),
        "response output is not reusable"
    ));

    let mut output = failing_output();
    let result = block_on(output.reject(Status::BAD_REQUEST, &never));
    assert!(is_io(result, io::ErrorKind::BrokenPipe));
    assert!(!output.is_usable());

    // Bytes accepted by the transport are not delivered until it flushes.
    let (mut output, bytes) = unflushable_output();
    let result = block_on(output.reject(Status::BAD_REQUEST, &never));
    assert!(is_io(result, io::ErrorKind::TimedOut));
    assert!(
        bytes
            .lock()
            .unwrap()
            .starts_with(b"HTTP/1.1 400 Bad Request\r\n")
    );
    assert!(!output.is_usable());
}

#[test]
fn a_cancelled_response_writes_nothing() {
    let (mut output, bytes) = output();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let mut response = Response::empty(Status::OK);
    let result = block_on(output.write_response(&exchange(GET), &mut response, &cancelled));
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(bytes.lock().unwrap().is_empty());
    assert!(output.is_usable());
}

#[test]
fn invalid_responses_are_rejected_before_any_output() {
    type Case = (&'static [u8], Response<Script>, &'static str);
    let response = |status: u16| Response::new(Status::new(status).unwrap(), Script::new([]));
    let with = |status: u16, fields: &[(&str, &str)]| {
        let mut response = response(status);
        for (name, value) in fields {
            response.fields.add(name, value).unwrap();
        }
        response
    };
    let with_protocol = |status: u16| {
        let mut response = response(status);
        response.protocol = Some(Protocol::new("echo").unwrap());
        response
    };
    let mut cases: Vec<Case> = vec![
        (GET, response(103), "final response required"),
        (
            GET,
            with(200, &[("Content-Length", "0"), ("Content-Length", "0")]),
            "duplicate singleton field",
        ),
        (
            GET,
            with(200, &[("Transfer-Encoding", "chunked"), ("Transfer-Encoding", "chunked")]),
            "duplicate singleton field",
        ),
        (GET, with(200, &[("Content-Length", "x")]), "invalid content-length"),
        (
            b"POST / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: echo\r\nContent-Length: 1\r\n\r\n",
            with_protocol(101),
            "switch with unread request body",
        ),
        (UPGRADE, with(101, &[("Upgrade", "echo"), ("Content-Length", "0")]), "framing fields on tunnel response"),
        (GET, with_protocol(101), "unsolicited upgrade"),
        (UPGRADE, response(101), "missing selected protocol"),
        (
            b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n",
            with_protocol(200),
            "protocol on traditional CONNECT",
        ),
        (GET, with_protocol(200), "protocol without upgrade status"),
        (GET_10, with(200, &[("Transfer-Encoding", "chunked")]), "HTTP/1.0 transfer-encoding"),
        (
            b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n",
            with(200, &[("Transfer-Encoding", "chunked")]),
            "trailers or transfer-encoding on bodyless response",
        ),
        (GET, with(204, &[("Content-Length", "0")]), "content-length on 204"),
        (GET, with(205, &[("Content-Length", "1")]), "content on 205"),
        (GET_10, with(200, &[("Trailer", "x-digest")]), "trailers require HTTP/1.1 chunked framing"),
    ];
    let mut versioned = response(200);
    versioned.version = Some(Version::Http10);
    cases.push((GET, versioned, "response version differs from request"));
    let mut declared = with(200, &[("Transfer-Encoding", "chunked")]);
    declared.content_length = Some(0);
    cases.push((GET, declared, "content-length with transfer-encoding"));
    let mut known = response(205);
    known.body.length = Some(1);
    cases.push((GET, known, "content on 205"));

    for (request, mut response, reason) in cases {
        let (mut output, bytes) = output();
        let result = block_on(output.write_response(
            &exchange(request),
            &mut response,
            &Cancellation::never(),
        ));
        assert!(is_invalid(result, reason), "{reason}");
        assert!(bytes.lock().unwrap().is_empty(), "{reason}");
    }
}

#[test]
fn reset_content_and_trailer_fields_select_their_framing() {
    let mut empty = Response::new(Status::RESET_CONTENT, BufferedBody::<Vec<u8>>::default());
    assert_eq!(
        write(GET, &mut empty).unwrap(),
        "HTTP/1.1 205 Reset Content\r\nContent-Length: 0\r\n\r\n"
    );

    let mut announced = Response::new(Status::RESET_CONTENT, BufferedBody::<Vec<u8>>::default());
    announced.fields.add("Trailer", "x-digest").unwrap();
    assert_eq!(
        write(GET, &mut announced).unwrap(),
        "HTTP/1.1 205 Reset Content\r\nTrailer: x-digest\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
    );

    let mut chunked = Response::new(Status::OK, BufferedBody::from(b"hi".to_vec()));
    chunked.fields.add("Transfer-Encoding", "chunked").unwrap();
    assert_eq!(
        write(GET, &mut chunked).unwrap(),
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n"
    );
}

#[test]
fn an_upgrade_replaces_the_application_upgrade_field_with_the_selected_protocol() {
    let mut response = Response::new(
        Status::SWITCHING_PROTOCOLS,
        BufferedBody::<Vec<u8>>::default(),
    );
    response.fields.add("Server", "example").unwrap();
    response.fields.add("upgrade", "ECHO").unwrap();
    assert_eq!(
        write(UPGRADE, &mut response).unwrap(),
        "HTTP/1.1 101 Switching Protocols\r\nServer: example\r\nUpgrade: ECHO\r\nConnection: upgrade\r\n\r\n"
    );
}

#[test]
fn trailers_on_an_http_10_body_are_rejected_after_its_data() {
    let mut trailers = Fields::new();
    trailers.add("x-digest", "1").unwrap();
    let mut body = Script::new([
        data(b"hi"),
        Ok(BodyFrame::Headers {
            fields: trailers,
            end: true,
        }),
    ]);
    body.length = Some(2);
    let mut response = Response::new(Status::OK, body);
    assert!(is_invalid(
        write(GET_10, &mut response),
        "trailers require chunked framing"
    ));
}

#[test]
fn output_failures_at_each_flush_point_are_reported() {
    const EXPECT: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: echo\r\nExpect: 100-continue\r\n\r\n";
    let switching = || {
        let mut response = Response::new(Status::SWITCHING_PROTOCOLS, Script::new([]));
        response.protocol = Some(Protocol::new("echo").unwrap());
        response
    };
    let mut not_ready = Script::new([]);
    not_ready.ready = false;
    let mut suspending = Script::new([]);
    suspending.suspend = true;
    for (request, mut response) in [
        (EXPECT, switching()),
        (UPGRADE, switching()),
        (GET, Response::new(Status::OK, not_ready)),
        (GET, Response::new(Status::OK, suspending)),
    ] {
        let mut output = failing_output();
        let result = block_on(output.write_response(
            &exchange(request),
            &mut response,
            &Cancellation::never(),
        ));
        assert!(is_io(result, io::ErrorKind::BrokenPipe));
        assert!(!output.is_usable());
    }
}

#[test]
fn cancellation_during_a_body_read_is_reported_as_cancellation() {
    for frame in [data(b"late"), Err(io::Error::other("stopped"))] {
        let cancellation = Cancellation::new();
        let mut body = Script::new([frame]);
        body.cancel = Some(cancellation.clone());
        let mut response = Response::new(Status::OK, body);
        let (mut output, _) = output();
        let result = block_on(output.write_response(&exchange(GET), &mut response, &cancellation));
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(!output.is_usable());
    }
}

#[test]
fn invalid_interim_responses_are_rejected_before_any_output() {
    let never = Cancellation::never();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let early_hints = || ResponseMetadata::new(Status::new(103).unwrap());
    let with_field = |name: &str| {
        let mut response = early_hints();
        response.fields.add(name, "0").unwrap();
        response
    };
    let mut length = early_hints();
    length.content_length = Some(0);
    let mut protocol = early_hints();
    protocol.protocol = Some(Protocol::new("echo").unwrap());
    for (version, response, cancellation, reason) in [
        (
            Version::Http11,
            ResponseMetadata::new(Status::OK),
            &never,
            Some("invalid interim status"),
        ),
        (
            Version::Http11,
            ResponseMetadata::new(Status::SWITCHING_PROTOCOLS),
            &never,
            Some("invalid interim status"),
        ),
        (
            Version::Http10,
            early_hints(),
            &never,
            Some("HTTP/1.0 interim response"),
        ),
        (
            Version::Http11,
            with_field("Content-Length"),
            &never,
            Some("invalid interim framing"),
        ),
        (
            Version::Http11,
            with_field("Transfer-Encoding"),
            &never,
            Some("invalid interim framing"),
        ),
        (
            Version::Http11,
            length,
            &never,
            Some("invalid interim framing"),
        ),
        (
            Version::Http11,
            protocol,
            &never,
            Some("invalid interim framing"),
        ),
        (Version::Http11, early_hints(), &cancelled, None),
    ] {
        let (mut output, bytes) = output();
        let result = block_on(
            output
                .interim(version)
                .on_interim_response(response, cancellation),
        );
        match reason {
            Some(reason) => assert!(is_invalid(result, reason), "{reason}"),
            None => assert!(matches!(result, Err(Error::Cancelled))),
        }
        assert!(bytes.lock().unwrap().is_empty());
        assert!(output.is_usable());
    }

    let (mut output, _) = output();
    block_on(output.reject(Status::BAD_REQUEST, &never)).unwrap();
    let result = block_on(
        output
            .interim(Version::Http11)
            .on_interim_response(early_hints(), &never),
    );
    assert!(is_invalid(result, "response output is not reusable"));
}
