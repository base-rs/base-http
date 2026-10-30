// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    observer,
    support::{Input, failing_input, input, is_invalid, is_io},
};
use base_http::{
    Fields, InterimResponses, Method, Protocol, Request, RequestMetadata, ResponseMetadata,
    Version,
    body::{BodyFrame, BufferedBody, ReadableBody},
};
use base_http1::{Error, Options, RequestWriter, ResponseReader};
use base_scheduler::{
    Cancellation, IoBufSequence, OperationResult, SocketSendFlags, SocketStreamWriter,
};
use futures_lite::future::block_on;
use std::{
    io::{self, IoSlice},
    sync::{Arc, Mutex},
};

fn unsupported<T>() -> OperationResult<T> {
    Err(io::Error::from(io::ErrorKind::Unsupported).into())
}

fn broken<T>() -> OperationResult<T> {
    Err(io::Error::from(io::ErrorKind::BrokenPipe).into())
}

/// Bytes accepted by the transport, and the number of upcoming writes and flushes that fail.
#[derive(Default)]
struct Wire {
    bytes: Vec<u8>,
    write_failures: usize,
    flush_failures: usize,
}

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Wire>>);

impl Output {
    fn bytes(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap().bytes).into_owned()
    }
    fn fail_writes(&self, count: usize) {
        self.0.lock().unwrap().write_failures = count;
    }
    fn fail_flushes(&self, count: usize) {
        self.0.lock().unwrap().flush_failures = count;
    }
}

impl SocketStreamWriter for Output {
    type Buffer = Vec<u8>;
    async fn write(&mut self, bytes: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        cancellation.check()?;
        let mut wire = self.0.lock().unwrap();
        if wire.write_failures > 0 {
            wire.write_failures -= 1;
            return broken();
        }
        wire.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    async fn write_vectored(
        &mut self,
        _: &[IoSlice<'_>],
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unsupported()
    }
    async fn submit(
        &mut self,
        buffer: Vec<u8>,
        _: &Cancellation,
    ) -> Result<(), (base_scheduler::OperationError, Vec<u8>)> {
        Err((io::Error::from(io::ErrorKind::Unsupported).into(), buffer))
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        cancellation.check()?;
        let mut wire = self.0.lock().unwrap();
        if wire.flush_failures > 0 {
            wire.flush_failures -= 1;
            return broken();
        }
        Ok(())
    }
    async fn write_vectored_with_flags(
        &mut self,
        _: &[IoSlice<'_>],
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unsupported()
    }
    async fn write_owned_vectored<B: IoBufSequence>(
        &mut self,
        buffers: B,
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> (OperationResult<usize>, B) {
        (unsupported(), buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn request_writer() -> (RequestWriter<Output>, Output) {
    let output = Output::default();
    (RequestWriter::new(output.clone()), output)
}

fn request(method: Method, body: &[u8]) -> Request<BufferedBody> {
    let mut request = Request::new(method, "/", BufferedBody::from(body.to_vec()));
    request.authority = Some("example".into());
    request
}

fn fields(pairs: &[(&str, &[u8])]) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in pairs {
        fields.add(name, value).unwrap();
    }
    fields
}

fn large_head() -> Request<BufferedBody> {
    let mut request = request(Method::GET, b"");
    request
        .fields
        .add("X-Large", "a".repeat(32 * 1024))
        .unwrap();
    request
}

fn not_active(result: Result<(), Error>) -> bool {
    matches!(result, Err(Error::Invalid("request output is not active")))
}

fn data(bytes: &[u8]) -> BodyFrame<Vec<u8>> {
    BodyFrame::Data {
        data: bytes.to_vec(),
        end: false,
    }
}

#[test]
fn invalid_request_heads_are_rejected_before_any_output() {
    let protocol = || Some(Protocol::new("websocket").unwrap());
    let cases: Vec<(Request<BufferedBody>, &str)> = vec![
        (
            {
                let mut request = request(Method::GET, b"");
                request.version = Some(Version::Http2);
                request
            },
            "unsupported request version",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.fields = fields(&[("Host", b"example"), ("Host", b"example")]);
                request
            },
            "duplicate singleton field",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.authority = None;
                request.fields = fields(&[("Host", b"\xff")]);
                request
            },
            "invalid host",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.authority = None;
                request
            },
            "missing host",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.version = Some(Version::Http10);
                request.protocol = protocol();
                request
            },
            "upgrade requires HTTP/1.1",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.protocol = protocol();
                request.fields = fields(&[("Upgrade", b"websocket"), ("Upgrade", b"websocket")]);
                request
            },
            "duplicate singleton field",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.protocol = protocol();
                request.fields = fields(&[("Upgrade", b"h2c")]);
                request
            },
            "conflicting upgrade protocol",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.fields = fields(&[("Connection", b"upgrade")]);
                request
            },
            "missing upgrade protocol",
        ),
        (
            {
                let mut request = request(Method::CONNECT, b"");
                request.authority = Some("example:443".into());
                request
            },
            "CONNECT cannot have a path",
        ),
        (
            {
                let mut request = request(Method::CONNECT, b"");
                request.path = None;
                request.authority = None;
                request.version = Some(Version::Http10);
                request
            },
            "CONNECT requires authority",
        ),
        (
            {
                let mut request = request(Method::POST, b"abc");
                request.fields = fields(&[("Transfer-Encoding", b"gzip")]);
                request
            },
            "unsupported transfer coding",
        ),
        (
            {
                let mut request = request(Method::POST, b"abc");
                request.content_length = Some(3);
                request.fields = fields(&[("Transfer-Encoding", b"chunked")]);
                request
            },
            "content-length with transfer-encoding",
        ),
        (
            {
                let mut request = request(Method::POST, b"abc");
                request.version = Some(Version::Http10);
                request.fields = fields(&[("Transfer-Encoding", b"chunked")]);
                request
            },
            "HTTP/1.0 transfer-encoding",
        ),
    ];
    block_on(async {
        for (mut request, reason) in cases {
            let (mut writer, output) = request_writer();
            let result = writer
                .write_request(&mut request, &Cancellation::never())
                .await;
            assert!(
                matches!(result, Err(Error::Invalid(actual)) if actual == reason),
                "{reason}: {result:?}"
            );
            assert!(output.bytes().is_empty());
            assert!(writer.is_reusable());
        }
        let (mut writer, output) = request_writer();
        let mut request = request(Method::POST, b"abc");
        request.content_length = Some(5);
        assert!(matches!(
            writer
                .write_request(&mut request, &Cancellation::never())
                .await,
            Err(Error::LengthMismatch)
        ));
        assert!(output.bytes().is_empty());
        let mut metadata = RequestMetadata::new(Method::POST);
        metadata.path = Some("/".into());
        metadata.version = Some(Version::Http10);
        assert!(matches!(
            writer
                .begin_request(&metadata, &Cancellation::never())
                .await,
            Err(Error::Invalid("HTTP/1.0 request requires a body length"))
        ));
        assert!(output.bytes().is_empty());
        assert!(writer.is_reusable());
    });
}

#[test]
fn generated_heads_complete_host_upgrade_connection_and_length_fields() {
    let protocol = || Some(Protocol::new("websocket").unwrap());
    let cases: Vec<(Request<BufferedBody>, bool, &str)> = vec![
        (
            {
                let mut request = request(Method::GET, b"");
                request.fields = fields(&[("Host", b"EXAMPLE")]);
                request
            },
            true,
            "GET / HTTP/1.1\r\nHost: EXAMPLE\r\n\r\n",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.protocol = protocol();
                request
            },
            true,
            "GET / HTTP/1.1\r\nHost: example\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.protocol = protocol();
                request.fields = fields(&[("Connection", b"Upgrade"), ("Upgrade", b"WebSocket")]);
                request
            },
            true,
            "GET / HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: WebSocket\r\nHost: example\r\n\r\n",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.version = Some(Version::Http10);
                request.persistent = Some(true);
                request
            },
            true,
            "GET / HTTP/1.0\r\nHost: example\r\nConnection: keep-alive\r\n\r\n",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.version = Some(Version::Http10);
                request.fields = fields(&[("Connection", b"keep-alive")]);
                request
            },
            true,
            "GET / HTTP/1.0\r\nConnection: keep-alive\r\nHost: example\r\n\r\n",
        ),
        (
            {
                let mut request = request(Method::GET, b"");
                request.version = Some(Version::Http10);
                request
            },
            false,
            "GET / HTTP/1.0\r\nHost: example\r\n\r\n",
        ),
        (
            request(Method::POST, b""),
            true,
            "POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 0\r\n\r\n",
        ),
    ];
    block_on(async {
        for (mut request, persistent, expected) in cases {
            let (mut writer, output) = request_writer();
            assert_eq!(
                writer
                    .write_request(&mut request, &Cancellation::never())
                    .await
                    .unwrap(),
                persistent,
                "{expected}"
            );
            assert_eq!(writer.is_reusable(), persistent);
            assert_eq!(output.bytes(), expected);
        }
    });
}

#[test]
fn request_output_failures_and_cancellation_retire_the_lane() {
    block_on(async {
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let (mut writer, output) = request_writer();
        assert!(matches!(
            writer
                .write_request(&mut request(Method::GET, b""), &cancelled)
                .await,
            Err(Error::Cancelled)
        ));
        assert!(writer.is_reusable());
        let mut close = request(Method::GET, b"");
        close.persistent = Some(false);
        assert!(
            !writer
                .write_request(&mut close, &Cancellation::never())
                .await
                .unwrap()
        );
        assert!(matches!(
            writer
                .write_request(&mut request(Method::GET, b""), &Cancellation::never())
                .await,
            Err(Error::Invalid("output is not reusable"))
        ));
        assert_eq!(
            output.bytes(),
            "GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n"
        );

        // A large head is written directly rather than batched; a small one is sent by the flush.
        let (mut writer, output) = request_writer();
        output.fail_writes(1);
        assert!(matches!(
            writer
                .write_request(&mut large_head(), &Cancellation::never())
                .await,
            Err(Error::Io(_))
        ));
        assert!(!writer.is_reusable());
        let (mut writer, output) = request_writer();
        output.fail_flushes(1);
        assert!(matches!(
            writer
                .write_request(&mut request(Method::GET, b""), &Cancellation::never())
                .await,
            Err(Error::Io(_))
        ));
        assert!(!writer.is_reusable());
    });
}

#[test]
fn failed_request_heads_never_accept_body_frames() {
    block_on(async {
        let metadata = request(Method::POST, b"").metadata;
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let (mut writer, output) = request_writer();
        assert!(matches!(
            writer.begin_request(&metadata, &cancelled).await,
            Err(Error::Cancelled)
        ));
        assert!(writer.is_reusable());
        assert!(not_active(
            writer
                .write_frame(data(b"abc"), &Cancellation::never())
                .await
        ));
        assert!(output.bytes().is_empty());

        // Each transport failure is transient, so frames accepted after the failed head would reach the wire.
        let (mut writer, output) = request_writer();
        output.fail_writes(1);
        assert!(matches!(
            writer
                .begin_request(&large_head().metadata, &Cancellation::never())
                .await,
            Err(Error::Io(_))
        ));
        assert!(not_active(
            writer
                .write_frame(data(b"abc"), &Cancellation::never())
                .await
        ));
        assert!(output.bytes().is_empty());
        let (mut writer, output) = request_writer();
        output.fail_flushes(1);
        assert!(matches!(
            writer
                .begin_request(&metadata, &Cancellation::never())
                .await,
            Err(Error::Io(_))
        ));
        assert!(!writer.is_reusable());
        assert!(not_active(
            writer
                .write_frame(data(b"abc"), &Cancellation::never())
                .await
        ));
        assert!(not_active(
            writer
                .write_frame(BodyFrame::<Vec<u8>>::End, &Cancellation::never())
                .await
        ));
        assert_eq!(
            output.bytes(),
            "POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n"
        );
    });
}

#[test]
fn imperative_request_frames_require_an_active_flushed_body() {
    block_on(async {
        let metadata = request(Method::POST, b"").metadata;
        let (mut writer, output) = request_writer();
        assert!(not_active(
            writer
                .write_frame(data(b"abc"), &Cancellation::never())
                .await
        ));
        writer
            .begin_request(&metadata, &Cancellation::never())
            .await
            .unwrap();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            writer.write_frame(data(b"abc"), &cancelled).await,
            Err(Error::Cancelled)
        ));
        writer
            .write_frame(data(b"abc"), &Cancellation::never())
            .await
            .unwrap();
        writer.flush(&Cancellation::never()).await.unwrap();
        assert!(!writer.is_reusable());
        output.fail_flushes(1);
        assert!(matches!(
            writer
                .write_frame(data(b"de"), &Cancellation::never())
                .await,
            Err(Error::Io(_))
        ));
        assert!(not_active(
            writer.write_frame(data(b"f"), &Cancellation::never()).await
        ));
        assert!(!writer.is_reusable());
        assert_eq!(
            output.bytes(),
            "POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n"
        );

        // A failed explicit flush ends the active body and the lane.
        let (mut writer, output) = request_writer();
        writer
            .begin_request(&metadata, &Cancellation::never())
            .await
            .unwrap();
        output.fail_flushes(1);
        assert!(matches!(
            writer.flush(&Cancellation::never()).await,
            Err(Error::Io(_))
        ));
        assert!(not_active(
            writer
                .write_frame(BodyFrame::<Vec<u8>>::End, &Cancellation::never())
                .await
        ));
        assert!(!writer.is_reusable());
    });
}

fn response_reader(bytes: &'static [u8]) -> ResponseReader<Input> {
    ResponseReader::new(input(bytes, usize::MAX), Options::default()).unwrap()
}

async fn read_error(bytes: &'static [u8], request: &RequestMetadata) -> Error {
    let mut reader = response_reader(bytes);
    let error = match reader
        .read_response(request, &mut observer(), &Cancellation::never())
        .await
    {
        Ok(response) => panic!("accepted {bytes:?}: {:?}", response.metadata),
        Err(error) => error,
    };
    assert!(!reader.is_reusable());
    error
}

#[test]
fn response_readers_validate_their_limits() {
    let options = Options {
        max_fields: 0,
        ..Options::default()
    };
    assert!(matches!(
        ResponseReader::new(input(b"", usize::MAX), options),
        Err(Error::Invalid("invalid HTTP/1 limits"))
    ));
}

#[test]
fn invalid_response_connection_options_and_interim_failures_are_errors() {
    block_on(async {
        let get = RequestMetadata::new(Method::GET);
        for bytes in [
            b"HTTP/1.1 103 Early Hints\r\nConnection: a@b\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nConnection: a@b\r\nContent-Length: 0\r\n\r\n",
        ] {
            assert!(matches!(
                read_error(bytes, &get).await,
                Error::Invalid("invalid connection option")
            ));
        }
        let mut reader = response_reader(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n\r\n");
        let mut interim = InterimResponses::new(|_: ResponseMetadata| {
            std::future::ready(Err::<(), _>(io::Error::other("interim handler failed")))
        });
        let error = reader
            .read_response(&get, &mut interim, &Cancellation::never())
            .await
            .map(|_| ())
            .unwrap_err();
        let Error::Body(source) = error else {
            panic!("unexpected {error:?}");
        };
        assert_eq!(source.to_string(), "interim handler failed");
        assert!(!reader.is_reusable());
    });
}

#[test]
fn switching_protocols_responses_must_select_an_offered_token() {
    block_on(async {
        let mut offer = RequestMetadata::new(Method::GET);
        offer.protocol = Some(Protocol::new("websocket").unwrap());
        for (bytes, reason) in [
            (
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n".as_slice(),
                "invalid upgrade response",
            ),
            (
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\nContent-Length: 0\r\n\r\n",
                "invalid upgrade response",
            ),
            (
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\nUpgrade: websocket\r\n\r\n",
                "duplicate singleton field",
            ),
            (
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\n\r\n",
                "missing selected protocol",
            ),
            (
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: \xff\r\n\r\n",
                "invalid selected protocol",
            ),
        ] {
            let error = read_error(bytes, &offer).await;
            assert!(
                matches!(error, Error::Invalid(actual) if actual == reason),
                "{reason}: {error:?}"
            );
        }

        // Offers may also be made with raw request fields, including forms `Protocol` cannot represent.
        let mut offer = RequestMetadata::new(Method::GET);
        offer.fields = fields(&[("Connection", b"upgrade"), ("Upgrade", b"h2c, h2c/1")]);
        let mut reader = response_reader(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: H2C\r\n\r\nraw",
        );
        let response = reader
            .read_response(&offer, &mut observer(), &Cancellation::never())
            .await
            .unwrap();
        assert_eq!(response.protocol.as_ref().unwrap().as_str(), "H2C");
        drop(response);
        assert!(!reader.is_reusable());
        assert!(matches!(
            read_error(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: h2c/1\r\n\r\n",
                &offer
            )
            .await,
            Error::Invalid("unsupported selected protocol")
        ));
    });
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
