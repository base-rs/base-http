// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod client;
mod input;
mod output;
mod support;

use base_http::{
    Fields, HttpHeaders, InterimHandler, InterimResponses, Method, Request, RequestMetadata,
    Response, ResponseMetadata, Status, Version,
    body::{BodyFrame, BodyReader, BufferedBody, ReadableBody},
};
use base_http1::{Error, Exchange, RequestReader, RequestWriter, ResponseReader, ResponseWriter};
use base_scheduler::{
    BufferLease, Cancellation, IoBufSequence, IoBufSequenceMut, OperationResult, SocketRecvFlags,
    SocketRecvResult, SocketSendFlags, SocketStreamReader, SocketStreamWriter,
};
use futures_lite::future::{block_on, poll_once};
use std::{
    io::{self, IoSlice, IoSliceMut},
    pin::pin,
    sync::{Arc, Mutex},
};

// Explicit collection belongs to the caller; production message reads return
// owned frames without mutating the initial fields.
async fn read_data<B: ReadableBody<Error = Error>>(
    body: &mut B,
    fields: &mut HttpHeaders,
    cancellation: &Cancellation,
) -> Result<Option<B::Data>, Error> {
    loop {
        match body.read(cancellation).await? {
            BodyFrame::Data { data, .. } => return Ok(Some(data)),
            BodyFrame::Headers {
                fields: trailers,
                end,
            } => {
                fields.append_trailers(trailers)?;
                if end {
                    return Ok(None);
                }
            }
            BodyFrame::End => return Ok(None),
        }
    }
}

struct Input {
    bytes: &'static [u8],
    step: usize,
}
fn unsupported<T>() -> OperationResult<T> {
    Err(io::Error::from(io::ErrorKind::Unsupported).into())
}
impl SocketStreamReader for Input {
    type Buffer = BufferLease;
    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<BufferLease>> {
        unsupported()
    }
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        let n = self.bytes.len().min(buffer.len()).min(self.step);
        buffer[..n].copy_from_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(n)
    }
    async fn read_vectored(
        &mut self,
        _: &mut [IoSliceMut<'_>],
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unsupported()
    }
    async fn read_vectored_with_flags(
        &mut self,
        _: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        unsupported()
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        buffers: B,
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        (unsupported(), buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum OutputEvent {
    Write(usize),
    Flush,
    Close,
}

struct Output {
    bytes: Arc<Mutex<Vec<u8>>>,
    step: usize,
    events: Option<Arc<Mutex<Vec<OutputEvent>>>>,
    fail_after: Option<usize>,
    released_read: Option<Arc<std::sync::atomic::AtomicBool>>,
}
impl SocketStreamWriter for Output {
    type Buffer = Vec<u8>;
    async fn write(&mut self, bytes: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        cancellation.check()?;
        if let Some(released) = &self.released_read {
            assert!(released.load(std::sync::atomic::Ordering::Acquire));
        }
        if self
            .fail_after
            .is_some_and(|limit| self.bytes.lock().unwrap().len() >= limit)
        {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        let n = bytes.len().min(self.step);
        self.bytes.lock().unwrap().extend_from_slice(&bytes[..n]);
        if let Some(events) = &self.events {
            events.lock().unwrap().push(OutputEvent::Write(n));
        }
        Ok(n)
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
        if let Some(events) = &self.events {
            events.lock().unwrap().push(OutputEvent::Flush);
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
        if let Some(events) = &self.events {
            events.lock().unwrap().push(OutputEvent::Close);
        }
        Ok(())
    }
}

fn reader(bytes: &'static [u8], step: usize) -> RequestReader<Input> {
    RequestReader::new(Input { bytes, step }, Default::default()).unwrap()
}
fn exchange() -> Exchange {
    exchange_from(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
}
fn exchange_from(bytes: &'static [u8]) -> Exchange {
    let mut input = reader(bytes, 1);
    block_on(input.read_request(&Cancellation::never()))
        .unwrap()
        .unwrap()
        .1
}

#[test]
fn application_mutation_cannot_change_captured_framing_decisions() {
    block_on(async {
        for (bytes, head, connect, version, persistent) in [
            (&b"HEAD /original HTTP/1.0\r\nHost: original\r\nConnection: keep-alive\r\nExpect: 100-continue\r\n\r\n"[..], true, false, Version::Http10, true),
            (&b"CONNECT original:443 HTTP/1.1\r\nHost: original:443\r\nConnection: close\r\n\r\n"[..], false, true, Version::Http11, false),
            (&b"GET /original HTTP/1.0\r\n\r\n"[..], false, false, Version::Http10, false),
        ] {
            let mut input = reader(bytes, usize::MAX);
            let (mut request, exchange) = input.read_request(&Cancellation::never()).await.unwrap().unwrap();
            let mut request_headers = HttpHeaders::new(request.fields.clone());
            let continued = exchange.expects_continue();
            request.method = Method::POST;
            request.version = Some(Version::Http11);
            request.authority = None;
            request.path = None;
            request_headers.clear();
            drop(request);
            assert_eq!(exchange.is_head(), head);
            assert_eq!(exchange.is_connect(), connect);
            assert_eq!(exchange.version(), version);
            assert_eq!(exchange.is_persistent(), persistent);
            assert_eq!(exchange.expects_continue(), continued);
            assert!(exchange.is_complete());
            let (mut output, bytes) = output(3);
            let mut response = Response::new(Status::OK, BufferedBody::from(&b"raw"[..]));
            assert_eq!(output.write_response(&exchange, &mut response, &Cancellation::never()).await.unwrap(), persistent && !connect);
            let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            assert!(bytes.starts_with(if version == Version::Http10 { "HTTP/1.0 200 OK\r\n" } else { "HTTP/1.1 200 OK\r\n" }));
            if head {
                assert!(bytes.ends_with("Content-Length: 3\r\nConnection: keep-alive\r\n\r\n"));
            } else {
                assert!(bytes.ends_with("\r\n\r\nraw"));
                if connect {
                    assert!(!bytes.contains("Content-Length"));
                } else {
                    assert!(bytes.contains("Connection: close\r\n"));
                }
            }
        }
    });
}

#[test]
fn upgrade_selection_uses_original_wire_offers_after_application_mutation() {
    block_on(async {
        for selected in ["ECHO2", "forged"] {
            let mut input = reader(b"GET / HTTP/1.1\r\nHost: original\r\nConnection: upgrade\r\nUpgrade: echo, alternate\r\nUpgrade: echo2\r\nExpect: 100-continue\r\n\r\n", usize::MAX);
            let (mut request, exchange) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            let mut request_headers = HttpHeaders::new(request.fields.clone());
            request.method = Method::HEAD;
            request.version = Some(Version::Http10);
            request_headers.clear();
            request.fields.add("Connection", "upgrade").unwrap();
            request.fields.add("Upgrade", "forged").unwrap();
            drop(request);
            let (mut output, bytes) = output(3);
            let mut response =
                Response::new(Status::SWITCHING_PROTOCOLS, BufferedBody::from(&b"raw"[..]));
            response.protocol = Some(base_http::Protocol::new(selected).unwrap());
            let result = output
                .write_response(&exchange, &mut response, &Cancellation::never())
                .await;
            let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            if selected == "forged" {
                assert!(result.is_err());
                assert!(bytes.is_empty());
            } else {
                assert!(!result.unwrap());
                assert!(bytes.starts_with(
                    "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 101 Switching Protocols\r\n"
                ));
                assert!(bytes.ends_with("Upgrade: ECHO2\r\nConnection: upgrade\r\n\r\nraw"));
            }
        }
    });
}

#[test]
fn switching_with_expect_sends_continue_before_101() {
    block_on(async {
        let exchange = exchange_from(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: echo\r\nExpect: 100-continue\r\n\r\n");
        let (mut output, bytes) = output(3);
        let mut response = Response::empty(Status::SWITCHING_PROTOCOLS);
        response.protocol = Some(base_http::Protocol::new("echo").unwrap());
        assert!(
            !output
                .write_response(&exchange, &mut response, &Cancellation::never())
                .await
                .unwrap()
        );
        let bytes = bytes.lock().unwrap();
        assert!(
            bytes.starts_with(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 101 Switching Protocols\r\n")
        );
    });
}

#[test]
fn upgrade_advertisement_can_appear_in_a_normal_response() {
    block_on(async {
        let (mut output, bytes) = output(3);
        let mut response = Response::empty(Status::new(426).unwrap());
        response.fields.add("Upgrade", "HTTP/2.0").unwrap();
        output
            .write_response(&exchange(), &mut response, &Cancellation::never())
            .await
            .unwrap();
        let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(bytes.contains("Upgrade: HTTP/2.0\r\nConnection: upgrade\r\n"));
        assert!(bytes.ends_with("Content-Length: 0\r\n\r\n"));
    });
}

#[test]
fn reset_content_never_emits_nonempty_data_even_with_unknown_length() {
    block_on(async {
        let (mut output, bytes) = output(3);
        let mut response = Response::new(
            Status::RESET_CONTENT,
            Unknown(BufferedBody::from(b"bad".to_vec())),
        );
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .is_err()
        );
        assert!(!bytes.lock().unwrap().ends_with(b"bad"));
        assert!(!output.is_usable());
    });
}

#[test]
fn every_successful_connect_uses_raw_framing_including_204_and_205() {
    block_on(async {
        for status in [200, 201, 204, 205, 299] {
            let exchange =
                exchange_from(b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n");
            let (mut output, bytes) = output(3);
            let mut response = Response::new(
                Status::new(status).unwrap(),
                BufferedBody::from(b"raw".to_vec()),
            );
            assert!(
                !output
                    .write_response(&exchange, &mut response, &Cancellation::never())
                    .await
                    .unwrap()
            );
            assert!(exchange.is_tunnel());
            let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            assert!(!bytes.contains("Content-Length"));
            assert!(!bytes.contains("Transfer-Encoding"));
            assert!(bytes.ends_with("\r\n\r\nraw"));
        }
    });
}

struct PendingBody(Arc<std::sync::atomic::AtomicBool>);
impl ReadableBody for PendingBody {
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    fn close(&mut self, _: Option<Self::Error>) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        std::future::pending().await
    }
}

#[test]
fn output_cancels_an_uncooperative_body_and_closes_it() {
    block_on(async {
        let (mut output, _) = output(3);
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut response = Response::new(Status::OK, PendingBody(closed.clone()));
        let cancellation = Cancellation::new();
        let exchange = exchange();
        {
            let mut send = pin!(output.write_response(&exchange, &mut response, &cancellation));
            assert!(poll_once(send.as_mut()).await.is_none());
            cancellation.cancel();
            assert!(matches!(send.await, Err(Error::Cancelled)));
        }
        assert!(!output.is_usable());
        assert!(closed.load(std::sync::atomic::Ordering::Acquire));
    });
}
fn output(step: usize) -> (ResponseWriter<Output>, Arc<Mutex<Vec<u8>>>) {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    (
        ResponseWriter::new(Output {
            bytes: bytes.clone(),
            step,
            events: None,
            fail_after: None,
            released_read: None,
        }),
        bytes,
    )
}

#[test]
fn every_short_write_is_retried_in_order() {
    block_on(async {
        let (mut output, bytes) = output(1);
        let mut response = Response::new(Status::OK, BufferedBody::from(b"hello".to_vec()));
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .unwrap()
        );
        assert_eq!(
            *bytes.lock().unwrap(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
        );
    });
}

#[test]
fn generated_framing_does_not_mutate_the_semantic_head_or_truncate_large_lengths() {
    struct Representation;
    impl ReadableBody for Representation {
        type Data = Vec<u8>;
        type Error = base_scheduler::Cancelled;
        fn length(&self) -> Option<u64> {
            Some(u64::MAX)
        }
        async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
            panic!("HEAD and 304 must not read representation data")
        }
        fn close(&mut self, _: Option<Self::Error>) {}
    }
    block_on(async {
        for (request, status) in [
            (&b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n"[..], Status::OK),
            (
                &b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"[..],
                Status::NOT_MODIFIED,
            ),
        ] {
            let (mut output, bytes) = output(3);
            let mut response = Response::new(status, Representation);
            response.fields.add("X-Keep", "original").unwrap();
            let fields = response.fields.pairs().to_vec();
            output
                .write_response(
                    &exchange_from(request),
                    &mut response,
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            assert_eq!(response.fields.pairs(), fields);
            assert_eq!(response.version, None);
            let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            assert!(
                bytes.ends_with("X-Keep: original\r\nContent-Length: 18446744073709551615\r\n\r\n")
            );
        }
    });
}

#[test]
fn known_length_body_trailers_select_chunked_framing_and_stay_out_of_metadata() {
    block_on(async {
        let (mut output, bytes) = output(2);
        let mut response = Response::new(
            Status::OK,
            trailing_body(
                [b"data".to_vec()],
                Fields::try_from_pairs([("x-result", "yes")]).unwrap(),
            ),
        );
        response.fields.add("Content-Type", "text/plain").unwrap();
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .unwrap()
        );
        let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(bytes.contains("Transfer-Encoding: chunked\r\n"));
        assert!(!bytes.contains("Content-Length"));
        assert!(bytes.ends_with("\r\n\r\n4\r\ndata\r\n0\r\nx-result: yes\r\n\r\n"));
        assert_eq!(response.fields.len(), 1);
        assert!(!response.fields.contains("x-result"));
    });
}

struct Unknown(BufferedBody);
impl ReadableBody for Unknown {
    type Data = Vec<u8>;
    type Error = base_scheduler::Cancelled;
    fn close(&mut self, error: Option<Self::Error>) {
        self.0.close(error)
    }
    async fn read(&mut self, cancel: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        self.0.read(cancel).await
    }
}

#[test]
fn too_short_and_too_long_bodies_poison_output() {
    block_on(async {
        for chunk in [b"ab".as_slice(), b"abcd"] {
            let (mut output, _) = output(2);
            let mut response =
                Response::new(Status::OK, Unknown(BufferedBody::from(chunk.to_vec())));
            response.fields.add("Content-Length", "3").unwrap();
            assert!(matches!(
                output
                    .write_response(&exchange(), &mut response, &Cancellation::never())
                    .await,
                Err(Error::LengthMismatch)
            ));
            assert!(!output.is_usable());
        }
    });
}

#[test]
fn conflicting_declared_length_is_rejected_before_any_wire_output() {
    block_on(async {
        let (mut output, bytes) = output(2);
        let mut response = Response::new(Status::OK, BufferedBody::from(b"hello".to_vec()));
        response.fields.add("Content-Length", "3").unwrap();
        assert!(matches!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await,
            Err(Error::LengthMismatch)
        ));
        assert!(bytes.lock().unwrap().is_empty());
    });
}

#[test]
fn zero_write_progress_is_an_error() {
    block_on(async {
        let (mut output, _) = output(0);
        let mut response = Response::empty(Status::OK);
        assert!(
            matches!(output.write_response(&exchange(),&mut response,&Cancellation::never()).await,Err(Error::Io(e)) if e.kind() == io::ErrorKind::WriteZero)
        );
        assert!(!output.is_usable());
    });
}

#[test]
fn abandoning_started_output_prevents_further_responses() {
    block_on(async {
        let (mut output, _) = output(2);
        let (body, _writer) = BodyReader::<Vec<u8>>::bounded(16, 1);
        let mut response = Response::new(Status::OK, body);
        let exchange = exchange();
        let never = Cancellation::never();
        {
            let mut send = pin!(output.write_response(&exchange, &mut response, &never));
            assert!(poll_once(send.as_mut()).await.is_none());
        }
        assert!(!output.is_usable());
        assert!(
            output
                .write_response(
                    &exchange,
                    &mut Response::empty(Status::OK),
                    &Cancellation::never()
                )
                .await
                .is_err()
        );
    });
}

#[test]
fn incoming_fixed_and_chunked_bodies_leave_the_next_request_intact_at_every_fragment_size() {
    for step in 1..80 {
        block_on(async {
            let mut input = reader(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcPOST /two HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nde\r\n0\r\nx-tail: yes\r\n\r\nGET /three HTTP/1.1\r\nHost: x\r\n\r\n",step);
            for expected in [b"abc".as_slice(), b"de", b""] {
                let (mut request, _) = input
                    .read_request(&Cancellation::never())
                    .await
                    .unwrap()
                    .unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                let mut data = Vec::new();
                while let Some(chunk) = read_data(
                    &mut request.body,
                    &mut request_headers,
                    &Cancellation::never(),
                )
                .await
                .unwrap()
                {
                    data.extend(chunk);
                }
                assert_eq!(data, expected);
                if expected == b"de" {
                    assert_eq!(request_headers.trailers()[0].value(), b"yes");
                }
            }
            assert!(
                input
                    .read_request(&Cancellation::never())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }
}

/// Read up to three requests, describing each outcome.
fn outcomes(bytes: &'static [u8], step: usize, max_head_bytes: usize) -> Vec<String> {
    let options = base_http1::Options {
        max_head_bytes,
        read_size: 4096,
        ..Default::default()
    };
    let mut input = RequestReader::new(Input { bytes, step }, options).unwrap();
    let mut outcomes = Vec::new();
    block_on(async {
        for _ in 0..3 {
            match input.read_request(&Cancellation::never()).await {
                Ok(Some((request, _))) => outcomes.push(format!(
                    "{:?} {:?} {:?} {:?}",
                    request.method, request.path, request.authority, request.fields
                )),
                Ok(None) => return outcomes.push("end".into()),
                Err(error) => return outcomes.push(format!("{error:?}")),
            }
        }
    });
    outcomes
}

#[test]
fn request_heads_are_read_identically_at_every_fragment_size_and_head_limit() {
    let streams: &[&'static [u8]] = &[
        b"GET / HTTP/1.1\r\nHost: h\r\n\r\n",
        b"GET /a HTTP/1.1\r\nHost: example.com\r\nUser-Agent: Mozilla/5.0 (X11; Linux x86_64)\r\nAccept: */*\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nX: \xff\xfe\r\n\r\nGET / HTTP/1.0\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x00b\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\n\r",
        b"GET / HTTP/1.1\r\nHost: h\r\n\r\nGET / HTTP/1.1\r\nHost",
        b"",
    ];
    for stream in streams {
        for max_head_bytes in [16, 26, 27, 28, 29, 60, 105, 106, 1000] {
            let expected = outcomes(stream, usize::MAX, max_head_bytes);
            for step in 1..stream.len() {
                let fragmented = outcomes(stream, step, max_head_bytes);
                // An invalid byte is reported as soon as it is read, while a
                // fragmented head may instead end or reach the limit first.
                let last = expected.len() - 1;
                assert_eq!(fragmented.len(), expected.len(), "{stream:?} {step}");
                assert_eq!(fragmented[..last], expected[..last], "{stream:?} {step}");
                if !expected[last].starts_with("Invalid") {
                    assert_eq!(fragmented[last], expected[last], "{stream:?} {step}");
                }
            }
        }
    }
    let head: &'static [u8] = b"GET / HTTP/1.1\r\nHost: h\r\n\r\n";
    assert_eq!(
        outcomes(head, usize::MAX, head.len()),
        [
            "Method(\"GET\") Some(Path(\"/\")) Some(Authority(\"h\")) Fields { pairs: [] }",
            "end"
        ]
    );
    assert_eq!(
        outcomes(head, usize::MAX, head.len() - 1)[0],
        "Limit(\"message head\")"
    );
    assert_eq!(
        outcomes(b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x00b", usize::MAX, 1000),
        ["Invalid(\"invalid field value\")"]
    );
}

#[test]
fn failed_body_reads_prevent_reuse_even_if_the_body_is_dropped() {
    block_on(async {
        let mut input = reader(
            b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\na",
            64,
        );
        {
            let (mut request, _) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            let mut request_headers = HttpHeaders::new(request.fields.clone());
            assert_eq!(
                read_data(
                    &mut request.body,
                    &mut request_headers,
                    &Cancellation::never()
                )
                .await
                .unwrap(),
                Some(b"a".to_vec())
            );
            assert!(matches!(
                read_data(
                    &mut request.body,
                    &mut request_headers,
                    &Cancellation::never()
                )
                .await,
                Err(Error::Truncated)
            ));
        }
        assert!(!input.is_reusable());
        assert!(input.read_request(&Cancellation::never()).await.is_err());
    });
}

#[test]
fn request_target_forms_reconstruct_semantic_components() {
    block_on(async {
        for (wire, scheme, authority, path) in [
            (
                b"GET http://example:80?x=1 HTTP/1.1\r\nHost: ignored\r\n\r\n".as_slice(),
                Some("http"),
                "example:80",
                Some("/?x=1"),
            ),
            (
                b"GET HTTPS://Example/a HTTP/1.1\r\nHost: ignored\r\n\r\n",
                Some("https"),
                "example",
                Some("/a"),
            ),
            (
                b"GET /a-path-longer-than-the-inline-capacity HTTP/1.1\r\nHost: A-Host-Longer-Than-Inline.Example:8080\r\n\r\n",
                None,
                "a-host-longer-than-inline.example:8080",
                Some("/a-path-longer-than-the-inline-capacity"),
            ),
            (
                b"CONNECT [::1]:443 HTTP/1.1\r\nHost: [::1]:443\r\n\r\n",
                None,
                "[::1]:443",
                None,
            ),
            (
                b"OPTIONS * HTTP/1.1\r\nHost: example\r\n\r\n",
                None,
                "example",
                Some("*"),
            ),
        ] {
            let mut input = reader(wire, 3);
            let (request, _) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.scheme.as_deref(), scheme);
            assert_eq!(request.authority.as_deref(), Some(authority));
            assert_eq!(request.path.as_deref(), path);
        }
    });
}

#[test]
fn precancelled_input_does_not_consume_the_request() {
    block_on(async {
        let mut input = reader(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", 1);
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(matches!(
            input.read_request(&cancelled).await,
            Err(Error::Cancelled)
        ));
        assert!(
            input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn custom_field_policy_cannot_allow_framing_fields_in_wire_trailers() {
    block_on(async {
        let mut policy = base_http::headers::Policy::default();
        policy
            .set(
                "content-length",
                base_http::headers::FieldPolicy::new(
                    base_http::headers::Combination::Singleton,
                    true,
                ),
            )
            .unwrap();
        let mut headers = HttpHeaders::with_policy(Fields::new(), policy);
        headers
            .append_trailers(Fields::try_from_pairs([("content-length", "1")]).unwrap())
            .unwrap();
        let (_, trailers) = headers.into_parts();
        let mut response = Response::new(Status::OK, trailing_body([], trailers.unwrap()));
        let (mut output, bytes) = output(10);
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .is_err()
        );
        assert!(!output.is_usable());
        assert!(!String::from_utf8_lossy(&bytes.lock().unwrap()).contains("content-length: 1"));
    });
}

fn client_output(step: usize) -> (RequestWriter<Output>, Arc<Mutex<Vec<u8>>>) {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    (
        RequestWriter::new(Output {
            bytes: bytes.clone(),
            step,
            events: None,
            fail_after: None,
            released_read: None,
        }),
        bytes,
    )
}

fn response_reader(bytes: &'static [u8], step: usize) -> ResponseReader<Input> {
    ResponseReader::new(Input { bytes, step }, Default::default()).unwrap()
}

fn observer() -> InterimResponses<impl InterimHandler> {
    InterimResponses::new(|_: ResponseMetadata| {
        std::future::ready(Ok::<(), std::convert::Infallible>(()))
    })
}

#[test]
fn client_requests_retry_short_writes_and_preserve_boundaries() {
    for step in 1..20 {
        block_on(async {
            let (mut output, bytes) = client_output(step);
            let mut request = Request::new(
                Method::POST,
                "/one?q=1",
                BufferedBody::from(b"abc".to_vec()),
            );
            request.authority = Some("example:8080".into());
            assert!(
                output
                    .write_request(&mut request, &Cancellation::never())
                    .await
                    .unwrap()
            );
            let mut second = Request::get("/two");
            second.authority = Some("example:8080".into());
            second.fields.add("Connection", "close").unwrap();
            assert!(
                !output
                    .write_request(&mut second, &Cancellation::never())
                    .await
                    .unwrap()
            );
            assert!(!output.is_reusable());
            assert_eq!(*bytes.lock().unwrap(), b"POST /one?q=1 HTTP/1.1\r\nHost: example:8080\r\nContent-Length: 3\r\n\r\nabcGET /two HTTP/1.1\r\nConnection: close\r\nHost: example:8080\r\n\r\n");
        });
    }
}

#[test]
fn client_requests_frame_streams_and_static_trailers() {
    block_on(async {
        let (mut output, bytes) = client_output(1);
        let mut request = Request::new(
            Method::POST,
            "/",
            Unknown(BufferedBody::from(b"abc".to_vec())),
        );
        request.authority = Some("example".into());
        output
            .write_request(&mut request, &Cancellation::never())
            .await
            .unwrap();
        let mut second = Request::new(
            Method::POST,
            "/two",
            trailing_body(
                [b"de".to_vec()],
                Fields::try_from_pairs([("x-tail", "yes")]).unwrap(),
            ),
        );
        second.authority = Some("example".into());
        output
            .write_request(&mut second, &Cancellation::never())
            .await
            .unwrap();
        let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(bytes.contains("Transfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\nPOST /two"));
        assert!(
            bytes
                .ends_with("Transfer-Encoding: chunked\r\n\r\n2\r\nde\r\n0\r\nx-tail: yes\r\n\r\n")
        );
    });
}

#[test]
fn client_rejects_invalid_heads_before_sending_and_bad_lengths_after_progress() {
    block_on(async {
        for target in ["/\r\nInjected: value", "/a b", "/#fragment", "é", ""] {
            let (mut output, bytes) = client_output(2);
            let mut request = Request::get(target);
            request.authority = Some("example".into());
            assert!(
                output
                    .write_request(&mut request, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(bytes.lock().unwrap().is_empty());
            assert!(output.is_reusable());
        }
        for data in [b"ab".as_slice(), b"abcd"] {
            let (mut output, _) = client_output(2);
            let mut request = Request::new(
                Method::POST,
                "/",
                Unknown(BufferedBody::from(data.to_vec())),
            );
            request.authority = Some("example".into());
            request.fields.add("Content-Length", "3").unwrap();
            assert!(matches!(
                output
                    .write_request(&mut request, &Cancellation::never())
                    .await,
                Err(Error::LengthMismatch)
            ));
            assert!(!output.is_reusable());
        }
        let (mut output, bytes) = client_output(2);
        let mut request = Request::new(
            Method::POST,
            "/",
            Unknown(BufferedBody::from(b"abc".to_vec())),
        );
        request.version = Some(Version::Http10);
        assert!(
            output
                .write_request(&mut request, &Cancellation::never())
                .await
                .is_err()
        );
        assert!(bytes.lock().unwrap().is_empty());
        let mut request = Request::get("/");
        request.authority = Some("example".into());
        request.fields.add("Host", "different").unwrap();
        assert!(
            output
                .write_request(&mut request, &Cancellation::never())
                .await
                .is_err()
        );
        assert!(bytes.lock().unwrap().is_empty());
    });
}

#[test]
fn client_responses_preserve_interims_trailers_and_boundaries_at_every_fragment_size() {
    for step in 1..200 {
        block_on(async {
            let mut input = response_reader(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nx-tail: yes\r\n\r\nHTTP/1.1 204 No Content\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nHTTP/1.1 304 Not Modified\r\nContent-Length: 12\r\n\r\nHTTP/1.1 205 Reset Content\r\nContent-Length: 0\r\n\r\n", step);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let capture = seen.clone();
            let mut interim = InterimResponses::new(move |head: ResponseMetadata| {
                capture.lock().unwrap().push(head.status);
                std::future::ready(Ok::<(), std::convert::Infallible>(()))
            });
            {
                let mut response = input
                    .read_response(
                        &RequestMetadata::new(Method::GET),
                        &mut interim,
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert!(interim.is_finished());
                assert_eq!(
                    *seen.lock().unwrap(),
                    [Status::CONTINUE, Status::EARLY_HINTS]
                );
                let mut data = Vec::new();
                while let Some(chunk) = read_data(
                    &mut response.body,
                    &mut response_headers,
                    &Cancellation::never(),
                )
                .await
                .unwrap()
                {
                    data.extend(chunk);
                }
                assert_eq!(data, b"abc");
                assert_eq!(response_headers.trailers()[0].value(), b"yes");
            }
            assert!(input.is_reusable());
            for (method, status, length) in [
                (Method::GET, 204, Some(0)),
                (Method::HEAD, 200, Some(7)),
                (Method::GET, 304, Some(12)),
                (Method::GET, 205, Some(0)),
            ] {
                let mut response = input
                    .read_response(
                        &RequestMetadata::new(method),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert_eq!(response.status.as_u16(), status);
                assert_eq!(response.body.length(), length);
                assert!(
                    read_data(
                        &mut response.body,
                        &mut response_headers,
                        &Cancellation::never()
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
            }
            assert!(input.is_reusable());
        });
    }
}

#[test]
fn client_close_delimited_responses_and_close_fields_disable_reuse() {
    block_on(async {
        for wire in [
            b"HTTP/1.0 200 OK\r\n\r\nabc".as_slice(),
            b"HTTP/1.1 200 OK\r\n\r\nabc",
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 3\r\n\r\nabc",
        ] {
            let mut input = response_reader(wire, 2);
            {
                let mut response = input
                    .read_response(
                        &RequestMetadata::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                let mut data = Vec::new();
                while let Some(chunk) = read_data(
                    &mut response.body,
                    &mut response_headers,
                    &Cancellation::never(),
                )
                .await
                .unwrap()
                {
                    data.extend(chunk);
                }
                assert_eq!(data, b"abc");
            }
            assert!(!input.is_reusable());
            assert!(
                input
                    .read_response(
                        &RequestMetadata::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never()
                    )
                    .await
                    .is_err()
            );
        }
    });
}

#[test]
fn client_rejects_malformed_and_ambiguous_response_heads() {
    block_on(async {
        for wire in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",
            b"HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"HTTP/1.1 204 No Content\r\nContent-Length: 1\r\n\r\n",
            b"HTTP/1.1 100 Continue\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.0 100 Continue\r\n\r\n",
            b" HTTP/1.1 200 OK\r\n\r\n",
            b"HTTP/1.1 200 OK\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"",
        ] {
            let mut input = response_reader(wire, 1);
            assert!(
                input
                    .read_response(
                        &RequestMetadata::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never()
                    )
                    .await
                    .is_err(),
                "accepted {wire:?}"
            );
            assert!(!input.is_reusable());
        }
    });
}

#[test]
fn client_truncated_invalid_and_abandoned_response_bodies_disable_reuse() {
    block_on(async {
        for wire in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\na".as_slice(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\na",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nabXX",
            b"HTTP/1.1 205 Reset Content\r\nContent-Length: 1\r\n\r\nx",
        ] {
            let mut input = response_reader(wire, 1);
            {
                let mut response = input
                    .read_response(
                        &RequestMetadata::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                loop {
                    match read_data(
                        &mut response.body,
                        &mut response_headers,
                        &Cancellation::never(),
                    )
                    .await
                    {
                        Ok(Some(_)) => {}
                        Ok(None) => panic!("accepted incomplete or forbidden body: {wire:?}"),
                        Err(_) => break,
                    }
                }
            }
            assert!(!input.is_reusable());
        }
        let mut input = response_reader(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", 64);
        drop(
            input
                .read_response(
                    &RequestMetadata::new(Method::GET),
                    &mut observer(),
                    &Cancellation::never(),
                )
                .await
                .unwrap(),
        );
        assert!(!input.is_reusable());
    });
}

#[test]
fn client_switch_responses_are_final_and_never_reusable() {
    block_on(async {
        for wire in [
            b"HTTP/1.1 200 OK\r\n\r\nraw".as_slice(),
            b"HTTP/1.1 204 No Content\r\nContent-Length: invalid-and-ignored\r\nTransfer-Encoding: ignored\r\n\r\nraw",
            b"HTTP/1.1 205 Reset Content\r\n\r\nraw",
        ] {
            let mut input = response_reader(wire, 1);
            {
                let mut response = input.read_response(&RequestMetadata::new(Method::CONNECT), &mut observer(), &Cancellation::never()).await.unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                let mut data = Vec::new();
                while let Some(chunk) = read_data(&mut response.body, &mut response_headers, &Cancellation::never()).await.unwrap() { data.extend(chunk); }
                assert_eq!(data, b"raw");
                assert_eq!(response.body.length(), None);
            }
            assert!(!input.is_reusable());
        }
        let mut input = response_reader(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\nraw",
            1,
        );
        let mut request = RequestMetadata::new(Method::GET);
        request.protocol = Some(base_http::Protocol::new("ECHO").unwrap());
        let mut interim = observer();
        {
            let mut response = input
                .read_response(&request, &mut interim, &Cancellation::never())
                .await
                .unwrap();
            let mut response_headers = HttpHeaders::new(response.fields.clone());
            assert_eq!(response.protocol.as_ref().unwrap().as_str(), "echo");
            assert!(interim.is_finished());
            let mut data = Vec::new();
            while let Some(chunk) = read_data(
                &mut response.body,
                &mut response_headers,
                &Cancellation::never(),
            )
            .await
            .unwrap()
            {
                data.extend(chunk);
            }
            assert_eq!(data, b"raw");
        }
        assert!(!input.is_reusable());
        let mut input = response_reader(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: other\r\n\r\n",
            1,
        );
        assert!(
            input
                .read_response(&request, &mut observer(), &Cancellation::never())
                .await
                .is_err()
        );
    });
}

#[test]
fn client_cancellation_closes_request_body_and_poisons_started_response_input() {
    block_on(async {
        let (mut output, _) = client_output(2);
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut request = Request::new(Method::POST, "/", PendingBody(closed.clone()));
        request.authority = Some("example".into());
        let cancel = Cancellation::new();
        {
            let mut send = pin!(output.write_request(&mut request, &cancel));
            assert!(poll_once(send.as_mut()).await.is_none());
            cancel.cancel();
            assert!(matches!(send.await, Err(Error::Cancelled)));
        }
        assert!(closed.load(std::sync::atomic::Ordering::Acquire));
        assert!(!output.is_reusable());
        let mut input = response_reader(b"HTTP/1.1 100 Continue\r\n\r\n", 1);
        let cancel = Cancellation::new();
        let mut interim = InterimResponses::new(|_: ResponseMetadata| {
            std::future::pending::<Result<(), io::Error>>()
        });
        let request = RequestMetadata::new(Method::GET);
        {
            let mut receive = pin!(input.read_response(&request, &mut interim, &cancel));
            assert!(poll_once(receive.as_mut()).await.is_none());
            cancel.cancel();
            assert!(matches!(receive.await, Err(Error::Cancelled)));
        }
        assert!(!input.is_reusable());
        assert!(interim.is_finished());
        let mut input = response_reader(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", 1);
        assert!(matches!(
            input
                .read_response(&request, &mut observer(), &cancel)
                .await,
            Err(Error::Cancelled)
        ));
        assert!(input.is_reusable());
        assert!(
            input
                .read_response(&request, &mut observer(), &Cancellation::never())
                .await
                .is_ok()
        );
    });
}

#[test]
fn early_response_allows_active_upload_but_reuse_requires_its_completion() {
    block_on(async {
        let mut input = reader(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET /again HTTP/1.1\r\nHost: x\r\n\r\n", 1);
        let (mut output, bytes) = output(1);
        {
            let (mut request, exchange) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            let mut request_headers = HttpHeaders::new(request.fields.clone());
            assert!(!exchange.is_complete());
            assert!(!exchange.is_abandoned());
            assert!(
                output
                    .write_response(
                        &exchange,
                        &mut Response::empty(Status::OK),
                        &Cancellation::never()
                    )
                    .await
                    .unwrap()
            );
            assert!(!String::from_utf8_lossy(&bytes.lock().unwrap()).contains("Connection: close"));
            while read_data(
                &mut request.body,
                &mut request_headers,
                &Cancellation::never(),
            )
            .await
            .unwrap()
            .is_some()
            {}
            assert!(exchange.is_complete());
        }
        assert!(input.is_reusable());
        assert!(
            input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn static_response_head_and_body_share_one_transport_write() {
    block_on(async {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut output = ResponseWriter::new(Output {
            bytes: bytes.clone(),
            step: usize::MAX,
            events: Some(events.clone()),
            fail_after: None,
            released_read: None,
        });
        for _ in 0..2 {
            let mut response = Response::new(Status::OK, BufferedBody::from(&b"hello"[..]));
            assert!(
                output
                    .write_response(&exchange(), &mut response, &Cancellation::never())
                    .await
                    .unwrap()
            );
        }
        let expected = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        assert_eq!(*bytes.lock().unwrap(), expected.repeat(2));
        assert_eq!(
            *events.lock().unwrap(),
            [
                OutputEvent::Write(expected.len()),
                OutputEvent::Flush,
                OutputEvent::Write(expected.len()),
                OutputEvent::Flush,
            ]
        );
    });
}

struct ReadyButSuspendingBody {
    reads: usize,
    gate: Arc<std::sync::atomic::AtomicBool>,
    completed_waits: Arc<std::sync::atomic::AtomicUsize>,
}
impl ReadableBody for ReadyButSuspendingBody {
    type Data = &'static [u8];
    type Error = std::convert::Infallible;
    fn is_ready(&self) -> bool {
        true
    }
    fn close(&mut self, _: Option<Self::Error>) {}
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Self::Data>, Self::Error> {
        self.reads += 1;
        match self.reads {
            1 => Ok(BodyFrame::Data {
                data: b"one",
                end: false,
            }),
            2 => {
                struct Guard(Arc<std::sync::atomic::AtomicUsize>);
                impl Drop for Guard {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                let _guard = Guard(self.completed_waits.clone());
                std::future::poll_fn(|_| {
                    if self.gate.load(std::sync::atomic::Ordering::SeqCst) {
                        std::task::Poll::Ready(())
                    } else {
                        std::task::Poll::Pending
                    }
                })
                .await;
                Ok(BodyFrame::Data {
                    data: b"two",
                    end: false,
                })
            }
            _ => Ok(BodyFrame::End),
        }
    }
}

#[test]
fn actual_body_suspension_flushes_ready_data_and_keeps_the_started_read() {
    block_on(async {
        let (mut output, bytes) = output(usize::MAX);
        let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut response = Response::new(
            Status::OK,
            ReadyButSuspendingBody {
                reads: 0,
                gate: gate.clone(),
                completed_waits: waits.clone(),
            },
        );
        let exchange = exchange();
        let never = Cancellation::never();
        {
            let mut send = pin!(output.write_response(&exchange, &mut response, &never));
            assert!(poll_once(send.as_mut()).await.is_none());
            assert_eq!(
                *bytes.lock().unwrap(),
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n"
            );
            assert_eq!(waits.load(std::sync::atomic::Ordering::SeqCst), 0);
            gate.store(true, std::sync::atomic::Ordering::SeqCst);
            assert!(send.await.unwrap());
        }
        assert_eq!(response.body.reads, 3);
        assert_eq!(waits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(bytes.lock().unwrap().ends_with(b"3\r\ntwo\r\n0\r\n\r\n"));
    });
}

#[test]
fn failed_batched_write_is_never_replayed_on_close_or_next_response() {
    block_on(async {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let mut output = ResponseWriter::new(Output {
            bytes: bytes.clone(),
            step: 3,
            events: None,
            fail_after: Some(3),
            released_read: None,
        });
        let mut response = Response::new(Status::OK, BufferedBody::from(&b"hello"[..]));
        assert!(
            matches!(output.write_response(&exchange(), &mut response, &Cancellation::never()).await,
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::BrokenPipe)
        );
        assert!(!output.is_usable());
        assert!(
            output
                .write_response(
                    &exchange(),
                    &mut Response::empty(Status::OK),
                    &Cancellation::never()
                )
                .await
                .is_err()
        );
        output.close().await.unwrap();
        assert_eq!(*bytes.lock().unwrap(), b"HTT");
    });
}

#[test]
fn large_heads_chunks_and_trailers_preserve_every_byte_across_batch_boundaries() {
    block_on(async {
        let payload = vec![b'x'; 70_000];
        let mut response = Response::new(
            Status::OK,
            trailing_body(
                [vec![b'a'; 12_000], payload.clone(), vec![b'b'; 12_000]],
                Fields::try_from_pairs([("X-Trailer", vec![b't'; 40_000])]).unwrap(),
            ),
        );
        response.fields.add("X-Large", vec![b'h'; 40_000]).unwrap();
        let (mut output, bytes) = output(1031);
        output
            .write_response(&exchange(), &mut response, &Cancellation::never())
            .await
            .unwrap();
        let mut expected = b"HTTP/1.1 200 OK\r\nX-Large: ".to_vec();
        expected.extend(vec![b'h'; 40_000]);
        expected.extend_from_slice(b"\r\nTransfer-Encoding: chunked\r\n\r\n2ee0\r\n");
        expected.extend(vec![b'a'; 12_000]);
        expected.extend_from_slice(b"\r\n11170\r\n");
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n2ee0\r\n");
        expected.extend(vec![b'b'; 12_000]);
        expected.extend_from_slice(b"\r\n0\r\nX-Trailer: ");
        expected.extend(vec![b't'; 40_000]);
        expected.extend_from_slice(b"\r\n\r\n");
        assert_eq!(*bytes.lock().unwrap(), expected);
    });
}

#[test]
fn configured_field_limits_work_below_and_above_stack_capacity_in_both_directions() {
    block_on(async {
        for count in [99, 100, 101, 120] {
            let fields = "X-Test: value\r\n".repeat(count - 1);
            let request: &'static [u8] = Box::leak(
                format!("GET / HTTP/1.1\r\nHost: x\r\n{fields}\r\n")
                    .into_bytes()
                    .into_boxed_slice(),
            );
            let response: &'static [u8] = Box::leak(
                format!("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n{fields}\r\n")
                    .into_bytes()
                    .into_boxed_slice(),
            );
            for max_fields in [count - 1, count] {
                let options = base_http1::Options {
                    max_fields,
                    ..Default::default()
                };
                let mut input = RequestReader::new(
                    Input {
                        bytes: request,
                        step: usize::MAX,
                    },
                    options.clone(),
                )
                .unwrap();
                let result = input.read_request(&Cancellation::never()).await;
                if max_fields == count {
                    assert_eq!(result.unwrap().unwrap().0.fields.len(), count - 1);
                } else {
                    assert!(matches!(result, Err(Error::Limit("field count"))));
                }
                let mut input = ResponseReader::new(
                    Input {
                        bytes: response,
                        step: usize::MAX,
                    },
                    options,
                )
                .unwrap();
                let result = input
                    .read_response(
                        &RequestMetadata::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await;
                if max_fields == count {
                    assert_eq!(result.unwrap().fields.len(), count - 1);
                } else {
                    assert!(matches!(result, Err(Error::Limit("field count"))));
                }
            }
        }
    });
}

struct ManyReadyChunks {
    remaining: usize,
    closed: bool,
}
impl ReadableBody for ManyReadyChunks {
    type Data = &'static [u8];
    type Error = std::convert::Infallible;
    fn is_ready(&self) -> bool {
        true
    }
    fn close(&mut self, _: Option<Self::Error>) {
        self.closed = true;
    }
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Self::Data>, Self::Error> {
        if self.remaining == 0 {
            std::future::pending().await
        } else {
            self.remaining -= 1;
            Ok(BodyFrame::Data {
                data: b"x",
                end: false,
            })
        }
    }
}

#[test]
fn many_ready_chunks_are_sent_in_bounded_batches_before_body_completion() {
    block_on(async {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut output = ResponseWriter::new(Output {
            bytes: bytes.clone(),
            step: usize::MAX,
            events: Some(events.clone()),
            fail_after: None,
            released_read: None,
        });
        let mut response = Response::new(
            Status::OK,
            ManyReadyChunks {
                remaining: 10_000,
                closed: false,
            },
        );
        let exchange = exchange();
        let cancellation = Cancellation::new();
        {
            let mut send = pin!(output.write_response(&exchange, &mut response, &cancellation));
            assert!(poll_once(send.as_mut()).await.is_none());
            let mut expected = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            expected.extend(b"1\r\nx\r\n".repeat(10_000));
            assert_eq!(*bytes.lock().unwrap(), expected);
            let writes: Vec<usize> = events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| {
                    if let OutputEvent::Write(size) = event {
                        Some(*size)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(writes.len() >= 4 && writes.len() < 16);
            assert!(writes.iter().all(|size| *size <= 16 * 1024));
            cancellation.cancel();
            assert!(matches!(send.await, Err(Error::Cancelled)));
        }
        assert!(response.body.closed);
        assert!(!output.is_usable());
        let sent = bytes.lock().unwrap().len();
        output.close().await.unwrap();
        assert_eq!(bytes.lock().unwrap().len(), sent);
    });
}

#[test]
fn fused_request_facts_preserve_repeated_tokens_and_upgrade_offers() {
    block_on(async {
        let bytes = b"GET / HTTP/1.1\r\nhOsT: example\r\nConnection: keep-alive\r\ncOnNeCtIoN: ClOsE, UpGrAdE\r\nExpect: other\r\neXpEcT: 100-CoNtInUe\r\nUpgrade: first\r\nuPgRaDe: second\r\nContent-Length: 0\r\n\r\n";
        for step in 1..=bytes.len() {
            let mut input = reader(bytes, step);
            let (request, exchange) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.fields.len(), 2);
            assert_eq!(request.authority.as_deref(), Some("example"));
            assert_eq!(request.content_length, Some(0));
            assert_eq!(request.fields.pairs()[0].name(), "Expect");
            assert!(!exchange.is_persistent());
            assert!(exchange.expects_continue());
            assert!(exchange.is_complete());
            drop(request);
            let (mut output, wire) = output(usize::MAX);
            let mut response = Response::empty(Status::SWITCHING_PROTOCOLS);
            response.protocol = Some(base_http::Protocol::new("second").unwrap());
            assert!(
                !output
                    .write_response(&exchange, &mut response, &Cancellation::never())
                    .await
                    .unwrap()
            );
            assert!(
                wire.lock().unwrap().starts_with(
                    b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 101 Switching Protocols\r\n"
                )
            );
            assert!(exchange.is_tunnel());
            assert!(!input.is_reusable());
        }
        let mut input = reader(b"GET / HTTP/1.0\r\nConnection: upgrade\r\n\r\n", 1);
        assert!(
            input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn fused_request_facts_reject_ambiguous_framing_and_invalid_lines() {
    block_on(async {
        let cases: &[&[u8]] = &[
            b"GET / HTTP/1.1\r\nHost: x\r\nhOsT: x\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\ncOnTeNt-LeNgTh: 0\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\ntRaNsFeR-EnCoDiNg: chunked\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 18446744073709551616\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: +1\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n",
            b"GET / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\n\r\n",
            b"GET / HTTP/1.1\nHost: x\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\nX: y\r\n\r\n",
            b" GET / HTTP/1.1\r\nHost: x\r\n\r\n",
        ];
        for &bytes in cases {
            for step in 1..=bytes.len() {
                let mut input = reader(bytes, step);
                assert!(
                    input.read_request(&Cancellation::never()).await.is_err(),
                    "{bytes:?} at {step}"
                );
                assert!(!input.is_reusable());
            }
        }
    });
}

#[test]
fn empty_server_input_and_chunked_completion_keep_exchange_clones_consistent() {
    block_on(async {
        let cases: &[(&[u8], bool)] = &[
            (b"GET / HTTP/1.1\r\nHost: x\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n", true),
            (b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n", true),
            (b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n", false),
        ];
        for &(bytes, complete) in cases {
            for step in [1, 3, usize::MAX] {
                let mut input = reader(bytes, step);
                let (mut request, exchange) = input
                    .read_request(&Cancellation::never())
                    .await
                    .unwrap()
                    .unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                let copy = exchange.clone();
                assert_eq!(copy.is_complete(), complete);
                assert!(!copy.is_tunnel());
                assert!(!copy.is_abandoned());
                request.method = Method::CONNECT;
                request.fields.set("Connection", "close, upgrade").unwrap();
                request.fields.add("Upgrade", "forged").unwrap();
                assert!(!copy.is_connect());
                assert!(copy.is_persistent());
                assert!(
                    read_data(
                        &mut request.body,
                        &mut request_headers,
                        &Cancellation::never()
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
                drop(request);
                assert!(copy.is_complete());
                assert!(input.is_reusable());
                let (request, _) = input
                    .read_request(&Cancellation::never())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(request.path.as_deref(), Some("/next"));
            }
        }
    });
}

#[test]
fn client_get_responses_still_track_live_input_and_abandonment() {
    block_on(async {
        for bytes in [
            &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"[..],
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\n\r\nabc"[..],
        ] {
            for consume in [false, true] {
                let mut input = response_reader(bytes, 1);
                let request = RequestMetadata::new(Method::GET);
                let mut response = input
                    .read_response(&request, &mut observer(), &Cancellation::never())
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert!(!response.body.is_empty());
                if consume {
                    let mut body = Vec::new();
                    while let Some(chunk) = read_data(
                        &mut response.body,
                        &mut response_headers,
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap()
                    {
                        body.extend_from_slice(&chunk);
                    }
                    assert_eq!(body, b"abc");
                }
                drop(response);
                let close_delimited = bytes == b"HTTP/1.1 200 OK\r\n\r\nabc";
                assert_eq!(input.is_reusable(), consume && !close_delimited);
            }
        }
    });
}

#[test]
fn response_fact_scan_preserves_upgrade_singletons_and_repeated_connection_tokens() {
    block_on(async {
        let exchange = exchange_from(
            b"GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade\r\nUpgrade: first, second\r\n\r\n",
        );
        let (mut output, bytes) = output(usize::MAX);
        let mut response = Response::empty(Status::SWITCHING_PROTOCOLS);
        response.protocol = Some(base_http::Protocol::new("first").unwrap());
        response.fields.add("Upgrade", "first").unwrap();
        response.fields.add("uPgRaDe", "second").unwrap();
        assert!(matches!(
            output
                .write_response(&exchange, &mut response, &Cancellation::never())
                .await,
            Err(Error::Invalid("duplicate singleton field"))
        ));
        assert!(bytes.lock().unwrap().is_empty());

        let exchange = exchange_from(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
        let (mut output, bytes) = self::output(usize::MAX);
        let mut response = Response::empty(Status::OK);
        response.fields.add("Connection", "KEEP-ALIVE").unwrap();
        response.fields.add("cOnNeCtIoN", "other, ClOsE").unwrap();
        assert!(
            !output
                .write_response(&exchange, &mut response, &Cancellation::never())
                .await
                .unwrap()
        );
        assert_eq!(*bytes.lock().unwrap(), b"HTTP/1.0 200 OK\r\nConnection: KEEP-ALIVE\r\ncOnNeCtIoN: other, ClOsE\r\nContent-Length: 0\r\n\r\n");
    });
}

#[test]
fn request_trailer_frames_remain_owned_across_close_drop_and_reuse() {
    for step in 1..80 {
        for disposition in 0..4 {
            block_on(async {
                let cancellation = Cancellation::never();
                let mut input = reader(b"POST /one HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx-tail: first\r\nx-tail: repeated\r\n\r\nPOST /two HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\nxPOST /three HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx-tail: third\r\n\r\nGET /four HTTP/1.1\r\nHost: x\r\n\r\n", step);
                let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                let BodyFrame::Headers {
                    fields: trailers,
                    end: true,
                } = request.body.read(&cancellation).await.unwrap()
                else {
                    panic!("expected owned trailers")
                };
                assert!(!request_headers.has_trailers());
                if disposition == 1 || disposition == 2 {
                    if disposition == 2 {
                        request.body.close(None);
                    }
                    request_headers.append_trailers(trailers).unwrap();
                    assert!(matches!(
                        request.body.read(&cancellation).await.unwrap(),
                        BodyFrame::End
                    ));
                    let values: Vec<_> = request_headers
                        .trailers()
                        .iter()
                        .map(|f| f.value())
                        .collect();
                    assert_eq!(values, [b"first".as_slice(), b"repeated"]);
                }
                if disposition == 3 {
                    // A safe forgotten completed body must not expose its fields
                    // through the next message, even though Drop never runs.
                    let (head, body) = request.into_parts();
                    drop(head);
                    std::mem::forget(body);
                } else {
                    drop(request);
                }
                assert!(input.is_reusable());
                let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                assert_eq!(
                    read_data(&mut request.body, &mut request_headers, &cancellation)
                        .await
                        .unwrap()
                        .unwrap(),
                    b"x"
                );
                assert!(
                    read_data(&mut request.body, &mut request_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!request_headers.has_trailers());
                drop(request);
                let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                assert!(
                    read_data(&mut request.body, &mut request_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert_eq!(request_headers.trailers().len(), 1);
                assert_eq!(request_headers.trailers()[0].value(), b"third");
                drop(request);
                let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                assert!(
                    read_data(&mut request.body, &mut request_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!request_headers.has_trailers());
                drop(request);
                assert!(input.is_reusable());
            });
        }
    }
}

#[test]
fn response_trailer_frames_remain_owned_across_interims_and_reuse() {
    for step in 1..120 {
        for disposition in 0..4 {
            block_on(async {
                let cancellation = Cancellation::never();
                let head = RequestMetadata::new(Method::GET);
                let mut input = response_reader(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx-tail: first\r\nx-tail: repeated\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nxHTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx-tail: third\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n", step);
                let mut response = input
                    .read_response(&head, &mut observer(), &cancellation)
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                let BodyFrame::Headers {
                    fields: trailers,
                    end: true,
                } = response.body.read(&cancellation).await.unwrap()
                else {
                    panic!("expected owned trailers")
                };
                assert!(!response_headers.has_trailers());
                if disposition == 1 || disposition == 2 {
                    if disposition == 2 {
                        response.body.close(None);
                    }
                    response_headers.append_trailers(trailers).unwrap();
                    assert!(matches!(
                        response.body.read(&cancellation).await.unwrap(),
                        BodyFrame::End
                    ));
                    let values: Vec<_> = response_headers
                        .trailers()
                        .iter()
                        .map(|f| f.value())
                        .collect();
                    assert_eq!(values, [b"first".as_slice(), b"repeated"]);
                }
                if disposition == 3 {
                    let (head, body) = response.into_parts();
                    drop(head);
                    std::mem::forget(body);
                } else {
                    drop(response);
                }
                assert!(input.is_reusable());
                let mut response = input
                    .read_response(&head, &mut observer(), &cancellation)
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert_eq!(
                    read_data(&mut response.body, &mut response_headers, &cancellation)
                        .await
                        .unwrap()
                        .unwrap(),
                    b"x"
                );
                assert!(
                    read_data(&mut response.body, &mut response_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!response_headers.has_trailers());
                drop(response);
                let mut response = input
                    .read_response(&head, &mut observer(), &cancellation)
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert!(
                    read_data(&mut response.body, &mut response_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(response.fields.is_empty());
                assert_eq!(response_headers.trailers()[0].value(), b"third");
                drop(response);
                let mut response = input
                    .read_response(&head, &mut observer(), &cancellation)
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert!(
                    read_data(&mut response.body, &mut response_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!response_headers.has_trailers());
                drop(response);
                assert!(input.is_reusable());
            });
        }
    }
}

#[test]
fn completed_body_read_future_is_dropped_before_output_writes_its_chunk() {
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };
    struct Reading {
        result: Option<BodyFrame<Vec<u8>>>,
        released: Arc<AtomicBool>,
    }
    impl Future for Reading {
        type Output = Result<BodyFrame<Vec<u8>>, std::convert::Infallible>;
        fn poll(mut self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(self.result.take().unwrap()))
        }
    }
    impl Drop for Reading {
        fn drop(&mut self) {
            self.released.store(true, Ordering::Release);
        }
    }
    struct Body {
        delivered: bool,
        released: Arc<AtomicBool>,
    }
    impl ReadableBody for Body {
        type Data = Vec<u8>;
        type Error = std::convert::Infallible;
        fn is_ready(&self) -> bool {
            true
        }
        fn is_empty(&self) -> bool {
            self.delivered
        }
        fn length(&self) -> Option<u64> {
            Some(32768)
        }
        fn may_have_trailers(&self) -> bool {
            false
        }
        fn close(&mut self, _: Option<Self::Error>) {
            self.delivered = true;
        }
        fn read(
            &mut self,
            _: &Cancellation,
        ) -> impl Future<Output = Result<BodyFrame<Vec<u8>>, Self::Error>> + Send {
            self.released.store(false, Ordering::Release);
            let result = if std::mem::replace(&mut self.delivered, true) {
                BodyFrame::End
            } else {
                BodyFrame::Data {
                    data: vec![b'a'; 32768],
                    end: true,
                }
            };
            Reading {
                result: Some(result),
                released: self.released.clone(),
            }
        }
    }
    block_on(async {
        let released = Arc::new(AtomicBool::new(true));
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let mut output = ResponseWriter::new(Output {
            bytes: bytes.clone(),
            step: usize::MAX,
            events: None,
            fail_after: None,
            released_read: Some(released.clone()),
        });
        let mut response = Response::new(
            Status::OK,
            Body {
                delivered: false,
                released: released.clone(),
            },
        );
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .unwrap()
        );
        assert!(released.load(Ordering::Acquire));
        assert!(bytes.lock().unwrap().ends_with(&vec![b'a'; 32768]));
    });
}

#[test]
fn empty_request_trailers_publish_the_boundary_only_once() {
    for step in 1..80 {
        for close in [false, true] {
            block_on(async {
                let cancellation = Cancellation::never();
                let mut input = reader(b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n", step);
                let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                let BodyFrame::Headers {
                    fields: trailers,
                    end: true,
                } = request.body.read(&cancellation).await.unwrap()
                else {
                    panic!("expected owned trailers")
                };
                assert!(!request_headers.has_trailers());
                if close {
                    request.body.close(None);
                }
                request_headers.append_trailers(trailers).unwrap();
                assert!(request_headers.has_trailers());
                assert!(request_headers.trailers().is_empty());
                request_headers.clear();
                assert!(matches!(
                    request.body.read(&cancellation).await.unwrap(),
                    BodyFrame::End
                ));
                assert!(!request_headers.has_trailers());
                drop(request);
                let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
                let mut request_headers = HttpHeaders::new(request.fields.clone());
                assert!(
                    read_data(&mut request.body, &mut request_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!request_headers.has_trailers());
            });
        }
    }
}

#[test]
fn empty_response_trailers_publish_the_boundary_only_once() {
    for step in 1..80 {
        for close in [false, true] {
            block_on(async {
                let cancellation = Cancellation::never();
                let head = RequestMetadata::new(Method::GET);
                let mut input = response_reader(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n", step);
                let mut response = input
                    .read_response(&head, &mut observer(), &cancellation)
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                let BodyFrame::Headers {
                    fields: trailers,
                    end: true,
                } = response.body.read(&cancellation).await.unwrap()
                else {
                    panic!("expected owned trailers")
                };
                assert!(!response_headers.has_trailers());
                if close {
                    response.body.close(None);
                }
                response_headers.append_trailers(trailers).unwrap();
                assert!(response_headers.has_trailers());
                assert!(response_headers.trailers().is_empty());
                response_headers.clear();
                assert!(matches!(
                    response.body.read(&cancellation).await.unwrap(),
                    BodyFrame::End
                ));
                assert!(!response_headers.has_trailers());
                drop(response);
                let mut response = input
                    .read_response(&head, &mut observer(), &cancellation)
                    .await
                    .unwrap();
                let mut response_headers = HttpHeaders::new(response.fields.clone());
                assert!(
                    read_data(&mut response.body, &mut response_headers, &cancellation)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(!response_headers.has_trailers());
            });
        }
    }
}

// Panics on an extra read: end flags must be consumed by the protocol writer.
struct Frames {
    frames: std::collections::VecDeque<BodyFrame<Vec<u8>>>,
    length: Option<u64>,
    reads: usize,
}
impl ReadableBody for Frames {
    fn may_have_trailers(&self) -> bool {
        self.frames
            .iter()
            .any(|frame| matches!(frame, BodyFrame::Headers { .. }))
    }
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    fn is_ready(&self) -> bool {
        true
    }
    fn length(&self) -> Option<u64> {
        self.length
    }
    fn close(&mut self, _: Option<Self::Error>) {}
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        self.reads += 1;
        Ok(self.frames.pop_front().expect("read after final frame"))
    }
}

#[test]
fn final_data_frames_finish_fixed_and_chunked_output_without_an_extra_read() {
    block_on(async {
        for length in [Some(5), None] {
            let (mut output, bytes) = output(usize::MAX);
            let mut response = Response::new(
                Status::OK,
                Frames {
                    frames: [BodyFrame::Data {
                        data: b"hello".to_vec(),
                        end: true,
                    }]
                    .into(),
                    length,
                    reads: 0,
                },
            );
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(response.body.reads, 1);
            let bytes = bytes.lock().unwrap();
            if length.is_some() {
                assert_eq!(*bytes, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
            } else {
                assert_eq!(
                    *bytes,
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
                );
            }
        }
        let (mut output, _) = output(usize::MAX);
        let mut response = Response::new(
            Status::OK,
            Frames {
                frames: [BodyFrame::Data {
                    data: b"short".to_vec(),
                    end: true,
                }]
                .into(),
                length: Some(6),
                reads: 0,
            },
        );
        assert!(matches!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await,
            Err(Error::LengthMismatch)
        ));
    });
}

#[test]
fn output_serializes_owned_header_frames_with_immediate_or_separate_completion() {
    block_on(async {
        for end in [false, true] {
            let (mut output, bytes) = output(1);
            let fields =
                Fields::try_from_pairs([("x-result", "yes"), ("x-result", "again")]).unwrap();
            let mut frames = std::collections::VecDeque::from([
                BodyFrame::Data {
                    data: Vec::new(),
                    end: false,
                },
                BodyFrame::Data {
                    data: b"data".to_vec(),
                    end: false,
                },
                BodyFrame::Headers { fields, end },
            ]);
            if !end {
                frames.push_back(BodyFrame::End);
            }
            let mut response = Response::new(
                Status::OK,
                Frames {
                    frames,
                    length: None,
                    reads: 0,
                },
            );

            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(response.body.reads, if end { 3 } else { 4 });
            assert!(response.fields.is_empty());
            assert!(!response.fields.contains("x-result"));
            assert!(
                bytes
                    .lock()
                    .unwrap()
                    .ends_with(b"4\r\ndata\r\n0\r\nx-result: yes\r\nx-result: again\r\n\r\n")
            );
        }
    });
}

#[test]
fn invalid_frame_order_and_forbidden_trailers_poison_output() {
    block_on(async {
        for extra in [
            BodyFrame::Data {
                data: vec![1],
                end: true,
            },
            BodyFrame::Headers {
                fields: Fields::new(),
                end: true,
            },
        ] {
            let (mut output, _) = output(usize::MAX);
            let mut response = Response::new(
                Status::OK,
                Frames {
                    frames: [
                        BodyFrame::Headers {
                            fields: Fields::new(),
                            end: false,
                        },
                        extra,
                    ]
                    .into(),
                    length: None,
                    reads: 0,
                },
            );
            assert!(matches!(
                output
                    .write_response(&exchange(), &mut response, &Cancellation::never())
                    .await,
                Err(Error::Invalid(_))
            ));
            assert!(!output.is_usable());
        }
        let (mut output, _) = output(usize::MAX);
        let fields = Fields::try_from_pairs([("Content-Length", "1")]).unwrap();
        let mut response = Response::new(
            Status::OK,
            Frames {
                frames: [BodyFrame::Headers { fields, end: true }].into(),
                length: None,
                reads: 0,
            },
        );
        assert!(matches!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await,
            Err(Error::Body(_))
        ));
        assert!(!output.is_usable());
    });
}

#[test]
fn incoming_final_frames_leave_the_next_request_unread_and_the_head_untouched() {
    for step in 1..80 {
        block_on(async {
            let cancel = Cancellation::never();
            let mut input = reader(b"POST /fixed HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nonePOST /chunked HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\ntwo\r\n0\r\nx-tail: owned\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n", step);
            let (mut request, exchange) = input.read_request(&cancel).await.unwrap().unwrap();
            let mut data = Vec::new();
            loop {
                let frame = request.read(&cancel).await.unwrap();
                let end = frame.is_end();
                let BodyFrame::Data { data: chunk, .. } = frame else {
                    panic!("fixed body must end with data")
                };
                data.extend(chunk);
                if end {
                    break;
                }
            }
            assert_eq!(data, b"one");
            assert!(exchange.is_complete());
            drop(request);
            let (mut request, _) = input.read_request(&cancel).await.unwrap().unwrap();
            let mut data = Vec::new();
            let trailers = loop {
                match request.read(&cancel).await.unwrap() {
                    BodyFrame::Data {
                        data: chunk,
                        end: false,
                    } => data.extend(chunk),
                    BodyFrame::Headers { fields, end: true } => break fields,
                    _ => panic!("expected final headers"),
                }
            };
            assert_eq!(data, b"two");
            assert!(!request.fields.contains("x-tail"));
            drop(request);
            let (request, _) = input.read_request(&cancel).await.unwrap().unwrap();
            assert_eq!(request.path.as_deref(), Some("/next"));
            assert_eq!(trailers.pairs()[0].value(), b"owned");
        });
    }
}

fn trailing_body(chunks: impl IntoIterator<Item = Vec<u8>>, fields: Fields) -> Frames {
    let mut length = 0;
    let mut frames: std::collections::VecDeque<_> = chunks
        .into_iter()
        .map(|data| {
            length += data.len() as u64;
            BodyFrame::Data { data, end: false }
        })
        .collect();
    frames.push_back(BodyFrame::Headers { fields, end: true });
    Frames {
        frames,
        length: Some(length),
        reads: 0,
    }
}

#[test]
fn request_controls_are_extracted_before_exposing_application_fields() {
    block_on(async {
        for step in 1..100 {
            let mut input = reader(b"POST / HTTP/1.1\r\nX-Hop: before\r\nHost: example\r\nContent-Length: 3\r\nX-Keep: first\r\nConnection: x-hop, close\r\nx-HOP: after\r\nX-Keep: second\r\n\r\nabc", step);
            let (mut request, exchange) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.authority.as_deref(), Some("example"));
            assert_eq!(request.content_length, Some(3));
            assert_eq!(request.persistent, None);
            assert_eq!(
                request
                    .fields
                    .pairs()
                    .iter()
                    .map(|field| (field.name(), field.value()))
                    .collect::<Vec<_>>(),
                [("X-Keep", b"first".as_slice()), ("X-Keep", b"second")]
            );
            assert!(!exchange.is_persistent());
            let mut headers = HttpHeaders::new(request.fields.clone());
            let mut data = Vec::new();
            while let Some(chunk) =
                read_data(&mut request.body, &mut headers, &Cancellation::never())
                    .await
                    .unwrap()
            {
                data.extend(chunk);
            }
            assert_eq!(data, b"abc");
        }
    });
}

#[test]
fn request_hop_by_hop_fields_are_not_exposed_without_connection() {
    block_on(async {
        let mut input = reader(b"GET / HTTP/1.1\r\nTE: trailers\r\nX-Keep: one\r\nHost: example\r\nKeep-Alive: timeout=5\r\nProxy-Connection: keep-alive\r\nUpgrade: echo\r\nX-Keep: two\r\n\r\n", usize::MAX);
        let (request, _exchange) = input
            .read_request(&Cancellation::never())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.authority.as_deref(), Some("example"));
        assert_eq!(
            request
                .fields
                .pairs()
                .iter()
                .map(|field| (field.name(), field.value()))
                .collect::<Vec<_>>(),
            [("X-Keep", b"one".as_slice()), ("X-Keep", b"two")]
        );
    });
}

#[test]
fn request_invalid_connection_options_are_rejected() {
    block_on(async {
        for head in [
            b"GET / HTTP/1.1\r\nHost: example\r\nConnection: a b\r\n\r\n".as_slice(),
            b"GET / HTTP/1.1\r\nX-Drop: one\r\nHost: example\r\nConnection: x-drop, a;b\r\n\r\n",
        ] {
            let mut input = reader(head, usize::MAX);
            assert!(matches!(
                input.read_request(&Cancellation::never()).await,
                Err(Error::Invalid("invalid connection option"))
            ));
        }
    });
}

#[test]
fn response_controls_are_extracted_and_bodyless_lengths_are_retained() {
    block_on(async {
        for (method, wire, length, protocol) in [
            (Method::HEAD, b"HTTP/1.1 200 OK\r\nContent-Length: 123\r\nConnection: x-hop, close\r\nX-Hop: hidden\r\nX-Keep: yes\r\n\r\n".as_slice(), 123, None),
            (Method::GET, b"HTTP/1.1 304 Not Modified\r\nContent-Length: 456\r\nX-Keep: yes\r\n\r\n", 456, None),
        ] {
            let mut input = response_reader(wire, 1);
            let mut response = input.read_response(&RequestMetadata::new(method), &mut observer(), &Cancellation::never()).await.unwrap();
            assert_eq!(response.content_length, Some(length));
            assert_eq!(response.body.length(), Some(length));
            assert_eq!(response.protocol, protocol);
            assert_eq!(response.fields.pairs().iter().map(|field| field.name()).collect::<Vec<_>>(), ["X-Keep"]);
            assert!(matches!(response.read(&Cancellation::never()).await.unwrap(), BodyFrame::End));
        }
        let mut input = response_reader(b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n", 1);
        let mut request = RequestMetadata::new(Method::GET);
        request.protocol = Some(base_http::Protocol::new("websocket").unwrap());
        let response = input
            .read_response(&request, &mut observer(), &Cancellation::never())
            .await
            .unwrap();
        assert_eq!(response.protocol.as_ref().unwrap().as_str(), "websocket");
        assert!(response.fields.is_empty());
    });
}

#[test]
fn typed_controls_are_serialized_without_mutating_application_fields() {
    block_on(async {
        let cancellation = Cancellation::never();
        let (mut output, bytes) = output(usize::MAX);
        let mut response = Response::new(Status::OK, BufferedBody::from(b"hello".to_vec()));
        response.content_length = Some(5);
        response.persistent = Some(false);
        response.fields.add("X-Keep", "yes").unwrap();
        assert!(
            !output
                .write_response(&exchange(), &mut response, &cancellation)
                .await
                .unwrap()
        );
        assert_eq!(*bytes.lock().unwrap(), b"HTTP/1.1 200 OK\r\nX-Keep: yes\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello");
        assert_eq!(response.fields.len(), 1);
        let (mut output, bytes) = client_output(usize::MAX);
        let mut request = Request::new(Method::POST, "/", BufferedBody::from(b"hello".to_vec()));
        request.authority = Some("example".into());
        request.content_length = Some(5);
        request.persistent = Some(false);
        assert!(
            !output
                .write_request(&mut request, &cancellation)
                .await
                .unwrap()
        );
        assert!(String::from_utf8_lossy(&bytes.lock().unwrap()).contains("Content-Length: 5\r\n"));
        assert!(request.fields.is_empty());
    });
}

#[test]
fn typed_length_conflicts_and_trailer_lengths_are_validated() {
    block_on(async {
        let cancellation = Cancellation::never();
        let (mut output, bytes) = output(usize::MAX);
        let mut response = Response::new(Status::OK, BufferedBody::from(b"hello".to_vec()));
        response.content_length = Some(5);
        response.fields.add("Content-Length", "4").unwrap();
        assert!(
            output
                .write_response(&exchange(), &mut response, &cancellation)
                .await
                .is_err()
        );
        assert!(bytes.lock().unwrap().is_empty());
        let (mut output, bytes) = client_output(usize::MAX);
        let mut request = Request::new(Method::POST, "/", BufferedBody::from(b"hello".to_vec()));
        request.authority = Some("example".into());
        request.content_length = Some(5);
        request.fields.add("Content-Length", "4").unwrap();
        assert!(
            output
                .write_request(&mut request, &cancellation)
                .await
                .is_err()
        );
        assert!(bytes.lock().unwrap().is_empty());
        for expected in [4, 5, 6] {
            let (mut output, bytes) = self::output(usize::MAX);
            let mut body = trailing_body(
                [b"hello".to_vec()],
                Fields::try_from_pairs([("digest", "value")]).unwrap(),
            );
            body.length = None;
            let mut response = Response::new(Status::OK, body);
            response.content_length = Some(expected);
            let result = output
                .write_response(&exchange(), &mut response, &cancellation)
                .await;
            if expected == 5 {
                assert!(result.unwrap());
                let bytes = bytes.lock().unwrap();
                assert!(!String::from_utf8_lossy(&bytes).contains("Content-Length"));
                assert!(bytes.ends_with(b"0\r\ndigest: value\r\n\r\n"));
            } else {
                assert!(matches!(result, Err(Error::LengthMismatch)));
                assert!(!output.is_usable());
            }
        }
    });
}

#[test]
fn typed_zero_length_and_bodyless_representation_lengths_reach_the_wire() {
    block_on(async {
        let cancellation = Cancellation::never();
        let (mut writer, bytes) = client_output(usize::MAX);
        let mut request = Request::get("/");
        request.authority = Some("example".into());
        request.content_length = Some(0);
        writer
            .write_request(&mut request, &cancellation)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes.lock().unwrap()).contains("Content-Length: 0\r\n"));
        for (request, status) in [
            (b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n".as_slice(), Status::OK),
            (b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", Status::NOT_MODIFIED),
        ] {
            let (mut writer, bytes) = output(usize::MAX);
            let mut response = Response::empty(status);
            response.content_length = Some(123);
            writer
                .write_response(&exchange_from(request), &mut response, &cancellation)
                .await
                .unwrap();
            assert!(
                bytes
                    .lock()
                    .unwrap()
                    .ends_with(b"Content-Length: 123\r\n\r\n")
            );
        }
    });
}

#[test]
fn connection_nominations_filter_both_field_sections_without_leaking_to_the_next_message() {
    block_on(async {
        let cancellation = Cancellation::never();
        for step in 1..120 {
            let mut input = reader(b"POST /one HTTP/1.1\r\nHost: example\r\nConnection: x-hop, ,\r\nTE: trailers\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Hop: hidden\r\nX-Keep: kept\r\n\r\nPOST /two HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Hop: visible\r\n\r\n", step);
            let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
            assert!(request.fields.is_empty());
            let BodyFrame::Headers { fields, end: true } =
                request.read(&cancellation).await.unwrap()
            else {
                panic!("expected trailers")
            };
            assert_eq!(
                fields
                    .pairs()
                    .iter()
                    .map(|field| (field.name(), field.value()))
                    .collect::<Vec<_>>(),
                [("X-Keep", b"kept".as_slice())]
            );
            drop(request);
            let (mut request, _) = input.read_request(&cancellation).await.unwrap().unwrap();
            let BodyFrame::Headers { fields, end: true } =
                request.read(&cancellation).await.unwrap()
            else {
                panic!("expected trailers")
            };
            assert_eq!(fields.values("x-hop").next(), Some(b"visible".as_slice()));
            drop(request);
            assert!(input.is_reusable());

            let mut input = response_reader(b"HTTP/1.1 200 OK\r\nConnection: x-hop, ,\r\nTE: trailers\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Hop: hidden\r\nX-Keep: kept\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Hop: visible\r\n\r\n", step);
            let metadata = RequestMetadata::new(Method::GET);
            let mut response = input
                .read_response(&metadata, &mut observer(), &cancellation)
                .await
                .unwrap();
            assert!(response.fields.is_empty());
            let BodyFrame::Headers { fields, end: true } =
                response.read(&cancellation).await.unwrap()
            else {
                panic!("expected trailers")
            };
            assert_eq!(
                fields
                    .pairs()
                    .iter()
                    .map(|field| (field.name(), field.value()))
                    .collect::<Vec<_>>(),
                [("X-Keep", b"kept".as_slice())]
            );
            drop(response);
            let mut response = input
                .read_response(&metadata, &mut observer(), &cancellation)
                .await
                .unwrap();
            let BodyFrame::Headers { fields, end: true } =
                response.read(&cancellation).await.unwrap()
            else {
                panic!("expected trailers")
            };
            assert_eq!(fields.values("x-hop").next(), Some(b"visible".as_slice()));
            drop(response);
            assert!(input.is_reusable());
        }
    });
}

#[test]
fn imperative_request_output_handles_short_writes_and_both_terminal_frame_shapes() {
    for step in 1..20 {
        for terminal_headers in [false, true] {
            block_on(async {
                let cancel = Cancellation::never();
                let (mut output, bytes) = client_output(step);
                let mut metadata = RequestMetadata::new(Method::POST);
                metadata.authority = Some("example".into());
                output.begin_request(&metadata, &cancel).await.unwrap();
                assert!(!output.is_reusable());
                assert!(
                    bytes
                        .lock()
                        .unwrap()
                        .ends_with(b"Transfer-Encoding: chunked\r\n\r\n")
                );
                output
                    .write_frame(
                        BodyFrame::Data {
                            data: b"ab",
                            end: false,
                        },
                        &cancel,
                    )
                    .await
                    .unwrap();
                assert!(bytes.lock().unwrap().ends_with(b"2\r\nab\r\n"));
                if terminal_headers {
                    output
                        .write_frame(
                            BodyFrame::<&[u8]>::Headers {
                                fields: Fields::try_from_pairs([("x-tail", "done")]).unwrap(),
                                end: true,
                            },
                            &cancel,
                        )
                        .await
                        .unwrap();
                    assert!(
                        bytes
                            .lock()
                            .unwrap()
                            .ends_with(b"0\r\nx-tail: done\r\n\r\n")
                    );
                } else {
                    output
                        .write_frame(
                            BodyFrame::Data {
                                data: b"c",
                                end: true,
                            },
                            &cancel,
                        )
                        .await
                        .unwrap();
                    assert!(bytes.lock().unwrap().ends_with(b"1\r\nc\r\n0\r\n\r\n"));
                }
                assert!(output.is_reusable());
                metadata.content_length = Some(3);
                output.begin_request(&metadata, &cancel).await.unwrap();
                output
                    .write_frame(
                        BodyFrame::Data {
                            data: b"xyz",
                            end: true,
                        },
                        &cancel,
                    )
                    .await
                    .unwrap();
                assert!(output.is_reusable());
                assert!(
                    bytes
                        .lock()
                        .unwrap()
                        .ends_with(b"Content-Length: 3\r\n\r\nxyz")
                );
            });
        }
    }
}

#[test]
fn imperative_request_output_rejects_invalid_trailer_order_and_fields() {
    block_on(async {
        for invalid in [false, true] {
            let cancel = Cancellation::never();
            let (mut output, _) = client_output(2);
            let mut metadata = RequestMetadata::new(Method::POST);
            metadata.authority = Some("example".into());
            output.begin_request(&metadata, &cancel).await.unwrap();
            if invalid {
                assert!(
                    output
                        .write_frame(
                            BodyFrame::<Vec<u8>>::Headers {
                                fields: Fields::try_from_pairs([("Content-Length", "0")]).unwrap(),
                                end: true,
                            },
                            &cancel
                        )
                        .await
                        .is_err()
                );
            } else {
                output
                    .write_frame(
                        BodyFrame::<Vec<u8>>::Headers {
                            fields: Fields::new(),
                            end: false,
                        },
                        &cancel,
                    )
                    .await
                    .unwrap();
                assert!(
                    output
                        .write_frame(
                            BodyFrame::Data {
                                data: b"late",
                                end: true
                            },
                            &cancel
                        )
                        .await
                        .is_err()
                );
            }
            assert!(!output.is_reusable());
            assert!(output.begin_request(&metadata, &cancel).await.is_err());
        }
    });
}

#[test]
fn outbound_validation_preserves_target_host_and_connection_rules() {
    block_on(async {
        for (method, target, host, connection) in [
            (Method::GET, "*", "example", None),
            (Method::GET, "relative", "example", None),
            (Method::GET, "ftp://example/", "example", None),
            (Method::GET, "http://bad@host/", "example", None),
            (Method::GET, "/", "example:99999", None),
            (Method::GET, "/", "[not-ipv6]", None),
            (Method::GET, "/", "example", Some("bad option")),
            (Method::GET, "/", "example", Some("upgrade")),
        ] {
            let (mut output, bytes) = client_output(1024);
            let mut metadata = RequestMetadata::new(method);
            metadata.path = Some(target.into());
            metadata.fields.add("Host", host).unwrap();
            if let Some(value) = connection {
                metadata.fields.add("Connection", value).unwrap();
            }
            assert!(
                output
                    .begin_request(&metadata, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(bytes.lock().unwrap().is_empty());
            assert!(output.is_reusable());
        }
        for target in ["example", "example:abc", "[::1]", "example:65536"] {
            let (mut output, bytes) = client_output(1024);
            let mut metadata = RequestMetadata::new(Method::CONNECT);
            metadata.authority = Some(target.into());
            assert!(
                output
                    .begin_request(&metadata, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(bytes.lock().unwrap().is_empty());
        }
        for (method, target) in [
            (Method::OPTIONS, "*"),
            (Method::GET, "http://example/?a"),
            (Method::GET, "/okay"),
        ] {
            let (mut output, bytes) = client_output(1024);
            let mut metadata = RequestMetadata::new(method);
            metadata.path = Some(target.into());
            metadata.authority = Some("example".into());
            metadata.content_length = Some(0);
            output
                .begin_request(&metadata, &Cancellation::never())
                .await
                .unwrap();
            assert!(
                String::from_utf8(bytes.lock().unwrap().clone())
                    .unwrap()
                    .contains(target)
            );
        }
    });
}

#[test]
fn values_that_would_change_http1_framing_are_never_sent() {
    block_on(async {
        for value in [b"a\r\nInjected: b".as_slice(), b"a\nb", b"a\rb", b"a\0b"] {
            let fields = || Fields::try_from_pairs([("x-value", value)]).unwrap();

            let (mut writer, bytes) = output(2);
            let mut response = Response::empty(Status::OK);
            response.fields = fields();
            assert!(
                writer
                    .write_response(&exchange(), &mut response, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(bytes.lock().unwrap().is_empty());
            assert!(writer.is_usable());

            let (mut writer, bytes) = output(2);
            let mut hint = ResponseMetadata::new(Status::EARLY_HINTS);
            hint.fields = fields();
            assert!(
                writer
                    .interim(Version::Http11)
                    .on_interim_response(hint, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(bytes.lock().unwrap().is_empty());

            let (mut writer, bytes) = output(2);
            let mut response = Response::new(Status::OK, trailing_body([], fields()));
            assert!(
                writer
                    .write_response(&exchange(), &mut response, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(!String::from_utf8_lossy(&bytes.lock().unwrap()).contains("x-value"));

            let (mut writer, bytes) = client_output(2);
            let mut request = Request::get("/");
            request.authority = Some("example".into());
            request.fields = fields();
            assert!(
                writer
                    .write_request(&mut request, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(bytes.lock().unwrap().is_empty());

            let (mut writer, bytes) = client_output(2);
            let mut request = Request::new(Method::POST, "/", trailing_body([], fields()));
            request.authority = Some("example".into());
            assert!(
                writer
                    .write_request(&mut request, &Cancellation::never())
                    .await
                    .is_err()
            );
            assert!(!String::from_utf8_lossy(&bytes.lock().unwrap()).contains("x-value"));
        }

        // Other control bytes and obs-text are not framing, and pass through.
        let (mut writer, bytes) = output(2);
        let mut response = Response::empty(Status::OK);
        response.fields.add("x-value", b"a\x01\x7f\xffb").unwrap();
        writer
            .write_response(&exchange(), &mut response, &Cancellation::never())
            .await
            .unwrap();
        assert!(
            bytes
                .lock()
                .unwrap()
                .windows(10)
                .any(|window| window == b"x-value: a")
        );
    });
}

#[test]
fn received_trailer_values_follow_the_field_value_grammar() {
    block_on(async {
        for (trailer, valid) in [
            (b"x-tail: caf\xc3\xa9\x80\tok".as_slice(), true),
            (b"x-tail: a\x01b", false),
            (b"x-tail: a\x7fb", false),
            (b"x-tail: a\0b", false),
        ] {
            let mut bytes =
                b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n".to_vec();
            bytes.extend_from_slice(trailer);
            bytes.extend_from_slice(b"\r\n\r\n");
            let mut input = reader(bytes.leak(), 3);
            let (mut request, _) = input
                .read_request(&Cancellation::never())
                .await
                .unwrap()
                .unwrap();
            let mut headers = HttpHeaders::new(request.fields.clone());
            let result = read_data(&mut request.body, &mut headers, &Cancellation::never()).await;
            assert_eq!(result.is_ok(), valid, "{}", trailer.escape_ascii());
            if valid {
                assert_eq!(headers.trailers()[0].value(), &trailer[8..]);
            }
        }
    });
}
