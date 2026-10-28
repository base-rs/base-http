// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    Fields, InterimHandler, InterimResponses, Method, Request, RequestHead, Response, ResponseHead,
    Status, Version,
    body::{BodyReader, BufferedBody, ReadableBody},
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
}
impl SocketStreamWriter for Output {
    type Buffer = Vec<u8>;
    async fn write(&mut self, bytes: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        cancellation.check()?;
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
    async fn read(&mut self, _: &Cancellation) -> Result<Option<Vec<u8>>, Self::Error> {
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
fn static_trailers_select_chunked_framing_and_stay_after_the_split() {
    block_on(async {
        let (mut output, bytes) = output(2);
        let mut response = Response::new(Status::OK, BufferedBody::from(b"data".to_vec()));
        response.fields.add("Content-Type", "text/plain").unwrap();
        response.fields.mark_trailers();
        response.fields.add("x-result", "yes").unwrap();
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .unwrap()
        );
        let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(bytes.contains("Transfer-Encoding: chunked\r\nTrailer: x-result\r\n"));
        assert!(!bytes.contains("Content-Length"));
        assert!(bytes.ends_with("\r\n\r\n4\r\ndata\r\n0\r\nx-result: yes\r\n\r\n"));
        assert_eq!(response.fields.headers().len(), 1);
        assert_eq!(response.fields.trailers().len(), 1);
    });
}

struct Unknown(BufferedBody);
impl ReadableBody for Unknown {
    type Data = Vec<u8>;
    type Error = base_scheduler::Cancelled;
    fn close(&mut self, error: Option<Self::Error>) {
        self.0.close(error)
    }
    async fn read(&mut self, cancel: &Cancellation) -> Result<Option<Vec<u8>>, Self::Error> {
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
                let mut data = Vec::new();
                while let Some(chunk) = request.read(&Cancellation::never()).await.unwrap() {
                    data.extend(chunk);
                }
                assert_eq!(data, expected);
                if expected == b"de" {
                    assert_eq!(request.fields.trailers()[0].value(), b"yes");
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
            assert_eq!(
                request.read(&Cancellation::never()).await.unwrap(),
                Some(b"a".to_vec())
            );
            assert!(matches!(
                request.read(&Cancellation::never()).await,
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
        let mut policy = base_http::fields::Policy::default();
        policy
            .set(
                "content-length",
                base_http::fields::FieldPolicy::new(
                    base_http::fields::Combination::Singleton,
                    true,
                ),
            )
            .unwrap();
        let mut response = Response::empty(Status::OK);
        response.fields = Fields::with_policy(policy);
        response.fields.mark_trailers();
        response.fields.add("content-length", "1").unwrap();
        let (mut output, bytes) = output(10);
        assert!(
            output
                .write_response(&exchange(), &mut response, &Cancellation::never())
                .await
                .is_err()
        );
        assert!(bytes.lock().unwrap().is_empty());
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
        }),
        bytes,
    )
}

fn response_reader(bytes: &'static [u8], step: usize) -> ResponseReader<Input> {
    ResponseReader::new(Input { bytes, step }, Default::default()).unwrap()
}

fn observer() -> InterimResponses<impl InterimHandler> {
    InterimResponses::new(|_: ResponseHead| {
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
        let mut second = Request::new(Method::POST, "/two", BufferedBody::from(b"de".to_vec()));
        second.authority = Some("example".into());
        second.fields.mark_trailers();
        second.fields.add("x-tail", "yes").unwrap();
        output
            .write_request(&mut second, &Cancellation::never())
            .await
            .unwrap();
        let bytes = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(bytes.contains("Transfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\nPOST /two"));
        assert!(bytes.ends_with("Transfer-Encoding: chunked\r\nTrailer: x-tail\r\n\r\n2\r\nde\r\n0\r\nx-tail: yes\r\n\r\n"));
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
            let mut interim = InterimResponses::new(move |head: ResponseHead| {
                capture.lock().unwrap().push(head.status);
                std::future::ready(Ok::<(), std::convert::Infallible>(()))
            });
            {
                let mut response = input
                    .read_response(
                        &RequestHead::new(Method::GET),
                        &mut interim,
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                assert!(interim.is_finished());
                assert_eq!(
                    *seen.lock().unwrap(),
                    [Status::CONTINUE, Status::EARLY_HINTS]
                );
                let mut data = Vec::new();
                while let Some(chunk) = response.read(&Cancellation::never()).await.unwrap() {
                    data.extend(chunk);
                }
                assert_eq!(data, b"abc");
                assert_eq!(response.fields.trailers()[0].value(), b"yes");
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
                        &RequestHead::new(method),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status.as_u16(), status);
                assert_eq!(response.body.length(), length);
                assert!(
                    response
                        .read(&Cancellation::never())
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
                        &RequestHead::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                let mut data = Vec::new();
                while let Some(chunk) = response.read(&Cancellation::never()).await.unwrap() {
                    data.extend(chunk);
                }
                assert_eq!(data, b"abc");
            }
            assert!(!input.is_reusable());
            assert!(
                input
                    .read_response(
                        &RequestHead::new(Method::GET),
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
                        &RequestHead::new(Method::GET),
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
                        &RequestHead::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap();
                loop {
                    match response.read(&Cancellation::never()).await {
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
                    &RequestHead::new(Method::GET),
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
                let mut response = input.read_response(&RequestHead::new(Method::CONNECT), &mut observer(), &Cancellation::never()).await.unwrap();
                let mut data = Vec::new();
                while let Some(chunk) = response.read(&Cancellation::never()).await.unwrap() { data.extend(chunk); }
                assert_eq!(data, b"raw");
                assert_eq!(response.body.length(), None);
            }
            assert!(!input.is_reusable());
        }
        let mut input = response_reader(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: echo\r\n\r\nraw",
            1,
        );
        let mut request = RequestHead::new(Method::GET);
        request.protocol = Some(base_http::Protocol::new("ECHO").unwrap());
        let mut interim = observer();
        {
            let mut response = input
                .read_response(&request, &mut interim, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(response.protocol.as_ref().unwrap().as_str(), "echo");
            assert!(interim.is_finished());
            let mut data = Vec::new();
            while let Some(chunk) = response.read(&Cancellation::never()).await.unwrap() {
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
        let mut interim = InterimResponses::new(|_: ResponseHead| {
            std::future::pending::<Result<(), io::Error>>()
        });
        let request = RequestHead::new(Method::GET);
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
            while request
                .read(&Cancellation::never())
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
    async fn read(&mut self, _: &Cancellation) -> Result<Option<Self::Data>, Self::Error> {
        self.reads += 1;
        match self.reads {
            1 => Ok(Some(b"one")),
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
                Ok(Some(b"two"))
            }
            _ => Ok(None),
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
            BufferedBody::new([vec![b'a'; 12_000], payload.clone(), vec![b'b'; 12_000]]),
        );
        response.fields.add("X-Large", vec![b'h'; 40_000]).unwrap();
        response.fields.mark_trailers();
        response
            .fields
            .add("X-Trailer", vec![b't'; 40_000])
            .unwrap();
        let (mut output, bytes) = output(1031);
        output
            .write_response(&exchange(), &mut response, &Cancellation::never())
            .await
            .unwrap();
        let mut expected = b"HTTP/1.1 200 OK\r\nX-Large: ".to_vec();
        expected.extend(vec![b'h'; 40_000]);
        expected.extend_from_slice(
            b"\r\nTransfer-Encoding: chunked\r\nTrailer: X-Trailer\r\n\r\n2ee0\r\n",
        );
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
                    assert_eq!(result.unwrap().unwrap().0.fields.len(), count);
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
                        &RequestHead::new(Method::GET),
                        &mut observer(),
                        &Cancellation::never(),
                    )
                    .await;
                if max_fields == count {
                    assert_eq!(result.unwrap().fields.len(), count);
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
    async fn read(&mut self, _: &Cancellation) -> Result<Option<Self::Data>, Self::Error> {
        if self.remaining == 0 {
            std::future::pending().await
        } else {
            self.remaining -= 1;
            Ok(Some(b"x"))
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
