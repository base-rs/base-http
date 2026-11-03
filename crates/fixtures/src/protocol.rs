// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The [`protocol_tests!`](crate::protocol_tests) suite, ported from `async-http`'s `AProtocol` shared behaviour.
//!
//! One application, [`App`], serves every case on its own route, so a runner only chooses how to serve and connect: the HTTP version, options and transport. Each case is a standalone `pub async fn` taking the running [`Context`], so a partial implementation can call a subset. The cases address the application through `H: Borrow<App>`, so a test can wrap it to change its behaviour.

use crate::{Context, Latch, Scheduler, body};
use base_http::{
    Fields, InterimHandler, InterimResponses, Method, Request, Response, ResponseMetadata, Status,
    Version,
    body::{BodyError, BodyFrame, BodyReader, BufferedBody, ReadableBody},
};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use futures_lite::future::zip;
use std::{
    borrow::Borrow,
    collections::VecDeque,
    convert::Infallible,
    io,
    sync::{Arc, Mutex},
};

/// The size of the large download, as in `AProtocol`'s "huge body".
pub const LARGE_BODY: u64 = 8 * 1024 * 1024;
const LARGE_CHUNK: u64 = 64 * 1024;
/// The size of each direction of the two-way stream in [`duplex_stream`], far more than the loopback path between client and server buffers in practice, so the upload cannot finish unless the echo is read at the same time.
pub const DUPLEX_BODY: u64 = 16 * 1024 * 1024;
/// The size of each chunk [`duplex_stream`] uploads.
pub const DUPLEX_CHUNK: usize = 16 * 1024;
const LINK: &str = "</style.css>; rel=preload; as=style";

/// The application serving every protocol case.
///
/// | Route | Response |
/// | --- | --- |
/// | `GET /`, `HEAD /` | `200` with `my-header: my-value` and `{method} {version}` |
/// | `POST /` | `200` streaming the request body back |
/// | `POST /duplex` | `200` streaming the request body back, for the two-way streaming case |
/// | `POST /length` | `200` with `Content Length: {n}`, the request body's length |
/// | `GET /interim` | `103 Early Hints` with a `link` field to HTTP/1.1 clients, then `200` with `Hello World` |
/// | `POST /request-trailer` | `200` with the request's `etag` trailer, after releasing [`App::request_received`] |
/// | `GET /response-trailer` | `200` with `response trailer`, then an `etag: abcd` trailer once [`App::release_response`] is called |
/// | `GET /large` | `200` with [`LARGE_BODY`] zero bytes |
/// | `GET /broken` | a handler error |
/// | `GET /incorrect-length` | `200` declaring 10 bytes and sending `Borked` |
///
/// Anything else receives `404`.
#[derive(Clone, Debug, Default)]
pub struct App {
    request_received: Latch,
    response_released: Latch,
}

impl App {
    /// A fresh application.
    pub fn new() -> Self {
        Self::default()
    }

    /// Released when `/request-trailer` has received its request head.
    pub fn request_received(&self) -> &Latch {
        &self.request_received
    }

    /// Released by [`App::release_response`]; `/response-trailer` waits for it before its trailer.
    pub fn response_released(&self) -> &Latch {
        &self.response_released
    }

    /// Let `/response-trailer` send its trailer.
    pub fn release_response(&self) {
        self.response_released.release();
    }
}

fn text(status: Status, text: impl Into<Vec<u8>>) -> Response<BufferedBody<Vec<u8>>> {
    Response::new(status, BufferedBody::from(text.into()))
}

impl Handler for App {
    type Error = io::Error;
    type Body<B: ReadableBody> = Reply<B>;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        mut request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<Reply<B>>, io::Error> {
        let version = request.version.unwrap_or(Version::Http11);
        let path = request.path.as_deref().unwrap_or_default().to_owned();
        let response = match (&request.method, path.as_str()) {
            (method, "/") if *method == Method::GET || *method == Method::HEAD => {
                let mut response = text(Status::OK, format!("{method} {version}"));
                response
                    .fields
                    .add("my-header", "my-value")
                    .expect("valid field");
                response
            }
            (method, "/") if *method == Method::POST => {
                return Ok(Response::new(Status::OK, Reply::Echo(request.body)));
            }
            (_, "/duplex") => {
                return Ok(Response::new(Status::OK, Reply::Echo(request.body)));
            }
            (_, "/length") => {
                let length = request
                    .body
                    .length()
                    .map_or("unknown".into(), |n| n.to_string());
                text(Status::OK, format!("Content Length: {length}"))
            }
            (_, "/interim") => {
                let mut hint = ResponseMetadata::new(Status::EARLY_HINTS);
                hint.fields.add("link", LINK).expect("valid field");
                // HTTP/1.0 has no interim responses, so the server refuses this one; a hint is advisory and its failure does not prevent the final response.
                let _ = interim.send_interim_response(hint, cancellation).await;
                text(Status::OK, "Hello World")
            }
            (_, "/request-trailer") => {
                self.request_received.release();
                let collected = body::collect(&mut request.body, cancellation)
                    .await
                    .map_err(io::Error::other)?;
                let etag = collected.trailers.as_ref().and_then(|trailers| {
                    trailers
                        .values("etag")
                        .next()
                        .map(|value| String::from_utf8_lossy(value).into_owned())
                });
                text(
                    Status::OK,
                    format!(
                        "{} with etag: {}",
                        String::from_utf8_lossy(&collected.data),
                        etag.unwrap_or_default()
                    ),
                )
            }
            (_, "/response-trailer") => {
                let mut trailers = Fields::new();
                trailers.add("etag", "abcd").expect("valid field");
                let mut response = Response::new(
                    Status::OK,
                    Reply::Staged(Staged::new(
                        [
                            Step::Data(b"response trailer".to_vec()),
                            Step::Wait(self.response_released.clone()),
                            Step::Trailers(trailers),
                        ],
                        None,
                    )),
                );
                response.fields.add("trailer", "etag").expect("valid field");
                return Ok(response);
            }
            (_, "/large") => {
                return Ok(Response::new(
                    Status::OK,
                    Reply::Zeroes(Zeroes::new(LARGE_BODY)),
                ));
            }
            (_, "/broken") => return Err(io::Error::other("simulated failure")),
            (_, "/incorrect-length") => {
                return Ok(Response::new(
                    Status::OK,
                    Reply::Staged(Staged::new([Step::Data(b"Borked".to_vec())], Some(10))),
                ));
            }
            _ => text(Status::NOT_FOUND, "Not Found"),
        };
        Ok(response.map_body(Reply::Buffered))
    }
}

/// A step of a [`Staged`] body.
#[derive(Clone, Debug)]
pub enum Step {
    /// Produce a data frame.
    Data(Vec<u8>),
    /// Wait for a latch before producing anything further.
    Wait(Latch),
    /// Produce a terminal trailer frame.
    Trailers(Fields),
}

/// A body producing scripted frames, waiting on latches between them, with an optional declared length that need not match its data.
#[derive(Clone, Debug)]
pub struct Staged {
    steps: VecDeque<Step>,
    length: Option<u64>,
}

impl Staged {
    /// A body performing `steps` in order, declaring `length`.
    pub fn new(steps: impl IntoIterator<Item = Step>, length: Option<u64>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            length,
        }
    }
}

impl ReadableBody for Staged {
    type Data = Vec<u8>;
    type Error = BodyError;

    fn may_have_trailers(&self) -> bool {
        self.steps
            .iter()
            .any(|step| matches!(step, Step::Trailers(_)))
    }

    fn is_empty(&self) -> bool {
        self.steps.iter().all(|step| matches!(step, Step::Wait(_)))
    }

    fn length(&self) -> Option<u64> {
        self.length
    }

    fn close(&mut self, _: Option<BodyError>) {
        self.steps.clear();
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, BodyError> {
        cancellation.check()?;
        loop {
            // Keep a wait queued until it completes, so a dropped read does not skip it.
            if let Some(Step::Wait(latch)) = self.steps.front() {
                latch.wait(cancellation).await?;
            }
            return Ok(match self.steps.pop_front() {
                Some(Step::Wait(_)) => continue,
                Some(Step::Data(data)) => BodyFrame::Data { data, end: false },
                Some(Step::Trailers(fields)) => BodyFrame::Headers { fields, end: true },
                None => BodyFrame::End,
            });
        }
    }
}

/// The body of an [`App`] response.
#[derive(Debug)]
pub enum Reply<B> {
    /// A static body.
    Buffered(BufferedBody<Vec<u8>>),
    /// The request body, streamed back.
    Echo(B),
    /// A scripted body.
    Staged(Staged),
    /// Zero bytes, produced on demand.
    Zeroes(Zeroes),
}

/// A body of zero bytes produced on demand, which reports its total length however much remains.
#[derive(Debug)]
pub struct Zeroes {
    length: u64,
    remaining: u64,
}

impl Zeroes {
    /// A body of `length` zero bytes.
    pub fn new(length: u64) -> Self {
        Self {
            length,
            remaining: length,
        }
    }
}

impl<B: ReadableBody> ReadableBody for Reply<B> {
    type Data = Vec<u8>;
    type Error = BodyError;

    fn is_ready(&self) -> bool {
        match self {
            Self::Buffered(body) => body.is_ready(),
            Self::Echo(body) => body.is_ready(),
            Self::Staged(_) => false,
            Self::Zeroes(_) => true,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Buffered(body) => body.is_empty(),
            Self::Echo(body) => body.is_empty(),
            Self::Staged(body) => body.is_empty(),
            Self::Zeroes(body) => body.remaining == 0,
        }
    }

    fn length(&self) -> Option<u64> {
        match self {
            Self::Buffered(body) => body.length(),
            Self::Echo(body) => body.length(),
            Self::Staged(body) => body.length(),
            Self::Zeroes(body) => Some(body.length),
        }
    }

    fn may_have_trailers(&self) -> bool {
        match self {
            Self::Buffered(body) => body.may_have_trailers(),
            Self::Echo(body) => body.may_have_trailers(),
            Self::Staged(body) => body.may_have_trailers(),
            Self::Zeroes(_) => false,
        }
    }

    fn close(&mut self, error: Option<BodyError>) {
        match self {
            Self::Buffered(body) => body.close(None),
            Self::Echo(body) => body.close(None),
            Self::Staged(body) => body.close(error),
            Self::Zeroes(body) => body.remaining = 0,
        }
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, BodyError> {
        match self {
            Self::Buffered(body) => Ok(body.read(cancellation).await?),
            Self::Echo(body) => Ok(body
                .read(cancellation)
                .await
                .map_err(BodyError::new)?
                .map_data(|data| data.as_ref().to_vec())),
            Self::Staged(body) => body.read(cancellation).await,
            Self::Zeroes(body) => {
                cancellation.check()?;
                if body.remaining == 0 {
                    return Ok(BodyFrame::End);
                }
                let length = body.remaining.min(LARGE_CHUNK);
                body.remaining -= length;
                Ok(BodyFrame::Data {
                    data: vec![0; length as usize],
                    end: body.remaining == 0,
                })
            }
        }
    }
}

fn never() -> Cancellation {
    Cancellation::never()
}

/// A GET succeeds with a known content length, the application's field and its version.
pub async fn get<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let mut response = context
        .client()
        .call(
            context.request(Method::GET, "/", BufferedBody::<Vec<u8>>::default()),
            &never(),
        )
        .await
        .expect("GET /");
    let expected = format!("GET {}", context.version());
    assert_eq!(response.status, Status::OK);
    assert_eq!(
        response.fields.values("my-header").collect::<Vec<_>>(),
        [b"my-value".as_slice()],
        "the application's field"
    );
    assert_eq!(response.body.length(), Some(expected.len() as u64));
    assert_eq!(body::payload(&mut response.body).await, expected.as_bytes());
}

/// A HEAD succeeds with an empty body that keeps the representation's length.
pub async fn head<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let mut response = context
        .client()
        .call(
            context.request(Method::HEAD, "/", BufferedBody::<Vec<u8>>::default()),
            &never(),
        )
        .await
        .expect("HEAD /");
    assert_eq!(response.status, Status::OK);
    assert!(
        response.body.is_empty(),
        "a HEAD response body must be empty"
    );
    let expected = format!("HEAD {}", context.version());
    assert_eq!(
        response.body.length(),
        Some(expected.len() as u64),
        "the representation length of a HEAD response"
    );
    assert!(body::payload(&mut response.body).await.is_empty());
}

/// A POST streams its body back.
pub async fn post<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let upload = BufferedBody::new([b"Hello".to_vec(), b" ".to_vec(), b"World".to_vec()]);
    let mut response = context
        .client()
        .call(context.request(Method::POST, "/", upload), &never())
        .await
        .expect("POST /");
    assert_eq!(response.status, Status::OK);
    assert_eq!(
        body::payload(&mut response.body).await,
        b"Hello World",
        "the echoed body"
    );
}

// A streamed upload which declares that it has no trailers. A `BodyReader` may always carry them, and HTTP/1.0, which cannot frame trailers, refuses to send such a body.
struct Untrailed(BodyReader<Vec<u8>>);

impl ReadableBody for Untrailed {
    type Data = Vec<u8>;
    type Error = BodyError;

    fn length(&self) -> Option<u64> {
        self.0.length()
    }

    fn may_have_trailers(&self) -> bool {
        false
    }

    fn close(&mut self, error: Option<BodyError>) {
        self.0.close(error);
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, BodyError> {
        self.0.read(cancellation).await
    }
}

/// The data of the two-way stream from `offset`: a pattern with a period of 251 bytes, which does not divide the chunk size, so a lost, repeated or reordered chunk changes the data or its length.
pub fn duplex_data(offset: u64, length: usize) -> Vec<u8> {
    (offset..offset + length as u64)
        .map(|index| (index % 251) as u8)
        .collect()
}

/// A request body streams up while its echo streams down at the same time, each [`DUPLEX_BODY`] long and under backpressure.
///
/// The upload is produced through a small bounded queue while the response is read concurrently. The stream is far larger than the sockets between client and server buffer in practice, so a client which finished its upload before reading the echo would stall the exchange until the test's deadline. The request declares its length and no trailers, so HTTP/1.0 can carry it too.
pub async fn duplex_stream<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let (upload, mut writer) = BodyReader::with_length(4 * DUPLEX_CHUNK, 4, DUPLEX_BODY);
    let request = context.request(Method::POST, "/duplex", Untrailed(upload));
    let produce = async {
        let mut offset = 0;
        while offset < DUPLEX_BODY {
            writer
                .write(duplex_data(offset, DUPLEX_CHUNK), &never())
                .await
                .expect("write the request body");
            offset += DUPLEX_CHUNK as u64;
        }
        writer.close_write(None);
    };
    let consume = async {
        let mut response = context
            .client()
            .call(request, &never())
            .await
            .expect("POST /duplex");
        assert_eq!(response.status, Status::OK);
        let echoed = body::collect(&mut response.body, &never())
            .await
            .map(|collected| collected.data);
        let intact = echoed.as_ref().is_ok_and(|data| {
            data.len() as u64 == DUPLEX_BODY && *data == duplex_data(0, data.len())
        });
        // Report the length or error rather than the data.
        assert!(
            intact,
            "the echoed stream: {:?} of {DUPLEX_BODY} bytes",
            echoed.as_ref().map(Vec::len)
        );
    };
    zip(produce, consume).await;
}

/// The server observes a request body's length.
pub async fn request_length<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let upload = BufferedBody::from(b"Hello World!".to_vec());
    let mut response = context
        .client()
        .call(context.request(Method::POST, "/length", upload), &never())
        .await
        .expect("POST /length");
    assert_eq!(response.body.length(), Some(18));
    assert_eq!(
        body::payload(&mut response.body).await,
        b"Content Length: 12",
        "the request length the server observed"
    );
}

/// An informational response reaches the client's observer before the final response, over HTTP/1.1; HTTP/1.0 has none.
pub async fn interim_response<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observer = observed.clone();
    let mut response = context
        .client()
        .call_with_interim(
            context.request(Method::GET, "/interim", BufferedBody::<Vec<u8>>::default()),
            move |metadata: ResponseMetadata| {
                let link = metadata.fields.values("link").next().map(<[u8]>::to_vec);
                observer.lock().unwrap().push((metadata.status, link));
                async { Ok::<_, Infallible>(()) }
            },
            &never(),
        )
        .await
        .expect("GET /interim");
    assert_eq!(response.status, Status::OK);
    assert_eq!(body::payload(&mut response.body).await, b"Hello World");
    let expected = match context.version() {
        Version::Http10 => vec![],
        _ => vec![(Status::EARLY_HINTS, Some(LINK.as_bytes().to_vec()))],
    };
    assert_eq!(
        *observed.lock().unwrap(),
        expected,
        "the interim responses observed"
    );
}

/// A large body downloads completely.
pub async fn large_body<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let mut response = context
        .client()
        .call(
            context.request(Method::GET, "/large", BufferedBody::<Vec<u8>>::default()),
            &never(),
        )
        .await
        .expect("GET /large");
    assert_eq!(response.status, Status::OK);
    assert_eq!(response.body.length(), Some(LARGE_BODY));
    let data = body::payload(&mut response.body).await;
    assert_eq!(data.len() as u64, LARGE_BODY);
    assert!(data.iter().all(|byte| *byte == 0), "unexpected data");
}

/// A failing handler produces a server error, and the client can still make further requests.
pub async fn broken_server<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let mut response = context
        .client()
        .call(
            context.request(Method::GET, "/broken", BufferedBody::<Vec<u8>>::default()),
            &never(),
        )
        .await
        .expect("GET /broken");
    assert_eq!(
        response.status,
        Status::INTERNAL_SERVER_ERROR,
        "the status of a failing handler"
    );
    assert!(body::payload(&mut response.body).await.is_empty());
    get(context).await;
}

/// A response body shorter than its declared length fails the client's read.
///
/// The server abandons the connection, so the cause depends on the transport: plaintext reports a truncated message, and TLS reports the missing `close_notify` instead.
pub async fn incorrect_length<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let mut response = context
        .client()
        .call(
            context.request(
                Method::GET,
                "/incorrect-length",
                BufferedBody::<Vec<u8>>::default(),
            ),
            &never(),
        )
        .await
        .expect("GET /incorrect-length");
    assert_eq!(response.status, Status::OK);
    assert_eq!(response.body.length(), Some(10));
    let error = body::collect(&mut response.body, &never())
        .await
        .expect_err("a truncated body must fail");
    let chain = crate::error_chain(&error);
    // The innermost cause each transport reports.
    const CAUSES: [(&str, &str); 2] = [
        ("http", "truncated HTTP/1 message"),
        ("https", "close_notify"),
    ];
    let scheme = context.endpoint().scheme();
    let (_, expected) = CAUSES
        .iter()
        .find(|(known, _)| *known == scheme)
        .expect("a known scheme");
    let cause = chain.last().expect("an error message").contains(expected);
    assert!(
        chain[0] == "body failed" && cause,
        "unexpected failure: {chain:?}"
    );
}

/// A request trailer sent after the server has received the request head reaches the handler.
pub async fn request_trailer<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let app: &App = context.handler().borrow();
    let (upload, mut writer) = BodyReader::bounded(1024, 4);
    let mut request = context.request(Method::POST, "/request-trailer", upload);
    request.fields.add("trailer", "etag").expect("valid field");
    let produce = async {
        writer
            .write(b"Hello".to_vec(), &never())
            .await
            .expect("write the request body");
        app.request_received()
            .wait(&never())
            .await
            .expect("wait for the request");
        let mut trailers = Fields::new();
        trailers.add("etag", "abcd").expect("valid field");
        writer
            .write_frame(
                BodyFrame::Headers {
                    fields: trailers,
                    end: true,
                },
                &never(),
            )
            .await
            .expect("write the request trailer");
    };
    let (response, ()) = zip(context.client().call(request, &never()), produce).await;
    let mut response = response.expect("POST /request-trailer");
    assert_eq!(response.status, Status::OK);
    assert_eq!(
        body::payload(&mut response.body).await,
        b"Hello with etag: abcd",
        "the request data and trailer the handler received"
    );
}

/// A response trailer, sent after the client has received the response head and data, reaches the client after the data.
pub async fn response_trailer<S: Scheduler, H: Borrow<App>>(context: &Context<'_, S, H>) {
    let app: &App = context.handler().borrow();
    let mut response = context
        .client()
        .call(
            context.request(
                Method::GET,
                "/response-trailer",
                BufferedBody::<Vec<u8>>::default(),
            ),
            &never(),
        )
        .await
        .expect("GET /response-trailer");
    assert_eq!(response.status, Status::OK);
    assert!(response.fields.contains("trailer"));
    assert!(!response.fields.contains("etag"));
    let frame = response.body.read(&never()).await.expect("read data");
    assert!(
        matches!(&frame, BodyFrame::Data { data, end: false } if data == b"response trailer"),
        "unexpected frame: {frame:?}"
    );
    app.release_response();
    let collected = body::collect(&mut response.body, &never())
        .await
        .expect("read the trailer");
    assert!(collected.data.is_empty());
    let trailers = collected.trailers.expect("a response trailer");
    assert_eq!(
        trailers.values("etag").collect::<Vec<_>>(),
        [b"abcd".as_slice()],
        "the response trailer"
    );
}

/// Generate `#[test]` functions for the version-independent protocol cases, ported from `async-http`'s `AProtocol`.
///
/// The runner receives a fresh [`App`] and an async closure taking the running [`Context`]. It chooses the version, options and transport, typically by serving the application with a [`ServerContext`](crate::ServerContext), and owns the deadline. [`ServerContext::run`](crate::ServerContext::run) runs the server, the client and the case on the test's thread, which is reproducible; [`ServerContext::run_spawned`](crate::ServerContext::run_spawned) runs the server on the scheduler's workers in parallel with the client, which can expose cross-thread wake-up bugs and races. Invoke it inside a dedicated module:
///
/// ```
/// use base_http::Version;
/// use base_http_fixtures::{Context, ServerContext, protocol::App};
/// use base_scheduler_native::NativeScheduler;
/// use std::sync::Arc;
///
/// fn http10(app: App, test: impl AsyncFnOnce(&Context<'_, NativeScheduler, App>)) {
///     ServerContext::new()
///         .handler(app)
///         .version(Version::Http10)
///         .run(test);
/// }
///
/// mod http10 {
///     base_http_fixtures::protocol_tests!(run: super::http10);
/// }
///
/// // The same cases with the server spawned onto the scheduler's workers, in parallel with the client.
/// fn http10_spawned(app: App, test: impl AsyncFnOnce(&Context<'_, NativeScheduler, Arc<App>>)) {
///     ServerContext::new()
///         .handler(app)
///         .version(Version::Http10)
///         .run_spawned(test);
/// }
///
/// mod http10_spawned {
///     base_http_fixtures::protocol_tests!(run: super::http10_spawned);
/// }
/// # fn main() {}
/// ```
#[macro_export]
macro_rules! protocol_tests {
    (run: $run:path $(,)?) => {
        $crate::__protocol_cases! { $run;
            get,
            head,
            post,
            request_length,
            interim_response,
            large_body,
            broken_server,
            incorrect_length,
            duplex_stream,
        }
    };
}

/// Generate `#[test]` functions for the protocol cases that need trailers, which HTTP/1.0 cannot send. The runner is as for [`protocol_tests!`](crate::protocol_tests).
#[macro_export]
macro_rules! protocol_trailer_tests {
    (run: $run:path $(,)?) => {
        $crate::__protocol_cases! { $run;
            request_trailer,
            response_trailer,
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __protocol_cases {
    ($run:path; $($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                $run(
                    $crate::protocol::App::new(),
                    async |context| $crate::protocol::$name(context).await,
                );
            }
        )+
    };
}
