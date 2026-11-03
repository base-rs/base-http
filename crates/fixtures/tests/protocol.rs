// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Each protocol case fails, at the assertion it exists for, against an application which breaks that case.
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, ResponseMetadata, Status, Version,
    body::{BufferedBody, ReadableBody},
};
use base_http_fixtures::{
    ServerContext, body,
    protocol::{self, App, LARGE_BODY, Reply, Staged, Step},
};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use std::{borrow::Borrow, io, time::Duration};

/// The shared application, sabotaged on every route the protocol cases use.
struct Sabotaged(App);

impl Borrow<App> for Sabotaged {
    fn borrow(&self) -> &App {
        &self.0
    }
}

fn reply<B>(data: impl Into<Vec<u8>>) -> Response<Reply<B>> {
    Response::new(Status::OK, Reply::Buffered(BufferedBody::from(data.into())))
}

fn route<B>(mut request: Request<B>, path: &str) -> Request<B> {
    request.path = Some(path.into());
    request
}

impl Handler for Sabotaged {
    type Error = io::Error;
    type Body<B: ReadableBody> = Reply<B>;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        mut request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<Reply<B>>, io::Error> {
        let path = request.path.as_deref().unwrap_or_default().to_owned();
        Ok(match (&request.method, path.as_str()) {
            // Omit the application's field.
            (method, "/") if *method == Method::GET => reply(format!(
                "GET {}",
                request.version.unwrap_or(Version::Http11)
            )),
            // Misreport the representation's length.
            (method, "/") if *method == Method::HEAD => reply(""),
            // Answer instead of echoing.
            (_, "/") => reply("not an echo"),
            // Misreport the request's length.
            (_, "/length") => reply("Content Length: 0!"),
            // Send a hint without its link.
            (_, "/interim") => {
                let _ = interim
                    .send_interim_response(ResponseMetadata::new(Status::EARLY_HINTS), cancellation)
                    .await;
                reply("Hello World")
            }
            // Send nonzero bytes.
            (_, "/large") => reply(vec![1; LARGE_BODY as usize]),
            // Succeed instead of failing.
            (_, "/broken") => {
                return self
                    .0
                    .call(route(request, "/"), interim, cancellation)
                    .await;
            }
            // Send the whole declared length.
            (_, "/incorrect-length") => reply("Borked!!!!"),
            // Drop the request trailer, or over HTTP/1.0, which drops it, claim to have received it.
            (_, "/request-trailer") => {
                self.0.request_received().release();
                let collected = body::collect(&mut request.body, cancellation)
                    .await
                    .map_err(io::Error::other)?;
                let etag = if request.version == Some(Version::Http10) {
                    "abcd"
                } else {
                    ""
                };
                reply(format!(
                    "{} with etag: {etag}",
                    String::from_utf8_lossy(&collected.data)
                ))
            }
            // Over HTTP/1.0, send the trailer as data after the release.
            (_, "/response-trailer") if request.version == Some(Version::Http10) => Response::new(
                Status::OK,
                Reply::Staged(Staged::new(
                    [
                        Step::Data(b"response trailer".to_vec()),
                        Step::Wait(self.0.response_released().clone()),
                        Step::Data(b"etag: abcd".to_vec()),
                    ],
                    None,
                )),
            ),
            // Wait for the release, then end without the trailer.
            (_, "/response-trailer") => {
                let mut response = Response::new(
                    Status::OK,
                    Reply::Staged(Staged::new(
                        [
                            Step::Data(b"response trailer".to_vec()),
                            Step::Wait(self.0.response_released().clone()),
                        ],
                        None,
                    )),
                );
                response.fields.add("trailer", "etag").expect("valid field");
                response
            }
            _ => unreachable!("no protocol case requests {path}"),
        })
    }
}

macro_rules! sabotaged {
    ($($name:ident => $message:literal),+ $(,)?) => {
        $(
            #[test]
            #[should_panic(expected = $message)]
            fn $name() {
                ServerContext::new()
                    .handler(Sabotaged(App::new()))
                    .run(async |context| protocol::$name(context).await);
            }
        )+
    };
}

sabotaged! {
    get => "the application's field",
    head => "the representation length of a HEAD response",
    post => "the echoed body",
    request_length => "the request length the server observed",
    interim_response => "the interim responses observed",
    large_body => "unexpected data",
    broken_server => "the status of a failing handler",
    incorrect_length => "a truncated body must fail",
    request_trailer => "the request data and trailer the handler received",
    response_trailer => "the response trailer",
}

// Over HTTP/1.0 the trailer cases expect the trailers to be dropped and the data intact.
#[test]
#[should_panic(expected = "the request data and trailer the handler received")]
fn request_trailer_http10() {
    ServerContext::new()
        .handler(Sabotaged(App::new()))
        .version(Version::Http10)
        .run(async |context| protocol::request_trailer(context).await);
}

#[test]
#[should_panic(expected = "data after the response trailer")]
fn response_trailer_http10() {
    ServerContext::new()
        .handler(Sabotaged(App::new()))
        .version(Version::Http10)
        .run(async |context| protocol::response_trailer(context).await);
}

/// The application behind a router that sends every request to a missing route.
struct Misrouted(App);

impl Borrow<App> for Misrouted {
    fn borrow(&self) -> &App {
        &self.0
    }
}

impl Handler for Misrouted {
    type Error = io::Error;
    type Body<B: ReadableBody> = Reply<B>;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<Reply<B>>, io::Error> {
        self.0
            .call(route(request, "/missing"), interim, cancellation)
            .await
    }
}

// Unknown routes are not found, and a case waiting on the application fails at its deadline.
#[test]
#[should_panic(expected = "test exceeded")]
fn a_misrouted_application_is_not_found() {
    ServerContext::new()
        .handler(Misrouted(App::new()))
        .timeout(Duration::from_millis(200))
        .run(async |context| {
            let request = context.request(Method::GET, "/", BufferedBody::<Vec<u8>>::default());
            let response = context
                .client()
                .call(request, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(response.status, Status::NOT_FOUND);
            drop(response);
            protocol::request_trailer(context).await;
        });
}

// An upload truncated after its head fails the handler's read, so the server rejects the request instead of answering it.
#[test]
fn a_truncated_trailer_upload_fails_the_handler() {
    ServerContext::new()
        .handler(App::new())
        .run(async |context| {
            let received = context.handler().request_received().clone();
            context
                .raw(move |mut peer| {
                    peer.write("POST /request-trailer HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n");
                    received.wait_blocking();
                    peer.shutdown_write();
                    let response = String::from_utf8(peer.read_to_end()).unwrap();
                    assert!(
                        !response.starts_with("HTTP/1.1 200 "),
                        "unexpected response: {response:?}"
                    );
                })
                .await;
        });
}
