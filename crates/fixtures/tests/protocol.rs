// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Each protocol case fails against an application which answers every request with `404`.
use base_http::{InterimHandler, InterimResponses, Request, Response, body::ReadableBody};
use base_http_fixtures::{
    ServerContext,
    protocol::{self, App, Reply},
};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use std::{borrow::Borrow, io, time::Duration};

/// The shared application behind a router that sends every request to a missing route.
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
        mut request: Request<B>,
        interim: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<Reply<B>>, io::Error> {
        request.path = Some("/missing".into());
        self.0.call(request, interim, cancellation).await
    }
}

macro_rules! misrouted {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            #[should_panic]
            fn $name() {
                ServerContext::new()
                    .handler(Misrouted(App::new()))
                    // The trailer cases wait for the application, which a missing route never releases.
                    .timeout(Duration::from_millis(200))
                    .run(async |context| protocol::$name(context).await);
            }
        )+
    };
}

misrouted! {
    get,
    head,
    post,
    request_length,
    interim_response,
    large_body,
    broken_server,
    incorrect_length,
    request_trailer,
    response_trailer,
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
