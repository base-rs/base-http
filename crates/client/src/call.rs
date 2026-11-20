// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Requests with a body to send: the calling future writes the body until the
//! response head arrives, then the client's driver writes any remainder.
use crate::{
    Client, Error, RequestBodyWriter, StreamingResponseBody, WritableRequest, driver::checked,
};
use base_http::{
    ClientContext, Handler, HandlerContext, InterimHandler, InterimRelay, Request, RequestMetadata,
    Response, ResponseMetadata,
    body::{Chunk, ReadableBody},
};
use base_scheduler::Cancellation;
use futures_util::future::{Either, select};
use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

/// A request body being written to its exchange's output.
struct Upload<'scope, B> {
    source: B,
    sink: RequestBodyWriter<'scope>,
    cancellation: Cancellation,
}

impl<B: ReadableBody> Upload<'_, B> {
    /// Write the rest of the body. Dropping this future loses nothing: the sink
    /// retains a write in progress, and a pending read consumes nothing.
    async fn run(&mut self) -> Result<(), Error> {
        loop {
            poll_fn(|cx| self.sink.poll_writable(cx, &self.cancellation)).await?;
            if self.sink.is_ended() {
                return Ok(());
            }
            // A read waiting for the source stops when the exchange ends
            // early, such as when its response is dropped unfinished.
            let read = async {
                self.source
                    .read(&self.cancellation)
                    .await
                    .map_err(Error::body)
            };
            let frame = checked(read, self.sink.exchange_cancellation()).await?;
            self.sink
                .start_frame(frame.map_data(Chunk::into_vec), &self.cancellation);
        }
    }

    /// Stop writing after a failure, abandoning the exchange's output.
    fn abort(mut self) {
        self.sink.abort();
        self.source.close(None);
    }
}

/// Poll `work` while writing `upload`. Returns `work`'s result and whether the
/// upload finished; an upload failure is returned in place of `work`'s result.
async fn alongside<B: ReadableBody, T>(
    upload: &mut Upload<'_, B>,
    work: impl Future<Output = Result<T, Error>>,
) -> (Result<T, Error>, bool) {
    let mut work = pin!(work);
    let mut finished = false;
    let mut run = pin!(upload.run());
    let result = poll_fn(|cx| {
        if !finished && let Poll::Ready(result) = run.as_mut().poll(cx) {
            result?;
            finished = true;
        }
        work.as_mut().poll(cx)
    })
    .await;
    (result, finished)
}

/// Split `request`, declaring its body's known length so the request is
/// framed by its body, as a server's response is: a body of known length is
/// sent with `Content-Length`, which a contradicting declaration cannot change.
fn framed<B: ReadableBody>(request: Request<B>) -> (RequestMetadata, B) {
    let (mut metadata, source) = request.into_parts();
    if let Some(length) = source.length() {
        metadata.content_length = Some(length);
    }
    (metadata, source)
}

fn ignore(_: ResponseMetadata) -> std::future::Ready<Result<(), Infallible>> {
    std::future::ready(Ok(()))
}

impl<'scope> Client<'scope> {
    /// Send `request` and return its response once the head arrives.
    ///
    /// This future writes the request body until then; any remainder is
    /// written by the client's driver, so the body must outlive the client's
    /// scope, as a server's request body does. The response body reads directly
    /// from the connection as it is read, so read it, or drop it to give the
    /// connection up. Interim responses are discarded; call the client through
    /// [`Handler`] with a [`ClientContext`] to observe them. A reused idle
    /// connection is checked before any request bytes are sent, and one the
    /// server closed is replaced. No automatic retries occur.
    pub async fn call<B: ReadableBody + 'scope>(
        &self,
        request: Request<B>,
        cancellation: &Cancellation,
    ) -> Result<Response<StreamingResponseBody<'scope>>, Error> {
        let (metadata, mut source) = framed(request);
        let opened = match self
            .open_request_with_interim(metadata, ignore, cancellation)
            .await
        {
            Ok(opened) => opened,
            Err(error) => {
                source.close(None);
                return Err(error);
            }
        };
        self.send(source, opened, |pending| pending, cancellation)
            .await
    }

    /// As [`call`](Self::call), relaying interim responses to `context`'s
    /// observer as they arrive.
    async fn call_with_context<B: ReadableBody + 'scope, I: InterimHandler>(
        &self,
        request: Request<B>,
        context: &mut ClientContext<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<StreamingResponseBody<'scope>>, Error> {
        let (metadata, mut source) = framed(request);
        // The exchange retains its observer for the client's scope, longer
        // than this borrow of the context, so it hands each interim response
        // to this call, which relays it while waiting for the final head.
        let relay = InterimRelay::new();
        let opened = match self
            .open_request_with_interim(metadata, relay.sender(), cancellation)
            .await
        {
            Ok(opened) => opened,
            Err(error) => {
                source.close(None);
                return Err(error);
            }
        };
        let relay = &relay;
        self.send(
            source,
            opened,
            |mut pending| async move {
                loop {
                    let interim = match select(&mut pending, pin!(relay.next())).await {
                        Either::Left((response, _)) => return response,
                        Either::Right((interim, _)) => interim,
                    };
                    let observed = context
                        .interim()
                        .send_interim_response(interim, cancellation)
                        .await;
                    relay.complete(observed);
                }
            },
            cancellation,
        )
        .await
    }

    async fn send<B, F, Head>(
        &self,
        source: B,
        opened: WritableRequest<'scope>,
        head: F,
        cancellation: &Cancellation,
    ) -> Result<Response<StreamingResponseBody<'scope>>, Error>
    where
        B: ReadableBody + 'scope,
        F: FnOnce(crate::PendingResponse<'scope>) -> Head,
        Head: Future<Output = Result<Response<StreamingResponseBody<'scope>>, Error>>,
    {
        let (sink, pending) = opened.split();
        let mut upload = Upload {
            source,
            sink,
            cancellation: cancellation.clone(),
        };
        let (response, finished) = alongside(&mut upload, head(pending)).await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                upload.abort();
                return Err(error);
            }
        };
        if !finished {
            // A closed queue drops this work, whose sink abandons the exchange.
            let _ = self.queue.push_work(Box::pin(async move {
                if upload.run().await.is_err() {
                    upload.abort();
                }
            }));
        }
        Ok(response)
    }
}

/// A client is the upstream end of the shared request interface: middleware
/// written for any context, or for [`ClientContext`], wraps it.
impl<'scope, I: InterimHandler> Handler<'scope, ClientContext<I>> for Client<'scope> {
    type Error = Error;
    type Body<B: ReadableBody + 'scope> = StreamingResponseBody<'scope>;

    fn call<B: ReadableBody + 'scope>(
        &self,
        request: Request<B>,
        context: &mut ClientContext<I>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Response<StreamingResponseBody<'scope>>, Error>> + Send {
        self.call_with_context(request, context, cancellation)
    }
}
