// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, driver::Work};
use base_http::{
    InterimHandler, ResponseMetadata,
    body::{BodyFrame, ReadableBody},
};
use base_scheduler::Cancellation;
use futures_util::{
    StreamExt,
    stream::{Stream, try_unfold},
};
use std::pin::Pin;

struct Guard<B: ReadableBody>(B);
impl<B: ReadableBody> Drop for Guard<B> {
    fn drop(&mut self) {
        self.0.close(None);
    }
}

// One allocation stores the monomorphized producer and its in-flight read
// state. Polling this stream does not allocate a boxed future per body frame.
type Producer<'scope> =
    Pin<Box<dyn Stream<Item = Result<BodyFrame<Vec<u8>>, Error>> + Send + 'scope>>;
pub(crate) struct OutgoingBody<'scope> {
    stream: Option<Producer<'scope>>,
    length: Option<u64>,
    trailers: bool,
    empty: bool,
}
impl<'scope> OutgoingBody<'scope> {
    pub fn new<B>(body: B, cancellation: Cancellation) -> Self
    where
        B: ReadableBody + 'scope,
        B::Data: Into<Vec<u8>>,
    {
        let length = body.length();
        let trailers = body.may_have_trailers();
        let empty = body.is_empty();
        let stream = try_unfold(
            (Guard(body), cancellation, false),
            |(mut body, cancellation, end)| async move {
                if end {
                    return Ok(None);
                }
                let frame = body
                    .0
                    .read(&cancellation)
                    .await
                    .map_err(|error| Error::Body(base_http::body::BodyError::new(error)))?;
                let end = frame.is_end();
                Ok(Some((
                    frame.map_data(Into::into),
                    (body, cancellation, end),
                )))
            },
        );
        Self {
            stream: Some(Box::pin(stream)),
            length,
            trailers,
            empty,
        }
    }
}
impl ReadableBody for OutgoingBody<'_> {
    type Data = Vec<u8>;
    type Error = Error;
    fn length(&self) -> Option<u64> {
        self.length
    }
    fn may_have_trailers(&self) -> bool {
        self.trailers
    }
    fn is_empty(&self) -> bool {
        self.empty
    }
    fn close(&mut self, _: Option<Error>) {
        self.stream = None;
        self.empty = true;
    }
    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let frame = match &mut self.stream {
            Some(stream) => stream.next().await.transpose()?.unwrap_or(BodyFrame::End),
            None => BodyFrame::End,
        };
        self.empty = frame.is_end();
        Ok(frame)
    }
}

pub(crate) trait Observer: Send {
    fn response<'a>(
        &'a mut self,
        metadata: ResponseMetadata,
        cancellation: &'a Cancellation,
    ) -> Work<'a, Result<(), Error>>;
}
impl<I: InterimHandler> Observer for I {
    fn response<'a>(
        &'a mut self,
        metadata: ResponseMetadata,
        cancellation: &'a Cancellation,
    ) -> Work<'a, Result<(), Error>> {
        Box::pin(async move {
            self.on_interim_response(metadata, cancellation)
                .await
                .map_err(|error| Error::Body(base_http::body::BodyError::new(error)))
        })
    }
}
pub(crate) struct Interim<'a>(pub &'a mut dyn Observer);
impl InterimHandler for Interim<'_> {
    type Error = Error;
    async fn on_interim_response(
        &mut self,
        metadata: ResponseMetadata,
        cancellation: &Cancellation,
    ) -> Result<(), Error> {
        self.0.response(metadata, cancellation).await
    }
}
