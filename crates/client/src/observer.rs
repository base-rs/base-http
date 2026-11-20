// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, driver::Work};
use base_http::{InterimHandler, ResponseMetadata};
use base_scheduler::Cancellation;

/// A type-erased interim-response observer, retained by an exchange's response
/// reader for the client's scope.
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
                .map_err(Error::body)
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use base_http::Status;
    use futures_lite::future::block_on;
    use std::convert::Infallible;

    // An interim handler which ignores informational responses.
    pub(crate) fn ignore(_: ResponseMetadata) -> std::future::Ready<Result<(), Infallible>> {
        std::future::ready(Ok(()))
    }

    #[test]
    fn interim_forwarding_reaches_the_observer() {
        let mut observer = ignore;
        let mut interim = Interim(&mut observer);
        let metadata = ResponseMetadata::new(Status::CONTINUE);
        block_on(interim.on_interim_response(metadata, &Cancellation::never())).unwrap();
    }
}
