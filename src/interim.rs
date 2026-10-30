// Released under the MIT License.
// Copyright, 2019-2026, by Samuel Williams.

//! Per-exchange interim-response hooks, independent of message heads and bodies.

use crate::{ResponseMetadata, Status, future::cancellable};
use base_scheduler::Cancellation;
use std::{error::Error, fmt, future::Future};

/// A protocol writer or client observer for an interim response head.
pub trait InterimHandler: Send {
    type Error: Error + Send + Sync + 'static;
    fn on_interim_response(
        &mut self,
        response: ResponseMetadata,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<F, Fut, E> InterimHandler for F
where
    F: FnMut(ResponseMetadata) -> Fut + Send,
    Fut: Future<Output = Result<(), E>> + Send,
    E: Error + Send + Sync + 'static,
{
    type Error = E;
    async fn on_interim_response(
        &mut self,
        response: ResponseMetadata,
        _: &Cancellation,
    ) -> Result<(), E> {
        self(response).await
    }
}

/// An interim response failure. `Handler` is transparent: it displays the
/// handler's error and reports that error's source.
#[derive(Debug)]
pub enum InterimError<E> {
    InvalidStatus(Status),
    Finished,
    Cancelled,
    Handler(E),
}

impl<E: fmt::Display> fmt::Display for InterimError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStatus(status) => write!(formatter, "{status} is not an interim response"),
            Self::Finished => formatter.write_str("interim responses have finished"),
            Self::Cancelled => formatter.write_str("interim response cancelled"),
            Self::Handler(error) => error.fmt(formatter),
        }
    }
}
impl<E: Error + 'static> Error for InterimError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Handler(error) => error.source(),
            _ => None,
        }
    }
}

/// Explicit server responder/client observer state for one exchange.
///
/// Accepted interim heads are delivered in order without accumulating a log.
/// `101` is a final protocol-switch response and is never sent to the hook.
/// The driver calls `finish` when committing/receiving its final head. Failure
/// or abandonment of a started send also ends this capability, since a handler
/// may have made partial wire progress. Invalid heads and pre-cancellation do
/// not consume it. The handler itself is statically dispatched.
pub struct InterimResponses<H> {
    handler: H,
    finished: bool,
}

impl<H> InterimResponses<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            finished: false,
        }
    }
    pub fn finish(&mut self) {
        self.finished = true;
    }
    pub fn is_finished(&self) -> bool {
        self.finished
    }
    pub fn into_inner(self) -> H {
        self.handler
    }
}

impl<H: InterimHandler> InterimResponses<H> {
    pub async fn send_interim_response(
        &mut self,
        response: ResponseMetadata,
        cancellation: &Cancellation,
    ) -> Result<(), InterimError<H::Error>> {
        cancellation.check().map_err(|_| InterimError::Cancelled)?;
        if self.finished {
            return Err(InterimError::Finished);
        }
        if !response.status.is_informational() || response.status.is_final() {
            return Err(InterimError::InvalidStatus(response.status));
        }
        self.finished = true;
        cancellable(
            self.handler.on_interim_response(response, cancellation),
            cancellation,
        )
        .await
        .map_err(|_| InterimError::Cancelled)?
        .map_err(InterimError::Handler)?;
        self.finished = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn handler_errors_are_transparent() {
        let error = InterimError::Handler(crate::body::BodyError::new(io::Error::other(
            "observer failed",
        )));
        assert_eq!(error.to_string(), "body failed");
        assert_eq!(error.source().unwrap().to_string(), "observer failed");
    }
}
