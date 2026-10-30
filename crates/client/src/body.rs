// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Error, driver::lock};
use base_http::body::{BodyError, BodyFrame, BodyReader, ReadableBody};
use base_scheduler::Cancellation;
use event_listener::Event;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Reading,
    Complete,
    Abandoned,
}

pub(crate) struct Consumption {
    state: Mutex<State>,
    changed: Event,
    cancellation: Cancellation,
}
impl Consumption {
    pub fn new(cancellation: Cancellation) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::Reading),
            changed: Event::new(),
            cancellation,
        })
    }
    pub(crate) fn complete(&self) {
        let mut state = lock(&self.state);
        if *state == State::Reading {
            *state = State::Complete;
        }
        drop(state);
        self.changed.notify(usize::MAX);
    }
    fn abandon(&self) {
        let mut state = lock(&self.state);
        if *state != State::Reading {
            return;
        }
        *state = State::Abandoned;
        drop(state);
        self.cancellation.cancel();
        self.changed.notify(usize::MAX);
    }
    pub async fn wait(&self) -> Result<(), Error> {
        loop {
            event_listener::listener!(self.changed => listener);
            match *lock(&self.state) {
                State::Complete => return Ok(()),
                State::Abandoned => return Err(Error::Abandoned),
                State::Reading => {}
            }
            listener.await;
        }
    }
}

/// Incoming response frames. Completion or abandonment is reported to the
/// client driver; response reads do not drive request upload or socket I/O.
pub struct ResponseBody {
    pub(crate) reader: BodyReader,
    pub(crate) consumption: Arc<Consumption>,
    pub(crate) length: Option<u64>,
    pub(crate) trailers: bool,
}
impl ReadableBody for ResponseBody {
    type Data = Vec<u8>;
    type Error = BodyError;
    fn is_ready(&self) -> bool {
        self.reader.is_ready()
    }
    fn is_empty(&self) -> bool {
        self.reader.is_empty()
    }
    fn length(&self) -> Option<u64> {
        self.length
    }
    fn may_have_trailers(&self) -> bool {
        self.trailers
    }
    fn close(&mut self, error: Option<BodyError>) {
        self.consumption.abandon();
        self.reader.close(error);
    }
    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, BodyError> {
        let frame = self.reader.read(cancellation).await?;
        if frame.is_end() {
            self.consumption.complete();
        }
        Ok(frame)
    }
}
impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.consumption.abandon();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;

    #[test]
    fn abandonment_is_final_and_cancels_the_exchange() {
        let cancellation = Cancellation::new();
        let consumption = Consumption::new(cancellation.clone());
        consumption.abandon();
        assert!(cancellation.is_cancelled());
        // A late end of input cannot turn an abandoned body into a reusable exchange.
        consumption.complete();
        let error = block_on(consumption.wait()).unwrap_err();
        assert_eq!(error.to_string(), "HTTP client response abandoned");
    }
}
