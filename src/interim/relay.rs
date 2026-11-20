// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Interim responses handed from a handler to the server that writes them.
//!
//! A server creates one [`InterimRelay`] per connection and gives each
//! exchange's handler an [`InterimSender`]. A send hands its response head to
//! the relay and waits until the server has written it. The server writes the
//! head while it continues to poll the handler, so the handler's other work
//! proceeds meanwhile. The sender borrows nothing from the server, so a context
//! holding it has no lifetime, and handing a head over allocates nothing.
use super::InterimHandler;
use crate::ResponseMetadata;
use base_scheduler::Cancellation;
use std::{
    error::Error,
    fmt,
    future::poll_fn,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Poll, Waker},
};

struct State {
    /// The exchange whose senders may hand over responses.
    exchange: u64,
    /// A response handed over and not yet taken by the server.
    pending: Option<ResponseMetadata>,
    /// The server is writing the response it took.
    writing: bool,
    /// How writing the taken response ended, until its sender collects it.
    result: Option<Result<(), InterimRelayError>>,
    /// The server waiting for a response, or a no-op waker.
    relay: Waker,
    /// The sender waiting for its result, or a no-op waker.
    sender: Waker,
}

struct Shared {
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn register(slot: &mut Waker, waker: &Waker) {
    slot.clone_from(waker);
}

/// Take the registered waker, leaving a no-op one.
fn take(slot: &mut Waker) -> Waker {
    std::mem::replace(slot, Waker::noop().clone())
}

/// The server's end of a connection's interim responses.
pub struct InterimRelay {
    shared: Arc<Shared>,
}

impl Default for InterimRelay {
    fn default() -> Self {
        Self::new()
    }
}

impl InterimRelay {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    exchange: 0,
                    pending: None,
                    writing: false,
                    result: None,
                    relay: Waker::noop().clone(),
                    sender: Waker::noop().clone(),
                }),
            }),
        }
    }

    /// A hook for the current exchange's handler.
    pub fn sender(&self) -> InterimSender {
        InterimSender {
            shared: self.shared.clone(),
            exchange: self.shared.lock().exchange,
        }
    }

    /// Wait for the next response a sender hands over. Dropping the wait takes
    /// nothing. Report how writing it ended with [`complete`](Self::complete).
    pub async fn next(&self) -> ResponseMetadata {
        poll_fn(|cx| {
            let mut state = self.shared.lock();
            match state.pending.take() {
                Some(response) => {
                    state.writing = true;
                    Poll::Ready(response)
                }
                None => {
                    register(&mut state.relay, cx.waker());
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Report how writing the response taken by [`next`](Self::next) ended,
    /// waking its sender.
    pub fn complete<E: Error + Send + Sync + 'static>(&self, result: Result<(), E>) {
        let sender = {
            let mut state = self.shared.lock();
            if !state.writing {
                return;
            }
            state.writing = false;
            state.result = Some(result.map_err(|error| InterimRelayError::Failed(Box::new(error))));
            take(&mut state.sender)
        };
        sender.wake();
    }

    /// End the current exchange. Its senders' later sends fail with
    /// [`InterimRelayError::Finished`], and one not yet taken is discarded.
    pub fn finish(&self) {
        let sender = {
            let mut state = self.shared.lock();
            state.exchange += 1;
            state.pending = None;
            state.writing = false;
            state.result = None;
            take(&mut state.sender)
        };
        sender.wake();
    }
}

/// A handler's hook for one exchange's interim responses, which the server
/// writes. Each send returns once the server has written and flushed the head.
pub struct InterimSender {
    shared: Arc<Shared>,
    exchange: u64,
}

/// Withdraws a handed-over response that the server has not taken, when its
/// send is dropped.
struct Withdraw<'a>(&'a InterimSender);

impl Drop for Withdraw<'_> {
    fn drop(&mut self) {
        let mut state = self.0.shared.lock();
        if state.exchange == self.0.exchange {
            state.pending = None;
            state.result = None;
        }
    }
}

impl InterimHandler for InterimSender {
    type Error = InterimRelayError;
    async fn on_interim_response(
        &mut self,
        response: ResponseMetadata,
        _: &Cancellation,
    ) -> Result<(), InterimRelayError> {
        let relay = {
            let mut state = self.shared.lock();
            if state.exchange != self.exchange {
                return Err(InterimRelayError::Finished);
            }
            state.pending = Some(response);
            state.result = None;
            take(&mut state.relay)
        };
        relay.wake();
        let _withdraw = Withdraw(self);
        poll_fn(|cx| {
            let mut state = self.shared.lock();
            if state.exchange != self.exchange {
                return Poll::Ready(Err(InterimRelayError::Finished));
            }
            if let Some(result) = state.result.take() {
                return Poll::Ready(result);
            }
            register(&mut state.sender, cx.waker());
            Poll::Pending
        })
        .await
    }
}

/// Why a relayed interim response was not sent. `Failed` is transparent: it
/// displays the server's error and reports that error's source.
#[derive(Debug)]
pub enum InterimRelayError {
    /// The exchange ended, so it accepts no more interim responses.
    Finished,
    /// The server failed to write the response.
    Failed(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for InterimRelayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finished => formatter.write_str("the exchange has finished"),
            Self::Failed(error) => error.fmt(formatter),
        }
    }
}

impl Error for InterimRelayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Finished => None,
            Self::Failed(error) => error.source(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InterimResponses, Status};
    use futures_lite::future::{block_on, poll_once, zip};
    use std::{io, pin::pin};

    /// An error with a source, as protocol writers report I/O failures.
    #[derive(Debug)]
    struct Wrapped(io::Error);
    impl fmt::Display for Wrapped {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("interim write failed")
        }
    }
    impl Error for Wrapped {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }

    fn hint() -> ResponseMetadata {
        ResponseMetadata::new(Status::EARLY_HINTS)
    }

    #[test]
    fn a_send_returns_once_the_server_has_written_its_response() {
        let relay = InterimRelay::default();
        let mut interim = InterimResponses::new(relay.sender());
        let never = Cancellation::never();
        let (sent, ()) = block_on(zip(interim.send_interim_response(hint(), &never), async {
            let response = relay.next().await;
            assert_eq!(response.status, Status::EARLY_HINTS);
            relay.complete(Ok::<_, io::Error>(()));
        }));
        sent.unwrap();
        // A completion with nothing being written changes nothing.
        relay.complete(Ok::<_, io::Error>(()));
    }

    #[test]
    fn a_failed_write_is_reported_transparently() {
        let relay = InterimRelay::new();
        let mut sender = relay.sender();
        let never = Cancellation::never();
        let (sent, ()) = block_on(zip(sender.on_interim_response(hint(), &never), async {
            relay.next().await;
            relay.complete(Err(Wrapped(io::Error::other("connection reset"))));
        }));
        let error = sent.unwrap_err();
        assert_eq!(error.to_string(), "interim write failed");
        assert_eq!(error.source().unwrap().to_string(), "connection reset");
        assert!(InterimRelayError::Finished.source().is_none());
    }

    #[test]
    fn senders_of_a_finished_exchange_are_refused() {
        let relay = InterimRelay::new();
        let mut stale = relay.sender();
        let never = Cancellation::never();
        {
            // A send waiting when its exchange finishes is refused too.
            let mut waiting = pin!(stale.on_interim_response(hint(), &never));
            assert!(block_on(poll_once(waiting.as_mut())).is_none());
            relay.finish();
            let refused = block_on(waiting).unwrap_err();
            assert_eq!(refused.to_string(), "the exchange has finished");
        }
        let refused = block_on(stale.on_interim_response(hint(), &never)).unwrap_err();
        assert_eq!(refused.to_string(), "the exchange has finished");
        // The next exchange's sender is accepted.
        let mut interim = InterimResponses::new(relay.sender());
        let (sent, ()) = block_on(zip(interim.send_interim_response(hint(), &never), async {
            relay.next().await;
            relay.complete(Ok::<_, io::Error>(()));
        }));
        sent.unwrap();
    }

    #[test]
    fn a_dropped_send_withdraws_its_response() {
        let relay = InterimRelay::new();
        let mut sender = relay.sender();
        let never = Cancellation::never();
        {
            let mut sending = pin!(sender.on_interim_response(hint(), &never));
            assert!(block_on(poll_once(sending.as_mut())).is_none());
        }
        assert!(block_on(poll_once(pin!(relay.next()))).is_none());
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut interim = InterimResponses::new(relay.sender());
        let refused = block_on(interim.send_interim_response(hint(), &cancelled)).unwrap_err();
        assert_eq!(refused.to_string(), "interim response cancelled");
    }
}
