// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Request input lent to one request body at a time.
//!
//! A server connection reads each request head itself, then lends its input to
//! the request body, which borrows it for one read at a time. The body is
//! therefore an independent value: a handler can pass it to work that outlives
//! the call, such as a client writing it upstream. Between reads the input
//! rests with the connection's shared slot, so the connection can take it back
//! whenever the body is not reading, and a read in progress when it does so
//! stops at once.
use crate::{
    Error, Exchange, IncomingBody, RequestReader,
    input::{BodyState, Input},
};
use base_endpoint::stream::ReadableStream;
use base_http::{Request, body::BodyFrame};
use base_scheduler::Cancellation;
use std::{
    future::{Future, poll_fn},
    io,
    ops::{Deref, DerefMut},
    pin::pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Poll, Waker},
};

struct Slot<R> {
    state: Mutex<State<R>>,
}

struct State<R> {
    /// The input while it rests between body reads.
    reader: Option<Box<RequestReader<R>>>,
    /// The lane that may borrow the input; earlier lanes are refused.
    generation: u64,
    /// Whether the current lane's body still exists and is not closed.
    held: bool,
    /// The connection took the input back while the body could still read.
    reclaimed: bool,
    /// The connection waiting for the input to rest.
    connection: Option<Waker>,
    /// A body read waiting for input, which a reclaim must stop.
    borrower: Option<Waker>,
}

impl<R> Slot<R> {
    fn lock(&self) -> MutexGuard<'_, State<R>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn register(slot: &mut Option<Waker>, waker: &Waker) {
    match slot {
        Some(registered) if registered.will_wake(waker) => {}
        _ => *slot = Some(waker.clone()),
    }
}

/// A connection's request input, lent to one request body at a time.
///
/// [`read_request`](Self::read_request) returns a request whose body owns its
/// lane to this input rather than borrowing the connection, so it lives as
/// long as `R`. Before reading the next request, the connection waits until
/// the previous body has ended, or has been closed or dropped; a body that
/// outlives its handler, such as one a client is still forwarding, delays the
/// next request only until then. [`reclaim`](Self::reclaim) takes the input
/// back at once, as the connection does when it is cancelled or fails, after
/// which the body's reads fail with [`Error::Closed`].
///
/// The slot is allocated once per connection; lending and reading allocate
/// nothing further.
pub struct LentInput<R> {
    slot: Arc<Slot<R>>,
    /// The input while the connection holds it.
    reader: Option<Box<RequestReader<R>>>,
    /// The exchange of the request whose body holds the input.
    lent: Option<Exchange>,
    reclaimed: bool,
}

impl<R: ReadableStream> LentInput<R> {
    pub fn new(reader: RequestReader<R>) -> Self {
        Self {
            slot: Arc::new(Slot {
                state: Mutex::new(State {
                    reader: None,
                    generation: 0,
                    held: false,
                    reclaimed: false,
                    connection: None,
                    borrower: None,
                }),
            }),
            reader: Some(Box::new(reader)),
            lent: None,
            reclaimed: false,
        }
    }

    /// Wait until the previous request body has returned the input, then read
    /// the next request head and lend the input to its body. Clean EOF between
    /// requests returns `None`; partial heads are errors. Cancellation takes
    /// the input back, as [`reclaim`](Self::reclaim) does.
    ///
    /// The body borrows nothing, so its lifetime is any that `R` outlives.
    pub async fn read_request<'r>(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<(Request<IncomingBody<'r, R>>, Exchange)>, Error>
    where
        R: 'r,
    {
        self.returned(cancellation).await?;
        if self.reclaimed {
            return Err(Error::Invalid("request input was taken back"));
        }
        let reader = self
            .reader
            .as_mut()
            .expect("returned input rests with the connection");
        let Some((head, exchange, framing, length)) =
            reader.read_request_head(cancellation).await?
        else {
            return Ok(None);
        };
        let generation = {
            let mut state = self.slot.lock();
            state.generation += 1;
            state.held = true;
            state.reader = self.reader.take();
            state.generation
        };
        self.lent = Some(exchange.clone());
        let lane = Lane {
            slot: self.slot.clone(),
            generation,
        };
        let body = IncomingBody::new(
            Input::Lent(lane),
            exchange.clone(),
            framing,
            length,
            false,
            false,
        );
        Ok(Some((Request::from_parts(head, body), exchange)))
    }

    /// Wait until the lent body no longer needs the input: it has ended, and
    /// no protocol switch made it raw input, or it was closed or dropped. The
    /// body keeps working after an end, returning end without input. On
    /// cancellation, take the input back and report `Cancelled`.
    pub async fn returned(&mut self, cancellation: &Cancellation) -> Result<(), Error> {
        if self.reader.is_some() {
            return Ok(());
        }
        let exchange = self.lent.clone();
        let done = |exchange: &Option<Exchange>| {
            exchange
                .as_ref()
                .is_some_and(|exchange| exchange.is_complete() && !exchange.is_tunnel())
        };
        let mut cancelled = pin!(cancellation.cancelled());
        let reader = poll_fn(|cx| {
            if !cancellation.is_never() && cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            let mut state = self.slot.lock();
            if state.reader.is_some() && (!state.held || done(&exchange)) {
                state.generation += 1;
                state.held = false;
                return Poll::Ready(state.reader.take());
            }
            register(&mut state.connection, cx.waker());
            Poll::Pending
        })
        .await;
        match reader {
            Some(reader) => {
                self.reader = Some(reader);
                self.lent = None;
                Ok(())
            }
            None => {
                self.reclaim().await;
                Err(Error::Cancelled)
            }
        }
    }

    /// Take the input back now. A body read in progress stops, losing no
    /// input, and every later read of the lent body fails with
    /// [`Error::Closed`]. The input lends no further bodies.
    pub async fn reclaim(&mut self) {
        self.reclaimed = true;
        if self.reader.is_some() {
            return;
        }
        let borrower = {
            let mut state = self.slot.lock();
            state.reclaimed = true;
            state.borrower.take()
        };
        if let Some(borrower) = borrower {
            borrower.wake();
        }
        let reader = poll_fn(|cx| {
            let mut state = self.slot.lock();
            match state.reader.take() {
                Some(reader) => {
                    state.generation += 1;
                    state.held = false;
                    Poll::Ready(reader)
                }
                None => {
                    register(&mut state.connection, cx.waker());
                    Poll::Pending
                }
            }
        })
        .await;
        self.reader = Some(reader);
        self.lent = None;
    }

    /// The input, while the connection holds it: before the first request,
    /// and after [`returned`](Self::returned) or [`reclaim`](Self::reclaim).
    pub fn input(&self) -> Option<&RequestReader<R>> {
        self.reader.as_deref()
    }

    /// Whether the held input can admit another request. Input still lent, or
    /// taken back from a body, cannot.
    pub fn is_reusable(&self) -> bool {
        !self.reclaimed && self.input().is_some_and(RequestReader::is_reusable)
    }

    /// Take the input back, then close it.
    pub async fn close(&mut self) -> io::Result<()> {
        self.reclaim().await;
        self.reader
            .as_mut()
            .expect("reclaimed input rests with the connection")
            .close()
            .await
    }
}

/// A request body's lane to its connection's lent input.
pub(crate) struct Lane<R> {
    slot: Arc<Slot<R>>,
    generation: u64,
}

/// The input, borrowed for one body read.
struct Borrowed<'a, R> {
    slot: &'a Slot<R>,
    reader: Option<Box<RequestReader<R>>>,
}

impl<R> Deref for Borrowed<'_, R> {
    type Target = RequestReader<R>;
    fn deref(&self) -> &RequestReader<R> {
        self.reader
            .as_ref()
            .expect("a borrow holds the input until dropped")
    }
}

impl<R> DerefMut for Borrowed<'_, R> {
    fn deref_mut(&mut self) -> &mut RequestReader<R> {
        self.reader
            .as_mut()
            .expect("a borrow holds the input until dropped")
    }
}

impl<R> Drop for Borrowed<'_, R> {
    fn drop(&mut self) {
        let connection = {
            let mut state = self.slot.lock();
            state.reader = self.reader.take();
            state.borrower = None;
            state.connection.take()
        };
        if let Some(connection) = connection {
            connection.wake();
        }
    }
}

impl<R: ReadableStream> Lane<R> {
    fn borrow(&self) -> Result<Borrowed<'_, R>, Error> {
        let mut state = self.slot.lock();
        if state.reclaimed || state.generation != self.generation {
            return Err(Error::Closed);
        }
        let reader = state.reader.take().ok_or(Error::Closed)?;
        Ok(Borrowed {
            slot: &self.slot,
            reader: Some(reader),
        })
    }

    /// Read one frame with the borrowed input, stopping if the connection
    /// takes it back meanwhile. A stopped read loses no input.
    pub(crate) async fn read(
        &self,
        state: &mut BodyState,
        cancellation: &Cancellation,
    ) -> Result<BodyFrame<Vec<u8>>, Error> {
        let mut input = self.borrow()?;
        let slot = input.slot;
        let mut read = pin!(state.read(&mut *input, cancellation));
        poll_fn(|cx| {
            if let Poll::Ready(result) = read.as_mut().poll(cx) {
                return Poll::Ready(result);
            }
            let mut state = slot.lock();
            if state.reclaimed {
                return Poll::Ready(Err(Error::Closed));
            }
            register(&mut state.borrower, cx.waker());
            Poll::Pending
        })
        .await
    }

    /// Whether the input has unread bytes, without waiting. A body whose
    /// input is borrowed elsewhere or taken back reports none.
    pub(crate) fn has_available(&self) -> bool {
        let state = self.slot.lock();
        state.generation == self.generation
            && state
                .reader
                .as_ref()
                .is_some_and(|reader| !reader.available().is_empty())
    }

    /// The body no longer reads: let the connection take the input back.
    pub(crate) fn release(&self) {
        let connection = {
            let mut state = self.slot.lock();
            if state.generation != self.generation || !state.held {
                return;
            }
            state.held = false;
            if state.reader.is_none() {
                return;
            }
            state.connection.take()
        };
        if let Some(connection) = connection {
            connection.wake();
        }
    }
}

impl<R> Drop for Lane<R> {
    fn drop(&mut self) {
        let connection = {
            let mut state = self.slot.lock();
            if state.generation != self.generation || !state.held {
                return;
            }
            state.held = false;
            state.connection.take()
        };
        if let Some(connection) = connection {
            connection.wake();
        }
    }
}
