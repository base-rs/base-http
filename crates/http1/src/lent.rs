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
use crate::{Error, Exchange, RequestReader, input::BodyState};
use base_endpoint::stream::ReadableStream;
use base_http::{
    Request,
    body::{BodyFrame, ReadableBody},
};
use base_scheduler::Cancellation;
use std::{
    future::{Future, poll_fn},
    io,
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
    pub async fn read_request(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Option<(Request<LentBody<R>>, Exchange)>, Error> {
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
        let body = LentBody {
            lane,
            state: BodyState::new(exchange.clone(), framing, length, false, false),
        };
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

/// A request body that borrows its connection's [`LentInput`] for one read at
/// a time, so it borrows nothing and can outlive its exchange. It frames input
/// as [`IncomingBody`](crate::IncomingBody) does, and its reads are likewise
/// cancellation safe. Once the connection takes the input back, reads fail
/// with [`Error::Closed`]; a body that has ended keeps returning end.
pub struct LentBody<R> {
    lane: Lane<R>,
    state: BodyState,
}

impl<R: ReadableStream + Send> ReadableBody for LentBody<R> {
    type Data = Vec<u8>;
    type Error = Error;
    fn may_have_trailers(&self) -> bool {
        self.state.may_have_trailers()
    }
    fn is_ready(&self) -> bool {
        self.state.is_ready(self.lane.has_available())
    }
    fn is_empty(&self) -> bool {
        self.state.is_empty()
    }
    fn length(&self) -> Option<u64> {
        self.state.length()
    }
    fn close(&mut self, _: Option<Error>) {
        self.state.close();
        self.lane.release();
    }
    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Error> {
        cancellation.check()?;
        // A finished body needs no input, which its connection may already
        // have taken back for the next request.
        if self.state.is_finished() {
            return Ok(BodyFrame::End);
        }
        self.lane.read(&mut self.state, cancellation).await
    }
}

impl<R> Drop for LentBody<R> {
    fn drop(&mut self) {
        self.state.abandon_unfinished();
    }
}

/// A request body's lane to its connection's lent input.
struct Lane<R> {
    slot: Arc<Slot<R>>,
    generation: u64,
}

/// The input, borrowed for one body read.
struct Borrowed<'a, R> {
    slot: &'a Slot<R>,
    reader: Option<Box<RequestReader<R>>>,
}

impl<R> Borrowed<'_, R> {
    fn reader(&mut self) -> &mut RequestReader<R> {
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
        // The connection takes the input only by advancing the generation or
        // reclaiming it, and this lane's own reads hold it only while borrowed.
        let reader = state
            .reader
            .take()
            .expect("a current lane finds its input at rest");
        Ok(Borrowed {
            slot: &self.slot,
            reader: Some(reader),
        })
    }

    /// Read one frame with the borrowed input, stopping if the connection
    /// takes it back meanwhile. A stopped read loses no input.
    async fn read(
        &self,
        state: &mut BodyState,
        cancellation: &Cancellation,
    ) -> Result<BodyFrame<Vec<u8>>, Error> {
        let mut input = self.borrow()?;
        let slot = input.slot;
        let mut read = pin!(state.read(input.reader(), cancellation));
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
    fn has_available(&self) -> bool {
        let state = self.slot.lock();
        state.generation == self.generation
            && state
                .reader
                .as_ref()
                .is_some_and(|reader| !reader.available().is_empty())
    }
}

impl<R> Lane<R> {
    /// The body no longer reads: let the connection take the input back.
    fn release(&self) {
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

impl<R> Drop for Lane<R> {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;
    use base_scheduler::OperationResult;
    use futures_lite::future::{block_on, poll_once};

    struct Bytes(&'static [u8]);
    impl ReadableStream for Bytes {
        async fn read_into(
            &mut self,
            buffer: &mut [u8],
            _: &Cancellation,
        ) -> OperationResult<usize> {
            self.try_read_into(buffer).map_err(Into::into)
        }
        fn try_read_into(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let n = self.0.len().min(buffer.len());
            buffer[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
        async fn close(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    const POST: &[u8] = b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello";

    fn lend(bytes: &'static [u8]) -> (LentInput<Bytes>, LentBody<Bytes>) {
        let mut input =
            LentInput::new(RequestReader::new(Bytes(bytes), Options::default()).unwrap());
        let (request, _) = block_on(input.read_request(&Cancellation::never()))
            .unwrap()
            .unwrap();
        (input, request.body)
    }

    #[test]
    fn a_lent_body_reports_its_framing() {
        let (_input, mut body) = lend(POST);
        assert_eq!(body.length(), Some(5));
        assert!(!body.may_have_trailers());
        assert!(!body.is_empty());
        assert!(body.is_ready());
        let frame = block_on(body.read(&Cancellation::never())).unwrap();
        assert!(matches!(frame, BodyFrame::Data { data, end: true } if data == b"hello"));
        assert!(body.is_empty());
    }

    #[test]
    fn dropping_a_held_body_wakes_its_waiting_connection() {
        let (mut input, body) = lend(POST);
        let never = Cancellation::never();
        {
            let mut returned = pin!(input.returned(&never));
            assert!(block_on(poll_once(returned.as_mut())).is_none());
            drop(body);
            block_on(returned).unwrap();
        }
        // The unread body was abandoned, so the input cannot be reused.
        assert!(!input.is_reusable());
    }

    #[test]
    fn closing_a_held_body_wakes_its_waiting_connection_once() {
        let (mut input, mut body) = lend(POST);
        let never = Cancellation::never();
        {
            let mut returned = pin!(input.returned(&never));
            assert!(block_on(poll_once(returned.as_mut())).is_none());
            body.close(None);
            block_on(returned).unwrap();
        }
        // The input has returned, so closing again changes nothing.
        body.close(None);
        assert!(block_on(body.read(&never)).unwrap().is_end());
    }

    #[test]
    fn taken_back_input_lends_no_further_bodies() {
        let (mut input, mut body) = lend(POST);
        block_on(input.reclaim());
        assert!(!body.is_ready());
        let closed = block_on(body.read(&Cancellation::never())).err().unwrap();
        assert_eq!(closed.to_string(), Error::Closed.to_string());
        assert!(!input.is_reusable());
        let refused = block_on(input.read_request(&Cancellation::never()))
            .err()
            .unwrap();
        assert_eq!(
            refused.to_string(),
            "invalid HTTP/1 message: request input was taken back"
        );
        block_on(input.close()).unwrap();
    }

    #[test]
    fn cancelled_reads_and_waits_report_cancellation() {
        let (mut input, mut body) = lend(POST);
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let read = block_on(body.read(&cancelled)).err().unwrap();
        assert_eq!(read.to_string(), Error::Cancelled.to_string());
        // Waiting for the held body is cancelled, which takes the input back.
        let next = block_on(input.read_request(&cancelled)).err().unwrap();
        assert_eq!(next.to_string(), Error::Cancelled.to_string());
        assert!(!input.is_reusable());
    }
}
