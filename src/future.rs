// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{Cancellation, Cancelled};
use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

/// Cancellation is checked before polling work, including already-ready work.
pub(crate) async fn cancellable<F: Future>(
    future: F,
    cancellation: &Cancellation,
) -> Result<F::Output, Cancelled> {
    cancellation.check()?;
    let mut future = pin!(future);
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            Poll::Ready(Err(Cancelled))
        } else {
            future.as_mut().poll(context).map(Ok)
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;
    use std::cell::Cell;

    #[test]
    fn cancelled_work_is_not_polled_even_when_ready() {
        let polls = Cell::new(0);
        let work = || {
            poll_fn(|_| {
                polls.set(polls.get() + 1);
                Poll::Ready(1)
            })
        };
        let cancellation = Cancellation::new();
        cancellation.cancel();
        assert_eq!(block_on(cancellable(work(), &cancellation)), Err(Cancelled));
        assert_eq!(polls.get(), 0);
        assert_eq!(block_on(cancellable(work(), &Cancellation::never())), Ok(1));
        assert_eq!(polls.get(), 1);
    }
}
