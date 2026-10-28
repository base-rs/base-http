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
