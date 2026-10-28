// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, TimeoutError};
use std::{
    future::{Future, poll_fn},
    io,
    pin::pin,
    task::Poll,
    time::{Duration, Instant},
};

/// Monotonic time and cooperative waits in one scheduler's clock domain.
///
/// Obtain current time through this trait rather than `Instant::now()` or `Instant::elapsed()` so simulated and paused clocks remain substitutable. Deadlines from independently advancing clock domains are not interchangeable.
pub trait Clock: Send + Sync {
    /// Observe this scheduler's monotonic, nondecreasing clock.
    fn now(&self) -> Instant;

    /// Wait until an absolute deadline, or until cancellation is observed.
    ///
    /// Futures are lazy. Pre-cancelled waits return cancellation without registering a timer, including for elapsed deadlines. Otherwise an elapsed deadline can complete immediately; no scheduler yield is promised. Once waiting, expiry wins if expiry and cancellation are observed together. Dropping the future releases its timer registration.
    fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    /// Wait for a duration measured from the first poll, not construction. An unrepresentable deadline returns `io::ErrorKind::InvalidInput`.
    fn sleep(
        &self,
        duration: Duration,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        async move {
            cancellation.check()?;
            let deadline = self.now().checked_add(duration).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sleep deadline is out of range",
                )
            })?;
            self.sleep_until(deadline, cancellation).await
        }
    }

    /// Await an operation for at most a relative duration, preserving its output inside `Ok`.
    ///
    /// The operation is polled first on every poll and wins if both it and the timer are ready, including with a zero duration. The timer starts when the operation first returns `Pending`; an immediately ready operation reads no clock and registers no timer. Constructing an unpolled timeout polls neither future.
    ///
    /// Expiry returns [`TimeoutError::Elapsed`]; timer failures, including an unrepresentable deadline, return [`TimeoutError::Clock`]. The losing future is dropped before returning. Expiry or timer failure drops the operation without requesting cooperative cancellation or awaiting backend retirement. Accepted I/O can continue after abandonment; callers remain responsible for protocol state and cleanup. This wrapper cannot preempt a long-running poll.
    ///
    /// ```
    /// use base_scheduler::{Clock, TimeoutError};
    /// use std::{future::Future, time::Duration};
    ///
    /// async fn receive<C: Clock>(
    ///     clock: &C,
    ///     operation: impl Future<Output = Result<String, &'static str>> + Send,
    /// ) {
    ///     match clock.timeout(Duration::from_secs(5), operation).await {
    ///         Ok(Ok(item)) => println!("Received: {item}"),
    ///         Ok(Err(error)) => eprintln!("Receive failed: {error}"),
    ///         Err(TimeoutError::Elapsed) => eprintln!("Receive timed out"),
    ///         Err(TimeoutError::Clock(error)) => eprintln!("Timer failed: {error}"),
    ///     }
    /// }
    /// ```
    fn timeout<FutureType>(
        &self,
        duration: Duration,
        operation: FutureType,
    ) -> impl Future<Output = Result<FutureType::Output, TimeoutError>> + Send
    where
        FutureType: Future + Send,
    {
        async move {
            let mut operation = pin!(operation);
            let never = Cancellation::never();
            let mut timer = pin!(async { self.sleep(duration, &never).await });
            poll_fn(move |context| {
                if let Poll::Ready(output) = operation.as_mut().poll(context) {
                    return Poll::Ready(Ok(output));
                }
                timer.as_mut().poll(context).map(|result| {
                    Err(match result {
                        Ok(()) => TimeoutError::Elapsed,
                        Err(error) => TimeoutError::Clock(error),
                    })
                })
            })
            .await
        }
    }
}
