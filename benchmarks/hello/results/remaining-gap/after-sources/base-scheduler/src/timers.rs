// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, Clock, OperationResult};
use std::{future::Future, time::Instant};

/// Experimental reusable timer capability for the timer benchmark. This is not a settled public contract.
///
/// Construct resources through `timer` to establish their scheduler compatibility before waiting. Waiting always receives its scheduler and cancellation explicitly; incompatible scheduler/resource pairings are rejected with `io::ErrorKind::InvalidInput`. Only one waiter can borrow the resource. Dropping a wait retains its registration, which may cause a later wake of the previous task. Dropping the resource retires it; forgetting it may leak registration capacity but cannot retain borrowed application memory. Cancellation observed after admission disarms its registration; pre-cancellation leaves existing state unchanged. Deadline replacement happens on first poll, including an earlier replacement.
///
/// ```
/// use base_scheduler::{Cancellation, OperationResult, Timers};
/// use std::time::Duration;
///
/// async fn delay<S: Timers>(scheduler: &S, cancellation: &Cancellation) -> OperationResult<()> {
///     let mut timer = scheduler.timer();
///     let deadline = scheduler.now() + Duration::from_secs(1);
///     scheduler.timer_wait_until(&mut timer, deadline, cancellation).await
/// }
/// ```
pub trait Timers: Clock {
    type Timer: Send;

    /// Construct reusable state compatible with this scheduler, without registering a deadline or selecting a worker. Construction is infallible even after closure; waits retain the backend's clock admission rules.
    fn timer(&self) -> Self::Timer;

    /// Wait until an absolute deadline using retained timer state. Constructing an unpolled future leaves the resource unchanged.
    fn timer_wait_until(
        &self,
        timer: &mut Self::Timer,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
