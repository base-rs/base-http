// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::TokioScheduler;
use base_scheduler::{Cancellation, Clock, OperationResult};
use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
    time::Instant,
};

impl Clock for TokioScheduler {
    fn now(&self) -> Instant {
        let _scope = self.runtime.enter();
        tokio::time::Instant::now().into_std()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.check_open()?;
        if self.now() >= deadline {
            return Ok(());
        }
        let sleep = {
            let _scope = self.runtime.enter();
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))
        };
        #[cfg(test)]
        let _registration = tests::Registration::new(&self.timers);
        let mut sleep = pin!(sleep);
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            let _scope = self.runtime.enter();
            if sleep.as_mut().poll(context).is_ready() {
                return Poll::Ready(Ok(()));
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests;
