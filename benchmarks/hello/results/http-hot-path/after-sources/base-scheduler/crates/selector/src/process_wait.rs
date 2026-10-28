// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    ReadinessSelector,
    process_io::{Process, ProcessId, operation_result},
};
use base_scheduler::{
    Cancellation, OperationResult, ProcessWait, ProcessWaitFlags, ProcessWaitResult,
};

use std::io;
#[cfg(unix)]
use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

impl ProcessWait for ReadinessSelector {
    type ProcessId = ProcessId;
    async fn process_wait(
        &self,
        process: ProcessId,
        flags: ProcessWaitFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<ProcessWaitResult>> {
        cancellation.check()?;
        if self.closed.is_cancelled() {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        let process = Process::open(process, flags)?;
        #[cfg(unix)]
        {
            let notification = match process.try_wait() {
                Ok(None) if !flags.nohang => process.notification()?,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => None,
                result => return operation_result(result, cancellation),
            };
            if let Some(notification) = notification {
                let notification = register(notification, async_io::Async::new_nonblocking)?;
                let mut ready = pin!(notification.readable());
                let mut cancelled = pin!(cancellation.cancelled());
                let mut closed = pin!(self.closed.cancelled());
                poll_fn(|context| {
                    if Future::poll(closed.as_mut(), context).is_ready() {
                        return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe).into()));
                    }
                    // Notification is advisory. Reaping below still uses a synchronous nonblocking call.
                    if let Poll::Ready(result) = Future::poll(ready.as_mut(), context) {
                        return Poll::Ready(
                            result
                                .map(drop)
                                .map_err(base_scheduler::OperationError::from),
                        );
                    }
                    if Future::poll(cancelled.as_mut(), context).is_ready() {
                        return Poll::Ready(Err(base_scheduler::Cancelled.into()));
                    }
                    Poll::Pending
                })
                .await?;
                match process.try_wait() {
                    Ok(None) => {}
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    result => return operation_result(result, cancellation),
                }
            }
        }
        let (result, ()) = self
            .work
            .execute((), cancellation, move |_| process.wait(false))
            .await;
        match result {
            Err(base_scheduler::OperationError::Io(error)) => {
                operation_result(Err(error), cancellation)
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(unix)]
fn register<T>(
    notification: std::os::fd::OwnedFd,
    operation: impl FnOnce(std::os::fd::OwnedFd) -> io::Result<T>,
) -> io::Result<T> {
    #[cfg(test)]
    if let Some(error) = tests::REGISTRATION_ERROR.take() {
        return Err(error);
    }
    operation(notification)
}
