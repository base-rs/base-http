// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use base_scheduler::{
    Cancellation, OperationResult, ProcessWait, ProcessWaitFlags, ProcessWaitResult,
};
use std::{
    future::poll_fn,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
};

// Model a native notification wait interrupted by shutdown before reaping. The assertion must collect the child's status after this waiter retires.
struct NotificationWaiter {
    closed: AtomicBool,
}

impl ProcessWait for NotificationWaiter {
    #[cfg(unix)]
    type ProcessId = i32;
    #[cfg(windows)]
    type ProcessId = u32;

    async fn process_wait(
        &self,
        _: Self::ProcessId,
        _: ProcessWaitFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<ProcessWaitResult>> {
        cancellation.check()?;
        poll_fn(|_| {
            if self.closed.load(Ordering::Acquire) {
                Poll::Ready(Err(
                    std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()
                ))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

#[test]
fn shutdown_retirement_without_reaping_preserves_child_status() {
    support::run(|| {
        let waiter = NotificationWaiter {
            closed: AtomicBool::new(false),
        };
        futures_lite::future::block_on(base_scheduler_conformance::process_wait::shutdown(
            &waiter,
            || waiter.closed.store(true, Ordering::Release),
            std::future::ready(()),
        ));
    });
}

#[test]
#[ignore = "child fixture, invoked by process-wait conformance assertions"]
fn process_child_fixture() {
    base_scheduler_conformance::process_wait::child();
}
