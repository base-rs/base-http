// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::task_service::{TaskRegistration, TaskState};
use base_scheduler::TaskError;
use std::{
    future::Future,
    marker::PhantomPinned,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

/// An owning task handle. Dropping it requests destruction after any active poll returns; it never detaches execution.
#[must_use = "await the task or retain its owning handle"]
pub struct NativeTask<T>(pub(crate) async_task::Task<TaskOutcome<T>, TaskState>);

impl<T> Future for NativeTask<T> {
    type Output = Result<T, TaskError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(context)
            .map(|mut outcome| outcome.0.take().unwrap())
    }
}

// async-task aborts if its future or stored output destructor unwinds. Keep both user-owned values behind guards, including when the task is abandoned.
pub(crate) struct TaskFuture<F: Future> {
    future: ManuallyDrop<F>,
    live: bool,
    _registration: TaskRegistration,
    _pin: PhantomPinned,
}

impl<F: Future> TaskFuture<F> {
    pub(crate) fn new(future: F, registration: TaskRegistration) -> Self {
        Self {
            future: ManuallyDrop::new(future),
            live: true,
            _registration: registration,
            _pin: PhantomPinned,
        }
    }

    fn drop_future(&mut self) {
        // Retire before invoking arbitrary user destruction, so an unwind cannot cause a second drop.
        if std::mem::replace(&mut self.live, false) {
            // SAFETY: The live future is dropped in place exactly once. The wrapper remains pinned until its own destruction, including after this field has retired.
            unsafe { ManuallyDrop::drop(&mut self.future) };
        }
    }
}

impl<F: Future> Future for TaskFuture<F> {
    type Output = TaskOutcome<F::Output>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: The wrapper structurally pins its future and never moves that field. Retirement drops it in place; PhantomPinned also prevents moving the wrapper after its field retires.
        let this = unsafe { self.get_unchecked_mut() };
        let mut result = match catch_unwind(AssertUnwindSafe(|| {
            assert!(this.live, "task future polled after completion");
            // SAFETY: The projection above preserves the future's pinned address, and live excludes access after retirement.
            unsafe { Pin::new_unchecked(&mut *this.future) }.poll(context)
        })) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) => Err(TaskError::Panicked(payload)),
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| this.drop_future())) {
            crate::panic::discard(result);
            result = Err(TaskError::Panicked(payload));
        }
        Poll::Ready(TaskOutcome(Some(result)))
    }
}

impl<F: Future> Drop for TaskFuture<F> {
    fn drop(&mut self) {
        crate::panic::contain(|| self.drop_future());
    }
}

pub(crate) struct TaskOutcome<T>(pub(crate) Option<Result<T, TaskError>>);

impl<T> Drop for TaskOutcome<T> {
    fn drop(&mut self) {
        crate::panic::discard(self.0.take());
    }
}
