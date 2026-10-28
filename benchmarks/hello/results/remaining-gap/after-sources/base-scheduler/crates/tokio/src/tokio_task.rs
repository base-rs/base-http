// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::task_service::TaskOutcome;
use base_scheduler::TaskError;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Owns a task on the selected Tokio runtime. Drop requests destruction after any active poll returns, without detaching normal execution or synchronously waiting for cleanup.
#[must_use = "await the task or retain its owning handle"]
pub struct TokioTask<T> {
    pub(crate) inner: tokio::task::JoinHandle<TaskOutcome<T>>,
    #[cfg(test)]
    pub(crate) status: std::sync::Arc<crate::task_service::tests::Status>,
}
impl<T> Future for TokioTask<T> {
    type Output = Result<T, TaskError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(test)]
        let waker = crate::task_service::tests::join_waker(&self.status, context.waker());
        #[cfg(test)]
        let context = &mut Context::from_waker(&waker);
        let poll = Pin::new(&mut self.inner)
            .poll(context)
            .map(|result| match result {
                Ok(mut outcome) => outcome.0.take().unwrap(),
                Err(error) if error.is_panic() => Err(TaskError::Panicked(error.into_panic())),
                Err(_) => Err(TaskError::Cancelled),
            });
        #[cfg(test)]
        crate::task_service::tests::joined(&self.status, &poll);
        poll
    }
}
impl<T> Drop for TokioTask<T> {
    fn drop(&mut self) {
        #[cfg(test)]
        crate::task_service::tests::abandon(&self.status);
        self.inner.abort();
    }
}

#[cfg(test)]
mod tests;
