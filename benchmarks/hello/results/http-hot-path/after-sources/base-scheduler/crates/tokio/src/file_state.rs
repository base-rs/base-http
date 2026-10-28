// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::operation_state::{OperationGuard, OperationState};
use crate::{TokioFile, TokioScheduler};
use base_scheduler::{BufferResult, Cancellation, IoBuf, OperationError};
use std::{future::Future, io};

// Reader and writer lanes share admission and ownership retirement, without a public monitor protocol.
pub(crate) struct FileState<'scheduler> {
    scheduler: &'scheduler TokioScheduler,
    file: TokioFile,
    operations: OperationState,
    closed: bool,
}
impl<'scheduler> FileState<'scheduler> {
    pub(crate) fn new(scheduler: &'scheduler TokioScheduler, file: &TokioFile) -> Self {
        Self {
            scheduler,
            file: TokioFile(file.retain()),
            operations: OperationState::default(),
            closed: false,
        }
    }
    fn begin<B>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<OperationGuard<B>, (OperationError, B)> {
        if let Err(error) = cancellation.check() {
            return Err((error.into(), buffer));
        }
        if self.closed {
            return Err((io::Error::from(io::ErrorKind::BrokenPipe).into(), buffer));
        }
        self.operations.begin(buffer)
    }
    pub(crate) async fn operate<B: IoBuf>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
        operation: impl FnOnce(&std::fs::File, &mut B) -> io::Result<usize> + Send + 'static,
    ) -> BufferResult<B> {
        let guard = match self.begin(buffer, cancellation) {
            Ok(guard) => guard,
            Err((error, buffer)) => return (Err(error), buffer),
        };
        let (result, guard) = self
            .scheduler
            .buffer_operation(&self.file, guard, cancellation, move |file, guard| {
                operation(file, guard.value_mut())
            })
            .await;
        (result, guard.into_inner())
    }
    pub(crate) async fn submit<B: IoBuf>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
        operation: impl FnOnce(&std::fs::File, &mut B) -> io::Result<usize> + Send + 'static,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        let guard = self.begin(buffer, cancellation)?;
        match self
            .scheduler
            .work
            .execute_submit_retiring(
                (Some(self.file.retain()), guard),
                cancellation,
                move |(file, guard)| operation(file.as_ref().unwrap(), guard.value_mut()),
                |(file, guard)| {
                    drop(file.take());
                    guard.retire();
                },
            )
            .await
        {
            Ok(completion) => Ok(async move {
                let (result, (_, guard)) = completion.await;
                (result, guard.into_inner())
            }),
            Err((error, (_, guard))) => Err((error, guard.into_inner())),
        }
    }
    pub(crate) async fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.operations.close().await;
        Ok(())
    }
}
