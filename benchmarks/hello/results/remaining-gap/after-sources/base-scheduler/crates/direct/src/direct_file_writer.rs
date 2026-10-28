// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{DirectFile, DirectScheduler, file_state::FileState};
use base_scheduler::{BufferResult, Cancellation, FileWriter, FileWriting, IoBuf, OperationError};
use std::{future::Future, io};

/// A sequential file writer bound to its constructing scheduler.
pub struct DirectFileWriter<'scheduler> {
    state: FileState<'scheduler>,
}

impl FileWriting for DirectScheduler {
    type Writer<'scheduler> = DirectFileWriter<'scheduler>;
    fn file_writer(&self, file: &DirectFile) -> io::Result<Self::Writer<'_>> {
        Ok(DirectFileWriter {
            state: FileState::new(self, file),
        })
    }
}

impl FileWriter for DirectFileWriter<'_> {
    async fn write<B: IoBuf>(&mut self, buffer: B, cancellation: &Cancellation) -> BufferResult<B> {
        self.state
            .operate(buffer, cancellation, |file, buffer| {
                crate::file_io::write(file, buffer.as_ref())
            })
            .await
    }
    async fn write_at<B: IoBuf>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.state
            .operate(buffer, cancellation, move |file, buffer| {
                crate::file_io::write_at(file, buffer.as_ref(), offset)
            })
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.state.close().await
    }

    /// Await admission separately from collection of the completed buffer. Accepted work progresses independently and retains ownership through retirement.
    async fn write_submit<B: IoBuf>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.state
            .submit(buffer, cancellation, |file, buffer| {
                crate::file_io::write(file, buffer.as_ref())
            })
            .await
    }
    /// Await positioned write admission. The completion returns the owner after backend access retires.
    async fn write_at_submit<B: IoBuf>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.state
            .submit(buffer, cancellation, move |file, buffer| {
                crate::file_io::write_at(file, buffer.as_ref(), offset)
            })
            .await
    }
}
