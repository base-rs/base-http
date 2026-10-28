// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{DirectFile, DirectScheduler, file_state::FileState};
use base_scheduler::{
    BufferResult, Cancellation, FileReader, FileReading, IoBufMut, OperationError,
};
use std::{future::Future, io};

/// A sequential file reader bound to its constructing scheduler.
pub struct DirectFileReader<'scheduler> {
    state: FileState<'scheduler>,
}

impl FileReading for DirectScheduler {
    type Reader<'scheduler> = DirectFileReader<'scheduler>;
    fn file_reader(&self, file: &DirectFile) -> io::Result<Self::Reader<'_>> {
        Ok(DirectFileReader {
            state: FileState::new(self, file),
        })
    }
}

impl FileReader for DirectFileReader<'_> {
    async fn read<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.state
            .operate(buffer, cancellation, |file, buffer| {
                crate::file_io::read(file, buffer.as_mut())
            })
            .await
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.state
            .operate(buffer, cancellation, move |file, buffer| {
                crate::file_io::read_at(file, buffer.as_mut(), offset)
            })
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.state.close().await
    }

    /// Await admission separately from collection of the completed buffer. Accepted work progresses independently and retains ownership through retirement.
    async fn read_submit<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.state
            .submit(buffer, cancellation, |file, buffer| {
                crate::file_io::read(file, buffer.as_mut())
            })
            .await
    }
    /// Await positioned read admission. The completion returns the owner after backend access retires.
    async fn read_at_submit<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.state
            .submit(buffer, cancellation, move |file, buffer| {
                crate::file_io::read_at(file, buffer.as_mut(), offset)
            })
            .await
    }
}
