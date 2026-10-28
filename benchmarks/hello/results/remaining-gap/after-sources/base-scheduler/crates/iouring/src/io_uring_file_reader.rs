// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoUringSelector;
use crate::io_uring_selector::file_monitor::IoUringFileRegistration;
use base_scheduler::{
    BufferResult, Cancellation, FileReader, FileReading, IoBufMut, OperationError,
};
use base_scheduler_selector::NativeFile;
use std::{future::Future, io};

/// One sequential file reader bound to its constructing selector.
pub struct IoUringFileReader<'scheduler> {
    selector: &'scheduler IoUringSelector,
    state: IoUringFileRegistration,
}
impl FileReading for IoUringSelector {
    type Reader<'scheduler> = IoUringFileReader<'scheduler>;
    fn file_reader(&self, file: &NativeFile) -> io::Result<Self::Reader<'_>> {
        Ok(IoUringFileReader {
            selector: self,
            state: self.file_monitor(file)?,
        })
    }
}
impl FileReader for IoUringFileReader<'_> {
    async fn read<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.selector
            .file_monitor_read(&mut self.state, buffer, cancellation)
            .await
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.selector
            .file_monitor_read_at(&mut self.state, buffer, offset, cancellation)
            .await
    }
    async fn read_submit<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.selector
            .file_monitor_read_submit(&mut self.state, buffer, cancellation)
            .await
    }
    async fn read_at_submit<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.selector
            .file_monitor_read_at_submit(&mut self.state, buffer, offset, cancellation)
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.selector.file_monitor_close(&mut self.state).await
    }
}
