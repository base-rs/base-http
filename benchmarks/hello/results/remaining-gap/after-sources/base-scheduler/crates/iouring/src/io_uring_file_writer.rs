// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoUringSelector;
use crate::io_uring_selector::file_monitor::IoUringFileRegistration;
use base_scheduler::{BufferResult, Cancellation, FileWriter, FileWriting, IoBuf, OperationError};
use base_scheduler_selector::NativeFile;
use std::{future::Future, io};

/// One sequential file writer bound to its constructing selector.
pub struct IoUringFileWriter<'scheduler> {
    selector: &'scheduler IoUringSelector,
    state: IoUringFileRegistration,
}
impl FileWriting for IoUringSelector {
    type Writer<'scheduler> = IoUringFileWriter<'scheduler>;
    fn file_writer(&self, file: &NativeFile) -> io::Result<Self::Writer<'_>> {
        Ok(IoUringFileWriter {
            selector: self,
            state: self.file_monitor(file)?,
        })
    }
}
impl FileWriter for IoUringFileWriter<'_> {
    async fn write<B: IoBuf>(&mut self, buffer: B, cancellation: &Cancellation) -> BufferResult<B> {
        self.selector
            .file_monitor_write(&mut self.state, buffer, cancellation)
            .await
    }
    async fn write_at<B: IoBuf>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.selector
            .file_monitor_write_at(&mut self.state, buffer, offset, cancellation)
            .await
    }
    async fn write_submit<B: IoBuf>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.selector
            .file_monitor_write_submit(&mut self.state, buffer, cancellation)
            .await
    }
    async fn write_at_submit<B: IoBuf>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = BufferResult<B>> + Send + 'static, (OperationError, B)> {
        self.selector
            .file_monitor_write_at_submit(&mut self.state, buffer, offset, cancellation)
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.selector.file_monitor_close(&mut self.state).await
    }
}
