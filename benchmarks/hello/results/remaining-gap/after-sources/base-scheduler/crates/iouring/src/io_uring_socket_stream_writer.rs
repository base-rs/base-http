// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::io_uring_selector::stream_writer::IoUringStreamWriter;
use crate::{IoUringSelector, MonitorStorage};
use base_scheduler::{
    Cancellation, IoBuf, OperationError, OperationResult, SocketSendFlags, SocketStreamWriter,
};
use base_scheduler_selector::NativeSocket;
use std::io::{self, IoSlice};

/// A scheduler-bound output lane combining borrowed writes with ordered owned submission. Construct through [`SocketStreamWriting`](base_scheduler::SocketStreamWriting).
pub(crate) struct IoUringSocketStreamWriter<'a, B: IoBuf> {
    scheduler: &'a IoUringSelector,
    operations: crate::operation_state::OperationState,
    output: IoUringStreamWriter<B>,
    monitor: MonitorStorage,
    closed: bool,
    submitted: bool,
}

impl<'a, B: IoBuf> IoUringSocketStreamWriter<'a, B> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a IoUringSelector, socket: &NativeSocket) -> io::Result<Self> {
        let output = scheduler.socket_stream_output(socket)?;
        let monitor = scheduler.socket_monitor(socket, Interest::Writable);
        Ok(Self {
            scheduler,
            operations: Default::default(),
            output,
            monitor,
            closed: false,
            submitted: false,
        })
    }

    fn check_closed(&self) -> OperationResult<()> {
        if self.closed {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "stream writer is closed").into())
        } else {
            Ok(())
        }
    }
    fn check_open(&self) -> OperationResult<()> {
        self.check_closed()?;
        self.operations.check_idle()
    }
}

impl<B: IoBuf> SocketStreamWriter for IoUringSocketStreamWriter<'_, B> {
    type Buffer = B;

    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.submitted {
            self.output.flush(self.scheduler, cancellation).await?;
            self.submitted = false;
        }
        self.scheduler
            .socket_monitor_send_poll(
                &mut self.monitor,
                buffer,
                SocketSendFlags::default(),
                cancellation,
            )
            .await
    }

    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.submitted {
            self.output.flush(self.scheduler, cancellation).await?;
            self.submitted = false;
        }
        self.scheduler
            .socket_monitor_sendmsg_poll(
                &mut self.monitor,
                buffers,
                None,
                SocketSendFlags::default(),
                cancellation,
            )
            .await
    }

    async fn submit(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        if let Err(error) = self.check_open() {
            return Err((error, buffer));
        }
        // Mark before polling admission: an abandoned admission may already have accepted output.
        self.submitted = true;
        self.output
            .submit(self.scheduler, buffer, cancellation)
            .await
    }

    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        self.check_open()?;
        self.output.flush(self.scheduler, cancellation).await?;
        self.submitted = false;
        Ok(())
    }

    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.submitted {
            self.output.flush(self.scheduler, cancellation).await?;
            self.submitted = false;
        }
        self.scheduler
            .socket_monitor_sendmsg_poll(&mut self.monitor, buffers, None, flags, cancellation)
            .await
    }
    async fn write_owned_vectored<Buffers: base_scheduler::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
        if let Err(error) = self.check_closed() {
            return (Err(error), buffers);
        }
        if self.submitted {
            if let Err(error) = self.output.flush(self.scheduler, cancellation).await {
                return (Err(error), buffers);
            }
            self.submitted = false;
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffers);
        }
        let buffers = match self.operations.begin(buffers) {
            Ok(buffers) => buffers,
            Err((error, buffers)) => return (Err(error), buffers),
        };
        let (result, message) = self
            .scheduler
            .socket_sendmsg(
                self.monitor.socket(),
                crate::SocketMessage {
                    buffers,
                    address: None,
                },
                flags,
                cancellation,
            )
            .await;
        (result, message.buffers.into_inner())
    }
    async fn write_owned<Owned: base_scheduler::IoBuf>(
        &mut self,
        buffer: Owned,
        cancellation: &Cancellation,
    ) -> base_scheduler::BufferResult<Owned> {
        if let Err(error) = self.check_closed() {
            return (Err(error), buffer);
        }
        if self.submitted {
            if let Err(error) = self.output.flush(self.scheduler, cancellation).await {
                return (Err(error), buffer);
            }
            self.submitted = false;
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        self.scheduler
            .socket_write_owned(
                self.monitor.socket(),
                buffer,
                &mut self.operations,
                cancellation,
            )
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.operations.close().await;
        let output = self.output.close(self.scheduler).await;
        let monitor = self.scheduler.socket_monitor_close(&mut self.monitor).await;
        output.and(monitor)
    }
}
