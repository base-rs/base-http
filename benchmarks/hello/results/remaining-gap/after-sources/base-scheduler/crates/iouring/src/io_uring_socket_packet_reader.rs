// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::{IoUringBufferPool, IoUringSelector, MonitorStorage};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, OperationResult,
    OwnedSocketRecvMessage, ReceiveBuffer, SocketPacketReader, SocketRecvFlags, SocketRecvResult,
    SocketType,
};
use base_scheduler_selector::NativeSocket;
use std::{
    future::poll_fn,
    io::{self, IoSliceMut},
    pin::pin,
    task::Poll,
};

/// A packet reader retaining one scheduler and readable monitor. Borrowed reads allocate no payload storage; the first owned read lazily constructs a two-slot, 64 KiB-per-slot pool. Idle readiness waits return their unused lease to the pool.
///
/// This demand-driven composition does not receive ahead or select a backend's provided-buffer receiver. Cancellation and ordinary receive errors leave it usable. Close retires its registration; drop the handle afterward to release its retained socket before exclusive socket close.
pub(crate) struct IoUringSocketPacketReader<'a> {
    scheduler: &'a IoUringSelector,
    operations: crate::operation_state::OperationState,
    monitor: MonitorStorage,
    pool: Option<IoUringBufferPool>,
    closing: bool,
}

impl<'a> IoUringSocketPacketReader<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a IoUringSelector, socket: &NativeSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? == SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a packet reader requires a packet socket",
            ));
        }
        Ok(Self {
            scheduler,
            operations: Default::default(),
            monitor: scheduler.socket_monitor(socket, Interest::Readable),
            pool: None,
            closing: false,
        })
    }

    /// Select reusable owned receive storage, including between completed reads. Outstanding messages retain their earlier pool independently. Borrowed reads never acquire from this pool.
    pub(crate) fn with_pool(mut self, pool: IoUringBufferPool) -> Self {
        self.pool = Some(pool);
        self
    }
}

impl SocketPacketReader for IoUringSocketPacketReader<'_> {
    type Message = crate::IoUringRecvMessage;

    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<Self::Message>> {
        cancellation.check()?;
        if self.closing {
            return Ok(None);
        }
        self.operations.check_idle()?;
        if self.pool.is_none() {
            self.pool = Some(
                self.scheduler
                    .buffer_pool(BufferPoolOptions::new(2, 64 * 1024))?,
            );
        }
        let pool = self.pool.as_ref().unwrap();
        loop {
            let mut buffer = pool.acquire(cancellation).await?;
            let result = {
                let mut vectors = [IoSliceMut::new(buffer.as_mut())];
                let mut receive = pin!(self.scheduler.socket_monitor_recvmsg_poll(
                    &mut self.monitor,
                    &mut vectors,
                    SocketRecvFlags::default(),
                    cancellation,
                ));
                poll_fn(|context| Poll::Ready(receive.as_mut().poll(context))).await
            };
            match result {
                Poll::Ready(result) => {
                    return result.map(|result| {
                        buffer.truncate(result.bytes);
                        Some(crate::IoUringRecvMessage::portable(
                            OwnedSocketRecvMessage::new(result, buffer),
                        ))
                    });
                }
                Poll::Pending => drop(buffer),
            }
            // The syscall-first probe consumed no message. Release scarce storage before waiting, including when this read future is later abandoned.
            self.scheduler
                .socket_monitor_wait(&mut self.monitor, cancellation)
                .await?;
        }
    }

    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.read_vectored(&mut [IoSliceMut::new(buffer)], cancellation)
            .await
    }

    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        cancellation.check()?;
        if self.closing {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.operations.check_idle()?;
        self.scheduler
            .socket_monitor_recvmsg_poll(
                &mut self.monitor,
                buffers,
                SocketRecvFlags::default(),
                cancellation,
            )
            .await
    }

    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<base_scheduler::SocketRecvResult> {
        cancellation.check()?;
        if self.closing {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.operations.check_idle()?;
        self.scheduler
            .socket_monitor_recvmsg_poll(&mut self.monitor, buffers, flags, cancellation)
            .await
    }
    async fn read_owned_vectored<Buffers: base_scheduler::IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<base_scheduler::SocketRecvResult>, Buffers) {
        if self.closing {
            return (
                Err(io::Error::from(io::ErrorKind::BrokenPipe).into()),
                buffers,
            );
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffers);
        }
        let buffers = match self.operations.begin(buffers) {
            Ok(buffers) => buffers,
            Err((error, buffers)) => return (Err(error), buffers),
        };
        let (result, buffers) = self
            .scheduler
            .socket_recvmsg(self.monitor.socket(), buffers, flags, cancellation)
            .await;
        (result, buffers.into_inner())
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closing = true;
        self.operations.close().await;
        self.scheduler.socket_monitor_close(&mut self.monitor).await
    }
}
