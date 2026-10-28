// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::{IoUringSelector, MonitorStorage};
use base_scheduler::{
    BufferResult, Cancellation, IoBuf, OperationResult, SocketAddress, SocketPacketWriter,
    SocketSendFlags, SocketType,
};
use base_scheduler_selector::NativeSocket;
use std::io::{self, IoSlice};

/// A packet writer retaining one scheduler and writable monitor. All writes use a single nonblocking message-send operation, without payload allocation or packet splitting. Cancellation and send errors leave it usable.
pub(crate) struct IoUringSocketPacketWriter<'a> {
    scheduler: &'a IoUringSelector,
    operations: crate::operation_state::OperationState,
    monitor: MonitorStorage,
    closing: bool,
}

impl<'a> IoUringSocketPacketWriter<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a IoUringSelector, socket: &NativeSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? == SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a packet writer requires a packet socket",
            ));
        }
        Ok(Self {
            scheduler,
            operations: Default::default(),
            monitor: scheduler.socket_monitor(socket, Interest::Writable),
            closing: false,
        })
    }
}

impl SocketPacketWriter for IoUringSocketPacketWriter<'_> {
    async fn write(
        &mut self,
        buffer: &[u8],
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.write_vectored(&[IoSlice::new(buffer)], address, cancellation)
            .await
    }

    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        if self.closing {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.operations.check_idle()?;
        self.scheduler
            .socket_monitor_sendmsg_poll(
                &mut self.monitor,
                buffers,
                address,
                SocketSendFlags::default(),
                cancellation,
            )
            .await
    }

    async fn write_owned<B: IoBuf>(
        &mut self,
        buffer: B,
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = self.write(buffer.as_ref(), address, cancellation).await;
        (result, buffer)
    }

    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        if self.closing {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.operations.check_idle()?;
        self.scheduler
            .socket_monitor_sendmsg_poll(&mut self.monitor, buffers, address, flags, cancellation)
            .await
    }
    async fn write_owned_vectored<Buffers: base_scheduler::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        address: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
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
        let (result, message) = self
            .scheduler
            .socket_sendmsg(
                self.monitor.socket(),
                crate::SocketMessage { buffers, address },
                flags,
                cancellation,
            )
            .await;
        (result, message.buffers.into_inner())
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closing = true;
        self.operations.close().await;
        self.scheduler.socket_monitor_close(&mut self.monitor).await
    }
}
