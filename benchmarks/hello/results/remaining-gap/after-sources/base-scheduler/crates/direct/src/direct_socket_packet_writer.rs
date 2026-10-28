// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{DirectScheduler, DirectSocket};
use base_scheduler::{
    BufferResult, Cancellation, IoBuf, OperationResult, SocketAddress, SocketPacketWriter,
    SocketSendFlags, SocketType,
};
use std::io::{self, IoSlice};

/// Scheduler-bound packet writer; registration remains private to this backend.
pub(crate) struct DirectSocketPacketWriter<'a> {
    scheduler: &'a DirectScheduler,
    operations: crate::operation_state::OperationState,
    socket: DirectSocket,
    closing: bool,
}

impl<'a> DirectSocketPacketWriter<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a DirectScheduler, socket: &DirectSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? == SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a packet writer requires a packet socket",
            ));
        }
        Ok(Self {
            scheduler,
            operations: Default::default(),
            socket: DirectSocket(socket.retain()),
            closing: false,
        })
    }
}

impl SocketPacketWriter for DirectSocketPacketWriter<'_> {
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
        self.write_vectored_with_flags(buffers, address, SocketSendFlags::default(), cancellation)
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
        buffers: &[std::io::IoSlice<'_>],
        address: Option<base_scheduler::SocketAddress>,
        flags: base_scheduler::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        if self.closing {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.operations.check_idle()?;
        self.scheduler
            .socket_sendmsg_poll(&self.socket, buffers, address, flags, cancellation)
            .await
    }

    async fn write_owned_vectored<Buffers: base_scheduler::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        address: Option<base_scheduler::SocketAddress>,
        flags: base_scheduler::SocketSendFlags,
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
        let message = crate::SocketMessage { buffers, address };
        let (result, message) = self
            .scheduler
            .socket_sendmsg(&self.socket, message, flags, cancellation)
            .await;
        (result, message.buffers.into_inner())
    }

    async fn close(&mut self) -> io::Result<()> {
        self.closing = true;
        self.operations.close().await;
        Ok(())
    }
}
