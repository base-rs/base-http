// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{TokioScheduler, TokioSocket};
use base_scheduler::{
    BufferResult, Cancellation, IoBuf, OperationResult, SocketAddress, SocketPacketWriter,
    SocketSendFlags, SocketType,
};
use std::io::{self, IoSlice};

/// Scheduler-bound packet writer; registration remains private to this backend.
pub(crate) struct TokioSocketPacketWriter<'a> {
    scheduler: &'a TokioScheduler,
    socket: TokioSocket,
    closing: bool,
}

impl<'a> TokioSocketPacketWriter<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a TokioScheduler, socket: &TokioSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? == SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a packet writer requires a packet socket",
            ));
        }
        Ok(Self {
            scheduler,
            socket: socket.retain(),
            closing: false,
        })
    }
}

impl SocketPacketWriter for TokioSocketPacketWriter<'_> {
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
        let message = crate::SocketMessage { buffers, address };
        let (result, message) = self
            .scheduler
            .socket_sendmsg(&self.socket, message, flags, cancellation)
            .await;
        (result, message.buffers)
    }

    async fn close(&mut self) -> io::Result<()> {
        self.closing = true;
        Ok(())
    }
}
