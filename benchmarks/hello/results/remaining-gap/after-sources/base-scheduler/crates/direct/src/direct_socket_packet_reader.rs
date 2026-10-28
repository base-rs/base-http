// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::{DirectScheduler, DirectSocket};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, OperationResult,
    OwnedSocketRecvMessage, SocketPacketReader, SocketRecvFlags, SocketRecvResult, SocketType,
};
use std::{
    future::poll_fn,
    io::{self, IoSliceMut},
    pin::pin,
    task::Poll,
};

/// Scheduler-bound packet reader; registration remains private to this backend.
pub(crate) struct DirectSocketPacketReader<'a> {
    scheduler: &'a DirectScheduler,
    operations: crate::operation_state::OperationState,
    socket: DirectSocket,
    pool: Option<base_scheduler::HeapBufferPool>,
    closing: bool,
}

impl<'a> DirectSocketPacketReader<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a DirectScheduler, socket: &DirectSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? == SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a packet reader requires a packet socket",
            ));
        }
        Ok(Self {
            scheduler,
            operations: Default::default(),
            socket: DirectSocket(socket.retain()),
            pool: None,
            closing: false,
        })
    }

    /// Select reusable owned receive storage, including between completed reads. Outstanding messages retain their earlier pool independently. Borrowed reads never acquire from this pool.
    pub(crate) fn with_pool(mut self, pool: base_scheduler::HeapBufferPool) -> Self {
        self.pool = Some(pool);
        self
    }
}

impl SocketPacketReader for DirectSocketPacketReader<'_> {
    type Message = OwnedSocketRecvMessage<<base_scheduler::HeapBufferPool as BufferPool>::Buffer>;

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
                let mut receive = pin!(self.scheduler.socket_recvmsg_poll(
                    &self.socket,
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
                        Some(OwnedSocketRecvMessage::new(result, buffer))
                    });
                }
                Poll::Pending => drop(buffer),
            }
            // The syscall-first probe consumed no message. Release scarce storage before waiting, including when this read future is later abandoned.
            self.scheduler
                .socket_wait(&self.socket, Interest::Readable, cancellation)
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
        self.read_vectored_with_flags(buffers, SocketRecvFlags::default(), cancellation)
            .await
    }

    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [std::io::IoSliceMut<'_>],
        flags: base_scheduler::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<base_scheduler::SocketRecvResult> {
        cancellation.check()?;
        if self.closing {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.operations.check_idle()?;
        let result = self
            .scheduler
            .socket_recvmsg_poll(&self.socket, buffers, flags, cancellation)
            .await?;
        Ok(result)
    }

    async fn read_owned_vectored<Buffers: base_scheduler::IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: base_scheduler::SocketRecvFlags,
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
            .socket_recvmsg(&self.socket, buffers, flags, cancellation)
            .await;
        let buffers = buffers.into_inner();
        (result, buffers)
    }

    async fn close(&mut self) -> io::Result<()> {
        self.closing = true;
        self.operations.close().await;
        Ok(())
    }
}
