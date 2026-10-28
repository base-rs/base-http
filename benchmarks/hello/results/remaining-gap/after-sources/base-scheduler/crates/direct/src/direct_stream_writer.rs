// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::socket_write_state::SocketWriteState;
use crate::{DirectScheduler, DirectSocket, DirectTask, socket_io, socket_retry::SocketRetry};
use base_scheduler::{
    Cancellation, IoBuf, OperationError, OperationResult, SocketSendFlags, SocketStreamWriter,
};
use std::{
    future::{Future, poll_fn},
    io::{self, IoSlice},
    marker::PhantomData,
    pin::pin,
    sync::Arc,
    task::Poll,
};

/// Ordered output which completes ready sends inline and retries stalled writes on the direct task service.
pub(crate) struct DirectStreamWriter<'scheduler, B> {
    scheduler: &'scheduler DirectScheduler,
    operations: crate::operation_state::OperationState,
    socket: DirectSocket,
    state: SocketWriteState<DirectTask<OperationResult<()>>>,
    buffer: PhantomData<fn(B)>,
}

impl DirectScheduler {
    pub(crate) fn socket_stream_output<B: IoBuf>(
        &self,
        socket: &DirectSocket,
    ) -> io::Result<DirectStreamWriter<'_, B>> {
        self.socket_retry.check_open()?;
        if socket_io::recvmsg_socket_type(&socket.0)? != base_scheduler::SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ordered writers require a stream socket",
            ));
        }
        Ok(DirectStreamWriter {
            scheduler: self,
            operations: Default::default(),
            socket: DirectSocket(socket.retain()),
            state: SocketWriteState::default(),
            buffer: PhantomData,
        })
    }
}

impl<B> DirectStreamWriter<'_, B> {
    fn check_open(&self) -> OperationResult<()> {
        self.state.check_open()?;
        self.operations.check_idle()?;
        Ok(())
    }
}

impl<B: IoBuf> SocketStreamWriter for DirectStreamWriter<'_, B> {
    type Buffer = B;

    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.state.is_pending() {
            self.state.flush(cancellation).await?;
        }
        self.scheduler
            .socket_send_poll(
                &self.socket,
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
        self.write_vectored_with_flags(buffers, SocketSendFlags::default(), cancellation)
            .await
    }

    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[std::io::IoSlice<'_>],
        flags: base_scheduler::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.state.is_pending() {
            self.state.flush(cancellation).await?;
        }
        self.scheduler
            .socket_sendmsg_poll(&self.socket, buffers, None, flags, cancellation)
            .await
    }

    async fn write_owned_vectored<Buffers: base_scheduler::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        flags: base_scheduler::SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
        if let Err(error) = self.state.check_open() {
            return (Err(error), buffers);
        }
        if self.state.is_pending() {
            if let Err(error) = self.state.flush(cancellation).await {
                return (Err(error), buffers);
            }
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffers);
        }
        let buffers = match self.operations.begin(buffers) {
            Ok(buffers) => buffers,
            Err((error, buffers)) => return (Err(error), buffers),
        };
        let message = crate::SocketMessage {
            buffers,
            address: None,
        };
        let (result, message) = self
            .scheduler
            .socket_sendmsg(&self.socket, message, flags, cancellation)
            .await;
        (result, message.buffers.into_inner())
    }

    async fn write_owned<Owned: base_scheduler::IoBuf>(
        &mut self,
        buffer: Owned,
        cancellation: &Cancellation,
    ) -> base_scheduler::BufferResult<Owned> {
        if let Err(error) = self.state.check_open() {
            return (Err(error), buffer);
        }
        if self.state.is_pending() {
            if let Err(error) = self.state.flush(cancellation).await {
                return (Err(error), buffer);
            }
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let buffer = match self.operations.begin(buffer) {
            Ok(buffer) => buffer,
            Err((error, buffer)) => return (Err(error), buffer),
        };
        let (result, buffer) = self
            .scheduler
            .socket_write(&self.socket, buffer, cancellation)
            .await;
        (result, buffer.into_inner())
    }

    async fn submit(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        let scheduler = self.scheduler;
        if let Err(error) = self.check_open() {
            return Err((error, buffer));
        }
        if let Err(error) = scheduler.socket_retry.check_open().map_err(Into::into) {
            return Err((error, buffer));
        }
        if let Err(error) = cancellation.check() {
            return Err((error.into(), buffer));
        }
        if let Err(error) = self.state.flush(cancellation).await {
            return Err((error, buffer));
        }
        if let Err(error) = cancellation.check() {
            return Err((error.into(), buffer));
        }
        if let Err(error) = scheduler.socket_retry.check_open() {
            return Err((error.into(), buffer));
        }
        let length = buffer.as_ref().len();
        if length == 0 {
            return Ok(());
        }
        let offset = match send(&self.socket.0, buffer.as_ref()) {
            Ok(count) if count == length => return Ok(()),
            Ok(0) => {
                self.state
                    .fail(io::Error::from(io::ErrorKind::WriteZero).into());
                return Ok(());
            }
            Ok(count) => count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                0
            }
            Err(error) => {
                self.state.fail(error.into());
                return Ok(());
            }
        };
        let socket = self.socket.retain();
        let retry = Arc::clone(&scheduler.socket_retry);
        let cancellation = cancellation.clone();
        let stopping = self.state.cancellation();
        let writing = async move {
            let mut buffer = buffer;
            let result = write_remaining(
                &socket,
                &mut buffer,
                offset,
                &retry,
                &cancellation,
                &stopping,
            )
            .await;
            drop(buffer);
            drop(socket);
            result
        };
        match scheduler.tasks.spawn(writing) {
            Ok(task) => self.state.accept(task),
            Err(error) => self.state.fail(io::Error::other(error.to_string()).into()),
        }
        Ok(())
    }

    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        self.operations.check_idle()?;
        self.state.flush(cancellation).await
    }

    async fn close(&mut self) -> io::Result<()> {
        self.state.stop();
        self.operations.close().await;
        self.state.close().await
    }
}

async fn write_remaining<B: IoBuf>(
    socket: &socket2::Socket,
    buffer: &mut B,
    mut offset: usize,
    retry: &SocketRetry,
    cancellation: &Cancellation,
    stopping: &Cancellation,
) -> OperationResult<()> {
    let mut stopped = pin!(stopping.cancelled());
    while offset < buffer.as_ref().len() {
        cancellation.check()?;
        stopping.check()?;
        match send(socket, &buffer.as_ref()[offset..]) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Ok(count) => offset += count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                let mut waiting = pin!(retry.wait(cancellation));
                poll_fn(|context| {
                    // A retry pulse reports no I/O progress; it must not postpone an already requested stop.
                    if stopped.as_mut().poll(context).is_ready() {
                        return Poll::Ready(Err(base_scheduler::Cancelled.into()));
                    }
                    waiting.as_mut().poll(context)
                })
                .await?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn send(socket: &socket2::Socket, bytes: &[u8]) -> io::Result<usize> {
    #[cfg(test)]
    if let Some(result) = tests::send_result() {
        return result;
    }
    socket_io::send_poll(socket, bytes, base_scheduler::SocketSendFlags::default())
}

#[cfg(test)]
mod tests;

impl base_scheduler::SocketStreamWriting for DirectScheduler {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl base_scheduler::SocketStreamWriter<
            Buffer = <Self::Pool as base_scheduler::BufferPool>::Buffer,
        > + 'scheduler,
    >
    where
        Self::Socket: 'static,
    {
        self.socket_stream_output(socket)
    }

    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl base_scheduler::SocketStreamWriter<Buffer = B> + 'scheduler>
    where
        Self::Socket: 'static,
    {
        self.socket_stream_output(socket)
    }
}
