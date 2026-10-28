// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::socket_write_state::SocketWriteState;
use crate::{TokioScheduler, TokioSocket, TokioTask, socket_io};
use base_scheduler::{
    Cancellation, IoBuf, OperationError, OperationResult, SocketSendFlags, SocketStreamWriter,
};
use std::{
    future::{Future, poll_fn},
    io::{self, IoSlice},
    marker::PhantomData,
    pin::pin,
    task::Poll,
};

/// Ordered output which completes ready sends inline and delegates stalled logical writes to the adapter's task service.
pub(crate) struct TokioStreamWriter<'scheduler, B> {
    scheduler: &'scheduler TokioScheduler,
    socket: TokioSocket,
    state: SocketWriteState<TokioTask<OperationResult<()>>>,
    buffer: PhantomData<fn(B)>,
}

impl TokioScheduler {
    pub(crate) fn socket_stream_output<B: IoBuf>(
        &self,
        socket: &TokioSocket,
    ) -> io::Result<TokioStreamWriter<'_, B>> {
        self.check_socket(socket)?;
        if socket_io::recvmsg_socket_type(socket.get_ref())? != base_scheduler::SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ordered writers require a stream socket",
            ));
        }
        Ok(TokioStreamWriter {
            scheduler: self,
            socket: socket.retain(),
            state: SocketWriteState::default(),
            buffer: PhantomData,
        })
    }
}

impl<B: IoBuf> SocketStreamWriter for TokioStreamWriter<'_, B> {
    type Buffer = B;

    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.state.check_open()?;
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
        self.state.check_open()?;
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
        let message = crate::SocketMessage {
            buffers,
            address: None,
        };
        let (result, message) = self
            .scheduler
            .socket_sendmsg(&self.socket, message, flags, cancellation)
            .await;
        (result, message.buffers)
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
        let (result, buffer) = self
            .scheduler
            .socket_write(&self.socket, buffer, cancellation)
            .await;
        (result, buffer)
    }

    async fn submit(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        let scheduler = self.scheduler;
        if let Err(error) = self.state.check_open() {
            return Err((error, buffer));
        }
        if let Err(error) = scheduler.check_socket(&self.socket).map_err(Into::into) {
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
        if let Err(error) = scheduler.check_socket(&self.socket) {
            return Err((error.into(), buffer));
        }
        let length = buffer.as_ref().len();
        if length == 0 {
            return Ok(());
        }
        // Admission precedes the first syscall. Errors from now on belong to this accepted logical write and are reported by flush or the next submission.
        let offset = match send(self.socket.get_ref(), buffer.as_ref()) {
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
        let cancellation = cancellation.clone();
        let stopping = self.state.cancellation();
        let writing = async move {
            let mut buffer = buffer;
            let result =
                write_remaining(&socket, &mut buffer, offset, &cancellation, &stopping).await;
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
        self.state.flush(cancellation).await
    }

    async fn close(&mut self) -> io::Result<()> {
        self.state.stop();
        self.state.close().await
    }
}

async fn write_remaining<B: IoBuf>(
    socket: &TokioSocket,
    buffer: &mut B,
    mut offset: usize,
    cancellation: &Cancellation,
    stopping: &Cancellation,
) -> OperationResult<()> {
    let mut cancelled = pin!(cancellation.cancelled());
    let mut stopped = pin!(stopping.cancelled());
    while offset < buffer.as_ref().len() {
        cancellation.check()?;
        stopping.check()?;
        let bytes = &buffer.as_ref()[offset..];
        let operation = socket.io_with(Interest::Writable, |socket| send(socket, bytes), false);
        let mut operation = pin!(operation);
        let count = poll_fn(|context| {
            if let Poll::Ready(result) = operation.as_mut().poll(context) {
                return Poll::Ready(result.map_err(OperationError::from));
            }
            if cancelled.as_mut().poll(context).is_ready()
                || stopped.as_mut().poll(context).is_ready()
            {
                return Poll::Ready(Err(base_scheduler::Cancelled.into()));
            }
            Poll::Pending
        })
        .await;
        match count {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Ok(count) => offset += count,
            Err(OperationError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn send(socket: &socket2::Socket, bytes: &[u8]) -> io::Result<usize> {
    #[cfg(test)]
    if tests::BLOCK_SENDS.with(std::cell::Cell::get) {
        return Err(io::ErrorKind::WouldBlock.into());
    }
    #[cfg(test)]
    if let Some(result) = tests::SEND_RESULTS.with(|results| results.borrow_mut().pop_front()) {
        return result;
    }
    socket_io::send_poll(socket, bytes, base_scheduler::SocketSendFlags::default())
}

#[cfg(test)]
mod tests;

impl base_scheduler::SocketStreamWriting for TokioScheduler {
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
