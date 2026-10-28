// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::SocketMessage;
use crate::{
    TokioScheduler, TokioSocket,
    blocking_pool::admission_error,
    socket_io::{self, IoVectors},
    tokio_scheduler::wait_close,
};
use base_scheduler::*;
use socket2::Socket;
use std::{io, net::Shutdown};

impl TokioScheduler {
    // All payload access happens synchronously inside the readiness callback. Pending futures retain their owners; the reactor retains no payload pointers.
    async fn socket_operation<Input: Send, Output: Send>(
        &self,
        socket: &TokioSocket,
        input: Input,
        cancellation: &Cancellation,
        interest: Interest,
        operation: impl FnMut(&Socket, &mut Input) -> io::Result<Output> + Send,
    ) -> (OperationResult<Output>, Input) {
        self.socket_operation_with(socket, input, cancellation, interest, operation, false)
            .await
    }

    async fn socket_operation_with<Input: Send, Output: Send>(
        &self,
        socket: &TokioSocket,
        mut input: Input,
        cancellation: &Cancellation,
        interest: Interest,
        mut operation: impl FnMut(&Socket, &mut Input) -> io::Result<Output> + Send,
        read_nonempty: bool,
    ) -> (OperationResult<Output>, Input) {
        let result = self
            .socket_result(socket, cancellation, || {
                socket.io_with(
                    interest,
                    |socket| operation(socket, &mut input),
                    read_nonempty,
                )
            })
            .await;
        (result, input)
    }

    async fn socket_result<Output: Send, Operation>(
        &self,
        socket: &TokioSocket,
        cancellation: &Cancellation,
        operation: impl FnOnce() -> Operation + Send,
    ) -> OperationResult<Output>
    where
        Operation: std::future::Future<Output = io::Result<Output>> + Send,
    {
        use std::{
            future::{Future, poll_fn},
            pin::pin,
            task::Poll,
        };
        cancellation.check()?;
        self.check_socket(socket)?;
        // The factory lets the compiler construct the operation in its pinned local, avoiding moves of nested readiness state.
        let mut operation = pin!(operation());
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            match operation.as_mut().poll(context) {
                Poll::Ready(result) => return Poll::Ready(result.map_err(Into::into)),
                Poll::Pending => (),
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(Cancelled.into()));
            }
            Poll::Pending
        })
        .await
    }
}

impl SocketOperation for TokioScheduler {
    type Socket = TokioSocket;
}

impl SocketOpen for TokioScheduler {
    async fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<TokioSocket> {
        cancellation.check()?;
        self.check_open()?;
        TokioSocket::new(&self.runtime, socket_io::open(domain, kind, protocol)?)
            .map_err(OperationError::Io)
    }
}

impl SocketBind for TokioScheduler {
    async fn socket_bind(
        &self,
        socket: &TokioSocket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(
            socket,
            (),
            cancellation,
            Interest::Writable,
            move |socket, _| socket.bind(&address.into()),
        )
        .await
        .0
    }
}

impl SocketListen for TokioScheduler {
    async fn socket_listen(
        &self,
        socket: &TokioSocket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(
            socket,
            (),
            cancellation,
            Interest::Writable,
            move |socket, _| socket.listen(backlog),
        )
        .await
        .0
    }
}

impl SocketLocalAddress for TokioScheduler {
    fn socket_local_address(&self, socket: &TokioSocket) -> io::Result<SocketAddress> {
        self.check_socket(socket)?;
        socket.local_addr()
    }
}

impl SocketPeerAddress for TokioScheduler {
    fn socket_peer_address(&self, socket: &TokioSocket) -> io::Result<SocketAddress> {
        self.check_socket(socket)?;
        socket.peer_addr()
    }
}

impl SocketConnect for TokioScheduler {
    async fn socket_connect(
        &self,
        socket: &TokioSocket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(
            socket,
            false,
            cancellation,
            Interest::Writable,
            move |socket, started| socket_io::connect(socket, address, started),
        )
        .await
        .0
    }
}

impl SocketAccept for TokioScheduler {
    async fn socket_accept(
        &self,
        socket: &TokioSocket,
        cancellation: &Cancellation,
    ) -> OperationResult<(TokioSocket, SocketAddress)> {
        self.socket_operation(socket, (), cancellation, Interest::Readable, |socket, _| {
            socket.accept().and_then(|(socket, address)| {
                TokioSocket::new(&self.runtime, socket)
                    .map(|socket| (socket, socket_io::address(address)))
            })
        })
        .await
        .0
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_read<B: IoBufMut>(
        &self,
        socket: &TokioSocket,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let length = buffer.as_mut().len();
        self.socket_operation_with(
            socket,
            buffer,
            cancellation,
            Interest::Readable,
            |socket, buffer| socket_io::recv(socket, buffer.as_mut()),
            length > 0,
        )
        .await
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_write<B: IoBuf>(
        &self,
        socket: &TokioSocket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.socket_operation(
            socket,
            buffer,
            cancellation,
            Interest::Writable,
            |socket, buffer| socket_io::send(socket, buffer.as_ref()),
        )
        .await
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_recv_poll(
        &self,
        socket: &TokioSocket,
        buffer: &mut [u8],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        let length = buffer.len();
        self.socket_operation_with(
            socket,
            buffer,
            cancellation,
            Interest::Readable,
            |socket, buffer| socket_io::recv_poll(socket, buffer, flags),
            length > 0,
        )
        .await
        .0
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_send_poll(
        &self,
        socket: &TokioSocket,
        buffer: &[u8],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.socket_operation(
            socket,
            buffer,
            cancellation,
            Interest::Writable,
            |socket, buffer| socket_io::send_poll(socket, buffer, flags),
        )
        .await
        .0
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_recvmsg_poll(
        &self,
        socket: &TokioSocket,
        buffer: &mut [std::io::IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.socket_operation(
            socket,
            buffer,
            cancellation,
            Interest::Readable,
            |socket, buffer| socket_io::recvmsg_poll(socket, buffer, flags),
        )
        .await
        .0
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_sendmsg_poll(
        &self,
        socket: &TokioSocket,
        buffer: &[std::io::IoSlice<'_>],
        address: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.socket_operation(
            socket,
            buffer,
            cancellation,
            Interest::Writable,
            |socket, buffer| socket_io::sendmsg_poll(socket, buffer, address, flags),
        )
        .await
        .0
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_wait(
        &self,
        socket: &TokioSocket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_result(socket, cancellation, || socket.wait(interest))
            .await
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_sendmsg<B: IoBufSequence>(
        &self,
        socket: &TokioSocket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        let mut vectors = IoVectors::new();
        self.socket_operation(
            socket,
            message,
            cancellation,
            Interest::Writable,
            move |socket, message| vectors.send(socket, &message.buffers, message.address, flags),
        )
        .await
    }
}

impl TokioScheduler {
    pub(crate) async fn socket_recvmsg<B: IoBufSequenceMut>(
        &self,
        socket: &TokioSocket,
        buffers: B,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        let mut vectors = IoVectors::new();
        self.socket_operation(
            socket,
            buffers,
            cancellation,
            Interest::Readable,
            move |socket, buffers| vectors.recv(socket, buffers, flags),
        )
        .await
    }
}

impl SocketClose for TokioScheduler {
    async fn socket_close(&self, socket: TokioSocket) -> io::Result<()> {
        self.check_socket(&socket)?;
        let close = socket.into_close();
        let completion = self
            .cleanup
            .submit(close)
            .await
            .map_err(|error| io::Error::from(admission_error(error)))?;
        wait_close(completion).await
    }

    async fn socket_close_submit(&self, socket: TokioSocket) -> io::Result<()> {
        self.check_socket(&socket)?;
        let close = socket.into_close();
        self.cleanup
            .submit(close)
            .await
            .map(drop)
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

impl SocketShutdown for TokioScheduler {
    async fn socket_shutdown(&self, socket: &TokioSocket, how: Shutdown) -> io::Result<()> {
        self.check_socket(socket)?;
        let socket = socket.retain();
        let completion = self
            .cleanup
            .submit(move || socket.get_ref().shutdown(how))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))?;
        wait_close(completion).await
    }

    async fn socket_shutdown_submit(&self, socket: &TokioSocket, how: Shutdown) -> io::Result<()> {
        self.check_socket(socket)?;
        let socket = socket.retain();
        self.cleanup
            .submit(move || socket.get_ref().shutdown(how))
            .await
            .map(drop)
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

#[cfg(test)]
mod tests;

impl TokioScheduler {
    pub(crate) fn socket_type(&self, socket: &TokioSocket) -> io::Result<SocketType> {
        self.check_socket(socket)?;
        crate::socket_io::recvmsg_socket_type(socket.get_ref())
    }
}
