// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{DirectScheduler, DirectSocket};
use base_scheduler::{
    Cancellation, OperationResult, SocketGetOption, SocketKeepalive, SocketOption,
    SocketRegistration, SocketReuseAddress, SocketSetOption, TcpNoDelay,
};
use socket2::Socket;
use std::{
    io,
    net::{TcpListener, TcpStream},
};

// Only identifiers with a native mapping are implemented by Direct; consumers still use portable option types.
trait NativeOption: SocketOption {
    fn get(self, socket: &Socket) -> io::Result<Self::Value>;
    fn set(self, socket: &Socket, value: Self::Value) -> io::Result<()>;
}
macro_rules! option {
    ($option:ty, $get:ident, $set:ident) => {
        impl NativeOption for $option {
            fn get(self, socket: &Socket) -> io::Result<bool> {
                socket.$get()
            }
            fn set(self, socket: &Socket, value: bool) -> io::Result<()> {
                socket.$set(value)
            }
        }
    };
}
option!(SocketReuseAddress, reuse_address, set_reuse_address);
option!(SocketKeepalive, keepalive, set_keepalive);
option!(TcpNoDelay, tcp_nodelay, set_tcp_nodelay);

impl<O: NativeOption + 'static> SocketGetOption<O> for DirectScheduler {
    async fn socket_get_option(
        &self,
        socket: &DirectSocket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        #[cfg(test)]
        let probe = std::sync::Arc::clone(&self.option_probe);
        self.work
            .execute(socket.retain(), cancellation, move |socket| {
                #[cfg(test)]
                probe.attempt(socket)?;
                option.get(socket)
            })
            .await
            .0
    }
}

impl<O: NativeOption + 'static> SocketSetOption<O> for DirectScheduler {
    async fn socket_set_option(
        &self,
        socket: &DirectSocket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        #[cfg(test)]
        let probe = std::sync::Arc::clone(&self.option_probe);
        self.work
            .execute(socket.retain(), cancellation, move |socket| {
                #[cfg(test)]
                probe.attempt(socket)?;
                option.set(socket, value)
            })
            .await
            .0
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Probe {
    pub(crate) calls: std::sync::atomic::AtomicUsize,
    pub(crate) failing: std::sync::atomic::AtomicUsize,
}
#[cfg(test)]
impl Probe {
    fn attempt(&self, socket: &std::sync::Arc<Socket>) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) == std::sync::Arc::as_ptr(socket) as usize {
            Err(io::ErrorKind::PermissionDenied.into())
        } else {
            Ok(())
        }
    }
}

impl DirectScheduler {
    fn register_tcp(&self, socket: Socket) -> io::Result<DirectSocket> {
        if self.tasks.is_closed() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ));
        }
        crate::socket_io::nonblocking(socket).map(DirectSocket::new)
    }
}
impl SocketRegistration for DirectScheduler {
    fn register_socket(&self, socket: TcpStream) -> io::Result<DirectSocket> {
        self.register_tcp(socket.into())
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<DirectSocket> {
        self.register_tcp(listener.into())
    }
}

#[cfg(test)]
mod tests;
