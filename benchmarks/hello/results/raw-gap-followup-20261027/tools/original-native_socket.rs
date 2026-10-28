// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use base_scheduler::{SocketRecvFlags, SocketRecvResult, SocketSendFlags};
use std::{io, net::SocketAddr, sync::Arc};
type RegisteredSocket = async_io::Async<socket2::Socket>;

/// An owned nonblocking socket with a retained readiness registration.
///
/// Usable by any native scheduler instance. Readiness operations retain their buffers in the waiting future without leaving payload pointers in the reactor. Completion backends retain the socket and buffers independently until their operations retire. Finish outstanding operations before exclusive close. Imported TCP resources must remain nonblocking through external aliases.
#[derive(Debug)]
pub struct NativeSocket {
    // Drop registrations before the final native owner. They hold only a weak native reference.
    #[cfg(unix)]
    pub(crate) registrations: Arc<crate::readiness_driver::SocketRegistrations>,
    pub(crate) inner: Arc<RegisteredSocket>,
}
impl NativeSocket {
    /// Retain the registered native socket for another I/O backend.
    ///
    /// # Safety
    /// Keep this owner alive until all submitted operations using its handle have retired, including after cancellation or abandonment. Do not close or replace the raw handle, remove its readiness registration, or change its nonblocking mode. Release the retained owner before reporting completion to callers that may immediately close the socket.
    pub unsafe fn retain_native(&self) -> Arc<RegisteredSocket> {
        Arc::clone(&self.inner)
    }

    pub(crate) fn new(socket: socket2::Socket) -> io::Result<Self> {
        Self::new_with(socket, RegisteredSocket::new)
    }
    fn new_with(
        socket: socket2::Socket,
        register: impl FnOnce(socket2::Socket) -> io::Result<RegisteredSocket>,
    ) -> io::Result<Self> {
        let inner = Arc::new(register(socket)?);
        Ok(Self {
            #[cfg(unix)]
            registrations: Arc::new(crate::readiness_driver::SocketRegistrations::new(&inner)),
            inner,
        })
    }
    pub(crate) fn retain(&self) -> Self {
        Self {
            #[cfg(unix)]
            registrations: Arc::clone(&self.registrations),
            inner: Arc::clone(&self.inner),
        }
    }
    pub(crate) fn into_close(self) -> impl FnOnce() -> io::Result<()> + Send + 'static {
        #[cfg(unix)]
        drop(self.registrations);
        let inner = Arc::try_unwrap(self.inner)
            .expect("cannot close a socket with outstanding operations; await them first");
        move || inner.into_inner().and_then(crate::socket_io::close)
    }
    pub(crate) fn get_ref(&self) -> &socket2::Socket {
        self.inner.get_ref()
    }
    pub(crate) async fn io<Output, Operation>(
        &self,
        interest: Interest,
        mut operation: Operation,
    ) -> io::Result<Output>
    where
        Operation: FnMut(&socket2::Socket) -> io::Result<Output> + Send,
    {
        loop {
            // Preserve immediate progress/errors. A pending descriptor must
            // observe actual readiness before retrying: an unrelated wake of
            // the containing task says nothing about this socket.
            let result = operation(self.get_ref());
            if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
                return result;
            }
            let ready = match interest {
                Interest::Readable => self.inner.readable().await,
                Interest::Writable => self.inner.writable().await,
            };
            #[cfg(test)]
            let ready = tests::readiness_result(ready);
            ready?;
        }
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().local_addr().map(crate::socket_io::address)
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().peer_addr().map(crate::socket_io::address)
    }

    /// Attempt a synchronous nonblocking receive. No buffer access survives this call; `WouldBlock` requires a separate readiness wait.
    #[inline]
    pub fn try_recv(&self, buffer: &mut [u8], flags: SocketRecvFlags) -> io::Result<usize> {
        crate::socket_io::recv_poll(self.get_ref(), buffer, flags)
    }

    /// Attempt a synchronous nonblocking send. No buffer access survives this call; `WouldBlock` requires a separate readiness wait.
    #[inline]
    pub fn try_send(&self, buffer: &[u8], flags: SocketSendFlags) -> io::Result<usize> {
        crate::socket_io::send_poll(self.get_ref(), buffer, flags)
    }

    /// Attempt a synchronous nonblocking message receive using the borrowed I/O vectors directly. No payload, vector, or metadata pointer survives this call.
    #[inline]
    pub fn try_recvmsg(
        &self,
        buffers: &mut [io::IoSliceMut<'_>],
        flags: SocketRecvFlags,
    ) -> io::Result<SocketRecvResult> {
        crate::socket_io::recvmsg_poll(self.get_ref(), buffers, flags)
    }

    /// Attempt a synchronous nonblocking message send using the borrowed I/O vectors directly. No payload, vector, or address pointer survives this call.
    #[inline]
    pub fn try_sendmsg(
        &self,
        buffers: &[io::IoSlice<'_>],
        address: Option<SocketAddr>,
        flags: SocketSendFlags,
    ) -> io::Result<usize> {
        crate::socket_io::sendmsg_poll(self.get_ref(), buffers, address, flags)
    }
}

#[cfg(test)]
mod tests;
