// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::runtime::{Handle, Id};

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct RegisteredSocket {
    socket: Arc<socket2::Socket>,
    runtime: Handle,
    stream: bool,
    connected_stream: AtomicBool,
    read_waiting: AtomicBool,
    write_waiting: AtomicBool,
    registration: tokio::sync::OnceCell<tokio::io::unix::AsyncFd<Arc<socket2::Socket>>>,
}

#[cfg(unix)]
impl RegisteredSocket {
    fn new(runtime: &Handle, socket: socket2::Socket) -> Self {
        let stream = socket
            .r#type()
            .is_ok_and(|kind| kind == socket2::Type::STREAM);
        Self {
            socket: Arc::new(socket),
            stream,
            connected_stream: AtomicBool::new(false),
            runtime: runtime.clone(),
            read_waiting: AtomicBool::new(false),
            write_waiting: AtomicBool::new(false),
            registration: tokio::sync::OnceCell::new(),
        }
    }

    fn get_ref(&self) -> &socket2::Socket {
        &self.socket
    }

    fn into_inner(self) -> socket2::Socket {
        drop(self.registration);
        Arc::try_unwrap(self.socket).expect("socket registration releases its private socket owner")
    }

    async fn registration(&self) -> io::Result<&tokio::io::unix::AsyncFd<Arc<socket2::Socket>>> {
        self.registration
            .get_or_try_init(|| async {
                let _scope = self.runtime.enter();
                // SAFETY: Both this wrapper and the registration own the same socket. No operation can close or replace its descriptor while registered, including if the wrapper or a pending future is forgotten.
                unsafe { tokio::io::unix::AsyncFd::register(Arc::clone(&self.socket)) }
                    .map_err(io::Error::from)
            })
            .await
    }
}

#[cfg(unix)]
struct PollWaiter<'a>(&'a AtomicBool);

#[cfg(unix)]
impl Drop for PollWaiter<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(windows)]
type RegisteredSocket = async_io::Async<socket2::Socket>;

/// A nonblocking socket associated with the adapter's Tokio runtime.
///
/// A descriptor can represent an unconnected, bound, listening, connected, or datagram socket. Unix readiness registration is deferred until an operation needs to wait, so an unconnected socket cannot retain stale closed readiness after connecting or listening. It is deliberately not `Clone`; operation futures borrow it, and accepted shutdown jobs privately retain its registration until completion. Explicit close consumes exclusive ownership.
pub struct TokioSocket {
    pub(crate) inner: Arc<RegisteredSocket>,
    runtime: Id,
}

impl TokioSocket {
    pub(super) fn new(runtime: &Handle, socket: socket2::Socket) -> io::Result<Self> {
        Self::new_with(
            runtime,
            socket,
            |socket| socket.set_nonblocking(true),
            |runtime, socket| {
                #[cfg(unix)]
                {
                    Ok(RegisteredSocket::new(runtime, socket))
                }

                #[cfg(windows)]
                {
                    let _ = runtime;
                    RegisteredSocket::new_nonblocking(socket)
                }
            },
        )
    }

    fn new_with(
        runtime: &Handle,
        socket: socket2::Socket,
        set_nonblocking: impl FnOnce(&socket2::Socket) -> io::Result<()>,
        register: impl FnOnce(&Handle, socket2::Socket) -> io::Result<RegisteredSocket>,
    ) -> io::Result<Self> {
        set_nonblocking(&socket)?;
        let inner = register(runtime, socket)?;

        Ok(Self {
            inner: Arc::new(inner),
            runtime: runtime.id(),
        })
    }

    pub(super) fn retain(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            runtime: self.runtime,
        }
    }

    pub(super) fn into_close(self) -> impl FnOnce() -> io::Result<()> + Send + 'static {
        let inner = Arc::try_unwrap(self.inner)
            .expect("cannot close a socket with outstanding operations; await them first");
        move || {
            #[cfg(unix)]
            {
                crate::socket_io::close(inner.into_inner())
            }
            #[cfg(windows)]
            {
                inner.into_inner().and_then(crate::socket_io::close)
            }
        }
    }

    pub(super) fn runtime(&self) -> Id {
        self.runtime
    }

    pub(super) fn get_ref(&self) -> &socket2::Socket {
        self.inner.get_ref()
    }

    #[cfg(test)]
    pub(super) async fn io<Output, Operation>(
        &self,
        interest: Interest,
        operation: Operation,
    ) -> io::Result<Output>
    where
        Operation: FnMut(&socket2::Socket) -> io::Result<Output> + Send,
    {
        self.io_with(interest, operation, false).await
    }

    pub(super) async fn io_with<Output, Operation>(
        &self,
        interest: Interest,
        mut operation: Operation,
        read_nonempty: bool,
    ) -> io::Result<Output>
    where
        Operation: FnMut(&socket2::Socket) -> io::Result<Output> + Send,
    {
        #[cfg(unix)]
        let known_connected = read_nonempty && self.inner.connected_stream.load(Ordering::Relaxed);
        #[cfg(unix)]
        let mark_connected = read_nonempty && self.inner.stream && !known_connected;
        #[cfg(unix)]
        let ready_first =
            read_nonempty && known_connected && self.inner.registration.get().is_some();
        // Only nonempty reads on a stream that already completed a read may skip the speculative syscall. Lifecycle operations, unknown socket states, and empty reads retain immediate OS results.
        #[cfg(unix)]
        if !ready_first {
            let result = operation(self.get_ref());
            if mark_connected && result.is_ok() {
                self.inner.connected_stream.store(true, Ordering::Relaxed);
            }
            if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
                return result;
            }
        }
        #[cfg(unix)]
        {
            self.ready_io(interest, operation, mark_connected).await
        }

        #[cfg(windows)]
        {
            let _ = read_nonempty;
            let result = operation(self.get_ref());
            if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
                return result;
            }
            match interest {
                Interest::Readable => self.inner.read_with(|socket| operation(socket)).await,
                Interest::Writable => self.inner.write_with(|socket| operation(socket)).await,
            }
        }
    }

    pub(super) async fn wait(&self, interest: Interest) -> io::Result<()> {
        #[cfg(unix)]
        if self.inner.registration.get().is_some() {
            // A registered socket already has advisory readiness state. Waiting on it needs no extra poll(2) probe before or after Tokio wakes us.
            return self.ready_io(interest, |_| Ok(()), false).await;
        }
        // Preserve syscall-first handling of unregistered sockets, including unconnected streams whose hangup must not become permanently cached readiness.
        self.io_with(
            interest,
            |socket| crate::socket_io::wait(socket, interest),
            false,
        )
        .await
    }

    #[cfg(unix)]
    async fn ready_io<Output, Operation>(
        &self,
        interest: Interest,
        mut operation: Operation,
        mark_connected: bool,
    ) -> io::Result<Output>
    where
        Operation: FnMut(&socket2::Socket) -> io::Result<Output> + Send,
    {
        // Linux can report EPOLLHUP for an unconnected TCP socket; Tokio retains closed readiness permanently, which would spin after the socket later connects and a syscall returns WouldBlock. Register only after a syscall needs to wait, then reuse the registration on subsequent waits.
        let registration = match self.inner.registration.get() {
            Some(registration) => registration,
            None => self.inner.registration().await?,
        };
        let interest = match interest {
            Interest::Readable => tokio::io::Interest::READABLE,
            Interest::Writable => tokio::io::Interest::WRITABLE,
        };
        // Tokio's poll methods keep one waker per direction. Reserve that slot for one operation until completion or drop; concurrent operations use Tokio's independent multi-waiter list. Forgetting a reservation only leaves later operations on the multi-waiter path.
        let waiting = if interest == tokio::io::Interest::READABLE {
            &self.inner.read_waiting
        } else {
            &self.inner.write_waiting
        };
        if waiting
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            loop {
                let mut ready = registration.ready(interest).await?;
                if let Ok(result) = ready.try_io(|_| operation(self.inner.get_ref())) {
                    if mark_connected && result.is_ok() {
                        self.inner.connected_stream.store(true, Ordering::Relaxed);
                    }
                    return result;
                }
            }
        }
        let _waiter = PollWaiter(waiting);
        std::future::poll_fn(|context| {
            loop {
                let mut ready = std::task::ready!(if interest == tokio::io::Interest::READABLE {
                    registration.poll_read_ready(context)
                } else {
                    registration.poll_write_ready(context)
                })?;
                if let Ok(result) = ready.try_io(|_| operation(self.inner.get_ref())) {
                    if mark_connected && result.is_ok() {
                        self.inner.connected_stream.store(true, Ordering::Relaxed);
                    }
                    return std::task::Poll::Ready(result);
                }
            }
        })
        .await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().local_addr().map(crate::socket_io::address)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().peer_addr().map(crate::socket_io::address)
    }
}

#[cfg(test)]
mod tests;
