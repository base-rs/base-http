// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Interest, NativeSocket, ReadinessSelector, readiness_driver::Registration, socket_io};
use base_scheduler::{
    BufferPool, Cancellation, IoBuf, OperationError, OperationResult, SocketSendFlags,
    SocketStreamWriter, SocketStreamWriting,
};
use std::{
    future::{Future, poll_fn},
    io::{self, IoSlice},
    marker::PhantomData,
    pin::{Pin, pin},
    sync::Arc,
    task::Poll,
};

type Completion = Pin<Box<dyn Future<Output = OperationResult<()>> + Send>>;

/// One ordered output lane using readiness for borrowed writes and the existing bounded service for independently progressing owned writes.
pub(crate) struct ReadinessSocketStreamWriter<'scheduler, B> {
    scheduler: &'scheduler ReadinessSelector,
    socket: NativeSocket,
    registration: Option<Arc<Registration>>,
    pending: Option<Completion>,
    stopping: Cancellation,
    error: Option<OperationError>,
    closed: bool,
    buffer: PhantomData<fn(B)>,
    #[cfg(test)]
    send: fn(&socket2::Socket, &[u8], SocketSendFlags) -> io::Result<usize>,
}
impl<'scheduler, B> ReadinessSocketStreamWriter<'scheduler, B> {
    fn new(scheduler: &'scheduler ReadinessSelector, socket: &NativeSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? != base_scheduler::SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream writers require a stream socket",
            ));
        }
        Ok(Self {
            scheduler,
            socket: socket.retain(),
            registration: None,
            pending: None,
            stopping: Cancellation::new(),
            error: None,
            closed: false,
            buffer: PhantomData,
            #[cfg(test)]
            send: socket_io::send_poll,
        })
    }
    fn check_open(&self) -> OperationResult<()> {
        if self.closed {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.result()
    }
    fn result(&self) -> OperationResult<()> {
        match &self.error {
            None => Ok(()),
            Some(OperationError::Cancelled(_)) => Err(base_scheduler::Cancelled.into()),
            Some(OperationError::Io(error)) => Err(match error.raw_os_error() {
                Some(code) => io::Error::from_raw_os_error(code),
                None => io::Error::new(error.kind(), error.to_string()),
            }
            .into()),
        }
    }
    async fn wait(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        if let Some(completion) = &mut self.pending {
            let mut cancelled = pin!(cancellation.cancelled());
            let result = poll_fn(|context| {
                if let Poll::Ready(result) = completion.as_mut().poll(context) {
                    return Poll::Ready(Ok(result));
                }
                cancelled
                    .as_mut()
                    .poll(context)
                    .map(|()| Err(base_scheduler::Cancelled))
            })
            .await?;
            self.pending = None;
            self.error = result.err();
        }
        self.result()
    }

    async fn write_with(
        &mut self,
        cancellation: &Cancellation,
        mut send: impl FnMut(&socket2::Socket) -> io::Result<usize> + Send,
    ) -> OperationResult<usize> {
        let scheduler = self.scheduler;
        let mut registered = false;
        if scheduler.readiness.is_some() {
            cancellation.check()?;
            scheduler.check_open()?;
            let direction = self
                .registration
                .as_ref()
                .map(|registration| registration.direction(Interest::Writable));
            let observed = direction.map(|direction| direction.snapshot());
            match send(self.socket.get_ref()) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if let Some(direction) = direction {
                        direction.clear(observed.unwrap());
                        registered = true;
                    }
                }
                result => return result.map_err(Into::into),
            }
        }
        let operation = pin!(async {
            let Some(group) = &self.scheduler.readiness else {
                return self.socket.io(Interest::Writable, send).await;
            };
            if self.registration.is_none() {
                self.registration = Some(
                    self.socket
                        .registrations
                        .get(group, self.socket.get_ref())?,
                );
            }
            let direction = self
                .registration
                .as_ref()
                .unwrap()
                .direction(Interest::Writable);
            if registered {
                direction.wait(group).await?;
            }
            loop {
                // Retry across lazy registration, and never clear a newer edge.
                let observed = direction.snapshot();
                match send(self.socket.get_ref()) {
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        direction.clear(observed);
                        direction.wait(group).await?;
                    }
                    result => return result,
                }
            }
        });
        scheduler.cancellable(cancellation, operation).await
    }
}
impl SocketStreamWriting for ReadinessSelector {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &NativeSocket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        ReadinessSocketStreamWriter::new(self, socket)
    }
    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &NativeSocket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler> {
        ReadinessSocketStreamWriter::new(self, socket)
    }
}
impl<B: IoBuf> SocketStreamWriter for ReadinessSocketStreamWriter<'_, B> {
    type Buffer = B;
    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.pending.is_some() {
            self.wait(cancellation).await?;
        }
        self.write_with(cancellation, |socket| {
            socket_io::send_poll(socket, buffer, SocketSendFlags::default())
        })
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
        buffers: &[IoSlice<'_>],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.pending.is_some() {
            self.wait(cancellation).await?;
        }
        self.write_with(cancellation, |socket| {
            socket_io::sendmsg_poll(socket, buffers, None, flags)
        })
        .await
    }
    async fn write_owned_vectored<Buffers: base_scheduler::IoBufSequence>(
        &mut self,
        buffers: Buffers,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
        if let Err(error) = self.check_open() {
            return (Err(error), buffers);
        }
        if self.pending.is_some() {
            if let Err(error) = self.wait(cancellation).await {
                return (Err(error), buffers);
            }
        }
        let (result, message) = self
            .scheduler
            .socket_sendmsg(
                &self.socket,
                crate::SocketMessage {
                    buffers,
                    address: None,
                },
                flags,
                cancellation,
            )
            .await;
        (result, message.buffers)
    }
    async fn write_owned<Owned: base_scheduler::IoBuf>(
        &mut self,
        buffer: Owned,
        cancellation: &Cancellation,
    ) -> base_scheduler::BufferResult<Owned> {
        if let Err(error) = self.check_open() {
            return (Err(error), buffer);
        }
        if self.pending.is_some() {
            if let Err(error) = self.wait(cancellation).await {
                return (Err(error), buffer);
            }
        }
        self.scheduler
            .socket_write(&self.socket, buffer, cancellation)
            .await
    }
    async fn submit(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        if let Err(error) = self.check_open() {
            return Err((error, buffer));
        }
        if let Err(error) = self.wait(cancellation).await {
            return Err((error, buffer));
        }
        if let Err(error) = cancellation.check() {
            return Err((error.into(), buffer));
        }
        if let Err(error) = self.scheduler.check_open() {
            return Err((error.into(), buffer));
        }
        let length = buffer.as_ref().len();
        if length == 0 {
            return Ok(());
        }
        // Admission precedes the first byte: abandoning a capacity wait cannot leave a truncated logical write on the wire.
        #[cfg(test)]
        let send = self.send;
        #[cfg(not(test))]
        let send = socket_io::send_poll;
        let socket = self.socket.retain();
        let stopping = self.stopping.clone();
        let closed = self.scheduler.closed.clone();
        let token = cancellation.clone();
        let completion_token = token.clone();
        match self
            .scheduler
            .work
            .execute_submit_retiring(
                (Some(socket), Some(buffer)),
                cancellation,
                move |(socket, buffer)| {
                    let socket = socket.as_ref().unwrap();
                    let buffer = buffer.as_ref().unwrap();
                    futures_lite::future::block_on(async {
                        let sending = async {
                            let mut written = 0;
                            while written < length {
                                token
                                    .check()
                                    .map_err(|_| io::Error::from(io::ErrorKind::Interrupted))?;
                                let bytes = &buffer.as_ref()[written..];
                                let count = socket
                                    .io(crate::Interest::Writable, |socket| {
                                        send(socket, bytes, SocketSendFlags::default())
                                    })
                                    .await?;
                                if count == 0 {
                                    return Err(io::ErrorKind::WriteZero.into());
                                }
                                written += count;
                            }
                            Ok(())
                        };
                        let stopped = async {
                            futures_lite::future::or(
                                token.cancelled(),
                                futures_lite::future::or(stopping.cancelled(), closed.cancelled()),
                            )
                            .await;
                            Err(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "stream output cancelled",
                            ))
                        };
                        futures_lite::future::or(sending, stopped).await
                    })
                },
                |(socket, buffer)| {
                    drop(socket.take());
                    drop(buffer.take());
                },
            )
            .await
        {
            Ok(completion) => {
                self.pending = Some(Box::pin(async move {
                    let (result, owners) = completion.await;
                    drop(owners);
                    match result {
                        Err(error)
                            if completion_token.is_cancelled()
                                && error.kind() == io::ErrorKind::Interrupted =>
                        {
                            Err(base_scheduler::Cancelled.into())
                        }
                        result => result,
                    }
                }));
                Ok(())
            }
            Err((error, (_socket, buffer))) => Err((error, buffer.unwrap())),
        }
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        self.check_open()?;
        self.wait(cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.stopping.cancel();
        let _ = self.wait(&Cancellation::never()).await;
        Ok(())
    }
}
impl<B> Drop for ReadinessSocketStreamWriter<'_, B> {
    fn drop(&mut self) {
        self.stopping.cancel();
    }
}

#[cfg(all(test, target_os = "macos"))]
#[path = "readiness_socket_stream_writer/tests.rs"]
mod readiness_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use base_scheduler::{SocketClose, SocketDomain};
    use futures_lite::future::block_on;

    #[test]
    fn submitted_nonempty_output_reports_zero_progress() {
        let scheduler = ReadinessSelector::new().unwrap();
        let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
            &scheduler,
            SocketDomain::Ipv4,
        ));
        let mut writer = ReadinessSocketStreamWriter::<Vec<u8>>::new(&scheduler, &sender).unwrap();
        // A resource-local seam reaches the defensive syscall outcome without affecting concurrent test sockets or introducing production dispatch.
        writer.send = |_, _, _| Ok(0);
        block_on(writer.submit(vec![1], &Cancellation::never())).unwrap();
        assert_eq!(
            block_on(writer.flush(&Cancellation::never()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WriteZero
        );
        block_on(writer.close()).unwrap();
        drop(writer);
        block_on(scheduler.socket_close(sender)).unwrap();
        block_on(scheduler.socket_close(receiver)).unwrap();
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use base_scheduler::{SocketClose, SocketDomain};
    use futures_lite::future::block_on;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Buffer {
        bytes: Vec<u8>,
        calls: AtomicUsize,
        cancellation: Cancellation,
    }
    impl AsRef<[u8]> for Buffer {
        fn as_ref(&self) -> &[u8] {
            // Submission first observes the length. The worker's next access requests cancellation after its initial check, as the first send is prepared.
            if self.calls.fetch_add(1, Ordering::Relaxed) == 1 {
                self.cancellation.cancel();
            }
            &self.bytes
        }
    }
    // SAFETY: The cancellation hook does not alter or alias the stable owned byte storage.
    unsafe impl IoBuf for Buffer {}

    fn assert_cancelled_output_is_terminal(
        send: fn(&socket2::Socket, &[u8], SocketSendFlags) -> io::Result<usize>,
    ) {
        let scheduler = ReadinessSelector::new().unwrap();
        let (sender, receiver) = block_on(base_scheduler_conformance::socket::stream_pair(
            &scheduler,
            SocketDomain::Ipv4,
        ));
        let token = Cancellation::new();
        let mut writer = ReadinessSocketStreamWriter::<Buffer>::new(&scheduler, &sender).unwrap();
        writer.send = send;
        block_on(writer.submit(
            Buffer {
                bytes: vec![1; 4],
                calls: AtomicUsize::new(0),
                cancellation: token.clone(),
            },
            &token,
        ))
        .map_err(|(error, _)| error)
        .unwrap();
        let never = Cancellation::never();
        for _ in 0..2 {
            let error = block_on(writer.flush(&never)).unwrap_err();
            assert!(
                std::error::Error::source(&error)
                    .unwrap()
                    .is::<base_scheduler::Cancelled>()
            );
        }
        assert!(block_on(writer.write(b"after failure", &never)).is_err());
        block_on(writer.close()).unwrap();
        drop(writer);
        block_on(scheduler.socket_close(sender)).unwrap();
        block_on(scheduler.socket_close(receiver)).unwrap();
    }

    #[test]
    fn cancellation_between_ready_short_sends_stops_accepted_output() {
        assert_cancelled_output_is_terminal(|_, _, _| Ok(1));
    }

    #[test]
    fn cancellation_interrupts_a_pending_send() {
        assert_cancelled_output_is_terminal(|_, _, _| Err(io::ErrorKind::WouldBlock.into()));
    }
}
