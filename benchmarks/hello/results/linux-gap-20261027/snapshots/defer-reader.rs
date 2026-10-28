// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
#[cfg(unix)]
use crate::readiness_driver::Registration;
use crate::socket_io;
use crate::{NativeSocket, ReadinessSelector};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, OperationResult, SocketRecvFlags,
    SocketStreamReader,
};
#[cfg(unix)]
use std::sync::Arc;
use std::{
    future::{Future, poll_fn},
    io::{self, IoSliceMut},
    pin::pin,
    task::Poll,
};

/// Scheduler-bound stream reader; registration remains private to this backend.
pub(crate) struct ReadinessSocketStreamReader<'a> {
    scheduler: &'a ReadinessSelector,
    socket: NativeSocket,
    #[cfg(unix)]
    registration: Option<Arc<Registration>>,
    #[cfg(unix)]
    established: bool,
    defer: bool,
    pool: Option<base_scheduler::HeapBufferPool>,
    eof: bool,
    closed: bool,
}

impl<'a> ReadinessSocketStreamReader<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a ReadinessSelector, socket: &NativeSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? != base_scheduler::SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream readers require a stream socket",
            ));
        }
        Ok(Self {
            scheduler,
            socket: socket.retain(),
            #[cfg(unix)]
            registration: None,
            #[cfg(unix)]
            established: false,
            defer: false,
            pool: None,
            eof: false,
            closed: false,
        })
    }

    /// Select shared storage for subsequent owned reads. Already returned chunks retain their original pool. Borrowed reads require no pool; otherwise the first owned read creates two reusable 64 KiB slots.
    pub(crate) fn with_pool(mut self, pool: base_scheduler::HeapBufferPool) -> Self {
        self.pool = Some(pool);
        self
    }

    fn check_open(&self) -> io::Result<()> {
        if self.closed {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream reader is closed",
            ))
        } else {
            Ok(())
        }
    }

    // Scalar consuming stream reads may use readiness before their syscall.
    // Message receives retain their separate syscall-first contract.
    #[cfg(unix)]
    async fn read_with(
        &mut self,
        capacity: usize,
        cancellation: &Cancellation,
        mut receive: impl FnMut(&socket2::Socket) -> io::Result<usize> + Send,
    ) -> OperationResult<usize> {
        let result = {
            let scheduler = self.scheduler;
            let operation = pin!(async {
                let Some(group) = &self.scheduler.readiness else {
                    return self.socket.io(Interest::Readable, receive).await;
                };
                if std::mem::take(&mut self.defer) {
                    futures_lite::future::yield_now().await;
                }
                let mut probe = !self.established;
                if self.registration.is_none() {
                    // Preserve immediate errors and retry across registration.
                    let result = receive(self.socket.get_ref());
                    if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
                        return result;
                    }
                    self.registration = Some(
                        self.socket
                            .registrations
                            .get(group, self.socket.get_ref())?,
                    );
                    probe = true;
                }
                let direction = self
                    .registration
                    .as_ref()
                    .expect("a waiting stream retains its registration")
                    .direction(Interest::Readable);
                loop {
                    let observed = direction.snapshot();
                    if !probe && observed & 1 == 0 {
                        direction.wait(group).await?;
                        continue;
                    }
                    probe = false;
                    match receive(self.socket.get_ref()) {
                        Ok(count) => {
                            // A short read must consume the event's reported
                            // bytes before clearing. An urgent-data boundary can
                            // leave bytes queued even after a short receive.
                            direction.clear_after_read(observed, count, capacity);
                            return Ok(count);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            direction.clear(observed);
                        }
                        Err(error) => return Err(error),
                    }
                }
            });
            scheduler.cancellable(cancellation, operation).await
        };
        self.defer = matches!(result, Ok(count) if count != 0 && count < capacity);
        if matches!(result, Ok(count) if count != 0) {
            self.established = true;
        }
        result
    }
    #[cfg(not(unix))]
    async fn read_with(
        &mut self,
        _capacity: usize,
        cancellation: &Cancellation,
        receive: impl FnMut(&socket2::Socket) -> io::Result<usize> + Send,
    ) -> OperationResult<usize> {
        self.scheduler
            .cancellable(cancellation, self.socket.io(Interest::Readable, receive))
            .await
    }
}

impl SocketStreamReader for ReadinessSocketStreamReader<'_> {
    type Buffer = <base_scheduler::HeapBufferPool as BufferPool>::Buffer;

    async fn read(&mut self, cancellation: &Cancellation) -> OperationResult<Option<Self::Buffer>> {
        cancellation.check()?;
        if self.closed || self.eof {
            return Ok(None);
        }
        if self.pool.is_none() {
            self.pool = Some(
                self.scheduler
                    .buffer_pool(BufferPoolOptions::new(2, 64 * 1024))?,
            );
        }
        let pool = self
            .pool
            .as_ref()
            .expect("owned reads initialize their pool");
        loop {
            let mut buffer = pool.acquire(cancellation).await?;
            let result = {
                let mut reading = pin!(self.scheduler.socket_recv_poll(
                    &self.socket,
                    buffer.as_mut(),
                    SocketRecvFlags::default(),
                    cancellation
                ));
                poll_fn(|context| Poll::Ready(reading.as_mut().poll(context))).await
            };
            match result {
                Poll::Ready(result) => {
                    let count = result?;
                    if count == 0 {
                        self.eof = true;
                        return Ok(None);
                    }
                    buffer.truncate(count);
                    return Ok(Some(buffer));
                }
                Poll::Pending => drop(buffer),
            }
            self.scheduler
                .socket_wait(&self.socket, Interest::Readable, cancellation)
                .await?;
        }
    }

    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.eof || buffer.is_empty() {
            cancellation.check()?;
            return Ok(0);
        }
        let count = self
            .read_with(buffer.len(), cancellation, |socket| {
                socket_io::recv_poll(socket, buffer, SocketRecvFlags::default())
            })
            .await?;
        if count == 0 {
            self.eof = true;
        }
        Ok(count)
    }

    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check_open()?;
        if self.eof || buffers.iter().all(|buffer| buffer.is_empty()) {
            cancellation.check()?;
            return Ok(0);
        }
        let result = self
            .scheduler
            .socket_recvmsg_poll(
                &self.socket,
                buffers,
                SocketRecvFlags::default(),
                cancellation,
            )
            .await?;
        if result.bytes == 0 {
            self.eof = true;
        }
        Ok(result.bytes)
    }

    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<base_scheduler::SocketRecvResult> {
        self.check_open()?;
        if self.eof || buffers.iter().all(|buffer| buffer.is_empty()) {
            cancellation.check()?;
            return Ok(base_scheduler::SocketRecvResult {
                bytes: 0,
                address: None,
                truncated: false,
                control_truncated: false,
            });
        }
        self.scheduler
            .socket_recvmsg_poll(&self.socket, buffers, flags, cancellation)
            .await
    }

    async fn read_owned_vectored<Buffers: base_scheduler::IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<base_scheduler::SocketRecvResult>, Buffers) {
        if let Err(error) = self.check_open() {
            return (Err(error.into()), buffers);
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffers);
        }
        if self.eof || (0..buffers.buffer_count()).all(|index| buffers.buffer(index).is_empty()) {
            return (
                Ok(base_scheduler::SocketRecvResult {
                    bytes: 0,
                    address: None,
                    truncated: false,
                    control_truncated: false,
                }),
                buffers,
            );
        }
        self.scheduler
            .socket_recvmsg(&self.socket, buffers, flags, cancellation)
            .await
    }

    async fn read_owned<B: base_scheduler::IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> base_scheduler::BufferResult<B> {
        if let Err(error) = self.check_open() {
            return (Err(error.into()), buffer);
        }
        if self.eof || buffer.as_ref().is_empty() {
            return (cancellation.check().map(|()| 0).map_err(Into::into), buffer);
        }
        let (result, buffer) = self
            .scheduler
            .socket_read(&self.socket, buffer, cancellation)
            .await;
        if matches!(result, Ok(0)) {
            self.eof = true;
        }
        (result, buffer)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
