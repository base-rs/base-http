// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::{NativeSocket, ReadinessSelector};
use crate::{
    readiness_driver::{ReadinessWait, Registration},
    socket_io,
};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, OperationResult, SocketRecvFlags,
    SocketStreamReader,
};
use std::{
    future::{Future, poll_fn},
    io::{self, IoSliceMut},
    pin::pin,
    sync::Arc,
    task::Poll,
};

/// Scheduler-bound stream reader; registration remains private to this backend.
pub(crate) struct ReadinessSocketStreamReader<'a> {
    scheduler: &'a ReadinessSelector,
    socket: NativeSocket,
    registration: Option<Arc<Registration>>,
    established: bool,
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
            registration: None,
            established: false,
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
    async fn read_with(
        &mut self,
        capacity: usize,
        cancellation: &Cancellation,
        mut receive: impl FnMut(&socket2::Socket) -> io::Result<usize> + Send,
    ) -> OperationResult<usize> {
        let result = {
            let scheduler = self.scheduler;
            if let Some(group) = &scheduler.readiness {
                let mut probe = !self.established;
                if self.registration.is_none() {
                    cancellation.check()?;
                    scheduler.check_open()?;
                    match receive(self.socket.get_ref()) {
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => (),
                        result => {
                            if matches!(result, Ok(count) if count != 0) {
                                self.established = true;
                            }
                            return result.map_err(Into::into);
                        }
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
                let mut waiting = pin!(None::<ReadinessWait<'_>>);
                let operation = pin!(poll_fn(|cx| loop {
                    // Finish the same wait before consulting readiness again.
                    // Its closure check wins even if a newer edge is also ready.
                    if let Some(wait) = waiting.as_mut().as_pin_mut() {
                        match wait.poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(result) => {
                                waiting.set(None);
                                if let Err(error) = result {
                                    return Poll::Ready(Err(error));
                                }
                                continue;
                            }
                        }
                    }
                    let observed = direction.snapshot();
                    if !probe && observed & 1 == 0 {
                        waiting.set(Some(direction.wait(group)));
                        continue;
                    }
                    probe = false;
                    match receive(self.socket.get_ref()) {
                        Ok(count) => {
                            direction.clear_after_read(observed, count, capacity);
                            return Poll::Ready(Ok(count));
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            direction.clear(observed);
                        }
                        Err(error) => return Poll::Ready(Err(error)),
                    }
                }));
                scheduler.cancellable(cancellation, operation).await
            } else {
                let operation = pin!(self.socket.io(Interest::Readable, receive));
                scheduler.cancellable(cancellation, operation).await
            }
        };
        if matches!(result, Ok(count) if count != 0) {
            self.established = true;
        }
        result
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

#[cfg(all(test, target_os = "macos"))]
mod tests;
