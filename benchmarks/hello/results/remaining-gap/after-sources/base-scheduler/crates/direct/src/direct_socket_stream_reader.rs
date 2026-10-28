// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::{DirectScheduler, DirectSocket};
use base_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, OperationResult, SocketRecvFlags,
    SocketStreamReader,
};
use std::{
    future::{Future, poll_fn},
    io::{self, IoSliceMut},
    pin::pin,
    task::Poll,
};

/// Scheduler-bound stream reader; registration remains private to this backend.
pub(crate) struct DirectSocketStreamReader<'a> {
    scheduler: &'a DirectScheduler,
    operations: crate::operation_state::OperationState,
    socket: DirectSocket,
    pool: Option<base_scheduler::HeapBufferPool>,
    eof: bool,
    closed: bool,
}

impl<'a> DirectSocketStreamReader<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a DirectScheduler, socket: &DirectSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? != base_scheduler::SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream readers require a stream socket",
            ));
        }
        Ok(Self {
            scheduler,
            operations: Default::default(),
            socket: DirectSocket(socket.retain()),
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
}

impl SocketStreamReader for DirectSocketStreamReader<'_> {
    type Buffer = <base_scheduler::HeapBufferPool as BufferPool>::Buffer;

    async fn read(&mut self, cancellation: &Cancellation) -> OperationResult<Option<Self::Buffer>> {
        cancellation.check()?;
        if self.closed || self.eof {
            return Ok(None);
        }
        self.operations.check_idle()?;
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
        self.operations.check_idle()?;
        if self.eof || buffer.is_empty() {
            cancellation.check()?;
            return Ok(0);
        }
        let count = self
            .scheduler
            .socket_recv_poll(
                &self.socket,
                buffer,
                SocketRecvFlags::default(),
                cancellation,
            )
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
        self.read_vectored_with_flags(buffers, SocketRecvFlags::default(), cancellation)
            .await
            .map(|result| result.bytes)
    }

    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [std::io::IoSliceMut<'_>],
        flags: base_scheduler::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<base_scheduler::SocketRecvResult> {
        self.check_open()?;
        self.operations.check_idle()?;
        if self.eof || buffers.iter().all(|buffer| buffer.is_empty()) {
            cancellation.check()?;
            return Ok(base_scheduler::SocketRecvResult {
                bytes: 0,
                address: None,
                truncated: false,
                control_truncated: false,
            });
        }
        let result = self
            .scheduler
            .socket_recvmsg_poll(&self.socket, buffers, flags, cancellation)
            .await?;
        if result.bytes == 0 {
            self.eof = true;
        }
        Ok(result)
    }

    async fn read_owned_vectored<Buffers: base_scheduler::IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: base_scheduler::SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<base_scheduler::SocketRecvResult>, Buffers) {
        if let Err(error) = self.check_open() {
            return (Err(error.into()), buffers);
        }
        if self.eof || (0..buffers.buffer_count()).all(|index| buffers.buffer(index).is_empty()) {
            return (
                cancellation
                    .check()
                    .map(|()| base_scheduler::SocketRecvResult {
                        bytes: 0,
                        address: None,
                        truncated: false,
                        control_truncated: false,
                    })
                    .map_err(Into::into),
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
        if matches!(&result, Ok(result) if result.bytes == 0) {
            self.eof = true;
        }
        (result, buffers)
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
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let buffer = match self.operations.begin(buffer) {
            Ok(buffer) => buffer,
            Err((error, buffer)) => return (Err(error), buffer),
        };
        let (result, buffer) = self
            .scheduler
            .socket_read(&self.socket, buffer, cancellation)
            .await;
        let buffer = buffer.into_inner();
        if matches!(result, Ok(0)) {
            self.eof = true;
        }
        (result, buffer)
    }

    async fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.operations.close().await;
        Ok(())
    }
}
