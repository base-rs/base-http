// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Controlled public resources used to challenge the conformance assertions.
use base_scheduler::*;
use std::io::{self, IoSlice, IoSliceMut};

pub trait SocketModel: SocketOperation<Socket: Clone + 'static> + Sync {
    fn recv(
        &self,
        socket: &Self::Socket,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize>;
    fn send(
        &self,
        socket: &Self::Socket,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize>;
}

pub struct Reader<'a, M: SocketModel> {
    scheduler: &'a M,
    socket: M::Socket,
}
pub struct Writer<'a, M: SocketModel, B: IoBuf = Vec<u8>> {
    scheduler: &'a M,
    socket: M::Socket,
    owner: std::marker::PhantomData<B>,
}
impl<'a, M: SocketModel> Reader<'a, M> {
    pub fn new(scheduler: &'a M, socket: &M::Socket) -> Self {
        Self {
            scheduler,
            socket: socket.clone(),
        }
    }
    async fn vectors(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        let mut storage = vec![0; buffers.iter().map(|buffer| buffer.len()).sum()];
        let result = self
            .scheduler
            .recv(&self.socket, &mut storage, cancellation);
        let mut offset = 0;
        for buffer in buffers {
            let end = offset + buffer.len();
            buffer.copy_from_slice(&storage[offset..end]);
            offset = end;
        }
        result.map(|bytes| SocketRecvResult {
            bytes,
            address: None,
            truncated: false,
            control_truncated: false,
        })
    }
    async fn owned<B: IoBufSequenceMut>(
        &mut self,
        mut buffers: B,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        let mut storage = Vec::new();
        for index in 0..buffers.buffer_count() {
            storage.extend_from_slice(buffers.buffer(index));
        }
        let result = self
            .scheduler
            .recv(&self.socket, &mut storage, cancellation);
        let mut offset = 0;
        for index in 0..buffers.buffer_count() {
            let buffer = buffers.buffer_mut(index);
            let end = offset + buffer.len();
            buffer.copy_from_slice(&storage[offset..end]);
            offset = end;
        }
        (
            result.map(|bytes| SocketRecvResult {
                bytes,
                address: None,
                truncated: false,
                control_truncated: false,
            }),
            buffers,
        )
    }
}
impl<'a, M: SocketModel, B: IoBuf> Writer<'a, M, B> {
    pub fn new(scheduler: &'a M, socket: &M::Socket) -> Self {
        Self {
            scheduler,
            socket: socket.clone(),
            owner: std::marker::PhantomData,
        }
    }
    async fn vectors(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        let bytes: Vec<u8> = buffers
            .iter()
            .flat_map(|buffer| buffer.iter().copied())
            .collect();
        self.scheduler.send(&self.socket, &bytes, cancellation)
    }
    async fn owned<Buffers: IoBufSequence>(
        &mut self,
        buffers: Buffers,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
        let bytes: Vec<u8> = (0..buffers.buffer_count())
            .flat_map(|index| buffers.buffer(index).iter().copied())
            .collect();
        (
            self.scheduler.send(&self.socket, &bytes, cancellation),
            buffers,
        )
    }
}
impl<M: SocketModel> SocketStreamReader for Reader<'_, M> {
    type Buffer = BufferLease;
    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<BufferLease>> {
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.scheduler.recv(&self.socket, buffer, cancellation)
    }
    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.vectors(buffers, cancellation)
            .await
            .map(|result| result.bytes)
    }
    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.vectors(buffers, cancellation).await
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        buffers: B,
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        self.owned(buffers, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<M: SocketModel, B: IoBuf> SocketStreamWriter for Writer<'_, M, B> {
    type Buffer = B;
    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.scheduler.send(&self.socket, buffer, cancellation)
    }
    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.vectors(buffers, cancellation).await
    }
    async fn submit(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        self.scheduler
            .send(&self.socket, buffer.as_ref(), cancellation)
            .map(|_| ())
            .map_err(|error| (error, buffer))
    }
    async fn flush(&mut self, _: &Cancellation) -> OperationResult<()> {
        Ok(())
    }
    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.vectors(buffers, cancellation).await
    }
    async fn write_owned_vectored<Buffers: IoBufSequence>(
        &mut self,
        buffers: Buffers,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
        self.owned(buffers, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<M: SocketModel> SocketPacketReader for Reader<'_, M> {
    type Message = OwnedSocketRecvMessage<BufferLease>;
    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<Self::Message>> {
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.scheduler
            .recv(&self.socket, buffer, cancellation)
            .map(|bytes| SocketRecvResult {
                bytes,
                address: None,
                truncated: false,
                control_truncated: false,
            })
    }
    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.vectors(buffers, cancellation).await
    }
    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.vectors(buffers, cancellation).await
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        buffers: B,
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        self.owned(buffers, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<M: SocketModel, B: IoBuf> SocketPacketWriter for Writer<'_, M, B> {
    async fn write(
        &mut self,
        buffer: &[u8],
        _: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.scheduler.send(&self.socket, buffer, cancellation)
    }
    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        _: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.vectors(buffers, cancellation).await
    }
    async fn write_owned<Buffer: IoBuf>(
        &mut self,
        buffer: Buffer,
        _: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> BufferResult<Buffer> {
        (
            self.scheduler
                .send(&self.socket, buffer.as_ref(), cancellation),
            buffer,
        )
    }
    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        _: Option<SocketAddress>,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.vectors(buffers, cancellation).await
    }
    async fn write_owned_vectored<Buffers: IoBufSequence>(
        &mut self,
        buffers: Buffers,
        _: Option<SocketAddress>,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, Buffers) {
        self.owned(buffers, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

macro_rules! socket_factories {
    ($model:ty) => {
        impl BufferPooling for $model {
            type Pool = HeapBufferPool;
            fn buffer_pool(&self, options: BufferPoolOptions) -> std::io::Result<Self::Pool> {
                HeapBufferPool::with_options(options)
            }
        }
        impl SocketStreamReading for $model {
            fn socket_stream_reader<'a>(
                &'a self,
                socket: &Self::Socket,
            ) -> std::io::Result<impl SocketStreamReader<Buffer = BufferLease> + 'a> {
                Ok(socket_model::Reader::new(self, socket))
            }
            fn socket_stream_reader_with_pool<'a>(
                &'a self,
                socket: &Self::Socket,
                _: Self::Pool,
            ) -> std::io::Result<impl SocketStreamReader<Buffer = BufferLease> + 'a> {
                Ok(socket_model::Reader::new(self, socket))
            }
        }
        impl SocketStreamWriting for $model {
            fn socket_stream_writer<'a>(
                &'a self,
                socket: &Self::Socket,
            ) -> std::io::Result<impl SocketStreamWriter<Buffer = BufferLease> + 'a> {
                Ok(socket_model::Writer::new(self, socket))
            }
            fn socket_stream_writer_with_buffer<'a, B: IoBuf>(
                &'a self,
                socket: &Self::Socket,
            ) -> std::io::Result<impl SocketStreamWriter<Buffer = B> + 'a> {
                Ok(socket_model::Writer::new(self, socket))
            }
        }
        impl SocketPacketReading for $model {
            fn socket_packet_reader<'a>(
                &'a self,
                socket: &Self::Socket,
            ) -> std::io::Result<
                impl SocketPacketReader<Message: SocketRecvMessageMut<Payload = BufferLease>> + 'a,
            > {
                Ok(socket_model::Reader::new(self, socket))
            }
            fn socket_packet_reader_with_pool<'a>(
                &'a self,
                socket: &Self::Socket,
                _: Self::Pool,
            ) -> std::io::Result<
                impl SocketPacketReader<Message: SocketRecvMessageMut<Payload = BufferLease>> + 'a,
            > {
                Ok(socket_model::Reader::new(self, socket))
            }
        }
        impl SocketPacketWriting for $model {
            fn socket_packet_writer<'a>(
                &'a self,
                socket: &Self::Socket,
            ) -> std::io::Result<impl SocketPacketWriter + 'a> {
                Ok(socket_model::Writer::<Self>::new(self, socket))
            }
        }
    };
}
pub(crate) use socket_factories;
