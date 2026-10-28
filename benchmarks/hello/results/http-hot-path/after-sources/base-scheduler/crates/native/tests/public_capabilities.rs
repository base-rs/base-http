// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// These factories depend only on the public resource capabilities.
use base_scheduler::*;
use base_scheduler_native::{NativeExecutorOptions, NativeScheduler};
use base_scheduler_selector::Selector;
use futures_lite::future::block_on;
use std::{
    io::{self, IoSlice, IoSliceMut},
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

type Buffer = <HeapBufferPool as BufferPool>::Buffer;

struct Capabilities<'state>(&'state AtomicUsize);
impl Selector for Capabilities<'_> {
    fn close(&self) {}
    async fn shutdown(&self) {}
}
impl SocketOperation for Capabilities<'_> {
    type Socket = u8;
}
impl FileOperation for Capabilities<'_> {
    type File = u8;
}
impl BufferPooling for Capabilities<'_> {
    type Pool = HeapBufferPool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<Self::Pool> {
        HeapBufferPool::with_options(options)
    }
}

struct Reader<'scheduler> {
    calls: &'scheduler AtomicUsize,
    resource: u8,
    pool: Option<HeapBufferPool>,
    closed: bool,
}
impl Reader<'_> {
    fn check(&self, cancellation: &Cancellation) -> OperationResult<()> {
        cancellation.check()?;
        if self.closed {
            Err(io::Error::from(io::ErrorKind::BrokenPipe).into())
        } else {
            Ok(())
        }
    }
    fn fill(&self, bytes: &mut [u8], cancellation: &Cancellation) -> OperationResult<usize> {
        self.check(cancellation)?;
        bytes.fill(self.resource);
        Ok(bytes.len())
    }
    fn finish(&mut self) -> io::Result<()> {
        self.closed = true;
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
impl SocketStreamReader for Reader<'_> {
    type Buffer = Buffer;
    async fn read(&mut self, cancellation: &Cancellation) -> OperationResult<Option<Buffer>> {
        cancellation.check()?;
        if self.closed {
            return Ok(None);
        }
        let pool = self
            .pool
            .get_or_insert_with(|| HeapBufferPool::new(1, 1).unwrap());
        let mut buffer = pool.acquire(cancellation).await?;
        buffer.as_mut().fill(self.resource);
        Ok(Some(buffer))
    }
    async fn read_into(
        &mut self,
        bytes: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.fill(bytes, cancellation)
    }
    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.check(cancellation)?;
        let mut count = 0;
        for buffer in buffers {
            buffer.fill(self.resource);
            count += buffer.len();
        }
        Ok(count)
    }
    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        SocketStreamReader::read_vectored(self, buffers, cancellation)
            .await
            .map(metadata)
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        mut buffers: B,
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        let result = self.check(cancellation).map(|()| {
            let mut count = 0;
            for i in 0..buffers.buffer_count() {
                let bytes = buffers.buffer_mut(i);
                bytes.fill(self.resource);
                count += bytes.len();
            }
            metadata(count)
        });
        (result, buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.finish()
    }
}
impl FileReader for Reader<'_> {
    async fn read<B: IoBufMut>(
        &mut self,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = self.fill(buffer.as_mut(), cancellation);
        (result, buffer)
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        _offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        FileReader::read(self, buffer, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.finish()
    }
}
fn metadata(bytes: usize) -> SocketRecvResult {
    SocketRecvResult {
        bytes,
        address: None,
        truncated: false,
        control_truncated: false,
    }
}
impl SocketPacketReader for Reader<'_> {
    type Message = OwnedSocketRecvMessage<Buffer>;
    async fn read(
        &mut self,
        cancellation: &Cancellation,
    ) -> OperationResult<Option<Self::Message>> {
        Ok(SocketStreamReader::read(self, cancellation)
            .await?
            .map(|buffer| OwnedSocketRecvMessage::new(metadata(buffer.as_ref().len()), buffer)))
    }
    async fn read_into(
        &mut self,
        bytes: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.fill(bytes, cancellation).map(metadata)
    }
    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        SocketStreamReader::read_vectored(self, buffers, cancellation)
            .await
            .map(metadata)
    }
    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        SocketStreamReader::read_vectored(self, buffers, cancellation)
            .await
            .map(metadata)
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        mut buffers: B,
        _: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        let result = self.check(cancellation).map(|()| {
            let mut count = 0;
            for i in 0..buffers.buffer_count() {
                let bytes = buffers.buffer_mut(i);
                bytes.fill(self.resource);
                count += bytes.len();
            }
            metadata(count)
        });
        (result, buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.finish()
    }
}

struct Writer<'scheduler, B> {
    calls: &'scheduler AtomicUsize,
    resource: u8,
    buffer: PhantomData<fn(B)>,
}
impl<B> Writer<'_, B> {
    fn count(&self, length: usize, cancellation: &Cancellation) -> OperationResult<usize> {
        cancellation.check()?;
        self.calls
            .fetch_add(usize::from(self.resource), Ordering::Relaxed);
        Ok(length)
    }
    fn finish(&self) -> io::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
impl<B: IoBuf> SocketStreamWriter for Writer<'_, B> {
    type Buffer = B;
    async fn write(&mut self, bytes: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        self.count(bytes.len(), cancellation)
    }
    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.count(
            buffers.iter().map(|buffer| buffer.len()).sum(),
            cancellation,
        )
    }
    async fn submit(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, B)> {
        match self.count(buffer.as_ref().len(), cancellation) {
            Ok(_) => Ok(()),
            Err(error) => Err((error, buffer)),
        }
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        cancellation.check().map_err(Into::into)
    }
    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        SocketStreamWriter::write_vectored(self, buffers, cancellation).await
    }
    async fn write_owned_vectored<T: IoBufSequence>(
        &mut self,
        buffers: T,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, T) {
        let result = self.count(
            (0..buffers.buffer_count())
                .map(|i| buffers.buffer(i).len())
                .sum(),
            cancellation,
        );
        (result, buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.finish()
    }
}
impl<B: Send> FileWriter for Writer<'_, B> {
    async fn write<T: IoBuf>(&mut self, buffer: T, cancellation: &Cancellation) -> BufferResult<T> {
        (self.count(buffer.as_ref().len(), cancellation), buffer)
    }
    async fn write_at<T: IoBuf>(
        &mut self,
        buffer: T,
        _offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<T> {
        FileWriter::write(self, buffer, cancellation).await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.finish()
    }
}
impl<B: Send> SocketPacketWriter for Writer<'_, B> {
    async fn write(
        &mut self,
        bytes: &[u8],
        _address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.count(bytes.len(), cancellation)
    }
    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        _address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.count(
            buffers.iter().map(|buffer| buffer.len()).sum(),
            cancellation,
        )
    }
    async fn write_owned<T: IoBuf>(
        &mut self,
        buffer: T,
        _address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> BufferResult<T> {
        (self.count(buffer.as_ref().len(), cancellation), buffer)
    }
    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        SocketPacketWriter::write_vectored(self, buffers, address, cancellation).await
    }
    async fn write_owned_vectored<T: IoBufSequence>(
        &mut self,
        buffers: T,
        _: Option<SocketAddress>,
        _: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, T) {
        let result = self.count(
            (0..buffers.buffer_count())
                .map(|i| buffers.buffer(i).len())
                .sum(),
            cancellation,
        );
        (result, buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        self.finish()
    }
}
impl Capabilities<'_> {
    fn reader(&self, resource: u8, pool: Option<HeapBufferPool>) -> Reader<'_> {
        Reader {
            calls: self.0,
            resource,
            pool,
            closed: false,
        }
    }
    fn writer<B>(&self, resource: u8) -> Writer<'_, B> {
        Writer {
            calls: self.0,
            resource,
            buffer: PhantomData,
        }
    }
}
impl FileReading for Capabilities<'_> {
    type Reader<'a>
        = Reader<'a>
    where
        Self: 'a;
    fn file_reader(&self, file: &u8) -> io::Result<Self::Reader<'_>> {
        Ok(self.reader(*file, None))
    }
}
impl FileWriting for Capabilities<'_> {
    type Writer<'a>
        = Writer<'a, Vec<u8>>
    where
        Self: 'a;
    fn file_writer(&self, file: &u8) -> io::Result<Self::Writer<'_>> {
        Ok(self.writer(*file))
    }
}
impl SocketStreamReading for Capabilities<'_> {
    fn socket_stream_reader<'a>(
        &'a self,
        socket: &u8,
    ) -> io::Result<impl SocketStreamReader<Buffer = Buffer> + 'a> {
        Ok(self.reader(*socket, None))
    }
    fn socket_stream_reader_with_pool<'a>(
        &'a self,
        socket: &u8,
        pool: HeapBufferPool,
    ) -> io::Result<impl SocketStreamReader<Buffer = Buffer> + 'a> {
        Ok(self.reader(*socket, Some(pool)))
    }
}
impl SocketPacketReading for Capabilities<'_> {
    fn socket_packet_reader<'a>(
        &'a self,
        socket: &u8,
    ) -> io::Result<impl SocketPacketReader<Message: SocketRecvMessageMut<Payload = Buffer>> + 'a>
    {
        Ok(self.reader(*socket, None))
    }
    fn socket_packet_reader_with_pool<'a>(
        &'a self,
        socket: &u8,
        pool: HeapBufferPool,
    ) -> io::Result<impl SocketPacketReader<Message: SocketRecvMessageMut<Payload = Buffer>> + 'a>
    {
        Ok(self.reader(*socket, Some(pool)))
    }
}
impl SocketStreamWriting for Capabilities<'_> {
    fn socket_stream_writer<'a>(
        &'a self,
        socket: &u8,
    ) -> io::Result<impl SocketStreamWriter<Buffer = Buffer> + 'a> {
        Ok(self.writer(*socket))
    }
    fn socket_stream_writer_with_buffer<'a, B: IoBuf>(
        &'a self,
        socket: &u8,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'a> {
        Ok(self.writer(*socket))
    }
}
impl SocketPacketWriting for Capabilities<'_> {
    fn socket_packet_writer<'a>(&'a self, socket: &u8) -> io::Result<impl SocketPacketWriter + 'a> {
        Ok(self.writer::<Vec<u8>>(*socket))
    }
}

#[test]
fn native_preserves_all_public_factories_without_backend_capabilities() {
    // The selector itself borrows local state. Neither it nor the factory resource reference needs to be 'static.
    let calls = AtomicUsize::new(0);
    let scheduler =
        NativeScheduler::with_selector(Capabilities(&calls), NativeExecutorOptions::default())
            .unwrap();
    block_on(async {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut reader = {
            let file = 7;
            scheduler.file_reader(&file).unwrap()
        };
        for token in [&never, &cancelled] {
            assert_eq!(
                FileReader::read(&mut reader, vec![0; 2], token)
                    .await
                    .0
                    .is_ok(),
                !token.is_cancelled()
            );
            assert_eq!(
                reader.read_at(vec![0; 2], 8, token).await.0.is_ok(),
                !token.is_cancelled()
            );
        }
        assert_eq!(
            FileReader::read(&mut reader, vec![0; 2], &never).await.1,
            [7, 7]
        );
        FileReader::close(&mut reader).await.unwrap();
        assert!(
            FileReader::read(&mut reader, vec![0; 2], &never)
                .await
                .0
                .is_err()
        );
        let mut writer = {
            let file = 5;
            scheduler.file_writer(&file).unwrap()
        };
        assert_eq!(
            FileWriter::write(&mut writer, vec![1, 2], &never)
                .await
                .0
                .unwrap(),
            2
        );
        assert_eq!(writer.write_at(vec![1], 0, &never).await.0.unwrap(), 1);
        FileWriter::close(&mut writer).await.unwrap();
        let mut input = {
            let socket = 3;
            scheduler.socket_stream_reader(&socket).unwrap()
        };
        for token in [&never, &cancelled] {
            assert_eq!(input.read(token).await.is_ok(), !token.is_cancelled());
            assert_eq!(
                input.read_into(&mut [0; 2], token).await.is_ok(),
                !token.is_cancelled()
            );
            assert_eq!(
                input
                    .read_vectored(&mut [IoSliceMut::new(&mut [0; 2])], token)
                    .await
                    .is_ok(),
                !token.is_cancelled()
            );
        }
        assert_eq!(input.read_into(&mut [0; 2], &never).await.unwrap(), 2);
        input.close().await.unwrap();
        assert!(input.read(&never).await.unwrap().is_none());
        assert!(input.read_into(&mut [0], &never).await.is_err());
        let pool = scheduler.buffer_pool(BufferPoolOptions::new(1, 2)).unwrap();
        let mut pooled = scheduler
            .socket_stream_reader_with_pool(&4, pool.clone())
            .unwrap();
        let chunk = pooled.read(&never).await.unwrap().unwrap();
        assert_eq!(chunk.as_ref(), [4, 4]);
        drop(chunk);
        pooled.close().await.unwrap();
        let mut packets = {
            let socket = 9;
            scheduler.socket_packet_reader(&socket).unwrap()
        };
        for token in [&never, &cancelled] {
            assert_eq!(packets.read(token).await.is_ok(), !token.is_cancelled());
            assert_eq!(
                packets.read_into(&mut [0; 2], token).await.is_ok(),
                !token.is_cancelled()
            );
            assert_eq!(
                packets
                    .read_vectored(&mut [IoSliceMut::new(&mut [0; 2])], token)
                    .await
                    .is_ok(),
                !token.is_cancelled()
            );
        }
        packets.close().await.unwrap();
        assert!(packets.read(&never).await.unwrap().is_none());
        let mut packets = scheduler
            .socket_packet_reader_with_pool(&8, pool.clone())
            .unwrap();
        assert_eq!(
            packets.read(&never).await.unwrap().unwrap().payload(),
            [8, 8]
        );
        packets.close().await.unwrap();
        let mut output = {
            let socket = 2;
            scheduler.socket_stream_writer(&socket).unwrap()
        };
        let buffer = pool.acquire(&never).await.unwrap();
        output.submit(buffer, &never).await.unwrap();
        let mut typed = scheduler
            .socket_stream_writer_with_buffer::<Vec<u8>>(&2)
            .unwrap();
        for token in [&never, &cancelled] {
            assert_eq!(
                typed.write(b"a", token).await.is_ok(),
                !token.is_cancelled()
            );
            assert_eq!(
                typed
                    .write_vectored(&[IoSlice::new(b"b")], token)
                    .await
                    .is_ok(),
                !token.is_cancelled()
            );
            assert_eq!(
                typed.submit(vec![1], token).await.is_ok(),
                !token.is_cancelled()
            );
            assert_eq!(typed.flush(token).await.is_ok(), !token.is_cancelled());
        }
        output.flush(&never).await.unwrap();
        output.close().await.unwrap();
        typed.close().await.unwrap();
        let mut output = scheduler.socket_packet_writer(&6).unwrap();
        assert_eq!(output.write(b"a", None, &never).await.unwrap(), 1);
        assert_eq!(
            output
                .write_vectored(&[IoSlice::new(b"b")], None, &never)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            output.write_owned(vec![1], None, &never).await.0.unwrap(),
            1
        );
        output.close().await.unwrap();
        scheduler.shutdown().await;
    });
    assert!(calls.load(Ordering::Relaxed) > 0);
}
