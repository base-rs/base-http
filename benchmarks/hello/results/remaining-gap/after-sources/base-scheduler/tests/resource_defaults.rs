// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::*;
use futures_lite::future::block_on;
use std::io::{self, IoSlice, IoSliceMut};

struct File {
    bytes: Vec<u8>,
    position: usize,
    limit: usize,
    fail: bool,
}
impl File {
    fn new() -> Self {
        Self {
            bytes: b"abcdef".to_vec(),
            position: 0,
            limit: 2,
            fail: false,
        }
    }
    fn transfer<B: IoBufMut>(
        &mut self,
        mut buffer: B,
        position: usize,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = cancellation.check().map_err(Into::into).map(|()| {
            let count = buffer
                .as_mut()
                .len()
                .min(self.bytes.len().saturating_sub(position));
            buffer.as_mut()[..count].copy_from_slice(&self.bytes[position..position + count]);
            count
        });
        (result, buffer)
    }
    fn send<B: IoBuf>(
        &mut self,
        buffer: B,
        position: usize,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = cancellation
            .check()
            .map_err(OperationError::from)
            .and_then(|()| {
                if self.fail {
                    return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
                }
                let count = buffer.as_ref().len().min(self.limit);
                self.bytes.resize(self.bytes.len().max(position + count), 0);
                self.bytes[position..position + count].copy_from_slice(&buffer.as_ref()[..count]);
                Ok(count)
            });
        (result, buffer)
    }
}
impl FileReader for File {
    async fn read<B: IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = self.transfer(buffer, self.position, cancellation);
        if let Ok(count) = result.0 {
            self.position += count;
        }
        result
    }
    async fn read_at<B: IoBufMut>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.transfer(buffer, offset as usize, cancellation)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl FileWriter for File {
    async fn write<B: IoBuf>(&mut self, buffer: B, cancellation: &Cancellation) -> BufferResult<B> {
        let result = self.send(buffer, self.position, cancellation);
        if let Ok(count) = result.0 {
            self.position += count;
        }
        result
    }
    async fn write_at<B: IoBuf>(
        &mut self,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.send(buffer, offset as usize, cancellation)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn file_defaults_return_original_owners_and_handle_short_writes() {
    block_on(async {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut file = File::new();
        let owner = vec![0; 2];
        let pointer = owner.as_ptr();
        let completion = file.read_submit(owner, &never).await.unwrap();
        let (result, owner) = completion.await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(owner, b"ab");
        assert_eq!(owner.as_ptr(), pointer);
        let (result, owner) = file.read_at_submit(owner, 3, &never).await.unwrap().await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(owner, b"de");
        let (result, owner) = file.write_submit(owner, &never).await.unwrap().await;
        assert_eq!(result.unwrap(), 2);
        let (result, owner) = file.write_at_submit(owner, 0, &never).await.unwrap().await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(owner.as_ptr(), pointer);
        assert_eq!(file.bytes, b"dedeef");
        for result in [
            file.read_submit(owner.clone(), &cancelled)
                .await
                .err()
                .unwrap(),
            file.read_at_submit(owner.clone(), 0, &cancelled)
                .await
                .err()
                .unwrap(),
            file.write_submit(owner.clone(), &cancelled)
                .await
                .err()
                .unwrap(),
            file.write_at_submit(owner, 0, &cancelled)
                .await
                .err()
                .unwrap(),
        ] {
            assert!(matches!(result.0, OperationError::Cancelled(_)));
            assert_eq!(result.1, b"de");
        }
        file.position = 0;
        let owner = b"012345".to_vec();
        let pointer = owner.as_ptr();
        let (result, owner) = file.write_all(owner, &never).await;
        assert_eq!(result.unwrap(), 6);
        assert_eq!(file.bytes, b"012345");
        assert_eq!(owner.as_ptr(), pointer);
        assert_eq!(file.write_all(Vec::new(), &never).await.0.unwrap(), 0);
        file.limit = 0;
        assert_eq!(
            file.write_all(vec![1], &never).await.0.unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        file.fail = true;
        assert_eq!(
            file.write_all(vec![1], &never).await.0.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(matches!(
            file.write_all(vec![1], &cancelled).await.0,
            Err(OperationError::Cancelled(_))
        ));
    });
}

struct Stream {
    bytes: &'static [u8],
    peek: bool,
}
impl SocketStreamReader for Stream {
    type Buffer = BufferLease;
    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<BufferLease>> {
        Ok(None)
    }
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        Ok(self
            .read_vectored_with_flags(
                &mut [IoSliceMut::new(buffer)],
                SocketRecvFlags::default(),
                cancellation,
            )
            .await?
            .bytes)
    }
    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        Ok(self
            .read_vectored_with_flags(buffers, SocketRecvFlags::default(), cancellation)
            .await?
            .bytes)
    }
    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        cancellation.check()?;
        self.peek = flags.peek;
        let mut bytes = 0;
        for buffer in buffers {
            let count = buffer.len().min(self.bytes.len() - bytes);
            buffer[..count].copy_from_slice(&self.bytes[bytes..bytes + count]);
            bytes += count;
        }
        if !flags.peek {
            self.bytes = &self.bytes[bytes..];
        }
        Ok(SocketRecvResult {
            bytes,
            address: None,
            truncated: false,
            control_truncated: false,
        })
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        buffers: B,
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        (
            Err(io::Error::from(io::ErrorKind::Unsupported).into()),
            buffers,
        )
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct StreamWriter {
    bytes: Vec<u8>,
    dont_route: bool,
}
impl SocketStreamWriter for StreamWriter {
    type Buffer = Vec<u8>;
    async fn write(
        &mut self,
        buffer: &[u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }
    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.write_vectored_with_flags(buffers, SocketSendFlags::default(), cancellation)
            .await
    }
    async fn submit(
        &mut self,
        buffer: Vec<u8>,
        cancellation: &Cancellation,
    ) -> Result<(), (OperationError, Vec<u8>)> {
        match self.write(&buffer, cancellation).await {
            Ok(_) => Ok(()),
            Err(error) => Err((error, buffer)),
        }
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        cancellation.check()?;
        Ok(())
    }
    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        self.dont_route = flags.dont_route;
        for buffer in buffers {
            self.bytes.extend_from_slice(buffer);
        }
        Ok(buffers.iter().map(|buffer| buffer.len()).sum())
    }
    async fn write_owned_vectored<B: IoBufSequence>(
        &mut self,
        buffers: B,
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> (OperationResult<usize>, B) {
        (
            Err(io::Error::from(io::ErrorKind::Unsupported).into()),
            buffers,
        )
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Packet(Stream);
impl SocketPacketReader for Packet {
    type Message = OwnedSocketRecvMessage<BufferLease>;
    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<Self::Message>> {
        Ok(None)
    }
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.0
            .read_vectored_with_flags(
                &mut [IoSliceMut::new(buffer)],
                SocketRecvFlags::default(),
                cancellation,
            )
            .await
    }
    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.0
            .read_vectored_with_flags(buffers, SocketRecvFlags::default(), cancellation)
            .await
    }
    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.0
            .read_vectored_with_flags(buffers, flags, cancellation)
            .await
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        buffers: B,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        self.0
            .read_owned_vectored(buffers, flags, cancellation)
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct PacketWriter(StreamWriter, Option<SocketAddress>);
impl SocketPacketWriter for PacketWriter {
    async fn write(
        &mut self,
        buffer: &[u8],
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.1 = address;
        self.0.write(buffer, cancellation).await
    }
    async fn write_vectored(
        &mut self,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.write_vectored_with_flags(buffers, address, SocketSendFlags::default(), cancellation)
            .await
    }
    async fn write_owned<B: IoBuf>(
        &mut self,
        buffer: B,
        address: Option<SocketAddress>,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        (
            self.write(buffer.as_ref(), address, cancellation).await,
            buffer,
        )
    }
    async fn write_vectored_with_flags(
        &mut self,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.1 = address;
        self.0
            .write_vectored_with_flags(buffers, flags, cancellation)
            .await
    }
    async fn write_owned_vectored<B: IoBufSequence>(
        &mut self,
        buffers: B,
        _: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, B) {
        self.0
            .write_owned_vectored(buffers, flags, cancellation)
            .await
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn socket_defaults_preserve_owners_and_forward_flags_and_addresses() {
    block_on(async {
        let never = Cancellation::never();
        let cancelled = Cancellation::new();
        cancelled.cancel();
        let mut reader = Stream {
            bytes: b"abc",
            peek: false,
        };
        let mut buffer = [0; 2];
        assert_eq!(
            reader
                .read_into_with_flags(&mut buffer, SocketRecvFlags { peek: true }, &never)
                .await
                .unwrap(),
            2
        );
        assert_eq!(buffer, *b"ab");
        assert!(reader.peek);
        let owner = vec![0; 2];
        let pointer = owner.as_ptr();
        let (result, owner) = reader.read_owned(owner, &never).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(owner, b"ab");
        assert_eq!(owner.as_ptr(), pointer);
        let (result, owner) = reader.read_owned(owner, &cancelled).await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(owner.as_ptr(), pointer);
        let mut writer = StreamWriter {
            bytes: Vec::new(),
            dont_route: false,
        };
        assert_eq!(
            writer
                .write_with_flags(b"ab", SocketSendFlags { dont_route: true }, &never)
                .await
                .unwrap(),
            2
        );
        assert!(writer.dont_route);
        let (result, owner) = writer.write_owned(owner, &never).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(owner.as_ptr(), pointer);
        let (result, owner) = writer.write_owned(owner, &cancelled).await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(owner.as_ptr(), pointer);
        let mut reader = Packet(Stream {
            bytes: b"abc",
            peek: false,
        });
        assert_eq!(
            reader
                .read_into_with_flags(&mut buffer, SocketRecvFlags { peek: true }, &never)
                .await
                .unwrap()
                .bytes,
            2
        );
        assert!(reader.0.peek);
        let (result, owner) = reader.read_owned(owner, &never).await;
        assert_eq!(result.unwrap().bytes, 2);
        assert_eq!(owner.as_ptr(), pointer);
        let (result, _) = reader.read_owned(owner, &cancelled).await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        let address = Some("127.0.0.1:1234".parse().unwrap());
        let mut writer = PacketWriter(writer, None);
        assert_eq!(
            writer
                .write_with_flags(
                    b"packet",
                    address,
                    SocketSendFlags { dont_route: true },
                    &never
                )
                .await
                .unwrap(),
            6
        );
        assert_eq!(writer.1, address);
        assert!(writer.0.dont_route);
    });
}
