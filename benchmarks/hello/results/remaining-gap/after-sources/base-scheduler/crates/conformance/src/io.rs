// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Test-only operations composed exclusively from the public bound resources.
use crate::SocketTestScheduler;
use base_scheduler::*;
use std::io::{IoSlice, IoSliceMut};

pub(crate) struct SocketMessage<B> {
    pub buffers: B,
    pub address: Option<SocketAddress>,
}

pub(crate) async fn socket_read<S: SocketTestScheduler, B: IoBufMut>(
    scheduler: &S,
    socket: &S::Socket,
    buffer: B,
    cancellation: &Cancellation,
) -> BufferResult<B> {
    let mut reader = scheduler.socket_stream_reader(socket).unwrap();
    let result = reader.read_owned(buffer, cancellation).await;
    reader.close().await.unwrap();
    result
}
pub(crate) async fn socket_write<S: SocketTestScheduler, B: IoBuf>(
    scheduler: &S,
    socket: &S::Socket,
    buffer: B,
    cancellation: &Cancellation,
) -> BufferResult<B> {
    let mut writer = scheduler.socket_stream_writer(socket).unwrap();
    let result = writer.write_owned(buffer, cancellation).await;
    writer.close().await.unwrap();
    result
}
pub(crate) async fn socket_recv_poll<S: SocketTestScheduler>(
    scheduler: &S,
    socket: &S::Socket,
    buffer: &mut [u8],
    flags: SocketRecvFlags,
    cancellation: &Cancellation,
) -> OperationResult<usize> {
    let mut reader = scheduler.socket_stream_reader(socket).unwrap();
    let result = reader
        .read_into_with_flags(buffer, flags, cancellation)
        .await;
    reader.close().await.unwrap();
    result
}
pub(crate) async fn socket_send_poll<S: SocketTestScheduler>(
    scheduler: &S,
    socket: &S::Socket,
    buffer: &[u8],
    flags: SocketSendFlags,
    cancellation: &Cancellation,
) -> OperationResult<usize> {
    let mut writer = scheduler.socket_stream_writer(socket).unwrap();
    let result = writer.write_with_flags(buffer, flags, cancellation).await;
    writer.close().await.unwrap();
    result
}
pub(crate) async fn socket_recvmsg<S: SocketTestScheduler, B: IoBufSequenceMut>(
    scheduler: &S,
    socket: &S::Socket,
    buffer: B,
    flags: SocketRecvFlags,
    cancellation: &Cancellation,
) -> (OperationResult<SocketRecvResult>, B) {
    if let Ok(mut reader) = scheduler.socket_stream_reader(socket) {
        let result = reader
            .read_owned_vectored(buffer, flags, cancellation)
            .await;
        reader.close().await.unwrap();
        result
    } else {
        let mut reader = scheduler.socket_packet_reader(socket).unwrap();
        let result = reader
            .read_owned_vectored(buffer, flags, cancellation)
            .await;
        reader.close().await.unwrap();
        result
    }
}
pub(crate) async fn socket_sendmsg<S: SocketTestScheduler, B: IoBufSequence>(
    scheduler: &S,
    socket: &S::Socket,
    message: SocketMessage<B>,
    flags: SocketSendFlags,
    cancellation: &Cancellation,
) -> (OperationResult<usize>, SocketMessage<B>) {
    let SocketMessage { buffers, address } = message;
    let (result, buffers) = if let Ok(mut writer) = scheduler.socket_stream_writer(socket) {
        let result = writer
            .write_owned_vectored(buffers, flags, cancellation)
            .await;
        writer.close().await.unwrap();
        result
    } else {
        let mut writer = scheduler.socket_packet_writer(socket).unwrap();
        let result = writer
            .write_owned_vectored(buffers, address, flags, cancellation)
            .await;
        writer.close().await.unwrap();
        result
    };
    (result, SocketMessage { buffers, address })
}
pub(crate) async fn socket_recvmsg_poll<S: SocketTestScheduler>(
    scheduler: &S,
    socket: &S::Socket,
    buffers: &mut [IoSliceMut<'_>],
    flags: SocketRecvFlags,
    cancellation: &Cancellation,
) -> OperationResult<SocketRecvResult> {
    if let Ok(mut reader) = scheduler.socket_stream_reader(socket) {
        let result = reader
            .read_vectored_with_flags(buffers, flags, cancellation)
            .await;
        reader.close().await.unwrap();
        result
    } else {
        let mut reader = scheduler.socket_packet_reader(socket).unwrap();
        let result = reader
            .read_vectored_with_flags(buffers, flags, cancellation)
            .await;
        reader.close().await.unwrap();
        result
    }
}
pub(crate) async fn socket_sendmsg_poll<S: SocketTestScheduler>(
    scheduler: &S,
    socket: &S::Socket,
    buffers: &[IoSlice<'_>],
    address: Option<SocketAddress>,
    flags: SocketSendFlags,
    cancellation: &Cancellation,
) -> OperationResult<usize> {
    if let Ok(mut writer) = scheduler.socket_stream_writer(socket) {
        let result = writer
            .write_vectored_with_flags(buffers, flags, cancellation)
            .await;
        writer.close().await.unwrap();
        result
    } else {
        let mut writer = scheduler.socket_packet_writer(socket).unwrap();
        let result = writer
            .write_vectored_with_flags(buffers, address, flags, cancellation)
            .await;
        writer.close().await.unwrap();
        result
    }
}
pub(crate) async fn file_read<S: FileReading, B: IoBufMut>(
    scheduler: &S,
    file: &S::File,
    buffer: B,
    cancellation: &Cancellation,
) -> BufferResult<B> {
    let mut handle = scheduler.file_reader(file).unwrap();
    let result = handle.read(buffer, cancellation).await;
    handle.close().await.unwrap();
    result
}
pub(crate) async fn file_read_at<S: FileReading, B: IoBufMut>(
    scheduler: &S,
    file: &S::File,
    buffer: B,
    offset: u64,
    cancellation: &Cancellation,
) -> BufferResult<B> {
    let mut handle = scheduler.file_reader(file).unwrap();
    let result = handle.read_at(buffer, offset, cancellation).await;
    handle.close().await.unwrap();
    result
}
pub(crate) async fn file_write<S: FileWriting, B: IoBuf>(
    scheduler: &S,
    file: &S::File,
    buffer: B,
    cancellation: &Cancellation,
) -> BufferResult<B> {
    let mut handle = scheduler.file_writer(file).unwrap();
    let result = handle.write(buffer, cancellation).await;
    handle.close().await.unwrap();
    result
}
pub(crate) async fn file_write_at<S: FileWriting, B: IoBuf>(
    scheduler: &S,
    file: &S::File,
    buffer: B,
    offset: u64,
    cancellation: &Cancellation,
) -> BufferResult<B> {
    let mut handle = scheduler.file_writer(file).unwrap();
    let result = handle.write_at(buffer, offset, cancellation).await;
    handle.close().await.unwrap();
    result
}
