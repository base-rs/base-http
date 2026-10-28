// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoUringSelector;
use base_scheduler::*;
use std::io;
impl SocketStreamReading for IoUringSelector {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        crate::io_uring_socket_stream_reader::IoUringSocketStreamReader::new(self, socket)
    }
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        crate::io_uring_socket_stream_reader::IoUringSocketStreamReader::new(self, socket)
            .map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketReading for IoUringSelector {
    fn socket_packet_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    > {
        crate::io_uring_socket_packet_reader::IoUringSocketPacketReader::new(self, socket)
    }
    fn socket_packet_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    > {
        crate::io_uring_socket_packet_reader::IoUringSocketPacketReader::new(self, socket)
            .map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketWriting for IoUringSelector {
    fn socket_packet_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketPacketWriter + 'scheduler> {
        crate::io_uring_socket_packet_writer::IoUringSocketPacketWriter::new(self, socket)
    }
}
impl SocketStreamWriting for IoUringSelector {
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        crate::io_uring_socket_stream_writer::IoUringSocketStreamWriter::new(self, socket)
    }
    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler> {
        crate::io_uring_socket_stream_writer::IoUringSocketStreamWriter::new(self, socket)
    }
}
