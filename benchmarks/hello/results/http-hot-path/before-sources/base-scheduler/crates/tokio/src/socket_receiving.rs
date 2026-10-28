// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::TokioScheduler;
use crate::tokio_socket_packet_reader::TokioSocketPacketReader;
use crate::tokio_socket_packet_writer::TokioSocketPacketWriter;
use crate::tokio_socket_stream_reader::TokioSocketStreamReader;
use base_scheduler::*;
use std::io;
impl SocketStreamReading for TokioScheduler {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        TokioSocketStreamReader::new(self, socket)
    }
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        TokioSocketStreamReader::new(self, socket).map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketReading for TokioScheduler {
    fn socket_packet_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    > {
        TokioSocketPacketReader::new(self, socket)
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
        TokioSocketPacketReader::new(self, socket).map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketWriting for TokioScheduler {
    fn socket_packet_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketPacketWriter + 'scheduler> {
        TokioSocketPacketWriter::new(self, socket)
    }
}

#[cfg(test)]
mod tests;
