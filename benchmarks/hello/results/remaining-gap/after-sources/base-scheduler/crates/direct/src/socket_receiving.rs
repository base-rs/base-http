// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::DirectScheduler;
use crate::direct_socket_packet_reader::DirectSocketPacketReader;
use crate::direct_socket_packet_writer::DirectSocketPacketWriter;
use crate::direct_socket_stream_reader::DirectSocketStreamReader;
use base_scheduler::*;
use std::io;
impl SocketStreamReading for DirectScheduler {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        DirectSocketStreamReader::new(self, socket)
    }
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        DirectSocketStreamReader::new(self, socket).map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketReading for DirectScheduler {
    fn socket_packet_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    > {
        DirectSocketPacketReader::new(self, socket)
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
        DirectSocketPacketReader::new(self, socket).map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketWriting for DirectScheduler {
    fn socket_packet_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketPacketWriter + 'scheduler> {
        DirectSocketPacketWriter::new(self, socket)
    }
}

#[cfg(test)]
mod tests;
