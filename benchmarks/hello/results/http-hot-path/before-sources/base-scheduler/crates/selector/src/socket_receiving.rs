// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::ReadinessSelector;
use crate::readiness_socket_packet_reader::ReadinessSocketPacketReader;
use crate::readiness_socket_packet_writer::ReadinessSocketPacketWriter;
use crate::readiness_socket_stream_reader::ReadinessSocketStreamReader;
use base_scheduler::*;
use std::io;
impl SocketStreamReading for ReadinessSelector {
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        ReadinessSocketStreamReader::new(self, socket)
    }
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    {
        ReadinessSocketStreamReader::new(self, socket).map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketReading for ReadinessSelector {
    fn socket_packet_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    > {
        ReadinessSocketPacketReader::new(self, socket)
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
        ReadinessSocketPacketReader::new(self, socket).map(|reader| reader.with_pool(pool))
    }
}
impl SocketPacketWriting for ReadinessSelector {
    fn socket_packet_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketPacketWriter + 'scheduler> {
        ReadinessSocketPacketWriter::new(self, socket)
    }
}
