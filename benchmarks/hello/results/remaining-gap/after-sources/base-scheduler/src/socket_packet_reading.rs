// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferPool, BufferPooling, SocketOperation, SocketPacketReader, SocketRecvMessageMut};
use std::io;

/// Construct sequential packet input bound to this scheduler. Backends choose the static reader and completed message representation without exposing their receive mechanism.
pub trait SocketPacketReading: SocketOperation + BufferPooling {
    /// Retain the socket and reusable backend state without submitting a receive or allocating a payload pool. Owned reads initialize default storage lazily.
    fn socket_packet_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    >
    where
        Self: Sized,
        Self::Socket: 'static;

    /// Construct a reader using the supplied pool for owned messages. Borrowed reads do not acquire pool storage; completed messages retain their pool independently.
    fn socket_packet_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<
        impl SocketPacketReader<
            Message: SocketRecvMessageMut<Payload = <Self::Pool as BufferPool>::Buffer>,
        > + 'scheduler,
    >
    where
        Self: Sized,
        Self::Socket: 'static;
}
