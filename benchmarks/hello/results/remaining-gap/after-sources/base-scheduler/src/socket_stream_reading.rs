// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferPool, BufferPooling, SocketOperation, SocketStreamReader};
use std::io;

/// Construct sequential stream input bound to this scheduler. Backends can substitute their own static reader while keeping protocol code independent of registration and receive mechanisms.
pub trait SocketStreamReading: SocketOperation + BufferPooling {
    /// Retain the socket and reusable backend state without submitting a receive or allocating a payload pool. Owned reads initialize default storage lazily.
    fn socket_stream_reader<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    where
        Self::Socket: 'static;

    /// Construct a reader using the supplied pool for owned chunks. Borrowed reads do not acquire pool storage. Returned chunks retain their pool independently of the reader.
    fn socket_stream_reader_with_pool<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
        pool: Self::Pool,
    ) -> io::Result<impl SocketStreamReader<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    where
        Self::Socket: 'static;
}
