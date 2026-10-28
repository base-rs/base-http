// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferPool, BufferPooling, IoBuf, SocketOperation, SocketStreamWriter};
use std::io;

/// Construct sequential stream output bound to this scheduler. Backends select a static writer without requiring protocols to choose registration or output-progress mechanisms. Construction performs no writes and rejects non-stream sockets.
pub trait SocketStreamWriting: SocketOperation + BufferPooling {
    /// Retain the socket and reusable backend state. Owned submissions accept this scheduler's pooled chunks, so the default reader and writer compose without choosing a buffer representation. Borrowed writes do not allocate a payload pool. Subsequent operations take only payloads and cancellation, never another scheduler.
    fn socket_stream_writer<'scheduler>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = <Self::Pool as BufferPool>::Buffer> + 'scheduler>
    where
        Self::Socket: 'static;

    /// Select another stable owner type, such as `Vec<u8>` or an application lease. This changes storage compatibility, not the writer's ordering or backend execution strategy.
    fn socket_stream_writer_with_buffer<'scheduler, B: IoBuf>(
        &'scheduler self,
        socket: &Self::Socket,
    ) -> io::Result<impl SocketStreamWriter<Buffer = B> + 'scheduler>
    where
        Self::Socket: 'static;
}
