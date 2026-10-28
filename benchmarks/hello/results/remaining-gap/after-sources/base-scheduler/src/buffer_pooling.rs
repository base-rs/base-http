// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferPool, BufferPoolOptions};
use std::io;

/// Construct a backend-selected pool with portable ownership semantics. Applications choose the payload budget and alignment; implementations choose allocation and registration strategy. Pool construction performs no I/O on application resources.
///
/// `options.buffers` is the exact shared payload capacity, independent of worker count. Clones use that same budget. Internal worker caches must make returned capacity available to acquisitions on other workers; they must not silently partition the pool or multiply its capacity. Receivers and accepted I/O may retain slots, including while idle, so use a separate pool for independently required scratch storage.
///
/// Pools may retain backend resources for registration lifetime, but bound handles select their scheduler at construction and receive cancellation per operation. Backends document instance affinity and reject incompatible resources before submitting I/O. Closing a scheduler does not invalidate completed buffers or their retained views.
///
/// ```
/// use base_scheduler::{BufferPoolOptions, BufferPooling, Cancellation, IoBuf,
///     OperationResult, ReceiveBuffer, SocketPacketReading, SocketPacketReader, SocketRecvMessage};
///
/// async fn receive<S: SocketPacketReading>(scheduler: &S, socket: &S::Socket,
///     cancellation: &Cancellation) -> OperationResult<()>
/// where S::Socket: 'static {
///     let pool = scheduler.buffer_pool(BufferPoolOptions::new(128, 64 * 1024))?;
///     let mut reader = scheduler.socket_packet_reader_with_pool(socket, pool)?;
///     let received = reader.read(cancellation).await;
///     reader.close().await?;
///     drop(reader);
///     if let Some(message) = received? {
///         let frozen = message.into_payload().freeze();
///         let frame = frozen.clone().slice(..);
///         drop((frame, frozen));
///     }
///     Ok(())
/// }
/// ```
pub trait BufferPooling {
    type Pool: BufferPool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> io::Result<Self::Pool>;
}
