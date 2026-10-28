// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::SocketAddress;

/// Owned scatter/gather payload and optional destination for one send. `None` uses the connected peer. `Buffers` is the entire collection owner, such as `[Vec<u8>; 2]`, `Vec<Vec<u8>>`, or a pooled sequence. The operation returns this value on success, error, and cancellation without copying its payload or replacing its collection owner.
#[derive(Debug)]
pub(crate) struct SocketMessage<Buffers = Vec<Vec<u8>>> {
    pub buffers: Buffers,
    pub address: Option<SocketAddress>,
}
