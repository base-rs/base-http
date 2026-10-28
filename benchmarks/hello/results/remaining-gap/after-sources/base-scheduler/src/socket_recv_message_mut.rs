// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{IoBufMut, SocketRecvMessage};

/// A completed message with exclusively writable payload storage. Immutable receive implementations need not implement this capability.
pub trait SocketRecvMessageMut: SocketRecvMessage<Payload: IoBufMut> {
    /// The same completed range as `payload()`, with exclusive access for parsing or decryption. Changes are visible through subsequent payload access and ownership transfer; metadata is outside this range.
    fn payload_mut(&mut self) -> &mut [u8];
}
