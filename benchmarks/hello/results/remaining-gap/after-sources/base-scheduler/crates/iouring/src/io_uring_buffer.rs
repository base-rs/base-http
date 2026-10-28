// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{BufferLease, BufferSlice, FrozenBuffer, IoBuf, IoBufMut, ReceiveBuffer};
use std::ops::Range;

/// An exclusive initialized payload in an io_uring pool. Receive metadata stays outside the exposed range. Completed storage can move between threads; its final owner returns it directly to its original slab.
#[derive(Debug)]
pub struct IoUringBuffer {
    buffer: BufferLease,
    range: Range<usize>,
}
impl IoUringBuffer {
    pub(crate) fn new(buffer: BufferLease, offset: usize, length: usize) -> Self {
        assert!(offset <= buffer.as_ref().len() && length <= buffer.as_ref().len() - offset);
        Self {
            buffer,
            range: offset..offset + length,
        }
    }
}
impl AsRef<[u8]> for IoUringBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_ref()[self.range.clone()]
    }
}
impl AsMut<[u8]> for IoUringBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer.as_mut()[self.range.clone()]
    }
}
impl ReceiveBuffer for IoUringBuffer {
    type Frozen = BufferSlice<FrozenBuffer>;
    fn truncate(&mut self, length: usize) {
        assert!(length <= self.range.len(), "buffer length out of bounds");
        self.range.end = self.range.start + length;
    }
    fn freeze(self) -> Self::Frozen {
        self.buffer.freeze().slice(self.range)
    }
}
// SAFETY: The range retains its unique stable heap lease; moving the wrapper changes neither storage nor initialized length. Freezing consumes mutable access and retains the same allocation.
unsafe impl IoBuf for IoUringBuffer {}
unsafe impl IoBufMut for IoUringBuffer {}

impl crate::io_uring_recv_message::MessageStorage for IoUringBuffer {
    fn storage(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}
