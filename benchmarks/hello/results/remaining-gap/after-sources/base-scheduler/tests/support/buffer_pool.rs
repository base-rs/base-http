// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::*;

// A distinct pool and lease representation ensures generic consumers do not depend on the heap implementation's concrete owner types.
#[derive(Clone)]
pub struct TestPool(pub HeapBufferPool);
pub struct TestBuffer(BufferLease);

impl BufferPool for TestPool {
    type Buffer = TestBuffer;
    fn capacity(&self) -> usize {
        self.0.capacity()
    }
    fn buffer_size(&self) -> usize {
        self.0.buffer_size()
    }
    fn alignment(&self) -> usize {
        self.0.alignment()
    }
    fn try_acquire(&self) -> Option<TestBuffer> {
        self.0.try_acquire().map(TestBuffer)
    }
    async fn acquire(&self, cancellation: &Cancellation) -> Result<TestBuffer, Cancelled> {
        self.0.acquire(cancellation).await.map(TestBuffer)
    }
}
impl AsRef<[u8]> for TestBuffer {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
impl AsMut<[u8]> for TestBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}
// SAFETY: The wrapper preserves its exclusive lease's stable initialized storage and ownership through retirement.
unsafe impl IoBuf for TestBuffer {}
unsafe impl IoBufMut for TestBuffer {}
impl ReceiveBuffer for TestBuffer {
    type Frozen = FrozenBuffer;
    fn truncate(&mut self, length: usize) {
        self.0.truncate(length);
    }
    fn freeze(self) -> FrozenBuffer {
        self.0.freeze()
    }
}
