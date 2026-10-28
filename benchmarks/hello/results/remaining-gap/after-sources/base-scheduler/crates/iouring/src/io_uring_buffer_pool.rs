// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::*;
use std::io;

/// Shared reusable payload storage for demand-driven reads. Completed leases remain valid independently of the pool and return storage after their final owner retires.
#[derive(Clone)]
pub struct IoUringBufferPool(HeapBufferPool);

impl IoUringBufferPool {
    pub(crate) fn new(options: BufferPoolOptions) -> io::Result<Self> {
        HeapBufferPool::with_options(options).map(Self)
    }
}

impl BufferPool for IoUringBufferPool {
    type Buffer = crate::IoUringBuffer;
    fn capacity(&self) -> usize {
        self.0.capacity()
    }
    fn buffer_size(&self) -> usize {
        self.0.buffer_size()
    }
    fn alignment(&self) -> usize {
        self.0.alignment()
    }
    fn try_acquire(&self) -> Option<Self::Buffer> {
        self.0
            .try_acquire()
            .map(|buffer| crate::IoUringBuffer::new(buffer, 0, self.buffer_size()))
    }
    async fn acquire(&self, cancellation: &Cancellation) -> Result<Self::Buffer, Cancelled> {
        self.0
            .acquire(cancellation)
            .await
            .map(|buffer| crate::IoUringBuffer::new(buffer, 0, self.buffer_size()))
    }
}
