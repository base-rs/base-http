// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

impl BufferPooling for IoUringSelector {
    type Pool = crate::IoUringBufferPool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> std::io::Result<Self::Pool> {
        self.check_open()?;
        crate::IoUringBufferPool::new(options)
    }
}
