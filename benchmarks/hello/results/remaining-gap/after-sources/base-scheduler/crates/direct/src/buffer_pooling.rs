// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::DirectScheduler;
use base_scheduler::{BufferPoolOptions, BufferPooling, HeapBufferPool};

impl BufferPooling for DirectScheduler {
    type Pool = HeapBufferPool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> std::io::Result<Self::Pool> {
        self.socket_retry.check_open()?;
        HeapBufferPool::with_options(options)
    }
}
