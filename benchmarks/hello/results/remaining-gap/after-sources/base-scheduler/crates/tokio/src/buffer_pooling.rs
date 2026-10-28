// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::TokioScheduler;
use base_scheduler::{BufferPoolOptions, BufferPooling, HeapBufferPool};

impl BufferPooling for TokioScheduler {
    type Pool = HeapBufferPool;
    fn buffer_pool(&self, options: BufferPoolOptions) -> std::io::Result<Self::Pool> {
        self.check_open()?;
        HeapBufferPool::with_options(options)
    }
}
