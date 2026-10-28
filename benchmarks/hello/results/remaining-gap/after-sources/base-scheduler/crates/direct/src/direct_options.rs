// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Limits for ordinary direct work. Cleanup has its own worker and bounded queue.
#[derive(Clone, Copy, Debug)]
pub struct DirectOptions {
    /// Maximum number of simultaneous synchronous operations, and separately the number of task-polling workers. Must be nonzero.
    pub threads: usize,
    /// Maximum admitted jobs, including queued and running work. Must be nonzero.
    pub capacity: usize,
}

impl Default for DirectOptions {
    fn default() -> Self {
        Self {
            threads: 4,
            capacity: 64,
        }
    }
}
