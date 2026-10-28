// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Limits for synchronous Tokio adapter work. Cleanup has its own worker and bounded queue.
#[derive(Clone, Copy, Debug)]
pub struct TokioOptions {
    /// Maximum number of simultaneous synchronous operations. Must be nonzero.
    pub threads: usize,
    /// Maximum admitted jobs, including queued and running work. Must be nonzero.
    pub capacity: usize,
}

impl Default for TokioOptions {
    fn default() -> Self {
        Self {
            threads: 4,
            capacity: 64,
        }
    }
}
