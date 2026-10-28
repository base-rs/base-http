// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Limits for synchronous work. Cleanup uses a separate worker with the same admission capacity.
#[derive(Clone, Copy, Debug)]
pub struct SelectorOptions {
    /// Maximum simultaneous synchronous operations. Must be nonzero.
    pub threads: usize,
    /// Maximum admitted blocking jobs, including queued and running work. Must be nonzero.
    pub capacity: usize,
}
impl Default for SelectorOptions {
    fn default() -> Self {
        Self {
            threads: 4,
            capacity: 64,
        }
    }
}
