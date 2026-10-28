// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Task and clock limits when composing with an independently configured selector.
#[derive(Clone, Copy, Debug)]
pub struct NativeExecutorOptions {
    /// Number of work-stealing task workers. Must be nonzero.
    pub workers: usize,
    /// Reserve at least this many simultaneous timer registrations per worker. Must be nonzero. A full owning queue returns `WouldBlock`; spare capacity on other workers is not borrowed.
    pub timer_capacity: usize,
}

impl Default for NativeExecutorOptions {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism().map_or(1, usize::from),
            timer_capacity: 1024,
        }
    }
}
