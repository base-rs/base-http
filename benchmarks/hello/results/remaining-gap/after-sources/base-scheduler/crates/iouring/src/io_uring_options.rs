// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler_selector::SelectorOptions;

/// Limits for the ring, retained requests, and readiness fallback.
#[derive(Clone, Copy, Debug)]
pub struct IoUringOptions {
    /// Requested submission queue entries. Must be nonzero.
    pub entries: u32,
    /// Maximum accepted original ring requests, including queued and in-flight requests. Synchronous socket attempts do not consume capacity. Must be nonzero.
    pub capacity: usize,
    /// Maximum cached fixed-file registrations per ring. Zero disables this optional optimization; unavailable registration or exhausted capacity falls back to ordinary descriptors.
    pub fixed_files: u32,
    /// Limits for synchronous fallback operations and cleanup.
    pub fallback: SelectorOptions,
}
impl Default for IoUringOptions {
    fn default() -> Self {
        Self {
            entries: 256,
            capacity: 256,
            fixed_files: 256,
            fallback: SelectorOptions::default(),
        }
    }
}
