// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BufferSlice, IoBuf, heap_buffer_pool::Slab};
use std::{
    ops::{Range, RangeBounds},
    ptr::NonNull,
};

/// An immutable pool-backed chunk. Cloning or retaining a range shares its existing allocation; the final view returns the slot to the pool. Views keep the backing slab alive even after the pool is dropped, so parsers may choose to copy unusually long-lived fragments.
pub struct FrozenBuffer {
    slab: NonNull<Slab>,
    index: usize,
    range: Range<usize>,
}
impl FrozenBuffer {
    pub(crate) fn new(slab: NonNull<Slab>, index: usize, range: Range<usize>) -> Self {
        Self { slab, index, range }
    }

    /// Retain an independently owned range without copying payload bytes or allocating a reference-count wrapper.
    pub fn view(&self, range: impl RangeBounds<usize>) -> BufferSlice<Self> {
        self.clone().slice(range)
    }
}
impl Clone for FrozenBuffer {
    fn clone(&self) -> Self {
        // SAFETY: All views retain the slot's one persistent slab claim together.
        unsafe { self.slab.as_ref() }.retain(self.index);
        Self::new(self.slab, self.index, self.range.clone())
    }
}
impl AsRef<[u8]> for FrozenBuffer {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: Every view retains the slot's reader count and persistent slab claim. No mutable lease can exist until the final view releases it.
        unsafe {
            std::slice::from_raw_parts(
                self.slab.as_ref().pointer(self.index).add(self.range.start),
                self.range.len(),
            )
        }
    }
}
impl Drop for FrozenBuffer {
    fn drop(&mut self) {
        // SAFETY: Drop consumes this reader; the final reader returns the slot claim.
        unsafe { Slab::release_reader(self.slab, self.index) };
    }
}
// SAFETY: Shared views expose only immutable initialized bytes at a stable allocation and range, retained through the last view's release.
unsafe impl IoBuf for FrozenBuffer {}

// SAFETY: Shared views keep the slab alive and expose only immutable initialized bytes.
unsafe impl Send for FrozenBuffer {}
unsafe impl Sync for FrozenBuffer {}

impl std::fmt::Debug for FrozenBuffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrozenBuffer")
            .field("length", &self.range.len())
            .finish_non_exhaustive()
    }
}
