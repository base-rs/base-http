// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{FrozenBuffer, IoBuf, IoBufMut, heap_buffer_pool::Slab};
use std::{ops::Range, ptr::NonNull, sync::atomic::Ordering};

/// An exclusive writable pool lease. Its initialized length can be shortened after a read, without copying or borrowing the receiver. Drop returns storage to the pool; an I/O backend must retain this owner until its access retires. A lease keeps its backing slab alive even after the pool is dropped.
pub struct BufferLease {
    slab: Option<NonNull<Slab>>,
    index: usize,
    length: usize,
}

impl BufferLease {
    pub(crate) fn new(slab: NonNull<Slab>, index: usize, length: usize) -> Self {
        Self {
            slab: Some(slab),
            index,
            length,
        }
    }

    /// Limit the visible initialized prefix. Panics if the requested length exceeds the current prefix.
    pub fn truncate(&mut self, length: usize) {
        assert!(length <= self.length, "buffer length out of bounds");
        self.length = length;
    }

    /// Transfer this lease into immutable, independently retainable storage. No allocation or payload copy is required.
    pub fn freeze(mut self) -> FrozenBuffer {
        let slab = self.slab.take().unwrap();
        // SAFETY: This lease owns the slot's persistent slab claim.
        unsafe { slab.as_ref() }.slots[self.index]
            .readers
            .store(1, Ordering::Relaxed);
        FrozenBuffer::new(
            slab,
            self.index,
            Range {
                start: 0,
                end: self.length,
            },
        )
    }
}

impl AsRef<[u8]> for BufferLease {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: This lease owns its slot's persistent slab claim; the allocation and initialized prefix remain stable until release.
        let slab = unsafe { self.slab.unwrap().as_ref() };
        unsafe { std::slice::from_raw_parts(slab.pointer(self.index), self.length) }
    }
}
impl AsMut<[u8]> for BufferLease {
    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: The unique lease and exclusive borrow prevent other accesses to this slot. Frozen views cannot coexist with a mutable lease.
        let slab = unsafe { self.slab.unwrap().as_ref() };
        unsafe { std::slice::from_raw_parts_mut(slab.pointer(self.index), self.length) }
    }
}
impl Drop for BufferLease {
    fn drop(&mut self) {
        if let Some(slab) = self.slab.take() {
            // SAFETY: Drop consumes the unique mutable slot claim.
            unsafe { Slab::release(slab, self.index) };
        }
    }
}

// SAFETY: Pool storage is initialized, fixed in size, and stable as its exclusive lease moves. Only the owned lease permits mutation; operations retain it through retirement.
unsafe impl IoBuf for BufferLease {}
unsafe impl IoBufMut for BufferLease {}

// SAFETY: The owned slot claim keeps the slab alive, and only an exclusive borrow permits writes.
unsafe impl Send for BufferLease {}
unsafe impl Sync for BufferLease {}

impl std::fmt::Debug for BufferLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BufferLease")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl crate::ReceiveBuffer for BufferLease {
    type Frozen = FrozenBuffer;
    fn truncate(&mut self, length: usize) {
        BufferLease::truncate(self, length);
    }
    fn freeze(self) -> FrozenBuffer {
        BufferLease::freeze(self)
    }
}
