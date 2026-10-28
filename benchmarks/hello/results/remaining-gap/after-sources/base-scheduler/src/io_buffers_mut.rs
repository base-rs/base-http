// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::buffer_sequence_owner::BufferSequenceOwner;
use crate::{IoBufSequence, IoBufSequenceMut};
#[cfg(any(unix, windows))]
use std::io::IoSlice;
use std::io::IoSliceMut;

/// An owned writable scatter/gather package with I/O vectors prepared once. Like [`IoBuffers`](crate::IoBuffers), it retains the complete source sequence and supports explicit advancement after retirement.
///
/// Access to the source owner is limited to `into_inner()`: modifying or resizing source owners while cached vectors exist could invalidate their pointers. Read and write access to active bytes goes through the sequence traits instead.
///
/// Prepared writable vectors cannot escape their exclusive package borrow:
///
/// ```compile_fail
/// use base_scheduler::{IoBuffersMut, IoBufSequenceMut};
/// let mut buffers = IoBuffersMut::new([vec![0; 4]]);
/// let vectors = buffers.io_slices_mut().unwrap();
/// let owner = buffers.into_inner();
/// assert_eq!(vectors[0].len(), 4);
/// ```
pub struct IoBuffersMut<Buffers> {
    // No 'static view escapes. All byte access uses these vectors, never a fresh borrow from the source owners, preserving the writable pointers' provenance. Vectors drop before owners.
    vectors: Vec<IoSliceMut<'static>>,
    buffers: BufferSequenceOwner<Buffers>,
    start: usize,
}

impl<Buffers: IoBufSequenceMut> IoBuffersMut<Buffers> {
    /// Prepare writable vectors once and retain the original sequence. This allocates vector storage once; ordinary operations and advancement reuse it without allocation.
    ///
    /// Panics on Windows if an individual byte range exceeds the `IoSlice`/`IoSliceMut` 4 GiB limit.
    pub fn new(buffers: Buffers) -> Self {
        let mut buffers = BufferSequenceOwner::new(buffers);
        let vectors = (0..buffers.get().buffer_count())
            .map(|index| {
                // SAFETY: IoBufSequenceMut guarantees exclusive stable ranges. The retained owner is never accessed again until vectors are destroyed. Views borrow this package and never expose the erased lifetime.
                unsafe {
                    std::mem::transmute::<IoSliceMut<'_>, IoSliceMut<'static>>(IoSliceMut::new(
                        buffers.get_mut().buffer_mut(index),
                    ))
                }
            })
            .collect();
        Self {
            vectors,
            buffers,
            start: 0,
        }
    }

    /// Advance active byte ranges explicitly after a completed operation, without copying payloads or rebuilding vectors. Panics without changing the package if `bytes` exceeds the remaining length. Advancing by zero skips leading empty vectors.
    pub fn advance(&mut self, bytes: usize) {
        let (count, rest) =
            crate::io_buffers::advance_position(&self.vectors[self.start..], bytes, |vector| {
                vector.len()
            });
        if rest > 0 {
            self.vectors[self.start + count].advance(rest);
        }
        self.start += count;
    }

    /// Recover all original source owners and ranges, including received bytes in consumed ranges.
    pub fn into_inner(self) -> Buffers {
        drop(self.vectors);
        self.buffers.into_inner()
    }
}

// SAFETY: The private source owner keeps every allocation alive. Reads use the cached writable pointers through shared borrows, excluding concurrent mutation. Advancement requires exclusive access after retirement.
unsafe impl<Buffers: IoBufSequenceMut> IoBufSequence for IoBuffersMut<Buffers> {
    fn buffer_count(&self) -> usize {
        self.vectors.len() - self.start
    }

    fn buffer(&self, index: usize) -> &[u8] {
        &self.vectors[self.start..][index]
    }

    #[cfg(any(unix, windows))]
    fn io_slices(&self) -> Option<&[IoSlice<'_>]> {
        let vectors = &self.vectors[self.start..];
        // SAFETY: Both standard types are ABI-compatible with iovec on Unix and WSABUF on Windows, including array stride. The shared borrow permits only reads and ties every byte view to self. The writable vectors cannot be accessed exclusively until this borrow ends.
        Some(unsafe {
            std::slice::from_raw_parts(vectors.as_ptr().cast::<IoSlice<'_>>(), vectors.len())
        })
    }
}

// SAFETY: Exclusive borrowing prevents simultaneous byte access, vector metadata cannot be replaced through the shared outer slice, and the erased lifetime is shortened to the exclusive package borrow.
unsafe impl<Buffers: IoBufSequenceMut> IoBufSequenceMut for IoBuffersMut<Buffers> {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        &mut self.vectors[self.start..][index]
    }

    fn io_slices_mut(&mut self) -> Option<&[IoSliceMut<'_>]> {
        let vectors = &self.vectors[self.start..];
        // SAFETY: Only the inner lifetime changes. The outer shared slice prevents replacing an IoSliceMut, and both its byte views and its native pointers are bounded by the exclusive borrow of self.
        Some(unsafe {
            std::slice::from_raw_parts(vectors.as_ptr().cast::<IoSliceMut<'_>>(), vectors.len())
        })
    }
}
