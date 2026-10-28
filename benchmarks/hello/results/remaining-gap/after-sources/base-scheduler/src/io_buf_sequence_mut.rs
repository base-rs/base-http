// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{IoBufMut, IoBufSequence};
use std::io::IoSliceMut;

/// An owned sequence of writable buffers. Receiving fills their initialized byte ranges without resizing them or replacing their owners.
///
/// # Safety
/// `buffer_mut()` and any prepared writable I/O vectors must expose the same byte ranges and order as `buffer()`, with exclusive access. Writable ranges must not overlap, and borrowing one range must not invalidate previously obtained pointers into other ranges. Each byte range must uphold the stable storage guarantees of `IoBufMut`; no other owner may access the writable byte ranges during an operation. A backend must not replace or invalidate buffer owners while their byte storage may still be accessed by a submitted operation.
pub unsafe trait IoBufSequenceMut: IoBufSequence {
    /// Borrow an active writable byte range. Panics if `index` is out of bounds.
    fn buffer_mut(&mut self, index: usize) -> &mut [u8];

    /// Borrow prepared writable I/O vectors, when available. The shared outer slice prevents replacing vectors or changing ranges; the exclusive sequence borrow permits a backend to write through their native pointers until retirement.
    ///
    /// These vectors have the same address-stability requirements as `IoBufSequence::io_slices`. Safe byte access uses `buffer_mut()` instead.
    fn io_slices_mut(&mut self) -> Option<&[IoSliceMut<'_>]> {
        None
    }
}

// SAFETY: Vec gives exclusive access to its elements, each of which implements IoBufMut.
unsafe impl<B: IoBufMut> IoBufSequenceMut for Vec<B> {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        self[index].as_mut()
    }
}

// SAFETY: Arrays give exclusive access to their elements, each of which implements IoBufMut.
unsafe impl<B: IoBufMut, const N: usize> IoBufSequenceMut for [B; N] {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        self[index].as_mut()
    }
}

// SAFETY: The boxed slice gives exclusive access to its elements, each of which implements IoBufMut.
unsafe impl<B: IoBufMut> IoBufSequenceMut for Box<[B]> {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        self[index].as_mut()
    }
}
