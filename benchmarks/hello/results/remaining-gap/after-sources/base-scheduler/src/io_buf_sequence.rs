// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoBuf;
use std::io::IoSlice;

/// An owned sequence of buffers for scatter/gather I/O. The collection itself may move; each buffer's byte storage must remain stable. Arrays avoid allocating a separate collection, and custom owners can retain pooled storage.
///
/// Inline byte arrays are not stable buffer owners, even when the outer collection is an array:
///
/// ```compile_fail
/// use base_scheduler::IoBufSequence;
/// fn accepts_sequence(_: impl IoBufSequence) {}
/// accepts_sequence([[0u8; 16]; 2]);
/// ```
///
/// # Safety
/// While an operation owns the sequence, `buffer_count()` and `buffer()` must expose the same initialized byte ranges in the same order. Each range must remain valid even if the sequence moves. The stable storage and exclusive ownership guarantees of `IoBuf` must remain valid until the sequence is dropped, including after the waiting future is abandoned. Any prepared I/O vectors must remain consistent with these ranges. Interior mutation or another owner must not replace, remove, or invalidate those buffers. A backend may explicitly modify the sequence through exclusive access only when it no longer retains pointers into the affected buffers, such as before submission or after retirement.
pub unsafe trait IoBufSequence: Send + 'static {
    /// Number of active byte ranges, including empty ranges.
    fn buffer_count(&self) -> usize;

    /// Borrow an active initialized byte range. Panics if `index` is out of bounds.
    fn buffer(&self, index: usize) -> &[u8];

    /// Borrow prepared I/O vectors, when available. These must describe exactly the same ranges as `buffer()`. Backends can otherwise build vectors in their own reusable storage.
    ///
    /// The vector array is borrowed storage: a completion backend must keep its address stable until retirement, as well as retaining the sequence owner. Moving an inline array after submission is not permitted.
    fn io_slices(&self) -> Option<&[IoSlice<'_>]> {
        None
    }
}

// SAFETY: These collections retain their elements and order as they move, and each element supplies stable byte storage through IoBuf.
unsafe impl<B: IoBuf> IoBufSequence for Vec<B> {
    fn buffer_count(&self) -> usize {
        self.len()
    }

    fn buffer(&self, index: usize) -> &[u8] {
        self[index].as_ref()
    }
}

// SAFETY: Moving the array moves its buffer owners, without moving their byte storage or changing the sequence.
unsafe impl<B: IoBuf, const N: usize> IoBufSequence for [B; N] {
    fn buffer_count(&self) -> usize {
        self.len()
    }

    fn buffer(&self, index: usize) -> &[u8] {
        self[index].as_ref()
    }
}

// SAFETY: The boxed slice retains its buffer owners and order until drop.
unsafe impl<B: IoBuf> IoBufSequence for Box<[B]> {
    fn buffer_count(&self) -> usize {
        self.len()
    }

    fn buffer(&self, index: usize) -> &[u8] {
        self[index].as_ref()
    }
}
