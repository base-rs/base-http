// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::mem::{ManuallyDrop, MaybeUninit};

// Keep source owners behind a union before borrowing their bytes. Moving an ordinary field containing Box may retag the boxed allocation and invalidate cached pointers; moving MaybeUninit does not recursively retag its contents. This wrapper always contains an initialized owner and retains its ordinary Send/Sync bounds.
pub(crate) struct BufferSequenceOwner<Buffers> {
    buffers: MaybeUninit<Buffers>,
}

impl<Buffers> BufferSequenceOwner<Buffers> {
    pub(crate) fn new(buffers: Buffers) -> Self {
        Self {
            buffers: MaybeUninit::new(buffers),
        }
    }

    pub(crate) fn get(&self) -> &Buffers {
        // SAFETY: new initializes the owner, and no method removes it without consuming this wrapper.
        unsafe { self.buffers.assume_init_ref() }
    }

    pub(crate) fn get_mut(&mut self) -> &mut Buffers {
        // SAFETY: The owner is initialized and this borrow is exclusive. Constructors finish all source access before publishing prepared pointers.
        unsafe { self.buffers.assume_init_mut() }
    }

    pub(crate) fn into_inner(self) -> Buffers {
        let owner = ManuallyDrop::new(self);
        // SAFETY: The owner is initialized and is moved out exactly once. ManuallyDrop prevents the wrapper from subsequently dropping it. Callers destroy cached vectors before recovering this owner.
        unsafe { owner.buffers.assume_init_read() }
    }
}

impl<Buffers> Drop for BufferSequenceOwner<Buffers> {
    fn drop(&mut self) {
        // SAFETY: The owner is initialized and has not been moved out. The packages drop their vector fields first, so no cached pointers are used after this access.
        unsafe { self.buffers.assume_init_drop() }
    }
}
