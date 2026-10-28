// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoBufSequence;
use crate::buffer_sequence_owner::BufferSequenceOwner;
use std::io::IoSlice;

/// An owned scatter/gather package with I/O vectors prepared once at construction. The vector allocation and source owners are reused between operations; payload bytes are never copied.
///
/// ```
/// use base_scheduler::{IoBuffers, IoBufSequence};
/// let mut buffers = IoBuffers::new([b"header".to_vec(), b"body".to_vec()]);
/// // After an operation returns its owner and reports a short transfer:
/// buffers.advance(7);
/// assert_eq!(buffers.buffer(0), b"ody");
/// let [header, body] = buffers.into_inner();
/// assert_eq!(header, b"header");
/// assert_eq!(body, b"body");
/// ```
///
/// Borrowed I/O vectors cannot outlive the package, even when copied:
///
/// ```compile_fail
/// use base_scheduler::{IoBuffers, IoBufSequence};
/// let vector = {
///     let buffers = IoBuffers::new([vec![1, 2]]);
///     buffers.io_slices().unwrap()[0]
/// };
/// assert_eq!(&*vector, &[1, 2]);
/// ```
pub struct IoBuffers<Buffers> {
    // Private lifetime erasure retains native layout without borrowing a sibling field. No 'static view escapes: all access is tied to a borrow of this package. Drop the vectors before releasing the owners.
    vectors: Vec<IoSlice<'static>>,
    buffers: BufferSequenceOwner<Buffers>,
    start: usize,
}

impl<Buffers: IoBufSequence> IoBuffers<Buffers> {
    /// Prepare one vector per source range, retaining the complete sequence owner. This allocates vector storage once; subsequent moves, borrows, and advancement do not allocate.
    ///
    /// Panics on Windows if an individual byte range exceeds the `IoSlice`/`IoSliceMut` 4 GiB limit.
    pub fn new(buffers: Buffers) -> Self {
        let buffers = BufferSequenceOwner::new(buffers);
        let vectors = (0..buffers.get().buffer_count())
            .map(|index| {
                // SAFETY: IoBufSequence guarantees stable storage across owner moves. The source owner remains private and retained until vectors are destroyed; public views shorten this lifetime to a borrow of the package.
                unsafe {
                    std::mem::transmute::<IoSlice<'_>, IoSlice<'static>>(IoSlice::new(
                        buffers.get().buffer(index),
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

    /// Skip completed bytes after an operation has returned the package. Fully consumed vectors are skipped; only the first partially consumed vector is adjusted. Source owners and their original ranges are preserved.
    ///
    /// Panics without changing the package if `bytes` exceeds the remaining length. Advancing by zero also skips leading empty vectors. Schedulers never advance the package automatically.
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

    /// Recover the original sequence, including fully consumed buffers and their original ranges.
    pub fn into_inner(self) -> Buffers {
        drop(self.vectors);
        self.buffers.into_inner()
    }
}

// Compute the cursor before modifying anything, so over-advancement leaves the package intact. Visit only consumed vectors and the first remaining vector.
pub(crate) fn advance_position<T>(
    vectors: &[T],
    mut bytes: usize,
    len: impl Fn(&T) -> usize,
) -> (usize, usize) {
    let mut count = 0;
    for vector in vectors {
        let length = len(vector);
        if bytes < length {
            break;
        }
        bytes -= length;
        count += 1;
    }
    assert!(
        count < vectors.len() || bytes == 0,
        "advance beyond remaining buffers"
    );
    (count, bytes)
}

// SAFETY: The private sequence retains all byte storage, and only exclusive, caller-controlled advancement changes exposed ranges. Cached vectors remain valid across moves and are borrowed only for the lifetime of the package.
unsafe impl<Buffers: IoBufSequence> IoBufSequence for IoBuffers<Buffers> {
    fn buffer_count(&self) -> usize {
        self.vectors.len() - self.start
    }

    fn buffer(&self, index: usize) -> &[u8] {
        &self.vectors[self.start..][index]
    }

    fn io_slices(&self) -> Option<&[IoSlice<'_>]> {
        Some(&self.vectors[self.start..])
    }
}
