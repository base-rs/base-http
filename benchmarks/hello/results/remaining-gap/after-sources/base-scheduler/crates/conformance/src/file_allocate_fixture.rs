// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{FileAllocate, FileOperation};

/// A writable regular file containing `abcdef` with cursor 3, plus a read-only open of that file. Run only on a filesystem/profile supporting real allocation and `KeepSize`; an unsupported operation is not a passing allocation test.
pub trait FileAllocateFixture {
    type Scheduler: FileAllocate;
    fn scheduler(&self) -> &Self::Scheduler;
    fn file(&self) -> &<Self::Scheduler as FileOperation>::File;
    fn read_only_file(&self) -> &<Self::Scheduler as FileOperation>::File;
    /// Independently inspect backing-store contents/length and the writable descriptor's cursor.
    fn assert_state(&self, contents: &[u8], cursor: u64);
    /// Verify real reservation of the range, rather than an implementation that merely resizes the file or writes zeroes. Use native allocation observations or a controlled syscall boundary.
    fn assert_reserved(&self, offset: u64, length: u64);
    fn calls(&self) -> usize;
}
