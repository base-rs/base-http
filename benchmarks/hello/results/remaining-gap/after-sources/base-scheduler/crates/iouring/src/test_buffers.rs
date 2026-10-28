// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::ptr::NonNull;

// A real initialized range just beyond the native message limit. Anonymous pages are demand-zero; validation rejects this range without committing payload pages.
pub(crate) struct LargeMapping {
    pointer: NonNull<u8>,
}
impl LargeMapping {
    const LENGTH: usize = i32::MAX as usize + 1;
    pub(crate) fn new() -> Self {
        // SAFETY: This reserves an independent, writable anonymous mapping. No file, address hint, or preexisting Rust allocation is involved.
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                Self::LENGTH,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        let error = std::io::Error::last_os_error();
        assert_ne!(pointer, libc::MAP_FAILED, "{error}");
        Self {
            pointer: NonNull::new(pointer.cast()).unwrap(),
        }
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        // SAFETY: The live mapping owns LENGTH initialized zero bytes. A shared borrow prevents mutable access or unmapping until the slice expires.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), Self::LENGTH) }
    }
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: This exclusive borrow owns the whole live mapping; anonymous demand-zero pages are initialized even before physical allocation.
        unsafe { std::slice::from_raw_parts_mut(self.pointer.as_ptr(), Self::LENGTH) }
    }
}
impl Drop for LargeMapping {
    fn drop(&mut self) {
        // SAFETY: This is the original mapping and length; all borrowed slices have expired before its owner can be dropped.
        let result = unsafe { libc::munmap(self.pointer.as_ptr().cast(), Self::LENGTH) };
        assert_eq!(result, 0);
    }
}
