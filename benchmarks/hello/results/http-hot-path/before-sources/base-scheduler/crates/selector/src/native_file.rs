// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fs::File, sync::Arc};

/// An owned file for native scheduler instances. Operations borrow it; explicit close consumes it. It is deliberately not `Clone`.
///
/// The backend privately retains the file while an operation uses it. Dropping an operation future does not establish completion; await the operation before explicitly closing the file. Cancellation-aware work can first receive a cooperative cancellation request; accepted submission-only work retains the descriptor until it finishes. Ordinary handle destruction releases ownership without waiting or reporting close errors.
///
/// Open files through [`FileOpen::file_open`](base_scheduler::FileOpen::file_open). Safe native import and access are unavailable because external append flags or descriptor aliases would bypass positioned-write and exclusive-close guarantees. Backend integrations can explicitly retain the native file through [`Self::retain_native`].
///
/// ```compile_fail
/// use base_scheduler_selector::NativeFile;
/// fn duplicate(file: &NativeFile) -> NativeFile {
///     file.clone() // Cloning this reference cannot produce another owned handle.
/// }
/// ```
///
/// ```compile_fail
/// use base_scheduler_selector::NativeFile;
/// let append_file = std::fs::OpenOptions::new().append(true).open("log").unwrap();
/// let file = NativeFile::from(append_file);
/// ```
///
/// ```compile_fail
/// use base_scheduler_selector::NativeFile;
/// fn native(file: &NativeFile) -> &std::fs::File {
///     file.as_ref()
/// }
/// ```
#[derive(Debug)]
pub struct NativeFile(pub(crate) Arc<File>);

impl NativeFile {
    /// Whether a retained native owner refers to this exact file handle. Backends can validate cached registrations without retaining another owner or inspecting the handle.
    pub fn owns_native(&self, file: &Arc<File>) -> bool {
        Arc::ptr_eq(&self.0, file)
    }

    /// Retain the native file for another I/O backend.
    ///
    /// # Safety
    /// Keep this owner alive until every submitted operation using its handle has retired, including after cancellation or abandonment. Do not close the raw handle, change flags such as append mode, or let aliases bypass exclusive-close and positioned-I/O guarantees. Release the retained owner before reporting completion to callers that may immediately close the resource.
    pub unsafe fn retain_native(&self) -> Arc<File> {
        self.retain()
    }

    // Only files opened with backend-controlled options may enter this wrapper.
    pub(crate) fn from_std(file: File) -> Self {
        Self(Arc::new(file))
    }
    pub(crate) fn retain(&self) -> Arc<File> {
        Arc::clone(&self.0)
    }

    pub(crate) fn into_file(self) -> File {
        Arc::try_unwrap(self.0)
            .expect("cannot close a file with outstanding operations; cancel and await them first")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_retention_keeps_the_file_alive_until_backend_retirement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"retained").unwrap();
        let file = NativeFile::from_std(File::open(path).unwrap());
        // SAFETY: This simulated backend only observes metadata, makes no raw-handle or flag changes, and releases its owner before exclusive close.
        let retained = unsafe { file.retain_native() };
        assert!(file.owns_native(&retained));
        let other = Arc::new(File::open(directory.path().join("file")).unwrap());
        assert!(!file.owns_native(&other));
        assert_eq!(Arc::strong_count(&file.0), 2);
        assert_eq!(retained.metadata().unwrap().len(), 8);
        drop(retained);
        assert_eq!(file.into_file().metadata().unwrap().len(), 8);
    }
}
