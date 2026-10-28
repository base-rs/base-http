// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fs::File, sync::Arc};

/// An owned file for direct scheduler instances. Operations borrow it; explicit close consumes it. It is deliberately not `Clone`.
///
/// The backend privately retains the file while an operation uses it. Dropping an operation future does not establish completion; await the operation before explicitly closing the file. Cancellation-aware work can first receive a cooperative cancellation request; accepted submission-only work retains the descriptor until it finishes. Ordinary handle destruction releases ownership without waiting or reporting close errors.
///
/// Open files through [`FileOpen::file_open`](base_scheduler::FileOpen::file_open). Native file import and access are deliberately unavailable: externally configured append flags or descriptor aliases would bypass positioned-write and exclusive-close guarantees.
///
/// ```compile_fail
/// use base_scheduler_direct::DirectFile;
/// fn duplicate(file: &DirectFile) -> DirectFile {
///     file.clone() // Cloning this reference cannot produce another owned handle.
/// }
/// ```
///
/// ```compile_fail
/// use base_scheduler_direct::DirectFile;
/// let append_file = std::fs::OpenOptions::new().append(true).open("log").unwrap();
/// let file = DirectFile::from(append_file);
/// ```
///
/// ```compile_fail
/// use base_scheduler_direct::DirectFile;
/// fn native(file: &DirectFile) -> &std::fs::File {
///     file.as_ref()
/// }
/// ```
#[derive(Debug)]
pub struct DirectFile(pub(crate) Arc<File>);

impl DirectFile {
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
