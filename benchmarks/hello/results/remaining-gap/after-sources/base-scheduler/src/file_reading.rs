// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{FileOperation, FileReader};
use std::io;

/// Construct a sequential file reader bound to this scheduler. Backends select the handle representation; protocols need no monitor or raw dispatch capabilities.
pub trait FileReading: FileOperation {
    type Reader<'scheduler>: FileReader + 'scheduler
    where
        Self: 'scheduler;

    /// Retain the file and reusable backend state without submitting I/O. The file reference need not live as long as the returned handle. Cancellation remains an operation argument.
    fn file_reader(&self, file: &Self::File) -> io::Result<Self::Reader<'_>>;
}
