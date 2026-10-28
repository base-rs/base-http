// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{FileOperation, FileWriter};
use std::io;

/// Construct a sequential file writer bound to this scheduler. Backends select the handle representation; protocols need no monitor or raw dispatch capabilities.
pub trait FileWriting: FileOperation {
    type Writer<'scheduler>: FileWriter + 'scheduler
    where
        Self: 'scheduler;

    /// Retain the file and reusable backend state without submitting I/O. The file reference need not live as long as the returned handle. Cancellation remains an operation argument.
    fn file_writer(&self, file: &Self::File) -> io::Result<Self::Writer<'_>>;
}
