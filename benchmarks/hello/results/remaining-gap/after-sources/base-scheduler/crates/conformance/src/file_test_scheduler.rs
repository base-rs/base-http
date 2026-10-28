// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{
    FileClose, FileOpen, FileReading, FileSeek, FileStat, FileSync, FileTruncate, FileWriting,
};

/// The complete regular-file operation profile required by the shared full-suite fixture. Application code requests individual capabilities instead.
pub trait FileTestScheduler:
    FileOpen + FileClose + FileStat + FileTruncate + FileSync + FileSeek + FileReading + FileWriting
{
}

impl<S: ?Sized> FileTestScheduler for S where
    S: FileOpen
        + FileClose
        + FileStat
        + FileTruncate
        + FileSync
        + FileSeek
        + FileReading
        + FileWriting
{
}
