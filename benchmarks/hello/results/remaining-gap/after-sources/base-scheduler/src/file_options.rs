// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Portable regular-file opening options. Fields are visible to alternative implementations, including simulated filesystems. Append mode is omitted: positioned writes must target the supplied offset.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
}

impl FileOptions {
    pub const READ: Self = Self {
        read: true,
        write: false,
        create: false,
        create_new: false,
        truncate: false,
    };
    pub const CREATE: Self = Self {
        read: false,
        write: true,
        create: true,
        create_new: false,
        truncate: true,
    };
}
