// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Read request bodies into structures declared with `base-web-content`, doing the I/O it leaves to the server: reading the body as it arrives, refusing a `Content-Length` larger than the declaration could accept before reading it, streaming uploads to storage, and saving uploads to files safely.
#![doc = include_str!("../readme.md")]
#![forbid(unsafe_code)]

// The test support module names this crate, as the integration tests do.
#[cfg(test)]
extern crate self as base_http_content;

mod file_store;
mod read_content;
mod read_error;
mod upload_store;

pub use file_store::{FileStore, FileUpload};
pub use read_content::{ReadOptions, read_content, read_content_with};
pub use read_error::{ReadError, ReadErrorKind};
pub use upload_store::{NoUploads, NoUploadsError, UploadStore};

#[cfg(test)]
mod tests;
