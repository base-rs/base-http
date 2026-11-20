// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{borrow::Cow, sync::Arc};

/// Body data: bytes that can also become an owned buffer.
///
/// A consumer that needs owned storage, such as a protocol writer whose
/// completion I/O holds the buffer until the write finishes, converts each
/// chunk with [`into_vec`](Self::into_vec). Converting a `Vec<u8>` moves it, so
/// forwarding a body whose chunks are already vectors copies nothing. Borrowed
/// and shared chunks are copied once.
///
/// This is a Base trait rather than `Into<Vec<u8>>` so that shared standard
/// types such as `Arc<[u8]>`, which the standard library does not convert, can
/// be body data. Implement it for an application's own chunk type to make that
/// type body data.
pub trait Chunk: AsRef<[u8]> + Send {
    /// Convert into an owned buffer, moving it when the chunk already owns one.
    fn into_vec(self) -> Vec<u8>;
}

impl Chunk for Vec<u8> {
    fn into_vec(self) -> Vec<u8> {
        self
    }
}

impl Chunk for Box<[u8]> {
    fn into_vec(self) -> Vec<u8> {
        Vec::from(self)
    }
}

impl Chunk for String {
    fn into_vec(self) -> Vec<u8> {
        self.into_bytes()
    }
}

impl Chunk for &[u8] {
    fn into_vec(self) -> Vec<u8> {
        self.to_vec()
    }
}

impl Chunk for &str {
    fn into_vec(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Chunk for Cow<'_, [u8]> {
    fn into_vec(self) -> Vec<u8> {
        self.into_owned()
    }
}

/// Copies: a shared buffer may have other owners.
impl Chunk for Arc<[u8]> {
    fn into_vec(self) -> Vec<u8> {
        self.to_vec()
    }
}
